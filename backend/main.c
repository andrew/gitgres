/*
 * gitgres-backend: CLI tool for moving git objects between local repos
 * and a PostgreSQL-backed git object store.
 *
 * Usage:
 *   gitgres-backend init     <conninfo> <reponame>
 *   gitgres-backend push     <conninfo> <reponame> <local-repo-path>
 *   gitgres-backend clone    <conninfo> <reponame> <dest-dir>
 *   gitgres-backend ls-refs  <conninfo> <reponame>
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <git2.h>
#include <git2/sys/repository.h>
#include <git2/sys/odb_backend.h>
#include <git2/sys/refdb_backend.h>
#include <libpq-fe.h>
#include "odb_postgres.h"
#include "refdb_postgres.h"

static void die(const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "fatal: ");
	vfprintf(stderr, fmt, ap);
	fprintf(stderr, "\n");
	va_end(ap);
	exit(1);
}

static void check_lg2(int error, const char *msg) {
	if (error < 0) {
		const git_error *e = git_error_last();
		die("%s: %s", msg, e ? e->message : "unknown error");
	}
}

static PGconn *pg_connect(const char *conninfo) {
	PGconn *conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
		die("connection to database failed: %s", PQerrorMessage(conn));
	return conn;
}

static int get_or_create_repo(PGconn *conn, const char *name) {
	const char *params[1] = { name };
	PGresult *res = PQexecParams(conn,
		"INSERT INTO repositories (name) VALUES ($1) "
		"ON CONFLICT (name) DO UPDATE SET name = $1 "
		"RETURNING id",
		1, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die("get_or_create_repo: %s", PQerrorMessage(conn));

	int repo_id = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
	return repo_id;
}

static int get_repo(PGconn *conn, const char *name) {
	const char *params[1] = { name };
	PGresult *res = PQexecParams(conn,
		"SELECT id FROM repositories WHERE name = $1",
		1, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die("get_repo: %s", PQerrorMessage(conn));

	if (PQntuples(res) == 0) {
		PQclear(res);
		return -1;
	}

	int repo_id = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
	return repo_id;
}

/*
 * Build a libgit2 repository object whose ODB and refdb are backed by
 * postgres.  The repo has no workdir -- it's the equivalent of a bare
 * repo, but all storage goes through our custom backends.
 */
static git_repository *open_pg_repo(PGconn *conn, int repo_id) {
	git_repository *repo = NULL;
	git_odb *odb = NULL;
	git_odb_backend *odb_backend = NULL;
	git_refdb *refdb = NULL;
	git_refdb_backend *refdb_backend = NULL;

	check_lg2(git_repository_new(&repo), "create repo");

	check_lg2(git_odb_new(&odb), "create odb");
	check_lg2(git_odb_backend_postgres(&odb_backend, conn, repo_id),
		"create odb backend");
	check_lg2(git_odb_add_backend(odb, odb_backend, 1),
		"add odb backend");
	git_repository_set_odb(repo, odb);

	check_lg2(git_refdb_new(&refdb, repo), "create refdb");
	check_lg2(git_refdb_backend_postgres(&refdb_backend, conn, repo_id),
		"create refdb backend");
	check_lg2(git_refdb_set_backend(refdb, refdb_backend),
		"set refdb backend");
	git_repository_set_refdb(repo, refdb);

	git_odb_free(odb);
	git_refdb_free(refdb);

	return repo;
}

/* ------------------------------------------------------------------ */
/* init: create repository record in postgres                         */
/* ------------------------------------------------------------------ */

static void cmd_init(const char *conninfo, const char *reponame) {
	PGconn *conn = pg_connect(conninfo);
	int repo_id = get_or_create_repo(conn, reponame);
	printf("Repository '%s' ready (id=%d)\n", reponame, repo_id);
	PQfinish(conn);
}

/* ------------------------------------------------------------------ */
/* push: copy objects and refs from a local repo into postgres        */
/* ------------------------------------------------------------------ */

struct copy_ctx {
	git_odb *src;
	git_odb *dst;
	int count;
	int errors;
};

static int copy_object_cb(const git_oid *oid, void *payload) {
	struct copy_ctx *ctx = (struct copy_ctx *)payload;
	git_odb_object *obj = NULL;

	int err = git_odb_read(&obj, ctx->src, oid);
	if (err < 0) {
		fprintf(stderr, "warning: could not read object %s\n",
			git_oid_tostr_s(oid));
		ctx->errors++;
		return 0; /* keep going */
	}

	git_oid written;
	err = git_odb_write(&written, ctx->dst,
		git_odb_object_data(obj),
		git_odb_object_size(obj),
		git_odb_object_type(obj));
	git_odb_object_free(obj);

	if (err < 0) {
		fprintf(stderr, "warning: could not write object %s\n",
			git_oid_tostr_s(oid));
		ctx->errors++;
		return 0;
	}

	ctx->count++;
	return 0;
}

static void push_head(PGconn *conn, int repo_id, git_repository *local_repo) {
	git_reference *head = NULL;
	if (git_reference_lookup(&head, local_repo, "HEAD") != 0)
		return;

	if (git_reference_type(head) == GIT_REFERENCE_SYMBOLIC) {
		const char *target = git_reference_symbolic_target(head);
		char repo_id_str[32];
		snprintf(repo_id_str, sizeof(repo_id_str), "%d", repo_id);
		const char *params[3] = { repo_id_str, "HEAD", target };
		PGresult *res = PQexecParams(conn,
			"INSERT INTO refs (repo_id, name, symbolic) "
			"VALUES ($1, $2, $3) "
			"ON CONFLICT (repo_id, name) "
			"DO UPDATE SET oid = NULL, symbolic = $3",
			3, NULL, params, NULL, NULL, 0);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			fprintf(stderr, "warning: failed to set HEAD: %s",
				PQerrorMessage(conn));
		PQclear(res);
	}
	git_reference_free(head);
}

static void cmd_push(const char *conninfo, const char *reponame,
	const char *local_path)
{
	PGconn *conn = pg_connect(conninfo);
	int repo_id = get_or_create_repo(conn, reponame);
	git_repository *pg_repo = open_pg_repo(conn, repo_id);

	git_repository *local_repo = NULL;
	check_lg2(git_repository_open(&local_repo, local_path),
		"open local repo");

	/* Copy all objects */
	git_odb *local_odb = NULL;
	git_odb *pg_odb = NULL;
	check_lg2(git_repository_odb(&local_odb, local_repo), "get local odb");
	check_lg2(git_repository_odb(&pg_odb, pg_repo), "get pg odb");

	struct copy_ctx ctx = { .src = local_odb, .dst = pg_odb,
		.count = 0, .errors = 0 };
	check_lg2(git_odb_foreach(local_odb, copy_object_cb, &ctx),
		"iterate local objects");

	printf("Pushed %d objects", ctx.count);
	if (ctx.errors > 0)
		printf(" (%d errors)", ctx.errors);
	printf("\n");

	/* Copy refs (not HEAD -- handled separately) */
	git_reference_iterator *iter = NULL;
	git_reference *ref = NULL;
	int ref_count = 0;

	check_lg2(git_reference_iterator_new(&iter, local_repo),
		"create ref iterator");

	while (git_reference_next(&ref, iter) == 0) {
		const char *name = git_reference_name(ref);

		if (git_reference_type(ref) == GIT_REFERENCE_DIRECT) {
			const git_oid *oid = git_reference_target(ref);
			git_reference *new_ref = NULL;
			int err = git_reference_create(&new_ref, pg_repo,
				name, oid, 1, "push");
			if (err < 0) {
				fprintf(stderr, "warning: could not push ref %s: %s\n",
					name,
					git_error_last() ? git_error_last()->message
						: "unknown");
			} else {
				ref_count++;
				git_reference_free(new_ref);
			}
		} else if (git_reference_type(ref) == GIT_REFERENCE_SYMBOLIC) {
			const char *target = git_reference_symbolic_target(ref);
			git_reference *new_ref = NULL;
			int err = git_reference_symbolic_create(&new_ref, pg_repo,
				name, target, 1, "push");
			if (err < 0) {
				fprintf(stderr, "warning: could not push ref %s: %s\n",
					name,
					git_error_last() ? git_error_last()->message
						: "unknown");
			} else {
				ref_count++;
				git_reference_free(new_ref);
			}
		}
		git_reference_free(ref);
	}
	git_reference_iterator_free(iter);

	/* Push HEAD as a symbolic ref directly via SQL */
	push_head(conn, repo_id, local_repo);

	printf("Pushed %d refs\n", ref_count);

	git_odb_free(local_odb);
	git_odb_free(pg_odb);
	git_repository_free(local_repo);
	git_repository_free(pg_repo);
	PQfinish(conn);
}

/* ------------------------------------------------------------------ */
/* clone: copy objects and refs from postgres into a new local repo   */
/* ------------------------------------------------------------------ */

static int clone_object_cb(const git_oid *oid, void *payload) {
	struct copy_ctx *ctx = (struct copy_ctx *)payload;
	git_odb_object *obj = NULL;

	int err = git_odb_read(&obj, ctx->src, oid);
	if (err < 0) {
		fprintf(stderr, "warning: could not read object %s from pg\n",
			git_oid_tostr_s(oid));
		ctx->errors++;
		return 0;
	}

	git_oid written;
	err = git_odb_write(&written, ctx->dst,
		git_odb_object_data(obj),
		git_odb_object_size(obj),
		git_odb_object_type(obj));
	git_odb_object_free(obj);

	if (err < 0) {
		fprintf(stderr, "warning: could not write object %s to local\n",
			git_oid_tostr_s(oid));
		ctx->errors++;
		return 0;
	}

	ctx->count++;
	return 0;
}

static void cmd_clone(const char *conninfo, const char *reponame,
	const char *dest_path)
{
	PGconn *conn = pg_connect(conninfo);
	int repo_id = get_repo(conn, reponame);
	if (repo_id < 0)
		die("repository '%s' not found", reponame);

	git_repository *pg_repo = open_pg_repo(conn, repo_id);

	/* Create a new local repo at dest_path */
	git_repository *local_repo = NULL;
	check_lg2(git_repository_init(&local_repo, dest_path, 0),
		"init local repo");

	/* Copy all objects from pg to local */
	git_odb *pg_odb = NULL;
	git_odb *local_odb = NULL;
	check_lg2(git_repository_odb(&pg_odb, pg_repo), "get pg odb");
	check_lg2(git_repository_odb(&local_odb, local_repo), "get local odb");

	struct copy_ctx ctx = { .src = pg_odb, .dst = local_odb,
		.count = 0, .errors = 0 };
	check_lg2(git_odb_foreach(pg_odb, clone_object_cb, &ctx),
		"iterate pg objects");

	printf("Cloned %d objects", ctx.count);
	if (ctx.errors > 0)
		printf(" (%d errors)", ctx.errors);
	printf("\n");

	git_odb_free(pg_odb);
	git_odb_free(local_odb);

	/* Copy refs from postgres to local.
	 * Query the refs table directly since our refdb backend supports
	 * iteration, but we also need to handle HEAD specially.
	 */
	char repo_id_str[32];
	snprintf(repo_id_str, sizeof(repo_id_str), "%d", repo_id);
	const char *params[1] = { repo_id_str };
	PGresult *res = PQexecParams(conn,
		"SELECT name, encode(oid, 'hex'), symbolic FROM refs "
		"WHERE repo_id = $1 ORDER BY name",
		1, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die("query refs: %s", PQerrorMessage(conn));

	int ref_count = 0;
	const char *head_target = NULL;

	for (int i = 0; i < PQntuples(res); i++) {
		const char *name = PQgetvalue(res, i, 0);
		int oid_null = PQgetisnull(res, i, 1);
		const char *oid_hex = PQgetvalue(res, i, 1);
		int sym_null = PQgetisnull(res, i, 2);
		const char *symbolic = PQgetvalue(res, i, 2);

		if (strcmp(name, "HEAD") == 0) {
			if (!sym_null && strlen(symbolic) > 0)
				head_target = symbolic;
			continue;
		}

		if (!oid_null && strlen(oid_hex) > 0) {
			git_oid oid;
			if (git_oid_fromstr(&oid, oid_hex) != 0) {
				fprintf(stderr, "warning: bad oid for ref %s\n", name);
				continue;
			}
			git_reference *new_ref = NULL;
			int err = git_reference_create(&new_ref, local_repo,
				name, &oid, 1, "clone from gitgres");
			if (err < 0) {
				fprintf(stderr, "warning: could not create ref %s: %s\n",
					name,
					git_error_last() ? git_error_last()->message
						: "unknown");
			} else {
				ref_count++;
				git_reference_free(new_ref);
			}
		} else if (!sym_null && strlen(symbolic) > 0) {
			git_reference *new_ref = NULL;
			int err = git_reference_symbolic_create(&new_ref, local_repo,
				name, symbolic, 1, "clone from gitgres");
			if (err < 0) {
				fprintf(stderr, "warning: could not create ref %s: %s\n",
					name,
					git_error_last() ? git_error_last()->message
						: "unknown");
			} else {
				ref_count++;
				git_reference_free(new_ref);
			}
		}
	}

	/* Set HEAD */
	if (head_target) {
		check_lg2(git_repository_set_head(local_repo, head_target),
			"set HEAD");
	}

	printf("Cloned %d refs\n", ref_count);

	PQclear(res);

	/* Checkout working directory */
	git_checkout_options checkout_opts = GIT_CHECKOUT_OPTIONS_INIT;
	checkout_opts.checkout_strategy = GIT_CHECKOUT_FORCE;
	int err = git_checkout_head(local_repo, &checkout_opts);
	if (err < 0) {
		const git_error *e = git_error_last();
		fprintf(stderr, "warning: checkout failed: %s\n",
			e ? e->message : "unknown");
	} else {
		printf("Checked out working directory\n");
	}

	git_repository_free(local_repo);
	git_repository_free(pg_repo);
	PQfinish(conn);
}

/* ------------------------------------------------------------------ */
/* ls-refs: list all refs stored in postgres for a repo               */
/* ------------------------------------------------------------------ */

static void cmd_ls_refs(const char *conninfo, const char *reponame) {
	PGconn *conn = pg_connect(conninfo);
	int repo_id = get_repo(conn, reponame);
	if (repo_id < 0)
		die("repository '%s' not found", reponame);

	char repo_id_str[32];
	snprintf(repo_id_str, sizeof(repo_id_str), "%d", repo_id);
	const char *params[1] = { repo_id_str };
	PGresult *res = PQexecParams(conn,
		"SELECT name, encode(oid, 'hex'), symbolic FROM refs "
		"WHERE repo_id = $1 ORDER BY name",
		1, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die("query refs: %s", PQerrorMessage(conn));

	for (int i = 0; i < PQntuples(res); i++) {
		const char *name = PQgetvalue(res, i, 0);
		int oid_null = PQgetisnull(res, i, 1);
		const char *oid_hex = PQgetvalue(res, i, 1);
		int sym_null = PQgetisnull(res, i, 2);
		const char *symbolic = PQgetvalue(res, i, 2);

		if (!sym_null && strlen(symbolic) > 0) {
			printf("-> %-40s %s\n", symbolic, name);
		} else if (!oid_null && strlen(oid_hex) > 0) {
			printf("%-42s %s\n", oid_hex, name);
		}
	}

	PQclear(res);
	PQfinish(conn);
}

/* ------------------------------------------------------------------ */
/* main                                                               */
/* ------------------------------------------------------------------ */

static void usage(void) {
	fprintf(stderr,
		"Usage: gitgres-backend <command> [args]\n"
		"\n"
		"Commands:\n"
		"    init     <conninfo> <reponame>\n"
		"    push     <conninfo> <reponame> <local-repo-path>\n"
		"    clone    <conninfo> <reponame> <dest-dir>\n"
		"    ls-refs  <conninfo> <reponame>\n");
	exit(1);
}

int main(int argc, char **argv) {
	if (argc < 2)
		usage();

	git_libgit2_init();

	const char *cmd = argv[1];

	if (strcmp(cmd, "init") == 0) {
		if (argc != 4) usage();
		cmd_init(argv[2], argv[3]);
	} else if (strcmp(cmd, "push") == 0) {
		if (argc != 5) usage();
		cmd_push(argv[2], argv[3], argv[4]);
	} else if (strcmp(cmd, "clone") == 0) {
		if (argc != 5) usage();
		cmd_clone(argv[2], argv[3], argv[4]);
	} else if (strcmp(cmd, "ls-refs") == 0) {
		if (argc != 4) usage();
		cmd_ls_refs(argv[2], argv[3]);
	} else {
		fprintf(stderr, "Unknown command: %s\n", cmd);
		usage();
	}

	git_libgit2_shutdown();
	return 0;
}
