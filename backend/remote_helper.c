/*
 * git-remote-gitgres: a git remote helper that stores objects and refs
 * in PostgreSQL.
 *
 * Git invokes this as:
 *   git-remote-gitgres <remote-name> <url>
 *
 * where <url> is everything after "gitgres::" in the remote URL.
 * For example:
 *   git remote add pg gitgres::dbname=mydb/myrepo
 *   git push pg main
 *   git clone gitgres::dbname=mydb/myrepo
 *
 * The URL format is: <conninfo>/<reponame>
 * The last path component is the repo name, everything before it is
 * the libpq connection string.
 *
 * Protocol reference: gitremote-helpers(7)
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

static FILE *debug_fp;

static void debug(const char *fmt, ...) {
	if (!debug_fp) return;
	va_list ap;
	va_start(ap, fmt);
	fprintf(debug_fp, "[git-remote-gitgres] ");
	vfprintf(debug_fp, fmt, ap);
	fprintf(debug_fp, "\n");
	fflush(debug_fp);
	va_end(ap);
}

static void die(const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "fatal: git-remote-gitgres: ");
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

static void parse_url(const char *url, char **conninfo, char **reponame) {
	const char *last_slash = strrchr(url, '/');
	if (!last_slash || last_slash == url)
		die("invalid URL: expected <conninfo>/<reponame>, got '%s'", url);

	size_t ci_len = last_slash - url;
	*conninfo = malloc(ci_len + 1);
	memcpy(*conninfo, url, ci_len);
	(*conninfo)[ci_len] = '\0';

	*reponame = strdup(last_slash + 1);
	if (strlen(*reponame) == 0)
		die("empty repository name in URL '%s'", url);
}

static PGconn *pg_connect(const char *conninfo) {
	PGconn *conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
		die("connection failed: %s", PQerrorMessage(conn));
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

static char *chomp(char *line) {
	size_t len = strlen(line);
	if (len > 0 && line[len - 1] == '\n')
		line[len - 1] = '\0';
	return line;
}

/* ------------------------------------------------------------------ */
/* capabilities                                                       */
/* ------------------------------------------------------------------ */

static void cmd_capabilities(void) {
	printf("fetch\n");
	printf("push\n");
	printf("\n");
	fflush(stdout);
}

/* ------------------------------------------------------------------ */
/* list                                                               */
/* ------------------------------------------------------------------ */

static void cmd_list(PGconn *conn, int repo_id) {
	char repo_id_str[32];
	snprintf(repo_id_str, sizeof(repo_id_str), "%d", repo_id);
	const char *params[1] = { repo_id_str };

	PGresult *res = PQexecParams(conn,
		"SELECT name, encode(oid, 'hex'), symbolic FROM refs "
		"WHERE repo_id = $1 ORDER BY name",
		1, NULL, params, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die("list refs: %s", PQerrorMessage(conn));

	const char *head_oid = NULL;
	const char *head_symbolic = NULL;

	for (int i = 0; i < PQntuples(res); i++) {
		const char *name = PQgetvalue(res, i, 0);
		int oid_null = PQgetisnull(res, i, 1);
		const char *oid_hex = PQgetvalue(res, i, 1);
		int sym_null = PQgetisnull(res, i, 2);
		const char *symbolic = PQgetvalue(res, i, 2);

		if (strcmp(name, "HEAD") == 0) {
			if (!sym_null && strlen(symbolic) > 0)
				head_symbolic = symbolic;
			else if (!oid_null && strlen(oid_hex) > 0)
				head_oid = oid_hex;
			continue;
		}

		if (!oid_null && strlen(oid_hex) > 0) {
			printf("%s %s\n", oid_hex, name);
			debug("list: %s %s", oid_hex, name);
		}
	}

	if (head_symbolic) {
		/* Resolve to find the OID */
		for (int i = 0; i < PQntuples(res); i++) {
			if (strcmp(PQgetvalue(res, i, 0), head_symbolic) == 0
				&& !PQgetisnull(res, i, 1)) {
				head_oid = PQgetvalue(res, i, 1);
				break;
			}
		}
		if (head_oid) {
			printf("@%s HEAD\n", head_symbolic);
			debug("list: @%s HEAD (-> %s)", head_symbolic, head_oid);
		}
	} else if (head_oid) {
		printf("%s HEAD\n", head_oid);
		debug("list: %s HEAD", head_oid);
	}

	printf("\n");
	fflush(stdout);
	PQclear(res);
}

/* ------------------------------------------------------------------ */
/* fetch                                                              */
/* ------------------------------------------------------------------ */

struct fetch_ctx {
	git_odb *pg_odb;
	git_odb *local_odb;
	int count;
};

static int fetch_copy_cb(const git_oid *oid, void *payload) {
	struct fetch_ctx *ctx = (struct fetch_ctx *)payload;

	if (git_odb_exists(ctx->local_odb, oid))
		return 0;

	git_odb_object *obj = NULL;
	if (git_odb_read(&obj, ctx->pg_odb, oid) < 0)
		return 0;

	git_oid written;
	git_odb_write(&written, ctx->local_odb,
		git_odb_object_data(obj),
		git_odb_object_size(obj),
		git_odb_object_type(obj));
	git_odb_object_free(obj);
	ctx->count++;
	return 0;
}

/*
 * The first "fetch" line was already read by the main loop.
 * Read remaining fetch lines until blank, then copy objects.
 */
static void cmd_fetch(git_repository *pg_repo, const char *git_dir) {
	char line[1024];
	while (fgets(line, sizeof(line), stdin)) {
		chomp(line);
		if (line[0] == '\0') break;
		debug("fetch: %s", line);
	}

	git_repository *local_repo = NULL;
	check_lg2(git_repository_open(&local_repo, git_dir),
		"open local repo for fetch");

	git_odb *pg_odb = NULL;
	git_odb *local_odb = NULL;
	check_lg2(git_repository_odb(&pg_odb, pg_repo), "get pg odb");
	check_lg2(git_repository_odb(&local_odb, local_repo), "get local odb");

	struct fetch_ctx ctx = { .pg_odb = pg_odb, .local_odb = local_odb,
		.count = 0 };
	git_odb_foreach(pg_odb, fetch_copy_cb, &ctx);
	debug("fetched %d new objects", ctx.count);

	git_odb_free(pg_odb);
	git_odb_free(local_odb);
	git_repository_free(local_repo);

	printf("\n");
	fflush(stdout);
}

/* ------------------------------------------------------------------ */
/* push                                                               */
/* ------------------------------------------------------------------ */

typedef struct {
	char src[512];
	char dst[512];
	int force;
} push_spec;

struct push_copy_ctx {
	git_odb *src;
	git_odb *dst;
	int count;
};

static int push_copy_cb(const git_oid *oid, void *payload) {
	struct push_copy_ctx *ctx = (struct push_copy_ctx *)payload;

	if (git_odb_exists(ctx->dst, oid))
		return 0;

	git_odb_object *obj = NULL;
	if (git_odb_read(&obj, ctx->src, oid) < 0)
		return 0;

	git_oid written;
	git_odb_write(&written, ctx->dst,
		git_odb_object_data(obj),
		git_odb_object_size(obj),
		git_odb_object_type(obj));
	git_odb_object_free(obj);
	ctx->count++;
	return 0;
}

static void parse_push_spec(const char *raw, push_spec *out) {
	const char *p = raw;
	out->force = 0;
	if (*p == '+') { out->force = 1; p++; }

	const char *colon = strchr(p, ':');
	if (!colon) {
		out->src[0] = '\0';
		strncpy(out->dst, p, sizeof(out->dst) - 1);
		out->dst[sizeof(out->dst) - 1] = '\0';
		return;
	}

	size_t src_len = colon - p;
	if (src_len >= sizeof(out->src)) src_len = sizeof(out->src) - 1;
	memcpy(out->src, p, src_len);
	out->src[src_len] = '\0';

	strncpy(out->dst, colon + 1, sizeof(out->dst) - 1);
	out->dst[sizeof(out->dst) - 1] = '\0';
}

/*
 * The first "push" line was already read by the main loop.
 * first_line contains its content. Read remaining push lines
 * until blank, then transfer objects and update refs.
 */
static void cmd_push(PGconn *conn, int repo_id,
	git_repository *pg_repo, const char *git_dir,
	const char *first_line)
{
	push_spec specs[128];
	int nspecs = 0;

	/* Parse the first line */
	if (strncmp(first_line, "push ", 5) == 0) {
		parse_push_spec(first_line + 5, &specs[nspecs]);
		debug("push: %s -> %s%s", specs[nspecs].src, specs[nspecs].dst,
			specs[nspecs].force ? " (force)" : "");
		nspecs++;
	}

	/* Read remaining push lines */
	char line[1024];
	while (fgets(line, sizeof(line), stdin)) {
		chomp(line);
		if (line[0] == '\0') break;
		if (strncmp(line, "push ", 5) == 0 && nspecs < 128) {
			parse_push_spec(line + 5, &specs[nspecs]);
			debug("push: %s -> %s%s", specs[nspecs].src,
				specs[nspecs].dst,
				specs[nspecs].force ? " (force)" : "");
			nspecs++;
		}
	}

	/* Open local repo */
	git_repository *local_repo = NULL;
	check_lg2(git_repository_open(&local_repo, git_dir),
		"open local repo for push");

	/* Copy all local objects that postgres doesn't have */
	git_odb *local_odb = NULL;
	git_odb *pg_odb = NULL;
	check_lg2(git_repository_odb(&local_odb, local_repo), "get local odb");
	check_lg2(git_repository_odb(&pg_odb, pg_repo), "get pg odb");

	struct push_copy_ctx pctx = { .src = local_odb, .dst = pg_odb,
		.count = 0 };
	git_odb_foreach(local_odb, push_copy_cb, &pctx);
	debug("copied %d new objects", pctx.count);

	git_odb_free(local_odb);
	git_odb_free(pg_odb);

	/* Update refs */
	char repo_id_str[32];
	snprintf(repo_id_str, sizeof(repo_id_str), "%d", repo_id);

	for (int i = 0; i < nspecs; i++) {
		if (strlen(specs[i].src) == 0) {
			/* Delete */
			const char *params[2] = { repo_id_str, specs[i].dst };
			PGresult *res = PQexecParams(conn,
				"DELETE FROM refs WHERE repo_id=$1 AND name=$2",
				2, NULL, params, NULL, NULL, 0);
			printf("ok %s\n", specs[i].dst);
			PQclear(res);
			continue;
		}

		/* Resolve source ref to OID */
		git_reference *ref = NULL;
		git_oid oid;
		int resolved = 0;

		if (git_reference_lookup(&ref, local_repo, specs[i].src) == 0) {
			git_reference *peeled = NULL;
			if (git_reference_resolve(&peeled, ref) == 0) {
				git_oid_cpy(&oid, git_reference_target(peeled));
				resolved = 1;
				git_reference_free(peeled);
			}
			git_reference_free(ref);
		}
		if (!resolved && git_oid_fromstr(&oid, specs[i].src) == 0)
			resolved = 1;

		if (!resolved) {
			printf("error %s cannot resolve '%s'\n",
				specs[i].dst, specs[i].src);
			continue;
		}

		char oid_hex[GIT_OID_SHA1_HEXSIZE + 1];
		git_oid_tostr(oid_hex, sizeof(oid_hex), &oid);

		const char *params[3] = { repo_id_str, specs[i].dst, oid_hex };
		PGresult *res = PQexecParams(conn,
			"INSERT INTO refs (repo_id, name, oid) "
			"VALUES ($1, $2, decode($3, 'hex')) "
			"ON CONFLICT (repo_id, name) "
			"DO UPDATE SET oid = decode($3, 'hex'), symbolic = NULL",
			3, NULL, params, NULL, NULL, 0);

		if (PQresultStatus(res) == PGRES_COMMAND_OK) {
			printf("ok %s\n", specs[i].dst);
			debug("ref %s -> %s", specs[i].dst, oid_hex);
		} else {
			printf("error %s %s\n", specs[i].dst,
				PQerrorMessage(conn));
		}
		PQclear(res);
	}

	/* Ensure HEAD exists */
	{
		const char *params[1] = { repo_id_str };
		PGresult *res = PQexecParams(conn,
			"SELECT 1 FROM refs WHERE repo_id=$1 AND name='HEAD'",
			1, NULL, params, NULL, NULL, 0);
		if (PQresultStatus(res) == PGRES_TUPLES_OK &&
			PQntuples(res) == 0 && nspecs > 0) {
			const char *hparams[3] = {
				repo_id_str, "HEAD", specs[0].dst
			};
			PGresult *hres = PQexecParams(conn,
				"INSERT INTO refs (repo_id, name, symbolic) "
				"VALUES ($1, $2, $3) "
				"ON CONFLICT (repo_id, name) DO NOTHING",
				3, NULL, hparams, NULL, NULL, 0);
			PQclear(hres);
			debug("created HEAD -> %s", specs[0].dst);
		}
		PQclear(res);
	}

	git_repository_free(local_repo);

	printf("\n");
	fflush(stdout);
}

/* ------------------------------------------------------------------ */
/* main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
	if (argc < 3) {
		fprintf(stderr,
			"Usage: git-remote-gitgres <remote-name> <url>\n"
			"\n"
			"This is a git remote helper. Use it via:\n"
			"  git remote add <name> gitgres::<conninfo>/<reponame>\n"
			"  git push <name> main\n"
			"  git clone gitgres::<conninfo>/<reponame>\n");
		return 1;
	}

	const char *debug_env = getenv("GIT_REMOTE_GITGRES_DEBUG");
	if (debug_env && strlen(debug_env) > 0)
		debug_fp = fopen(debug_env, "a");

	const char *url = argv[2];
	char *conninfo = NULL;
	char *reponame = NULL;
	parse_url(url, &conninfo, &reponame);

	debug("url='%s' conninfo='%s' repo='%s'", url, conninfo, reponame);

	git_libgit2_init();

	PGconn *conn = pg_connect(conninfo);
	int repo_id = get_or_create_repo(conn, reponame);
	git_repository *pg_repo = open_pg_repo(conn, repo_id);

	const char *git_dir = getenv("GIT_DIR");
	if (!git_dir) git_dir = ".git";

	debug("repo_id=%d git_dir=%s", repo_id, git_dir);

	char line[1024];
	while (fgets(line, sizeof(line), stdin)) {
		chomp(line);
		debug("< '%s'", line);

		if (strcmp(line, "capabilities") == 0) {
			cmd_capabilities();
		} else if (strcmp(line, "list") == 0 ||
			   strcmp(line, "list for-push") == 0) {
			cmd_list(conn, repo_id);
		} else if (strncmp(line, "fetch ", 6) == 0) {
			cmd_fetch(pg_repo, git_dir);
		} else if (strncmp(line, "push ", 5) == 0) {
			cmd_push(conn, repo_id, pg_repo, git_dir, line);
		} else if (line[0] == '\0') {
			break;
		} else {
			debug("unknown command: '%s'", line);
		}
	}

	git_repository_free(pg_repo);
	PQfinish(conn);
	free(conninfo);
	free(reponame);
	git_libgit2_shutdown();
	if (debug_fp) fclose(debug_fp);

	return 0;
}
