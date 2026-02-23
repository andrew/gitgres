#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <arpa/inet.h>

#include <git2/sys/refs.h>
#include <git2/sys/errors.h>

#include "refdb_postgres.h"

typedef struct {
	git_refdb_backend parent;
	PGconn *conn;
	int repo_id;
} postgres_refdb_backend;

typedef struct {
	git_reference_iterator parent;
	postgres_refdb_backend *backend;
	PGresult *result;
	int current;
	int total;
} postgres_refdb_iterator;

/* Payload for the lock/unlock mechanism. Stores the advisory lock key
 * and the ref name so unlock can act on it. */
typedef struct {
	int64_t lock_key;
	char *refname;
} pg_ref_lock;

/* FNV-1a hash of a ref name, folded to 64 bits.  Used as the advisory
 * lock key so that concurrent writes to different refs don't block
 * each other. */
static int64_t hash_refname(int repo_id, const char *refname)
{
	uint64_t h = 14695981039346656037ULL;
	unsigned char buf[4];
	int i;

	/* Mix repo_id into the hash first */
	buf[0] = (repo_id >> 24) & 0xff;
	buf[1] = (repo_id >> 16) & 0xff;
	buf[2] = (repo_id >> 8) & 0xff;
	buf[3] = repo_id & 0xff;
	for (i = 0; i < 4; i++) {
		h ^= buf[i];
		h *= 1099511628211ULL;
	}

	while (*refname) {
		h ^= (unsigned char)*refname++;
		h *= 1099511628211ULL;
	}

	return (int64_t)h;
}

/* Build a git_reference from a PGresult row that has columns:
 *   0: name (text)   -- only used when name_override is NULL
 *   1: oid  (bytea, 20 bytes, may be NULL)
 *   2: symbolic (text, may be NULL)
 * name_override, if non-NULL, is used instead of column 0. */
static int ref_from_result(git_reference **out, PGresult *res, int row,
                           const char *name_override)
{
	const char *name;
	git_reference *ref;

	name = name_override ? name_override : PQgetvalue(res, row, 0);

	if (!PQgetisnull(res, row, 1)) {
		/* direct ref */
		git_oid oid;
		const unsigned char *raw = (const unsigned char *)PQgetvalue(res, row, 1);
		int len = PQgetlength(res, row, 1);

		if (len != GIT_OID_SHA1_SIZE) {
			git_error_set(GIT_ERROR_REFERENCE,
				"postgres refdb: oid has wrong length %d for ref %s",
				len, name);
			return -1;
		}
		git_oid_fromraw(&oid, raw);
		ref = git_reference__alloc(name, &oid, NULL);
	} else if (!PQgetisnull(res, row, 2)) {
		/* symbolic ref */
		const char *target = PQgetvalue(res, row, 2);
		ref = git_reference__alloc_symbolic(name, target);
	} else {
		git_error_set(GIT_ERROR_REFERENCE,
			"postgres refdb: ref %s has neither oid nor symbolic target", name);
		return -1;
	}

	if (!ref) {
		git_error_set_oom();
		return -1;
	}

	*out = ref;
	return 0;
}

/* Format repo_id as a decimal string into a caller-supplied buffer.
 * Returns pointer to buf. */
static const char *repo_id_str(char *buf, size_t bufsz, int repo_id)
{
	snprintf(buf, bufsz, "%d", repo_id);
	return buf;
}

/* ----------------------------------------------------------------
 * exists
 * ---------------------------------------------------------------- */

static int pg_refdb_exists(int *exists, git_refdb_backend *_backend,
                           const char *ref_name)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	char rid[16];
	const char *params[2];
	PGresult *res;

	repo_id_str(rid, sizeof(rid), backend->repo_id);
	params[0] = rid;
	params[1] = ref_name;

	res = PQexecParams(backend->conn,
		"SELECT 1 FROM refs WHERE repo_id = $1 AND name = $2",
		2, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}

	*exists = (PQntuples(res) > 0) ? 1 : 0;
	PQclear(res);
	return 0;
}

/* ----------------------------------------------------------------
 * lookup
 * ---------------------------------------------------------------- */

static int pg_refdb_lookup(git_reference **out, git_refdb_backend *_backend,
                           const char *ref_name)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	char rid[16];
	const char *params[2];
	PGresult *res;
	int error;

	repo_id_str(rid, sizeof(rid), backend->repo_id);
	params[0] = rid;
	params[1] = ref_name;

	res = PQexecParams(backend->conn,
		"SELECT name, oid, symbolic FROM refs WHERE repo_id = $1 AND name = $2",
		2, NULL, params, NULL, NULL, 1 /* binary result */);

	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}

	if (PQntuples(res) == 0) {
		PQclear(res);
		return GIT_ENOTFOUND;
	}

	/* Column 0 is name (text), column 1 is oid (bytea), column 2 is symbolic (text).
	 * We requested binary results but text columns come through fine --
	 * the binary format of text is just the raw bytes without a NUL,
	 * and bytea binary format is the raw bytes.  PQgetvalue gives us
	 * pointers with the right lengths via PQgetlength. However, text
	 * values in binary format are NOT NUL-terminated, so we use
	 * ref_name which we already have as the name. */
	error = ref_from_result(out, res, 0, ref_name);
	PQclear(res);
	return error;
}

/* ----------------------------------------------------------------
 * iterator
 * ---------------------------------------------------------------- */

static int pg_refdb_iter_next(git_reference **ref, git_reference_iterator *_iter)
{
	postgres_refdb_iterator *iter = (postgres_refdb_iterator *)_iter;

	if (iter->current >= iter->total)
		return GIT_ITEROVER;

	if (ref_from_result(ref, iter->result, iter->current, NULL) < 0)
		return -1;

	iter->current++;
	return 0;
}

static int pg_refdb_iter_next_name(const char **ref_name,
                                   git_reference_iterator *_iter)
{
	postgres_refdb_iterator *iter = (postgres_refdb_iterator *)_iter;

	if (iter->current >= iter->total)
		return GIT_ITEROVER;

	/* Column 0 is the ref name.  For text-format results PQgetvalue
	 * returns a NUL-terminated string.  The pointer remains valid
	 * until we PQclear the result, which happens in iter_free. */
	*ref_name = PQgetvalue(iter->result, iter->current, 0);
	iter->current++;
	return 0;
}

static void pg_refdb_iter_free(git_reference_iterator *_iter)
{
	postgres_refdb_iterator *iter = (postgres_refdb_iterator *)_iter;
	if (iter->result)
		PQclear(iter->result);
	free(iter);
}

static int pg_refdb_iterator(git_reference_iterator **out,
                             git_refdb_backend *_backend, const char *glob)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	postgres_refdb_iterator *iter;
	char rid[16];
	PGresult *res;

	repo_id_str(rid, sizeof(rid), backend->repo_id);

	if (glob && *glob) {
		/* Convert glob pattern: replace * with % for SQL LIKE */
		size_t len = strlen(glob);
		char *like = malloc(len + 1);
		size_t i;
		const char *params[2];

		if (!like) {
			git_error_set_oom();
			return -1;
		}
		for (i = 0; i < len; i++)
			like[i] = (glob[i] == '*') ? '%' : glob[i];
		like[len] = '\0';

		params[0] = rid;
		params[1] = like;

		res = PQexecParams(backend->conn,
			"SELECT name, oid, symbolic FROM refs "
			"WHERE repo_id = $1 AND name LIKE $2 "
			"ORDER BY name",
			2, NULL, params, NULL, NULL, 0);

		free(like);
	} else {
		const char *params[1];
		params[0] = rid;

		res = PQexecParams(backend->conn,
			"SELECT name, oid, symbolic FROM refs "
			"WHERE repo_id = $1 ORDER BY name",
			1, NULL, params, NULL, NULL, 0);
	}

	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}

	iter = calloc(1, sizeof(postgres_refdb_iterator));
	if (!iter) {
		git_error_set_oom();
		PQclear(res);
		return -1;
	}

	iter->parent.next = pg_refdb_iter_next;
	iter->parent.next_name = pg_refdb_iter_next_name;
	iter->parent.free = pg_refdb_iter_free;
	iter->backend = backend;
	iter->result = res;
	iter->current = 0;
	iter->total = PQntuples(res);

	*out = (git_reference_iterator *)iter;
	return 0;
}

/* ----------------------------------------------------------------
 * write reflog entry (helper)
 * ---------------------------------------------------------------- */

static int write_reflog_entry(postgres_refdb_backend *backend,
                              const char *ref_name,
                              const git_oid *old_oid,
                              const git_oid *new_oid,
                              const git_signature *who,
                              const char *message)
{
	char rid[16];
	char committer[512];
	char ts[32];
	char tz[8];
	const char *params[7];
	int param_lengths[7];
	int param_formats[7];
	PGresult *res;

	if (!who)
		return 0;

	repo_id_str(rid, sizeof(rid), backend->repo_id);
	snprintf(committer, sizeof(committer), "%s <%s>", who->name, who->email);
	snprintf(ts, sizeof(ts), "%lld", (long long)who->when.time);
	snprintf(tz, sizeof(tz), "%c%02d%02d",
		who->when.offset >= 0 ? '+' : '-',
		abs(who->when.offset) / 60,
		abs(who->when.offset) % 60);

	/* params: repo_id, ref_name, old_oid, new_oid, committer, timestamp_s, tz_offset, message */
	params[0] = rid;              param_lengths[0] = 0; param_formats[0] = 0;
	params[1] = ref_name;         param_lengths[1] = 0; param_formats[1] = 0;

	/* old_oid: binary bytea or NULL */
	if (old_oid && !git_oid_is_zero(old_oid)) {
		params[2] = (const char *)old_oid->id;
		param_lengths[2] = GIT_OID_SHA1_SIZE;
		param_formats[2] = 1;
	} else {
		params[2] = NULL;
		param_lengths[2] = 0;
		param_formats[2] = 1;
	}

	/* new_oid: binary bytea or NULL */
	if (new_oid && !git_oid_is_zero(new_oid)) {
		params[3] = (const char *)new_oid->id;
		param_lengths[3] = GIT_OID_SHA1_SIZE;
		param_formats[3] = 1;
	} else {
		params[3] = NULL;
		param_lengths[3] = 0;
		param_formats[3] = 1;
	}

	params[4] = committer;        param_lengths[4] = 0; param_formats[4] = 0;
	params[5] = ts;               param_lengths[5] = 0; param_formats[5] = 0;
	params[6] = tz;               param_lengths[6] = 0; param_formats[6] = 0;

	/* message may be NULL */
	if (message) {
		const char *all_params[8];
		int all_lengths[8];
		int all_formats[8];
		int i;

		for (i = 0; i < 7; i++) {
			all_params[i] = params[i];
			all_lengths[i] = param_lengths[i];
			all_formats[i] = param_formats[i];
		}
		all_params[7] = message;
		all_lengths[7] = 0;
		all_formats[7] = 0;

		res = PQexecParams(backend->conn,
			"INSERT INTO reflog (repo_id, ref_name, old_oid, new_oid, "
			"committer, timestamp_s, tz_offset, message) "
			"VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
			8, NULL, all_params, all_lengths, all_formats, 0);
	} else {
		res = PQexecParams(backend->conn,
			"INSERT INTO reflog (repo_id, ref_name, old_oid, new_oid, "
			"committer, timestamp_s, tz_offset) "
			"VALUES ($1, $2, $3, $4, $5, $6, $7)",
			7, NULL, params, param_lengths, param_formats, 0);
	}

	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}

	PQclear(res);
	return 0;
}

/* ----------------------------------------------------------------
 * write
 * ---------------------------------------------------------------- */

static int pg_refdb_write(git_refdb_backend *_backend,
                          const git_reference *ref, int force,
                          const git_signature *who, const char *message,
                          const git_oid *old, const char *old_target)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	const char *ref_name = git_reference_name(ref);
	git_reference_t type = git_reference_type(ref);
	char rid[16];
	PGresult *res;
	int error = 0;

	repo_id_str(rid, sizeof(rid), backend->repo_id);

	/* BEGIN transaction */
	res = PQexec(backend->conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}
	PQclear(res);

	/* If not forcing, perform compare-and-swap check */
	if (!force) {
		const char *sel_params[2] = { rid, ref_name };
		PGresult *sel;

		sel = PQexecParams(backend->conn,
			"SELECT oid, symbolic FROM refs "
			"WHERE repo_id = $1 AND name = $2 FOR UPDATE",
			2, NULL, sel_params, NULL, NULL, 1);

		if (PQresultStatus(sel) != PGRES_TUPLES_OK) {
			git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
			PQclear(sel);
			PQexec(backend->conn, "ROLLBACK");
			return -1;
		}

		if (old || old_target) {
			/* Ref must already exist */
			if (PQntuples(sel) == 0) {
				git_error_set(GIT_ERROR_REFERENCE,
					"reference %s does not exist for update", ref_name);
				PQclear(sel);
				PQexec(backend->conn, "ROLLBACK");
				return GIT_ENOTFOUND;
			}

			if (old) {
				/* Check current OID matches expected */
				if (PQgetisnull(sel, 0, 0)) {
					git_error_set(GIT_ERROR_REFERENCE,
						"reference %s is symbolic, expected direct", ref_name);
					PQclear(sel);
					PQexec(backend->conn, "ROLLBACK");
					return -1;
				}

				const unsigned char *cur_raw =
					(const unsigned char *)PQgetvalue(sel, 0, 0);
				int cur_len = PQgetlength(sel, 0, 0);

				if (cur_len != GIT_OID_SHA1_SIZE ||
				    memcmp(cur_raw, old->id, GIT_OID_SHA1_SIZE) != 0) {
					git_error_set(GIT_ERROR_REFERENCE,
						"reference %s value has changed", ref_name);
					PQclear(sel);
					PQexec(backend->conn, "ROLLBACK");
					return GIT_EEXISTS;
				}
			}

			if (old_target) {
				/* Check current symbolic target matches */
				if (PQgetisnull(sel, 0, 1)) {
					git_error_set(GIT_ERROR_REFERENCE,
						"reference %s is direct, expected symbolic", ref_name);
					PQclear(sel);
					PQexec(backend->conn, "ROLLBACK");
					return -1;
				}

				const char *cur_target = PQgetvalue(sel, 0, 1);
				if (strcmp(cur_target, old_target) != 0) {
					git_error_set(GIT_ERROR_REFERENCE,
						"reference %s symbolic target has changed", ref_name);
					PQclear(sel);
					PQexec(backend->conn, "ROLLBACK");
					return GIT_EEXISTS;
				}
			}
		} else {
			/* Neither old nor old_target: ref must NOT exist */
			if (PQntuples(sel) > 0) {
				git_error_set(GIT_ERROR_REFERENCE,
					"reference %s already exists", ref_name);
				PQclear(sel);
				PQexec(backend->conn, "ROLLBACK");
				return GIT_EEXISTS;
			}
		}

		PQclear(sel);
	}

	/* Upsert the ref */
	if (type == GIT_REFERENCE_DIRECT) {
		const git_oid *oid = git_reference_target(ref);
		const char *upsert_params[3];
		int upsert_lengths[3];
		int upsert_formats[3];

		upsert_params[0] = rid;
		upsert_lengths[0] = 0;
		upsert_formats[0] = 0;

		upsert_params[1] = ref_name;
		upsert_lengths[1] = 0;
		upsert_formats[1] = 0;

		upsert_params[2] = (const char *)oid->id;
		upsert_lengths[2] = GIT_OID_SHA1_SIZE;
		upsert_formats[2] = 1;

		res = PQexecParams(backend->conn,
			"INSERT INTO refs (repo_id, name, oid, symbolic) "
			"VALUES ($1, $2, $3, NULL) "
			"ON CONFLICT (repo_id, name) DO UPDATE "
			"SET oid = EXCLUDED.oid, symbolic = NULL",
			3, NULL, upsert_params, upsert_lengths, upsert_formats, 0);
	} else {
		/* symbolic ref */
		const char *target = git_reference_symbolic_target(ref);
		const char *upsert_params[3] = { rid, ref_name, target };

		res = PQexecParams(backend->conn,
			"INSERT INTO refs (repo_id, name, oid, symbolic) "
			"VALUES ($1, $2, NULL, $3) "
			"ON CONFLICT (repo_id, name) DO UPDATE "
			"SET oid = NULL, symbolic = EXCLUDED.symbolic",
			3, NULL, upsert_params, NULL, NULL, 0);
	}

	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		PQexec(backend->conn, "ROLLBACK");
		return -1;
	}
	PQclear(res);

	/* Write reflog entry */
	if (who) {
		const git_oid *new_oid = NULL;
		if (type == GIT_REFERENCE_DIRECT)
			new_oid = git_reference_target(ref);

		error = write_reflog_entry(backend, ref_name, old, new_oid, who, message);
		if (error < 0) {
			PQexec(backend->conn, "ROLLBACK");
			return error;
		}
	}

	/* COMMIT */
	res = PQexec(backend->conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		PQexec(backend->conn, "ROLLBACK");
		return -1;
	}
	PQclear(res);

	return 0;
}

/* ----------------------------------------------------------------
 * rename
 * ---------------------------------------------------------------- */

static int pg_refdb_rename(git_reference **out, git_refdb_backend *_backend,
                           const char *old_name, const char *new_name,
                           int force, const git_signature *who,
                           const char *message)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	char rid[16];
	PGresult *res;

	(void)who;
	(void)message;

	repo_id_str(rid, sizeof(rid), backend->repo_id);

	res = PQexec(backend->conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}
	PQclear(res);

	/* If not forcing, check the target name doesn't already exist */
	if (!force) {
		const char *chk_params[2] = { rid, new_name };
		PGresult *chk;

		chk = PQexecParams(backend->conn,
			"SELECT 1 FROM refs WHERE repo_id = $1 AND name = $2",
			2, NULL, chk_params, NULL, NULL, 0);

		if (PQresultStatus(chk) != PGRES_TUPLES_OK) {
			git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
			PQclear(chk);
			PQexec(backend->conn, "ROLLBACK");
			return -1;
		}

		if (PQntuples(chk) > 0) {
			git_error_set(GIT_ERROR_REFERENCE,
				"reference %s already exists", new_name);
			PQclear(chk);
			PQexec(backend->conn, "ROLLBACK");
			return GIT_EEXISTS;
		}
		PQclear(chk);
	} else {
		/* Force: delete the target name if it exists */
		const char *del_params[2] = { rid, new_name };
		PGresult *del;

		del = PQexecParams(backend->conn,
			"DELETE FROM refs WHERE repo_id = $1 AND name = $2",
			2, NULL, del_params, NULL, NULL, 0);
		PQclear(del);
	}

	/* Rename the ref */
	{
		const char *upd_params[3] = { new_name, rid, old_name };
		res = PQexecParams(backend->conn,
			"UPDATE refs SET name = $1 WHERE repo_id = $2 AND name = $3",
			3, NULL, upd_params, NULL, NULL, 0);

		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
			PQclear(res);
			PQexec(backend->conn, "ROLLBACK");
			return -1;
		}

		if (atoi(PQcmdTuples(res)) == 0) {
			git_error_set(GIT_ERROR_REFERENCE,
				"reference %s not found", old_name);
			PQclear(res);
			PQexec(backend->conn, "ROLLBACK");
			return GIT_ENOTFOUND;
		}
		PQclear(res);
	}

	/* Rename reflog entries */
	{
		const char *rl_params[3] = { new_name, rid, old_name };
		res = PQexecParams(backend->conn,
			"UPDATE reflog SET ref_name = $1 "
			"WHERE repo_id = $2 AND ref_name = $3",
			3, NULL, rl_params, NULL, NULL, 0);
		PQclear(res);
	}

	/* Fetch the renamed ref to return */
	{
		const char *sel_params[2] = { rid, new_name };
		PGresult *sel;

		sel = PQexecParams(backend->conn,
			"SELECT name, oid, symbolic FROM refs "
			"WHERE repo_id = $1 AND name = $2",
			2, NULL, sel_params, NULL, NULL, 0);

		if (PQresultStatus(sel) != PGRES_TUPLES_OK || PQntuples(sel) == 0) {
			git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
			PQclear(sel);
			PQexec(backend->conn, "ROLLBACK");
			return -1;
		}

		if (ref_from_result(out, sel, 0, NULL) < 0) {
			PQclear(sel);
			PQexec(backend->conn, "ROLLBACK");
			return -1;
		}
		PQclear(sel);
	}

	/* COMMIT */
	res = PQexec(backend->conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		PQexec(backend->conn, "ROLLBACK");
		git_reference_free(*out);
		*out = NULL;
		return -1;
	}
	PQclear(res);

	return 0;
}

/* ----------------------------------------------------------------
 * del
 * ---------------------------------------------------------------- */

static int pg_refdb_del(git_refdb_backend *_backend, const char *ref_name,
                        const git_oid *old_id, const char *old_target)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	char rid[16];
	PGresult *res;

	repo_id_str(rid, sizeof(rid), backend->repo_id);

	res = PQexec(backend->conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}
	PQclear(res);

	/* CAS check if old_id or old_target provided */
	if (old_id || old_target) {
		const char *sel_params[2] = { rid, ref_name };
		PGresult *sel;

		sel = PQexecParams(backend->conn,
			"SELECT oid, symbolic FROM refs "
			"WHERE repo_id = $1 AND name = $2 FOR UPDATE",
			2, NULL, sel_params, NULL, NULL, 1);

		if (PQresultStatus(sel) != PGRES_TUPLES_OK) {
			git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
			PQclear(sel);
			PQexec(backend->conn, "ROLLBACK");
			return -1;
		}

		if (PQntuples(sel) == 0) {
			PQclear(sel);
			PQexec(backend->conn, "ROLLBACK");
			return GIT_ENOTFOUND;
		}

		if (old_id) {
			if (PQgetisnull(sel, 0, 0) ||
			    PQgetlength(sel, 0, 0) != GIT_OID_SHA1_SIZE ||
			    memcmp(PQgetvalue(sel, 0, 0), old_id->id, GIT_OID_SHA1_SIZE) != 0) {
				git_error_set(GIT_ERROR_REFERENCE,
					"reference %s value has changed", ref_name);
				PQclear(sel);
				PQexec(backend->conn, "ROLLBACK");
				return GIT_EEXISTS;
			}
		}

		if (old_target) {
			if (PQgetisnull(sel, 0, 1) ||
			    strcmp(PQgetvalue(sel, 0, 1), old_target) != 0) {
				git_error_set(GIT_ERROR_REFERENCE,
					"reference %s symbolic target has changed", ref_name);
				PQclear(sel);
				PQexec(backend->conn, "ROLLBACK");
				return GIT_EEXISTS;
			}
		}

		PQclear(sel);
	}

	/* Delete the ref */
	{
		const char *del_params[2] = { rid, ref_name };
		res = PQexecParams(backend->conn,
			"DELETE FROM refs WHERE repo_id = $1 AND name = $2",
			2, NULL, del_params, NULL, NULL, 0);

		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
			PQclear(res);
			PQexec(backend->conn, "ROLLBACK");
			return -1;
		}
		PQclear(res);
	}

	/* Delete reflog entries for this ref */
	{
		const char *rl_params[2] = { rid, ref_name };
		res = PQexecParams(backend->conn,
			"DELETE FROM reflog WHERE repo_id = $1 AND ref_name = $2",
			2, NULL, rl_params, NULL, NULL, 0);
		PQclear(res);
	}

	/* COMMIT */
	res = PQexec(backend->conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		PQexec(backend->conn, "ROLLBACK");
		return -1;
	}
	PQclear(res);

	return 0;
}

/* ----------------------------------------------------------------
 * reflog stubs
 *
 * The git_reflog struct is opaque and there's no public API to
 * construct one from scratch.  Reflog entries are written as a
 * side effect of pg_refdb_write, which is the important path.
 * These callbacks satisfy the interface; full reflog read support
 * can be added once we expose a C extension or use internal APIs.
 * ---------------------------------------------------------------- */

static int pg_refdb_has_log(git_refdb_backend *_backend, const char *refname)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	char rid[16];
	const char *params[2];
	PGresult *res;
	int found;

	repo_id_str(rid, sizeof(rid), backend->repo_id);
	params[0] = rid;
	params[1] = refname;

	res = PQexecParams(backend->conn,
		"SELECT 1 FROM reflog WHERE repo_id = $1 AND ref_name = $2 LIMIT 1",
		2, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		PQclear(res);
		return -1;
	}

	found = PQntuples(res) > 0 ? 1 : 0;
	PQclear(res);
	return found;
}

static int pg_refdb_ensure_log(git_refdb_backend *_backend, const char *refname)
{
	(void)_backend;
	(void)refname;
	/* Reflog entries are always written by pg_refdb_write when a
	 * signature is provided.  Nothing to pre-create. */
	return 0;
}

static int pg_refdb_reflog_read(git_reflog **out, git_refdb_backend *_backend,
                                const char *name)
{
	(void)out;
	(void)_backend;
	(void)name;
	/* Cannot construct a git_reflog through the public API.
	 * Return ENOTFOUND so callers fall back gracefully. */
	return GIT_ENOTFOUND;
}

static int pg_refdb_reflog_write(git_refdb_backend *_backend, git_reflog *reflog)
{
	(void)_backend;
	(void)reflog;
	/* Reflog entries are written in pg_refdb_write as part of the
	 * ref update transaction.  This callback is a no-op. */
	return 0;
}

static int pg_refdb_reflog_rename(git_refdb_backend *_backend,
                                  const char *old_name, const char *new_name)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	char rid[16];
	const char *params[3];
	PGresult *res;

	repo_id_str(rid, sizeof(rid), backend->repo_id);
	params[0] = new_name;
	params[1] = rid;
	params[2] = old_name;

	res = PQexecParams(backend->conn,
		"UPDATE reflog SET ref_name = $1 "
		"WHERE repo_id = $2 AND ref_name = $3",
		3, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}

	PQclear(res);
	return 0;
}

static int pg_refdb_reflog_delete(git_refdb_backend *_backend, const char *name)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	char rid[16];
	const char *params[2];
	PGresult *res;

	repo_id_str(rid, sizeof(rid), backend->repo_id);
	params[0] = rid;
	params[1] = name;

	res = PQexecParams(backend->conn,
		"DELETE FROM reflog WHERE repo_id = $1 AND ref_name = $2",
		2, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		return -1;
	}

	PQclear(res);
	return 0;
}

/* ----------------------------------------------------------------
 * lock / unlock
 *
 * Uses PostgreSQL advisory locks keyed on a hash of repo_id + refname.
 * The lock is transaction-scoped (pg_advisory_xact_lock), so it
 * releases automatically on COMMIT or ROLLBACK.
 * ---------------------------------------------------------------- */

static int pg_refdb_lock(void **payload_out, git_refdb_backend *_backend,
                         const char *refname)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	pg_ref_lock *lock;
	char key_str[32];
	const char *params[1];
	PGresult *res;

	lock = calloc(1, sizeof(pg_ref_lock));
	if (!lock) {
		git_error_set_oom();
		return -1;
	}

	lock->lock_key = hash_refname(backend->repo_id, refname);
	lock->refname = strdup(refname);
	if (!lock->refname) {
		free(lock);
		git_error_set_oom();
		return -1;
	}

	/* Start a transaction to scope the advisory lock */
	res = PQexec(backend->conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		free(lock->refname);
		free(lock);
		return -1;
	}
	PQclear(res);

	snprintf(key_str, sizeof(key_str), "%lld", (long long)lock->lock_key);
	params[0] = key_str;

	res = PQexecParams(backend->conn,
		"SELECT pg_advisory_xact_lock($1::bigint)",
		1, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
		PQclear(res);
		PQexec(backend->conn, "ROLLBACK");
		free(lock->refname);
		free(lock);
		return -1;
	}
	PQclear(res);

	*payload_out = lock;
	return 0;
}

static int pg_refdb_unlock(git_refdb_backend *_backend, void *payload,
                           int success, int update_reflog,
                           const git_reference *ref,
                           const git_signature *sig, const char *message)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	pg_ref_lock *lock = (pg_ref_lock *)payload;
	int error = 0;

	if (success == 1) {
		/* Write/update the ref within the existing transaction */
		const char *ref_name = git_reference_name(ref);
		git_reference_t type = git_reference_type(ref);
		char rid[16];
		PGresult *res;

		repo_id_str(rid, sizeof(rid), backend->repo_id);

		if (type == GIT_REFERENCE_DIRECT) {
			const git_oid *oid = git_reference_target(ref);
			const char *params[3];
			int lengths[3];
			int formats[3];

			params[0] = rid;       lengths[0] = 0; formats[0] = 0;
			params[1] = ref_name;  lengths[1] = 0; formats[1] = 0;
			params[2] = (const char *)oid->id;
			lengths[2] = GIT_OID_SHA1_SIZE;
			formats[2] = 1;

			res = PQexecParams(backend->conn,
				"INSERT INTO refs (repo_id, name, oid, symbolic) "
				"VALUES ($1, $2, $3, NULL) "
				"ON CONFLICT (repo_id, name) DO UPDATE "
				"SET oid = EXCLUDED.oid, symbolic = NULL",
				3, NULL, params, lengths, formats, 0);
		} else {
			const char *target = git_reference_symbolic_target(ref);
			const char *params[3] = { rid, ref_name, target };

			res = PQexecParams(backend->conn,
				"INSERT INTO refs (repo_id, name, oid, symbolic) "
				"VALUES ($1, $2, NULL, $3) "
				"ON CONFLICT (repo_id, name) DO UPDATE "
				"SET oid = NULL, symbolic = EXCLUDED.symbolic",
				3, NULL, params, NULL, NULL, 0);
		}

		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
			error = -1;
		}
		PQclear(res);

		if (error == 0 && update_reflog && sig) {
			const git_oid *new_oid = NULL;
			if (type == GIT_REFERENCE_DIRECT)
				new_oid = git_reference_target(ref);
			error = write_reflog_entry(backend, ref_name, NULL, new_oid,
			                           sig, message);
		}
	} else if (success == 2) {
		/* Delete the ref */
		const char *ref_name = git_reference_name(ref);
		char rid[16];
		const char *params[2];
		PGresult *res;

		repo_id_str(rid, sizeof(rid), backend->repo_id);
		params[0] = rid;
		params[1] = ref_name;

		res = PQexecParams(backend->conn,
			"DELETE FROM refs WHERE repo_id = $1 AND name = $2",
			2, NULL, params, NULL, NULL, 0);

		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			git_error_set_str(GIT_ERROR_REFERENCE, PQerrorMessage(backend->conn));
			error = -1;
		}
		PQclear(res);

		/* Also delete reflog */
		if (error == 0) {
			res = PQexecParams(backend->conn,
				"DELETE FROM reflog WHERE repo_id = $1 AND ref_name = $2",
				2, NULL, params, NULL, NULL, 0);
			PQclear(res);
		}
	}
	/* success == 0: discard, just rollback */

	/* End the transaction (commits on success, rolls back on error) */
	{
		PGresult *res;
		if (error == 0)
			res = PQexec(backend->conn, "COMMIT");
		else
			res = PQexec(backend->conn, "ROLLBACK");
		PQclear(res);
	}

	free(lock->refname);
	free(lock);
	return error;
}

/* ----------------------------------------------------------------
 * free
 * ---------------------------------------------------------------- */

static void pg_refdb_free(git_refdb_backend *_backend)
{
	postgres_refdb_backend *backend = (postgres_refdb_backend *)_backend;
	free(backend);
}

/* ----------------------------------------------------------------
 * constructor
 * ---------------------------------------------------------------- */

int git_refdb_backend_postgres(git_refdb_backend **out, PGconn *conn,
                               int repo_id)
{
	postgres_refdb_backend *backend;

	backend = calloc(1, sizeof(postgres_refdb_backend));
	if (!backend) {
		git_error_set_oom();
		return -1;
	}

	backend->parent.version = GIT_REFDB_BACKEND_VERSION;
	backend->parent.exists = pg_refdb_exists;
	backend->parent.lookup = pg_refdb_lookup;
	backend->parent.iterator = pg_refdb_iterator;
	backend->parent.write = pg_refdb_write;
	backend->parent.rename = pg_refdb_rename;
	backend->parent.del = pg_refdb_del;
	backend->parent.compress = NULL;
	backend->parent.has_log = pg_refdb_has_log;
	backend->parent.ensure_log = pg_refdb_ensure_log;
	backend->parent.free = pg_refdb_free;
	backend->parent.reflog_read = pg_refdb_reflog_read;
	backend->parent.reflog_write = pg_refdb_reflog_write;
	backend->parent.reflog_rename = pg_refdb_reflog_rename;
	backend->parent.reflog_delete = pg_refdb_reflog_delete;
	backend->parent.lock = pg_refdb_lock;
	backend->parent.unlock = pg_refdb_unlock;

	backend->conn = conn;
	backend->repo_id = repo_id;

	*out = (git_refdb_backend *)backend;
	return 0;
}
