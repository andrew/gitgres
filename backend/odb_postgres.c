#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <git2/sys/errors.h>
#include "odb_postgres.h"

typedef struct {
    git_odb_backend parent;
    PGconn *conn;
    int repo_id;
} postgres_odb_backend;

/* Forward declaration for writepack constructor */
int pg_odb_writepack(
    git_odb_writepack **out,
    git_odb_backend *backend,
    git_odb *odb,
    git_indexer_progress_cb progress_cb,
    void *progress_payload);

static int pg_odb_read(
    void **data_p,
    size_t *len_p,
    git_object_t *type_p,
    git_odb_backend *backend,
    const git_oid *oid)
{
    postgres_odb_backend *pg = (postgres_odb_backend *)backend;
    uint32_t repo_id_n = htonl((uint32_t)pg->repo_id);

    const char *paramValues[2] = {
        (const char *)&repo_id_n,
        (const char *)oid->id
    };
    int paramLengths[2] = { sizeof(repo_id_n), GIT_OID_SHA1_SIZE };
    int paramFormats[2] = { 1, 1 };

    PGresult *res = PQexecParams(pg->conn,
        "SELECT type, size, content FROM objects WHERE repo_id=$1 AND oid=$2",
        2, NULL, paramValues, paramLengths, paramFormats, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        git_error_set_str(GIT_ERROR_ODB, PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }

    if (PQntuples(res) == 0) {
        PQclear(res);
        return GIT_ENOTFOUND;
    }

    int16_t type_val;
    memcpy(&type_val, PQgetvalue(res, 0, 0), sizeof(type_val));
    type_val = ntohs(type_val);

    int32_t size_val;
    memcpy(&size_val, PQgetvalue(res, 0, 1), sizeof(size_val));
    size_val = ntohl(size_val);

    int content_len = PQgetlength(res, 0, 2);
    void *content = PQgetvalue(res, 0, 2);

    void *buf = git_odb_backend_data_alloc(backend, content_len);
    if (!buf) {
        PQclear(res);
        return -1;
    }
    memcpy(buf, content, content_len);

    *data_p = buf;
    *len_p = (size_t)size_val;
    *type_p = (git_object_t)type_val;

    PQclear(res);
    return 0;
}

static int pg_odb_read_header(
    size_t *len_p,
    git_object_t *type_p,
    git_odb_backend *backend,
    const git_oid *oid)
{
    postgres_odb_backend *pg = (postgres_odb_backend *)backend;
    uint32_t repo_id_n = htonl((uint32_t)pg->repo_id);

    const char *paramValues[2] = {
        (const char *)&repo_id_n,
        (const char *)oid->id
    };
    int paramLengths[2] = { sizeof(repo_id_n), GIT_OID_SHA1_SIZE };
    int paramFormats[2] = { 1, 1 };

    PGresult *res = PQexecParams(pg->conn,
        "SELECT type, size FROM objects WHERE repo_id=$1 AND oid=$2",
        2, NULL, paramValues, paramLengths, paramFormats, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        git_error_set_str(GIT_ERROR_ODB, PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }

    if (PQntuples(res) == 0) {
        PQclear(res);
        return GIT_ENOTFOUND;
    }

    int16_t type_val;
    memcpy(&type_val, PQgetvalue(res, 0, 0), sizeof(type_val));
    type_val = ntohs(type_val);

    int32_t size_val;
    memcpy(&size_val, PQgetvalue(res, 0, 1), sizeof(size_val));
    size_val = ntohl(size_val);

    *len_p = (size_t)size_val;
    *type_p = (git_object_t)type_val;

    PQclear(res);
    return 0;
}

static int pg_odb_read_prefix(
    git_oid *out_oid,
    void **data_p,
    size_t *len_p,
    git_object_t *type_p,
    git_odb_backend *backend,
    const git_oid *short_oid,
    size_t prefix_len)
{
    postgres_odb_backend *pg = (postgres_odb_backend *)backend;

    /* Full OID lookup: use exact match */
    if (prefix_len == GIT_OID_SHA1_HEXSIZE)
        return pg_odb_read(data_p, len_p, type_p, backend, short_oid);

    uint32_t repo_id_n = htonl((uint32_t)pg->repo_id);

    /* prefix_len is in hex chars; byte length for the prefix */
    int byte_len = (int)((prefix_len + 1) / 2);
    int32_t byte_len_n = htonl((uint32_t)byte_len);

    const char *paramValues[3] = {
        (const char *)&repo_id_n,
        (const char *)&byte_len_n,
        (const char *)short_oid->id
    };
    int paramLengths[3] = {
        sizeof(repo_id_n),
        sizeof(byte_len_n),
        byte_len
    };
    int paramFormats[3] = { 1, 1, 1 };

    PGresult *res = PQexecParams(pg->conn,
        "SELECT oid, type, size, content FROM objects "
        "WHERE repo_id=$1 AND substring(oid from 1 for $2) = $3",
        3, NULL, paramValues, paramLengths, paramFormats, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        git_error_set_str(GIT_ERROR_ODB, PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }

    int nrows = PQntuples(res);
    if (nrows == 0) {
        PQclear(res);
        return GIT_ENOTFOUND;
    }
    if (nrows > 1) {
        PQclear(res);
        return GIT_EAMBIGUOUS;
    }

    /* Copy out the full OID */
    memcpy(out_oid->id, PQgetvalue(res, 0, 0), GIT_OID_SHA1_SIZE);

    int16_t type_val;
    memcpy(&type_val, PQgetvalue(res, 0, 1), sizeof(type_val));
    type_val = ntohs(type_val);

    int32_t size_val;
    memcpy(&size_val, PQgetvalue(res, 0, 2), sizeof(size_val));
    size_val = ntohl(size_val);

    int content_len = PQgetlength(res, 0, 3);
    void *content = PQgetvalue(res, 0, 3);

    void *buf = git_odb_backend_data_alloc(backend, content_len);
    if (!buf) {
        PQclear(res);
        return -1;
    }
    memcpy(buf, content, content_len);

    *data_p = buf;
    *len_p = (size_t)size_val;
    *type_p = (git_object_t)type_val;

    PQclear(res);
    return 0;
}

static int pg_odb_write(
    git_odb_backend *backend,
    const git_oid *oid,
    const void *data,
    size_t len,
    git_object_t type)
{
    postgres_odb_backend *pg = (postgres_odb_backend *)backend;
    uint32_t repo_id_n = htonl((uint32_t)pg->repo_id);
    int16_t type_n = htons((int16_t)type);
    uint32_t size_n = htonl((uint32_t)len);

    const char *paramValues[5] = {
        (const char *)&repo_id_n,
        (const char *)oid->id,
        (const char *)&type_n,
        (const char *)&size_n,
        (const char *)data
    };
    int paramLengths[5] = {
        sizeof(repo_id_n),
        GIT_OID_SHA1_SIZE,
        sizeof(type_n),
        sizeof(size_n),
        (int)len
    };
    int paramFormats[5] = { 1, 1, 1, 1, 1 };

    PGresult *res = PQexecParams(pg->conn,
        "INSERT INTO objects (repo_id, oid, type, size, content) "
        "VALUES ($1, $2, $3, $4, $5) "
        "ON CONFLICT (repo_id, oid) DO NOTHING",
        5, NULL, paramValues, paramLengths, paramFormats, 0);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        git_error_set_str(GIT_ERROR_ODB, PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }

    PQclear(res);
    return 0;
}

static int pg_odb_exists(git_odb_backend *backend, const git_oid *oid)
{
    postgres_odb_backend *pg = (postgres_odb_backend *)backend;
    uint32_t repo_id_n = htonl((uint32_t)pg->repo_id);

    const char *paramValues[2] = {
        (const char *)&repo_id_n,
        (const char *)oid->id
    };
    int paramLengths[2] = { sizeof(repo_id_n), GIT_OID_SHA1_SIZE };
    int paramFormats[2] = { 1, 1 };

    PGresult *res = PQexecParams(pg->conn,
        "SELECT 1 FROM objects WHERE repo_id=$1 AND oid=$2",
        2, NULL, paramValues, paramLengths, paramFormats, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        git_error_set_str(GIT_ERROR_ODB, PQresultErrorMessage(res));
        PQclear(res);
        return 0;
    }

    int found = PQntuples(res) > 0 ? 1 : 0;
    PQclear(res);
    return found;
}

static int pg_odb_exists_prefix(
    git_oid *out_oid,
    git_odb_backend *backend,
    const git_oid *short_oid,
    size_t prefix_len)
{
    postgres_odb_backend *pg = (postgres_odb_backend *)backend;

    /* Full OID: exact match */
    if (prefix_len == GIT_OID_SHA1_HEXSIZE) {
        if (!pg_odb_exists(backend, short_oid))
            return GIT_ENOTFOUND;
        git_oid_cpy(out_oid, short_oid);
        return 0;
    }

    uint32_t repo_id_n = htonl((uint32_t)pg->repo_id);
    int byte_len = (int)((prefix_len + 1) / 2);
    int32_t byte_len_n = htonl((uint32_t)byte_len);

    const char *paramValues[3] = {
        (const char *)&repo_id_n,
        (const char *)&byte_len_n,
        (const char *)short_oid->id
    };
    int paramLengths[3] = {
        sizeof(repo_id_n),
        sizeof(byte_len_n),
        byte_len
    };
    int paramFormats[3] = { 1, 1, 1 };

    PGresult *res = PQexecParams(pg->conn,
        "SELECT oid FROM objects "
        "WHERE repo_id=$1 AND substring(oid from 1 for $2) = $3",
        3, NULL, paramValues, paramLengths, paramFormats, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        git_error_set_str(GIT_ERROR_ODB, PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }

    int nrows = PQntuples(res);
    if (nrows == 0) {
        PQclear(res);
        return GIT_ENOTFOUND;
    }
    if (nrows > 1) {
        PQclear(res);
        return GIT_EAMBIGUOUS;
    }

    memcpy(out_oid->id, PQgetvalue(res, 0, 0), GIT_OID_SHA1_SIZE);
    PQclear(res);
    return 0;
}

static int pg_odb_foreach(
    git_odb_backend *backend,
    git_odb_foreach_cb cb,
    void *payload)
{
    postgres_odb_backend *pg = (postgres_odb_backend *)backend;
    uint32_t repo_id_n = htonl((uint32_t)pg->repo_id);

    const char *paramValues[1] = { (const char *)&repo_id_n };
    int paramLengths[1] = { sizeof(repo_id_n) };
    int paramFormats[1] = { 1 };

    PGresult *res = PQexecParams(pg->conn,
        "SELECT oid FROM objects WHERE repo_id=$1",
        1, NULL, paramValues, paramLengths, paramFormats, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        git_error_set_str(GIT_ERROR_ODB, PQresultErrorMessage(res));
        PQclear(res);
        return -1;
    }

    int nrows = PQntuples(res);
    for (int i = 0; i < nrows; i++) {
        git_oid oid;
        memcpy(oid.id, PQgetvalue(res, i, 0), GIT_OID_SHA1_SIZE);

        int error = cb(&oid, payload);
        if (error) {
            PQclear(res);
            return error;
        }
    }

    PQclear(res);
    return 0;
}

static void pg_odb_free(git_odb_backend *backend)
{
    free(backend);
}

int git_odb_backend_postgres(git_odb_backend **out, PGconn *conn, int repo_id)
{
    postgres_odb_backend *backend = calloc(1, sizeof(postgres_odb_backend));
    if (!backend)
        return -1;

    backend->parent.version = GIT_ODB_BACKEND_VERSION;
    backend->parent.read = pg_odb_read;
    backend->parent.read_header = pg_odb_read_header;
    backend->parent.read_prefix = pg_odb_read_prefix;
    backend->parent.write = pg_odb_write;
    backend->parent.exists = pg_odb_exists;
    backend->parent.exists_prefix = pg_odb_exists_prefix;
    backend->parent.foreach = pg_odb_foreach;
    backend->parent.writepack = pg_odb_writepack;
    backend->parent.free = pg_odb_free;

    backend->conn = conn;
    backend->repo_id = repo_id;

    *out = &backend->parent;
    return 0;
}
