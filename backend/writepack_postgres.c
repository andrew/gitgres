#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/stat.h>
#include <git2/sys/errors.h>
#include "odb_postgres.h"

typedef struct {
    git_odb_backend parent;
    PGconn *conn;
    int repo_id;
} postgres_odb_backend;

typedef struct {
    git_odb_writepack parent;
    postgres_odb_backend *odb_backend;
    git_odb *odb;
    git_indexer *indexer;
    git_indexer_progress_cb progress_cb;
    void *progress_payload;
    char *tmpdir;
} postgres_writepack;

/* Context for the foreach callback that copies objects from pack to postgres */
typedef struct {
    git_odb *pack_odb;
    git_odb_backend *pg_backend;
} copy_context;

static int copy_object_cb(const git_oid *oid, void *payload)
{
    copy_context *ctx = (copy_context *)payload;
    git_odb_object *obj = NULL;

    int error = git_odb_read(&obj, ctx->pack_odb, oid);
    if (error < 0)
        return error;

    error = ctx->pg_backend->write(
        ctx->pg_backend,
        oid,
        git_odb_object_data(obj),
        git_odb_object_size(obj),
        git_odb_object_type(obj));

    git_odb_object_free(obj);
    return error;
}

static int pg_writepack_append(
    git_odb_writepack *writepack,
    const void *data,
    size_t size,
    git_indexer_progress *stats)
{
    postgres_writepack *wp = (postgres_writepack *)writepack;
    return git_indexer_append(wp->indexer, data, size, stats);
}

static int pg_writepack_commit(
    git_odb_writepack *writepack,
    git_indexer_progress *stats)
{
    postgres_writepack *wp = (postgres_writepack *)writepack;
    int error;

    error = git_indexer_commit(wp->indexer, stats);
    if (error < 0)
        return error;

    /*
     * The indexer has written a .pack and .idx file in our temp directory.
     * Build the path to the .idx file, create a temporary ODB with a
     * one_pack backend, iterate all objects in the pack, and write each
     * one to postgres.
     */
    const char *name = git_indexer_name(wp->indexer);
    if (!name) {
        git_error_set_str(GIT_ERROR_ODB, "indexer produced no packfile name");
        return -1;
    }

    /* Build path: tmpdir/pack/pack-<name>.idx */
    char idx_path[1024];
    snprintf(idx_path, sizeof(idx_path), "%s/pack/pack-%s.idx", wp->tmpdir, name);

    git_odb *pack_odb = NULL;
    git_odb_backend *pack_backend = NULL;

    error = git_odb_new(&pack_odb);
    if (error < 0)
        return error;

    error = git_odb_backend_one_pack(&pack_backend, idx_path);
    if (error < 0) {
        git_odb_free(pack_odb);
        return error;
    }

    error = git_odb_add_backend(pack_odb, pack_backend, 1);
    if (error < 0) {
        git_odb_free(pack_odb);
        return error;
    }

    copy_context ctx = {
        .pack_odb = pack_odb,
        .pg_backend = &wp->odb_backend->parent
    };

    error = git_odb_foreach(pack_odb, copy_object_cb, &ctx);

    git_odb_free(pack_odb);
    return error;
}

/* Recursively remove a directory and its contents */
static void rmdir_recursive(const char *path)
{
    char cmd[1280];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    system(cmd);
}

static void pg_writepack_free(git_odb_writepack *writepack)
{
    postgres_writepack *wp = (postgres_writepack *)writepack;

    if (wp->indexer) {
        git_indexer_free(wp->indexer);
        wp->indexer = NULL;
    }

    if (wp->tmpdir) {
        rmdir_recursive(wp->tmpdir);
        free(wp->tmpdir);
        wp->tmpdir = NULL;
    }

    free(wp);
}

int pg_odb_writepack(
    git_odb_writepack **out,
    git_odb_backend *backend,
    git_odb *odb,
    git_indexer_progress_cb progress_cb,
    void *progress_payload)
{
    postgres_odb_backend *pg = (postgres_odb_backend *)backend;

    /* Create a temp directory for the indexer to write pack/idx files */
    char tmpdir[] = "/tmp/gitgres-writepack-XXXXXX";
    if (!mkdtemp(tmpdir)) {
        git_error_set_str(GIT_ERROR_ODB, "failed to create temp directory for writepack");
        return -1;
    }

    /*
     * The indexer expects a "pack" subdirectory structure. It writes
     * pack-<hash>.pack and pack-<hash>.idx into the given path inside
     * a "pack/" subdirectory.
     */
    char packdir[256];
    snprintf(packdir, sizeof(packdir), "%s/pack", tmpdir);
    if (mkdir(packdir, 0700) < 0) {
        git_error_set_str(GIT_ERROR_ODB, "failed to create pack subdirectory");
        rmdir_recursive(tmpdir);
        return -1;
    }

    git_indexer_options opts = {0};
    opts.version = GIT_INDEXER_OPTIONS_VERSION;
    opts.progress_cb = progress_cb;
    opts.progress_cb_payload = progress_payload;

    git_indexer *indexer = NULL;
    int error = git_indexer_new(&indexer, packdir, 0, odb, &opts);
    if (error < 0) {
        rmdir_recursive(tmpdir);
        return error;
    }

    postgres_writepack *wp = calloc(1, sizeof(postgres_writepack));
    if (!wp) {
        git_indexer_free(indexer);
        rmdir_recursive(tmpdir);
        return -1;
    }

    wp->parent.backend = backend;
    wp->parent.append = pg_writepack_append;
    wp->parent.commit = pg_writepack_commit;
    wp->parent.free = pg_writepack_free;

    wp->odb_backend = pg;
    wp->odb = odb;
    wp->indexer = indexer;
    wp->progress_cb = progress_cb;
    wp->progress_payload = progress_payload;
    wp->tmpdir = strdup(tmpdir);

    *out = &wp->parent;
    return 0;
}
