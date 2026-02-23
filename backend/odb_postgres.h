#ifndef ODB_POSTGRES_H
#define ODB_POSTGRES_H

#include <git2.h>
#include <git2/sys/odb_backend.h>
#include <libpq-fe.h>

int git_odb_backend_postgres(git_odb_backend **out, PGconn *conn, int repo_id);

#endif
