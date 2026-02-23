#ifndef REFDB_POSTGRES_H
#define REFDB_POSTGRES_H

#include <git2.h>
#include <git2/sys/refdb_backend.h>
#include <libpq-fe.h>

int git_refdb_backend_postgres(git_refdb_backend **out, PGconn *conn, int repo_id);

#endif
