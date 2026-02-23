#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
    CREATE EXTENSION IF NOT EXISTS gitgres;
SQL

for f in sql/schema.sql \
         sql/functions/object_hash.sql \
         sql/functions/object_read_write.sql \
         sql/functions/tree_parse.sql \
         sql/functions/commit_parse.sql \
         sql/functions/ref_manage.sql \
         sql/views/queryable.sql; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "/gitgres/$f"
done
