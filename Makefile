PG_CONFIG ?= pg_config

SQL_SCHEMA = sql/schema.sql \
             sql/functions/object_hash.sql \
             sql/functions/object_read_write.sql \
             sql/functions/tree_parse.sql \
             sql/functions/commit_parse.sql \
             sql/functions/ref_manage.sql

SQL_VIEWS = sql/views/queryable.sql

SQL_FILES = $(SQL_SCHEMA) $(SQL_VIEWS)

.PHONY: all backend ext install-sql test clean createdb dropdb

all: backend ext

backend:
	$(MAKE) -C backend PG_CONFIG=$(PG_CONFIG)

ext:
	$(MAKE) -C ext PG_CONFIG=$(PG_CONFIG)

install-sql:
	@for f in $(SQL_FILES); do \
		echo "Loading $$f..."; \
		psql -f $$f $(PGDATABASE); \
	done

createdb:
	createdb gitgres_test 2>/dev/null || true
	@for f in $(SQL_SCHEMA); do \
		psql -f $$f gitgres_test; \
	done
	@echo "Database gitgres_test ready."

dropdb:
	dropdb gitgres_test 2>/dev/null || true

test: createdb
	ruby -Itest -e 'Dir.glob("test/*_test.rb").each { |f| require_relative f }'

clean:
	$(MAKE) -C backend clean 2>/dev/null || true
	$(MAKE) -C ext clean 2>/dev/null || true
