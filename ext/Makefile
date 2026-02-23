EXTENSION = gitgres
MODULE_big = gitgres
OBJS = gitgres.o git_oid_type.o sha1_hash.o tree_parse.o
DATA = sql/gitgres--0.1.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

PG_CPPFLAGS = -I$(shell $(PG_CONFIG) --includedir-server)

# OpenSSL for SHA1
PG_CPPFLAGS += $(shell pkg-config --cflags openssl 2>/dev/null)
SHLIB_LINK += $(shell pkg-config --libs openssl 2>/dev/null || echo "-lssl -lcrypto")

include $(PGXS)
