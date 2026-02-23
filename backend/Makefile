PG_CONFIG ?= pg_config

LIBGIT2_CFLAGS := $(shell pkg-config --cflags libgit2)
LIBGIT2_LIBS := $(shell pkg-config --libs libgit2)
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)
PG_INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)

CC = cc
CFLAGS = -Wall -g -O2 $(LIBGIT2_CFLAGS) -I$(PG_INCLUDEDIR)
LDFLAGS = $(LIBGIT2_LIBS) -L$(PG_LIBDIR) -lpq

SHARED_OBJS = odb_postgres.o refdb_postgres.o writepack_postgres.o

all: gitgres-backend git-remote-gitgres

gitgres-backend: main.o $(SHARED_OBJS)
	$(CC) -o $@ main.o $(SHARED_OBJS) $(LDFLAGS)

git-remote-gitgres: remote_helper.o $(SHARED_OBJS)
	$(CC) -o $@ remote_helper.o $(SHARED_OBJS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o gitgres-backend git-remote-gitgres

.PHONY: all clean
