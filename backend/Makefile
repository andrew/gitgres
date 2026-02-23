PG_CONFIG ?= pg_config

LIBGIT2_CFLAGS := $(shell pkg-config --cflags libgit2)
LIBGIT2_LIBS := $(shell pkg-config --libs libgit2)
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)
PG_INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)

CC = cc
CFLAGS = -Wall -g -O2 $(LIBGIT2_CFLAGS) -I$(PG_INCLUDEDIR)
LDFLAGS = $(LIBGIT2_LIBS) -L$(PG_LIBDIR) -lpq

OBJECTS = main.o odb_postgres.o refdb_postgres.o writepack_postgres.o

TARGET = gitgres-backend

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) -o $@ $(OBJECTS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET)

.PHONY: all clean
