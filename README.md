# Gitgres

Store git objects and refs in PostgreSQL tables. Standard `git push`/`clone` work against the database through a libgit2-based backend.

Two implementations of the core functions: pure SQL (PL/pgSQL, works on any managed Postgres with pgcrypto) and a C extension (native `git_oid` type, faster SHA1 and tree parsing via OpenSSL).

## Setup

Requires PostgreSQL with pgcrypto, libgit2, and libpq.

```
brew install libgit2
```

Create a database and load the schema:

```
createdb gitgres
make install-sql PGDATABASE=gitgres
```

Build the libgit2 backend:

```
make backend
```

Build the C extension (optional, for performance):

```
make ext
```

## Usage

Initialize a repository in the database:

```
./backend/gitgres-backend init "dbname=gitgres" myrepo
```

Push a local git repo into Postgres:

```
./backend/gitgres-backend push "dbname=gitgres" myrepo /path/to/repo
```

Clone from Postgres back to disk:

```
./backend/gitgres-backend clone "dbname=gitgres" myrepo /path/to/dest
```

List refs stored in the database:

```
./backend/gitgres-backend ls-refs "dbname=gitgres" myrepo
```

Import a repo using the shell script (no compilation needed):

```
./import/gitgres-import.sh /path/to/repo "dbname=gitgres" myrepo
```

## Querying git data with SQL

After importing or pushing a repo, refresh the materialized views:

```sql
REFRESH MATERIALIZED VIEW commits_view;
REFRESH MATERIALIZED VIEW tree_entries_view;
```

Then query commits like a regular table:

```sql
SELECT sha, author_name, authored_at, message
FROM commits_view
ORDER BY authored_at DESC;
```

Walk a tree:

```sql
SELECT path, mode, encode(oid, 'hex')
FROM git_ls_tree_r(1, decode('abc123...', 'hex'));
```

## Tests

```
make test
```

Runs 30 Minitest tests against a `gitgres_test` database. Each test runs in a transaction that rolls back on teardown. Tests cover object hashing (verified against `git hash-object`), object store CRUD, tree and commit parsing, ref compare-and-swap updates, and a full push/clone roundtrip.

## How it works

Git objects (commits, trees, blobs, tags) are stored in an `objects` table with their raw content and a SHA1 OID computed the same way git does: `SHA1("<type> <size>\0<content>")`. Refs live in a `refs` table with compare-and-swap updates for safe concurrent access.

The libgit2 backend implements `git_odb_backend` and `git_refdb_backend`, the two interfaces libgit2 needs to treat any storage system as a git repository. The backend reads and writes objects and refs through libpq. When receiving a push, it uses libgit2's packfile indexer to extract individual objects from the incoming pack, then stores each one in Postgres.

The C extension adds a proper `git_oid` type to Postgres (20-byte fixed binary with hex I/O and btree/hash indexing) and C implementations of SHA1 hashing and tree parsing for better performance on large repos.

## Prior art

[libgit2-backends](https://github.com/libgit2/libgit2-backends) -- the official collection of pluggable ODB backends for libgit2. Includes MySQL, SQLite, Redis, and Memcached implementations. No Postgres backend, which is what prompted this project.

[gitbase](https://github.com/src-d/gitbase) -- a MySQL-compatible SQL interface for querying git repositories, built on [go-git](https://github.com/go-git/go-git). Reads from on-disk repos rather than storing objects in the database. The SQL query layer is the goal rather than the storage layer.

[JGit DFS](https://archive.eclipse.org/jgit/docs/jgit-2.0.0.201206130900-r/apidocs/org/eclipse/jgit/storage/dfs/DfsRepository.html) -- JGit's abstract distributed file system storage. Defines the interface for storing git pack files on arbitrary backends. Google's internal git infrastructure builds on this. An earlier attempt (JGit DHT) tried storing objects directly in databases but was abandoned because no database could keep up with the access patterns.

[Gitaly](https://gitlab.com/gitlab-org/gitaly) -- GitLab's git storage service. An RPC server that wraps git operations rather than replacing the storage layer. Still uses the filesystem for actual object storage.

[Dolt](https://github.com/dolthub/dolt) -- a SQL database with git-style versioning (branch, merge, diff) built on [prolly trees](https://github.com/attic-labs/noms). Comes at the problem from the opposite direction: it's a database that borrowed git's semantics, not git storage backed by a database.
