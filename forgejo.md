## Forgejo's git layer

Forgejo already stores everything except git data in Postgres: issues, pull requests, users, permissions, webhooks, branch protection rules, CI status. The git repos are the one thing on the filesystem. The entire git interaction goes through a single Go package, `modules/git`, which shells out to the `git` binary for every operation. No go-git, no libgit2, just `exec.Command("git", ...)` with output parsing. About 15,000 lines of Go across 84 files.

The core types are all concrete structs, not interfaces: `Repository` (wraps a filesystem path), `Commit`, `Tree`, `Blob`, `TreeEntry`, `Reference`, `Tag`. The rest of Forgejo imports this package and works with these types directly. There's no swappable backend abstraction. Replacing the git layer means replacing this package.

### What the web UI needs from git

**File browsing.** `view.go` calls `commit.GetTreeEntryByPath()` to navigate directories, then `entry.Blob().DataAsync()` to stream file content. Tree listing comes from `tree.ListEntries()`. Every directory page also shows last-commit-per-file via `GetCommitsInfo()`, which walks history to find when each entry last changed. In gitgres this is a join between `git_ls_tree_r()` and a commit walk query, or a materialized view that precomputes it.

**Commit log.** Paginated via `CommitsByRange(page, pageSize)`, searched via `SearchCommits()` with author/date/keyword filters, and filtered by path via `CommitsByFileAndRange()`. All parse `git log --format=...` output. The commits_view materialized view handles most of this directly. Path-filtered history needs a recursive walk of tree diffs between consecutive commits, which is doable in SQL but not trivial.

**Diffs.** `GetRawDiff()` runs `git diff -M` between two commits and streams unified diff output. `services/gitdiff/` parses this into `DiffFile`/`DiffSection`/`DiffLine` structs for rendering. Computing diffs in SQL means comparing two tree walks and then doing content-level diffing on changed blobs. The tree comparison is straightforward (join two `git_ls_tree_r` results, find mismatched OIDs). The content diff algorithm (Myers or patience) would need to be implemented in the application layer or as a Postgres function.

**Blame.** Uses `git blame --porcelain` streaming output. Each blame part has a commit SHA, line range, and optionally a previous path for renames. Reimplementing blame means walking commit history backwards per-line, which is one of the more expensive operations to do without git's optimized C implementation. Probably keep this as a SQL query that walks the commit graph and compares file content at each step, but it'll be slower than native git blame on large files.

**Grep/search.** `git grep` with regex support across a tree at a given commit. In Postgres this becomes a full-text search or `LIKE`/regex query against blob content joined through the tree. Could benefit from `pg_trgm` indexes on blob content for substring search.

**Branches and tags.** Direct queries against the refs table. Forgejo already mirrors branch metadata to its own database tables (`models/git/branch.go`), so the refs table in gitgres would replace both the git refs and that mirror table.

### Write operations

**Push/fetch/clone.** Smart HTTP protocol is implemented in `routers/web/repo/githttp.go`. For push, it runs `git-receive-pack --stateless-rpc`; for fetch, `git-upload-pack --stateless-rpc`. SSH does the same through `cmd/serv.go`. The git-remote-gitgres helper already handles the transport protocol. For a Forgejo fork, the smart HTTP handler would need to speak the git protocol directly against the Postgres storage rather than delegating to the git binary. libgit2 can do this, or we implement the pack protocol in Go using the existing gitgres SQL functions.

**File editing through the web UI.** `services/repository/files/` creates temporary clones, makes changes, commits, and pushes back. With Postgres storage, this becomes: read the current tree, modify it (add/update/delete entries), write the new tree and commit objects directly to the objects table, update the ref. No clone needed.

**Merges.** This is the hardest part. Forgejo supports multiple merge strategies: fast-forward, three-way merge, rebase, and squash. Each currently creates a temporary clone and runs git merge/rebase commands. The three-way merge algorithm itself (`merge-tree`, `read-tree`, `merge-one-file`) is deep git internals. Options: use libgit2's merge support through the Postgres backends, implement a merge algorithm in Go that reads from Postgres, or keep a thin git layer just for merge computation.

**Hooks.** Pre-receive hooks (`routers/private/hook_pre_receive.go`) check permissions, protected branch rules, and quotas before accepting a push. Post-receive hooks sync metadata to the database, fire webhooks, trigger CI, and update issue references. With git data in Postgres, pre-receive becomes a database trigger or a check in the push handler. Post-receive becomes a LISTEN/NOTIFY event or a trigger that fires after ref updates.

### The replacement surface

The `modules/git` package exports about 25 types and several hundred methods. Not all need reimplementation. Grouped by difficulty:

**Direct SQL replacements** (refs, trees, blobs, commits, tags, branches, commit log, tree listing, file content, ref listing). These map directly to queries against the objects and refs tables using the existing gitgres SQL functions. This covers the majority of read-path web UI operations.

**Needs new SQL or application logic** (diff computation, path-filtered commit history, last-commit-per-entry, archive generation, grep/search). Tree diffing is a join. Content diffing needs an algorithm. Path-filtered history needs commit graph walking with tree comparison at each step. Archive generation means reading a tree and streaming a zip/tar.

**Hard to replace** (three-way merge, rebase, blame, conflict detection). These rely on git's optimized algorithms. libgit2 handles merge and blame. For a pure-SQL approach, merge and blame would need new implementations.

**Protocol layer** (smart HTTP upload-pack/receive-pack, SSH transport). The git-remote-gitgres helper proves the protocol works. The server side needs an equivalent that speaks the git pack protocol against Postgres storage. libgit2 can generate and parse packfiles using the Postgres ODB backend.

### What stays the same

Everything above `modules/git` is unchanged. The web UI templates, the API handlers, the issue tracker, the pull request review system, the CI integration, user management, OAuth, webhooks, the notification system. All of that talks to Postgres already and doesn't care where git data comes from, as long as it gets the same types back from the git layer.

The `models/git/` package (branch metadata, commit status, protected branches, protected tags, LFS pointers) merges with the gitgres schema. Branch metadata currently duplicated between git and the database becomes a single source of truth.

### Approach

Fork Forgejo. Replace `modules/git` with a package that queries gitgres tables instead of shelling out to git. Start with read operations (tree, blob, commit, ref, log) since they cover the web UI. Then tackle write operations (push protocol, web file editing). Leave merge/rebase using libgit2 through the Postgres backends. Blame either goes through libgit2 or gets a SQL implementation that's slower but functional.

The `Repository` struct changes from holding a filesystem path to holding a database connection and repo_id. `Commit`, `Tree`, `Blob` become thin wrappers around SQL query results. The batch cat-file optimization (currently a persistent `git cat-file --batch` subprocess) becomes connection pooling, which Postgres already handles.

### What gitgres needs before this is possible

The SQL functions and libgit2 backends exist. What's missing:

- **Server-side pack protocol.** The remote helper is client-side. A Forgejo integration needs a server that speaks `upload-pack` and `receive-pack` against Postgres. libgit2 can build packs from the ODB backend; we need to wire that into an HTTP handler.
- **Diff computation.** Either a SQL function that compares two trees and returns changed paths, or a Go function that does this using SQL queries. Content-level diffs need Myers or patience algorithm in Go, operating on blobs fetched from Postgres.
- **Merge support.** Use libgit2's merge through the Postgres backends via cgo/git2go bindings, or implement in Go.
- **Blame.** Same choice: libgit2 or custom implementation.
- **Commit graph walking.** `git log --ancestry-path`, `merge-base`, reachability queries. These are graph traversals on the commit DAG. Could be recursive CTEs in Postgres, or application-level traversal.
- **Archive generation.** Read a tree recursively, stream as zip or tar. Straightforward given `git_ls_tree_r` and blob content access.

### Postgres features that help

`LISTEN/NOTIFY` on ref updates replaces post-receive hooks and webhook polling. Triggers on the refs table fire on every push.

Row-level security on the objects and refs tables gives multi-tenant isolation without application-level permission checks on every git read.

Logical replication lets you selectively replicate repos across instances. Partial replication of git data isn't possible with filesystem-based git.

`pg_trgm` indexes on blob content enable fast substring search across all repositories without a separate search index.

Recursive CTEs handle commit graph traversal (ancestry, merge-base, reachability) in a single query.

Read replicas give free scaling for the web UI. The read path (browsing, logs, diffs) hits replicas; writes (push, merge) go to primary. No need for a Gitaly-style RPC layer.

`pg_dump` backs up everything: git data, forge metadata, user data, CI state. One backup, one restore.

### Scale considerations

The objects table grows linearly with repository content. A large monorepo with 1M objects and 10GB of content is well within Postgres capabilities, but it's worth thinking about. Packfile delta compression (which git uses on disk) isn't replicated in the objects table, where each object stores its full content. A 10MB file modified 100 times stores 1GB in the objects table vs maybe 50MB in a git packfile. This is the main storage tradeoff.

Options: accept the storage cost (disk is cheap, queries are simpler), add application-level delta compression, or store packfiles as blobs and use libgit2 to index into them. The current design prioritizes queryability over storage efficiency, which seems right for the use case.
