CREATE MATERIALIZED VIEW commits_view AS
SELECT o.repo_id, o.oid AS commit_oid, encode(o.oid, 'hex') AS sha,
       c.tree_oid, c.parent_oids, c.author_name, c.author_email,
       to_timestamp(c.author_timestamp) AS authored_at,
       c.committer_name, c.committer_email,
       to_timestamp(c.committer_timestamp) AS committed_at,
       c.message
FROM objects o, LATERAL git_commit_parse(o.content) c
WHERE o.type = 1;

CREATE UNIQUE INDEX idx_commits_view_oid ON commits_view (repo_id, commit_oid);

CREATE MATERIALIZED VIEW tree_entries_view AS
SELECT o.repo_id, o.oid AS tree_oid, e.mode, e.name, e.entry_oid
FROM objects o, LATERAL git_tree_entries(o.content) e
WHERE o.type = 2;

CREATE INDEX idx_tree_entries_view_oid ON tree_entries_view (repo_id, tree_oid);
