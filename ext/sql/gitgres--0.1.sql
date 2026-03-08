-- git_oid type
CREATE TYPE git_oid;

CREATE FUNCTION git_oid_in(cstring) RETURNS git_oid
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION git_oid_out(git_oid) RETURNS cstring
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE git_oid (
    INPUT = git_oid_in,
    OUTPUT = git_oid_out,
    INTERNALLENGTH = 20,
    PASSEDBYVALUE = false,
    ALIGNMENT = char,
    STORAGE = plain
);

-- Comparison functions
CREATE FUNCTION git_oid_eq(git_oid, git_oid) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION git_oid_ne(git_oid, git_oid) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION git_oid_lt(git_oid, git_oid) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION git_oid_le(git_oid, git_oid) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION git_oid_gt(git_oid, git_oid) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION git_oid_ge(git_oid, git_oid) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION git_oid_cmp(git_oid, git_oid) RETURNS integer
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION git_oid_hash(git_oid) RETURNS integer
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

-- Operators
CREATE OPERATOR = (
    LEFTARG = git_oid, RIGHTARG = git_oid,
    FUNCTION = git_oid_eq,
    COMMUTATOR = =, NEGATOR = <>,
    RESTRICT = eqsel, JOIN = eqjoinsel,
    HASHES, MERGES
);

CREATE OPERATOR <> (
    LEFTARG = git_oid, RIGHTARG = git_oid,
    FUNCTION = git_oid_ne,
    COMMUTATOR = <>, NEGATOR = =,
    RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG = git_oid, RIGHTARG = git_oid,
    FUNCTION = git_oid_lt,
    COMMUTATOR = >, NEGATOR = >=,
    RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG = git_oid, RIGHTARG = git_oid,
    FUNCTION = git_oid_le,
    COMMUTATOR = >=, NEGATOR = >,
    RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

CREATE OPERATOR > (
    LEFTARG = git_oid, RIGHTARG = git_oid,
    FUNCTION = git_oid_gt,
    COMMUTATOR = <, NEGATOR = <=,
    RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR >= (
    LEFTARG = git_oid, RIGHTARG = git_oid,
    FUNCTION = git_oid_ge,
    COMMUTATOR = <=, NEGATOR = <,
    RESTRICT = scalargesel, JOIN = scalargejoinsel
);

-- B-tree operator class for indexing and ORDER BY
CREATE OPERATOR CLASS git_oid_ops
    DEFAULT FOR TYPE git_oid USING btree AS
        OPERATOR 1 <,
        OPERATOR 2 <=,
        OPERATOR 3 =,
        OPERATOR 4 >=,
        OPERATOR 5 >,
        FUNCTION 1 git_oid_cmp(git_oid, git_oid);

-- Hash operator class for hash indexes and hash joins
CREATE OPERATOR CLASS git_oid_hash_ops
    DEFAULT FOR TYPE git_oid USING hash AS
        OPERATOR 1 =,
        FUNCTION 1 git_oid_hash(git_oid);

-- Fast C SHA1 hash function
-- git_object_hash_c(type smallint, content bytea) RETURNS bytea
-- Type codes: 1=commit, 2=tree, 3=blob, 4=tag
CREATE FUNCTION git_object_hash_c(smallint, bytea) RETURNS bytea
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

-- Fast C tree entry parser
-- Parses binary tree content into (mode, name, entry_oid) rows
CREATE FUNCTION git_tree_entries_c(bytea)
    RETURNS TABLE(mode text, name text, entry_oid bytea)
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

-- ============================================================
-- Schema: tables
-- ============================================================

CREATE TABLE repositories (
    id          serial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE objects (
    repo_id     integer NOT NULL REFERENCES repositories(id),
    oid         bytea NOT NULL,
    type        smallint NOT NULL,
    size        integer NOT NULL,
    content     bytea NOT NULL,
    PRIMARY KEY (repo_id, oid)
);
CREATE INDEX idx_objects_oid ON objects (oid);

CREATE TABLE refs (
    repo_id     integer NOT NULL REFERENCES repositories(id),
    name        text NOT NULL,
    oid         bytea,
    symbolic    text,
    PRIMARY KEY (repo_id, name),
    CHECK ((oid IS NOT NULL) != (symbolic IS NOT NULL))
);

CREATE TABLE reflog (
    id          bigserial PRIMARY KEY,
    repo_id     integer NOT NULL REFERENCES repositories(id),
    ref_name    text NOT NULL,
    old_oid     bytea,
    new_oid     bytea,
    committer   text NOT NULL,
    timestamp_s bigint NOT NULL,
    tz_offset   text NOT NULL,
    message     text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_reflog_ref ON reflog (repo_id, ref_name, id);

-- ============================================================
-- Functions: object hashing
-- ============================================================

CREATE FUNCTION git_type_name(obj_type smallint)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE obj_type
        WHEN 1 THEN 'commit'
        WHEN 2 THEN 'tree'
        WHEN 3 THEN 'blob'
        WHEN 4 THEN 'tag'
    END;
$$;

CREATE FUNCTION git_object_hash(obj_type smallint, content bytea)
RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT digest(
        convert_to(git_type_name(obj_type) || ' ' || octet_length(content)::text, 'UTF8')
        || '\x00'::bytea
        || content,
        'sha1'
    );
$$;

-- ============================================================
-- Functions: object read/write
-- ============================================================

CREATE FUNCTION git_object_write(
    p_repo_id integer,
    p_type smallint,
    p_content bytea
)
RETURNS bytea
LANGUAGE plpgsql AS $$
DECLARE
    v_oid bytea;
BEGIN
    v_oid := git_object_hash(p_type, p_content);

    INSERT INTO objects (repo_id, oid, type, size, content)
    VALUES (p_repo_id, v_oid, p_type, octet_length(p_content), p_content)
    ON CONFLICT (repo_id, oid) DO NOTHING;

    RETURN v_oid;
END;
$$;

CREATE FUNCTION git_object_read(
    p_repo_id integer,
    p_oid bytea
)
RETURNS TABLE(type smallint, size integer, content bytea)
LANGUAGE sql STABLE STRICT AS $$
    SELECT o.type, o.size, o.content
    FROM objects o
    WHERE o.repo_id = p_repo_id AND o.oid = p_oid;
$$;

CREATE FUNCTION git_object_read_prefix(
    p_repo_id integer,
    p_prefix bytea,
    p_prefix_len integer
)
RETURNS TABLE(oid bytea, type smallint, size integer, content bytea)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_prefix_bytes integer;
BEGIN
    v_prefix_bytes := p_prefix_len / 2;

    RETURN QUERY
    SELECT o.oid, o.type, o.size, o.content
    FROM objects o
    WHERE o.repo_id = p_repo_id
      AND substring(o.oid FROM 1 FOR v_prefix_bytes) = substring(p_prefix FROM 1 FOR v_prefix_bytes);
END;
$$;

-- ============================================================
-- Functions: tree parsing
-- ============================================================

CREATE FUNCTION git_tree_entries(p_content bytea)
RETURNS TABLE(mode text, name text, entry_oid bytea)
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    v_pos integer := 1;
    v_len integer;
    v_space_pos integer;
    v_null_pos integer;
BEGIN
    v_len := octet_length(p_content);

    WHILE v_pos <= v_len LOOP
        v_space_pos := v_pos;
        WHILE v_space_pos <= v_len AND get_byte(p_content, v_space_pos - 1) != 32 LOOP
            v_space_pos := v_space_pos + 1;
        END LOOP;

        v_null_pos := v_space_pos + 1;
        WHILE v_null_pos <= v_len AND get_byte(p_content, v_null_pos - 1) != 0 LOOP
            v_null_pos := v_null_pos + 1;
        END LOOP;

        mode := convert_from(substring(p_content FROM v_pos FOR v_space_pos - v_pos), 'UTF8');
        name := convert_from(substring(p_content FROM v_space_pos + 1 FOR v_null_pos - v_space_pos - 1), 'UTF8');
        entry_oid := substring(p_content FROM v_null_pos + 1 FOR 20);

        RETURN NEXT;

        v_pos := v_null_pos + 21;
    END LOOP;
END;
$$;

CREATE FUNCTION git_ls_tree_r(
    p_repo_id integer,
    p_tree_oid bytea,
    p_prefix text DEFAULT ''
)
RETURNS TABLE(mode text, path text, oid bytea, obj_type text)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_content bytea;
    v_entry record;
BEGIN
    SELECT o.content INTO v_content
    FROM objects o
    WHERE o.repo_id = p_repo_id AND o.oid = p_tree_oid AND o.type = 2;

    IF v_content IS NULL THEN
        RETURN;
    END IF;

    FOR v_entry IN SELECT e.mode, e.name, e.entry_oid FROM git_tree_entries(v_content) e LOOP
        IF v_entry.mode = '40000' THEN
            path := p_prefix || v_entry.name || '/';
            mode := v_entry.mode;
            oid := v_entry.entry_oid;
            obj_type := 'tree';
            RETURN NEXT;

            RETURN QUERY SELECT * FROM git_ls_tree_r(p_repo_id, v_entry.entry_oid, p_prefix || v_entry.name || '/');
        ELSE
            path := p_prefix || v_entry.name;
            mode := v_entry.mode;
            oid := v_entry.entry_oid;
            obj_type := 'blob';
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;

-- ============================================================
-- Functions: commit parsing
-- ============================================================

CREATE FUNCTION git_commit_parse(p_content bytea)
RETURNS TABLE(
    tree_oid bytea,
    parent_oids bytea[],
    author_name text,
    author_email text,
    author_timestamp bigint,
    author_tz text,
    committer_name text,
    committer_email text,
    committer_timestamp bigint,
    committer_tz text,
    message text
)
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    v_text text;
    v_lines text[];
    v_line text;
    v_header_end integer;
    v_parents bytea[] := '{}';
    v_i integer;
    v_parts text[];
    v_ident text;
BEGIN
    v_text := convert_from(p_content, 'UTF8');

    v_header_end := position(E'\n\n' IN v_text);
    IF v_header_end = 0 THEN
        message := '';
        v_lines := string_to_array(v_text, E'\n');
    ELSE
        message := substring(v_text FROM v_header_end + 2);
        v_lines := string_to_array(substring(v_text FROM 1 FOR v_header_end - 1), E'\n');
    END IF;

    FOR v_i IN 1..array_length(v_lines, 1) LOOP
        v_line := v_lines[v_i];

        IF v_line LIKE 'tree %' THEN
            tree_oid := decode(substring(v_line FROM 6), 'hex');

        ELSIF v_line LIKE 'parent %' THEN
            v_parents := v_parents || decode(substring(v_line FROM 8), 'hex');

        ELSIF v_line LIKE 'author %' THEN
            v_ident := substring(v_line FROM 8);
            author_email := substring(v_ident FROM '<([^>]+)>');
            author_name := trim(substring(v_ident FROM 1 FOR position('<' IN v_ident) - 1));
            v_parts := regexp_matches(v_ident, '> (\d+) ([+-]\d{4})$');
            author_timestamp := v_parts[1]::bigint;
            author_tz := v_parts[2];

        ELSIF v_line LIKE 'committer %' THEN
            v_ident := substring(v_line FROM 11);
            committer_email := substring(v_ident FROM '<([^>]+)>');
            committer_name := trim(substring(v_ident FROM 1 FOR position('<' IN v_ident) - 1));
            v_parts := regexp_matches(v_ident, '> (\d+) ([+-]\d{4})$');
            committer_timestamp := v_parts[1]::bigint;
            committer_tz := v_parts[2];
        END IF;
    END LOOP;

    parent_oids := v_parents;
    RETURN NEXT;
END;
$$;

-- ============================================================
-- Functions: ref management
-- ============================================================

CREATE FUNCTION git_ref_update(
    p_repo_id integer,
    p_name text,
    p_new_oid bytea,
    p_old_oid bytea DEFAULT NULL,
    p_force boolean DEFAULT false
)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    v_current_oid bytea;
BEGIN
    SELECT oid INTO v_current_oid
    FROM refs
    WHERE repo_id = p_repo_id AND name = p_name
    FOR UPDATE;

    IF NOT FOUND THEN
        IF p_old_oid IS NOT NULL AND p_old_oid != '\x0000000000000000000000000000000000000000'::bytea THEN
            RETURN false;
        END IF;

        INSERT INTO refs (repo_id, name, oid)
        VALUES (p_repo_id, p_name, p_new_oid);
        RETURN true;
    END IF;

    IF NOT p_force AND p_old_oid IS NOT NULL
       AND p_old_oid != '\x0000000000000000000000000000000000000000'::bytea
       AND v_current_oid != p_old_oid THEN
        RETURN false;
    END IF;

    IF p_new_oid IS NULL OR p_new_oid = '\x0000000000000000000000000000000000000000'::bytea THEN
        DELETE FROM refs WHERE repo_id = p_repo_id AND name = p_name;
    ELSE
        UPDATE refs SET oid = p_new_oid, symbolic = NULL
        WHERE repo_id = p_repo_id AND name = p_name;
    END IF;

    RETURN true;
END;
$$;

CREATE FUNCTION git_ref_set_symbolic(
    p_repo_id integer,
    p_name text,
    p_target text
)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO refs (repo_id, name, symbolic)
    VALUES (p_repo_id, p_name, p_target)
    ON CONFLICT (repo_id, name) DO UPDATE
    SET oid = NULL, symbolic = p_target;
END;
$$;

-- ============================================================
-- Views
-- ============================================================

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
