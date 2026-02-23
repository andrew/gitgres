-- Write a git object: compute hash, insert, return OID
CREATE OR REPLACE FUNCTION git_object_write(
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

-- Read a git object by exact OID
CREATE OR REPLACE FUNCTION git_object_read(
    p_repo_id integer,
    p_oid bytea
)
RETURNS TABLE(type smallint, size integer, content bytea)
LANGUAGE sql STABLE STRICT AS $$
    SELECT o.type, o.size, o.content
    FROM objects o
    WHERE o.repo_id = p_repo_id AND o.oid = p_oid;
$$;

-- Read a git object by OID prefix (abbreviated OID)
-- Returns error if ambiguous (more than one match)
CREATE OR REPLACE FUNCTION git_object_read_prefix(
    p_repo_id integer,
    p_prefix bytea,
    p_prefix_len integer
)
RETURNS TABLE(oid bytea, type smallint, size integer, content bytea)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_count integer;
    v_mask bytea;
    v_prefix_bytes integer;
BEGIN
    -- Full bytes to compare
    v_prefix_bytes := p_prefix_len / 2;

    -- Check objects whose OID starts with the given prefix bytes
    RETURN QUERY
    SELECT o.oid, o.type, o.size, o.content
    FROM objects o
    WHERE o.repo_id = p_repo_id
      AND substring(o.oid FROM 1 FOR v_prefix_bytes) = substring(p_prefix FROM 1 FOR v_prefix_bytes);

    RETURN;
END;
$$;
