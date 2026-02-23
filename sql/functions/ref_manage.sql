-- Compare-and-swap ref update with optional force
CREATE OR REPLACE FUNCTION git_ref_update(
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
    -- Lock the row if it exists
    SELECT oid INTO v_current_oid
    FROM refs
    WHERE repo_id = p_repo_id AND name = p_name
    FOR UPDATE;

    IF NOT FOUND THEN
        -- Creating new ref
        IF p_old_oid IS NOT NULL AND p_old_oid != '\x0000000000000000000000000000000000000000'::bytea THEN
            -- Caller expected an existing ref
            RETURN false;
        END IF;

        INSERT INTO refs (repo_id, name, oid)
        VALUES (p_repo_id, p_name, p_new_oid);
        RETURN true;
    END IF;

    -- Ref exists: check CAS unless forced
    IF NOT p_force AND p_old_oid IS NOT NULL
       AND p_old_oid != '\x0000000000000000000000000000000000000000'::bytea
       AND v_current_oid != p_old_oid THEN
        RETURN false;
    END IF;

    IF p_new_oid IS NULL OR p_new_oid = '\x0000000000000000000000000000000000000000'::bytea THEN
        -- Delete the ref
        DELETE FROM refs WHERE repo_id = p_repo_id AND name = p_name;
    ELSE
        UPDATE refs SET oid = p_new_oid, symbolic = NULL
        WHERE repo_id = p_repo_id AND name = p_name;
    END IF;

    RETURN true;
END;
$$;

-- Set a symbolic ref
CREATE OR REPLACE FUNCTION git_ref_set_symbolic(
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
