-- Parse binary tree object content into rows of (mode, name, entry_oid)
-- Tree format: repeated entries of "<mode> <name>\0<20-byte-oid>"
CREATE OR REPLACE FUNCTION git_tree_entries(p_content bytea)
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
        -- Find the space between mode and name
        v_space_pos := v_pos;
        WHILE v_space_pos <= v_len AND get_byte(p_content, v_space_pos - 1) != 32 LOOP
            v_space_pos := v_space_pos + 1;
        END LOOP;

        -- Find the null byte after the name
        v_null_pos := v_space_pos + 1;
        WHILE v_null_pos <= v_len AND get_byte(p_content, v_null_pos - 1) != 0 LOOP
            v_null_pos := v_null_pos + 1;
        END LOOP;

        -- Extract fields
        mode := convert_from(substring(p_content FROM v_pos FOR v_space_pos - v_pos), 'UTF8');
        name := convert_from(substring(p_content FROM v_space_pos + 1 FOR v_null_pos - v_space_pos - 1), 'UTF8');
        entry_oid := substring(p_content FROM v_null_pos + 1 FOR 20);

        RETURN NEXT;

        v_pos := v_null_pos + 21;
    END LOOP;
END;
$$;

-- Recursive tree walk
CREATE OR REPLACE FUNCTION git_ls_tree_r(
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
            -- Directory: recurse
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
