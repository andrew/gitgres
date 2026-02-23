-- Parse commit object content into structured fields
CREATE OR REPLACE FUNCTION git_commit_parse(p_content bytea)
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
    v_ident_parts text[];
BEGIN
    v_text := convert_from(p_content, 'UTF8');

    -- Split header from message at double newline
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
            -- "Name <email> timestamp tz"
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
