-- Compute the git object hash: SHA1("<type> <size>\0<content>")
-- Type names: 1=commit, 2=tree, 3=blob, 4=tag
CREATE OR REPLACE FUNCTION git_type_name(obj_type smallint)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE obj_type
        WHEN 1 THEN 'commit'
        WHEN 2 THEN 'tree'
        WHEN 3 THEN 'blob'
        WHEN 4 THEN 'tag'
    END;
$$;

CREATE OR REPLACE FUNCTION git_object_hash(obj_type smallint, content bytea)
RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT digest(
        convert_to(git_type_name(obj_type) || ' ' || octet_length(content)::text, 'UTF8')
        || '\x00'::bytea
        || content,
        'sha1'
    );
$$;
