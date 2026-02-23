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
