#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: gitgres-import <repo-path> <conninfo> <repo-name>"
    echo
    echo "Import a git repository into the gitgres database."
    echo
    echo "Arguments:"
    echo "  repo-path   Path to the git repository"
    echo "  conninfo    PostgreSQL connection string (e.g., 'dbname=gitgres')"
    echo "  repo-name   Name for the repository in the database"
    exit 1
}

[ $# -eq 3 ] || usage

REPO_PATH="$1"
CONNINFO="$2"
REPO_NAME="$3"

if [ ! -d "$REPO_PATH/.git" ] && [ ! -f "$REPO_PATH/HEAD" ]; then
    echo "Error: $REPO_PATH does not look like a git repository" >&2
    exit 1
fi

GIT="git -C $REPO_PATH"

# Create or get the repository ID
REPO_ID=$(psql "$CONNINFO" -t -A -c "
    INSERT INTO repositories (name) VALUES ('$REPO_NAME')
    ON CONFLICT (name) DO UPDATE SET name = '$REPO_NAME'
    RETURNING id;
")

echo "Repository ID: $REPO_ID"

# Count objects
OBJECT_COUNT=$($GIT rev-list --objects --all | wc -l | tr -d ' ')
echo "Objects to import: $OBJECT_COUNT"

# Import objects using git cat-file --batch and psql COPY
# Format: each object is read via cat-file, then inserted
echo "Importing objects..."

IMPORTED=0
SKIPPED=0

$GIT rev-list --objects --all | cut -d' ' -f1 | sort -u | while read OID; do
    # Get object info
    INFO=$($GIT cat-file -t "$OID")
    SIZE=$($GIT cat-file -s "$OID")

    case "$INFO" in
        commit) TYPE=1 ;;
        tree)   TYPE=2 ;;
        blob)   TYPE=3 ;;
        tag)    TYPE=4 ;;
        *)      echo "Unknown type: $INFO for $OID" >&2; continue ;;
    esac

    # Get raw content and insert
    # Use base64 encoding to safely transport binary data
    CONTENT_B64=$($GIT cat-file "$INFO" "$OID" | base64)
    OID_BIN="\\\\x$OID"

    psql "$CONNINFO" -q -c "
        INSERT INTO objects (repo_id, oid, type, size, content)
        VALUES (
            $REPO_ID,
            decode('$OID', 'hex'),
            $TYPE,
            $SIZE,
            decode('$CONTENT_B64', 'base64')
        )
        ON CONFLICT (repo_id, oid) DO NOTHING;
    " 2>/dev/null

    IMPORTED=$((IMPORTED + 1))
    if [ $((IMPORTED % 100)) -eq 0 ]; then
        echo "  $IMPORTED / $OBJECT_COUNT objects..."
    fi
done

echo "Objects imported."

# Import refs
echo "Importing refs..."

$GIT show-ref | while read OID REFNAME; do
    psql "$CONNINFO" -q -c "
        INSERT INTO refs (repo_id, name, oid)
        VALUES ($REPO_ID, '$REFNAME', decode('$OID', 'hex'))
        ON CONFLICT (repo_id, name) DO UPDATE SET oid = decode('$OID', 'hex'), symbolic = NULL;
    "
    echo "  $REFNAME -> $OID"
done

# Import HEAD
HEAD_TARGET=$($GIT symbolic-ref HEAD 2>/dev/null || true)
if [ -n "$HEAD_TARGET" ]; then
    psql "$CONNINFO" -q -c "
        INSERT INTO refs (repo_id, name, symbolic)
        VALUES ($REPO_ID, 'HEAD', '$HEAD_TARGET')
        ON CONFLICT (repo_id, name) DO UPDATE SET oid = NULL, symbolic = '$HEAD_TARGET';
    "
    echo "  HEAD -> $HEAD_TARGET (symbolic)"
else
    HEAD_OID=$($GIT rev-parse HEAD)
    psql "$CONNINFO" -q -c "
        INSERT INTO refs (repo_id, name, oid)
        VALUES ($REPO_ID, 'HEAD', decode('$HEAD_OID', 'hex'))
        ON CONFLICT (repo_id, name) DO UPDATE SET oid = decode('$HEAD_OID', 'hex'), symbolic = NULL;
    "
    echo "  HEAD -> $HEAD_OID"
fi

# Verify
DB_COUNT=$(psql "$CONNINFO" -t -A -c "SELECT count(*) FROM objects WHERE repo_id = $REPO_ID")
REF_COUNT=$(psql "$CONNINFO" -t -A -c "SELECT count(*) FROM refs WHERE repo_id = $REPO_ID")

echo
echo "Import complete."
echo "  Objects in database: $DB_COUNT"
echo "  Refs in database: $REF_COUNT"
