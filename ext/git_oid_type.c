#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "access/hash.h"

#include <string.h>

PG_FUNCTION_INFO_V1(git_oid_in);
PG_FUNCTION_INFO_V1(git_oid_out);
PG_FUNCTION_INFO_V1(git_oid_eq);
PG_FUNCTION_INFO_V1(git_oid_ne);
PG_FUNCTION_INFO_V1(git_oid_lt);
PG_FUNCTION_INFO_V1(git_oid_le);
PG_FUNCTION_INFO_V1(git_oid_gt);
PG_FUNCTION_INFO_V1(git_oid_ge);
PG_FUNCTION_INFO_V1(git_oid_cmp);
PG_FUNCTION_INFO_V1(git_oid_hash);

/* git_oid is stored as a fixed 20-byte pass-by-reference type */
typedef struct {
    char data[20];
} GitOid;

/* Helper to convert hex char to nibble */
static int
hex_to_nibble(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static char
nibble_to_hex(int n)
{
    return "0123456789abcdef"[n & 0xf];
}

Datum
git_oid_in(PG_FUNCTION_ARGS)
{
    char   *str = PG_GETARG_CSTRING(0);
    GitOid *result;
    int     len = strlen(str);
    int     i;

    if (len != 40)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid git OID: must be exactly 40 hex characters")));

    result = (GitOid *) palloc(sizeof(GitOid));

    for (i = 0; i < 20; i++)
    {
        int hi = hex_to_nibble(str[i * 2]);
        int lo = hex_to_nibble(str[i * 2 + 1]);

        if (hi < 0 || lo < 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid hex character in git OID")));

        result->data[i] = (hi << 4) | lo;
    }

    PG_RETURN_POINTER(result);
}

Datum
git_oid_out(PG_FUNCTION_ARGS)
{
    GitOid *oid = (GitOid *) PG_GETARG_POINTER(0);
    char   *result = palloc(41);
    int     i;

    for (i = 0; i < 20; i++)
    {
        result[i * 2]     = nibble_to_hex((unsigned char) oid->data[i] >> 4);
        result[i * 2 + 1] = nibble_to_hex(oid->data[i] & 0xf);
    }
    result[40] = '\0';

    PG_RETURN_CSTRING(result);
}

Datum
git_oid_eq(PG_FUNCTION_ARGS)
{
    GitOid *a = (GitOid *) PG_GETARG_POINTER(0);
    GitOid *b = (GitOid *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(memcmp(a->data, b->data, 20) == 0);
}

Datum
git_oid_ne(PG_FUNCTION_ARGS)
{
    GitOid *a = (GitOid *) PG_GETARG_POINTER(0);
    GitOid *b = (GitOid *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(memcmp(a->data, b->data, 20) != 0);
}

Datum
git_oid_lt(PG_FUNCTION_ARGS)
{
    GitOid *a = (GitOid *) PG_GETARG_POINTER(0);
    GitOid *b = (GitOid *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(memcmp(a->data, b->data, 20) < 0);
}

Datum
git_oid_le(PG_FUNCTION_ARGS)
{
    GitOid *a = (GitOid *) PG_GETARG_POINTER(0);
    GitOid *b = (GitOid *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(memcmp(a->data, b->data, 20) <= 0);
}

Datum
git_oid_gt(PG_FUNCTION_ARGS)
{
    GitOid *a = (GitOid *) PG_GETARG_POINTER(0);
    GitOid *b = (GitOid *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(memcmp(a->data, b->data, 20) > 0);
}

Datum
git_oid_ge(PG_FUNCTION_ARGS)
{
    GitOid *a = (GitOid *) PG_GETARG_POINTER(0);
    GitOid *b = (GitOid *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(memcmp(a->data, b->data, 20) >= 0);
}

Datum
git_oid_cmp(PG_FUNCTION_ARGS)
{
    GitOid *a = (GitOid *) PG_GETARG_POINTER(0);
    GitOid *b = (GitOid *) PG_GETARG_POINTER(1);

    PG_RETURN_INT32(memcmp(a->data, b->data, 20));
}

Datum
git_oid_hash(PG_FUNCTION_ARGS)
{
    GitOid *oid = (GitOid *) PG_GETARG_POINTER(0);

    PG_RETURN_INT32(hash_any((unsigned char *) oid->data, 20));
}
