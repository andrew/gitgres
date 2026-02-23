#include "postgres.h"
#include "varatt.h"
#include "fmgr.h"
#include "utils/builtins.h"

#include <openssl/evp.h>
#include <stdio.h>
#include <string.h>

PG_FUNCTION_INFO_V1(git_object_hash_c);

/*
 * git_object_hash_c(type smallint, content bytea) RETURNS bytea
 *
 * Computes SHA1("<type> <size>\0<content>") matching git's object hashing.
 * Type codes: 1=commit, 2=tree, 3=blob, 4=tag.
 */
Datum
git_object_hash_c(PG_FUNCTION_ARGS)
{
    int16          type = PG_GETARG_INT16(0);
    bytea         *content = PG_GETARG_BYTEA_PP(1);
    const char    *type_name;
    int            content_len;
    char           header[64];
    int            header_len;
    EVP_MD_CTX    *ctx;
    unsigned char  hash[EVP_MAX_MD_SIZE];
    unsigned int   hash_len;
    bytea         *result;

    switch (type)
    {
        case 1: type_name = "commit"; break;
        case 2: type_name = "tree";   break;
        case 3: type_name = "blob";   break;
        case 4: type_name = "tag";    break;
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid git object type: %d", type)));
    }

    content_len = VARSIZE_ANY_EXHDR(content);
    header_len = snprintf(header, sizeof(header), "%s %d", type_name, content_len);
    header_len++; /* include the null terminator byte */

    ctx = EVP_MD_CTX_new();
    if (!ctx)
        ereport(ERROR, (errmsg("could not allocate EVP_MD_CTX")));

    if (!EVP_DigestInit_ex(ctx, EVP_sha1(), NULL) ||
        !EVP_DigestUpdate(ctx, header, header_len) ||
        !EVP_DigestUpdate(ctx, VARDATA_ANY(content), content_len) ||
        !EVP_DigestFinal_ex(ctx, hash, &hash_len))
    {
        EVP_MD_CTX_free(ctx);
        ereport(ERROR, (errmsg("SHA1 computation failed")));
    }

    EVP_MD_CTX_free(ctx);

    result = (bytea *) palloc(VARHDRSZ + 20);
    SET_VARSIZE(result, VARHDRSZ + 20);
    memcpy(VARDATA(result), hash, 20);

    PG_RETURN_BYTEA_P(result);
}
