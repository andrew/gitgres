#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/builtins.h"

#include <string.h>

PG_FUNCTION_INFO_V1(git_tree_entries_c);

typedef struct {
    char *data;
    int   len;
    int   pos;
} TreeParseState;

/*
 * git_tree_entries_c(content bytea)
 *   RETURNS TABLE(mode text, name text, entry_oid bytea)
 *
 * Parses git tree object binary format. Each entry is:
 *   <mode_ascii_digits> <name>\0<20_byte_sha1>
 */
Datum
git_tree_entries_c(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    TreeParseState  *state;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext  oldcontext;
        bytea         *content;
        TupleDesc      tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        content = PG_GETARG_BYTEA_PP(0);

        state = (TreeParseState *) palloc(sizeof(TreeParseState));
        state->len = VARSIZE_ANY_EXHDR(content);
        state->data = (char *) palloc(state->len);
        memcpy(state->data, VARDATA_ANY(content), state->len);
        state->pos = 0;

        funcctx->user_fctx = state;

        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("return type must be a row type")));

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    state = (TreeParseState *) funcctx->user_fctx;

    if (state->pos < state->len)
    {
        int        space_pos;
        int        null_pos;
        int        mode_len;
        int        name_len;
        char      *mode;
        char      *name;
        bytea     *oid;
        Datum      values[3];
        bool       nulls[3] = { false, false, false };
        HeapTuple  tuple;

        /* Find the space between mode and name */
        space_pos = state->pos;
        while (space_pos < state->len && state->data[space_pos] != ' ')
            space_pos++;

        if (space_pos >= state->len)
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("malformed tree entry: no space found")));

        /* Find the null terminator after the name */
        null_pos = space_pos + 1;
        while (null_pos < state->len && state->data[null_pos] != '\0')
            null_pos++;

        if (null_pos >= state->len)
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("malformed tree entry: no null terminator found")));

        /* Need at least 20 bytes after the null for the OID */
        if (null_pos + 20 >= state->len && null_pos + 21 > state->len)
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("malformed tree entry: truncated OID")));

        /* Extract mode string */
        mode_len = space_pos - state->pos;
        mode = (char *) palloc(mode_len + 1);
        memcpy(mode, state->data + state->pos, mode_len);
        mode[mode_len] = '\0';

        /* Extract name string */
        name_len = null_pos - space_pos - 1;
        name = (char *) palloc(name_len + 1);
        memcpy(name, state->data + space_pos + 1, name_len);
        name[name_len] = '\0';

        /* Extract 20-byte OID as bytea */
        oid = (bytea *) palloc(VARHDRSZ + 20);
        SET_VARSIZE(oid, VARHDRSZ + 20);
        memcpy(VARDATA(oid), state->data + null_pos + 1, 20);

        /* Advance past this entry */
        state->pos = null_pos + 21;

        /* Build the result tuple */
        values[0] = CStringGetTextDatum(mode);
        values[1] = CStringGetTextDatum(name);
        values[2] = PointerGetDatum(oid);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
