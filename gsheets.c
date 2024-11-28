#include "postgres.h"

#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/http_helpers.h"
#include "utils/jsonb.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"
#include "funcapi.h"

#define BASE_URL "https://sheets.googleapis.com/v4/spreadsheets"
#define SHEET_URL(id, range) psprintf("%s/%s/values/%s", BASE_URL, id, range)
#define METADATA_URL(id) psprintf("%s/%s", BASE_URL, id)
#define TYPEINFER_FIELDS "sheets(data(rowData(values(userEnteredFormat%2FnumberFormat%2CuserEnteredValue))%2CstartColumn%2CstartRow))"

typedef struct write_state {
    int tcount;
    int count;
    char *sheet_name;
    char *spreadsheet_id;
    StringInfoData buff;
} write_state;

static char *access_token = NULL;
static bool enable_infer_types = false;

static bool validate_url(const char *url);
static char *extract_id(const char* url);
static List *infer_types(const char *id, const char *sheet, bool has_header);

static void initialize_buffer(StringInfoData *buff);
static void close_buffer(StringInfoData *buff);
static void remove_trailing_comma(StringInfoData *buff);

static void create_new_sheet(char **spreadsheet_id, char *spreadsheet_name);
static void write_header(Jsonb *jb, write_state *state);
static char *extract_text_from_jsonb(Jsonb *jb, char *field);
static void write_to_gsheet(write_state *state);

PG_MODULE_MAGIC;

void _PG_init(void);

void _PG_init(void)
{
    DefineCustomStringVariable("gsheets.access_token",
                               "Access token for Google Sheets",
                               NULL,
                               &access_token,
                               "",
                               PGC_USERSET,
                               0,
                               NULL,
                               NULL,
                               NULL);
    DefineCustomBoolVariable("gsheets.enable_infer_types",
                             "Enable dynamic datatype inference",
                             NULL,
                             &enable_infer_types,
                             false,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);
    MarkGUCPrefixReserved("gsheets");
    http_init();
}

void _PG_fini(void);

void _PG_fini(void)
{
    http_cleanup();
}

static char *extract_id(const char *url)
{
    const char *start, *end;
    char *id;
    size_t id_length;

    start = strstr(url, "/d/");
    if (start == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid URL")));

    start += 3;
    end = strchr(start, '/');
    if (end == NULL)
        end = start + strlen(start);

    id_length = end - start;

    if (id_length != 44)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid Sheet id")));
            
    id = (char *)malloc(id_length + 1);
    if (id == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));

    strncpy(id, start, id_length);
    id[id_length] = '\0';

    return id;
}

static List *infer_types(const char *id, const char *sheet, bool has_header)
{
    char *response;
    Jsonb *jsonb;
    JsonbValue v;
    JsonbIterator *it;
    JsonbIteratorToken r;
    Jsonb *values;
    Datum *elems;
    bool is_null = false;
    char *params[] = {
        psprintf("ranges=%s!A%d:Z", sheet, has_header ? 2 : 1),
        "fields=" TYPEINFER_FIELDS
    };
    List *types = NIL;

    response = http_get(METADATA_URL(id), params, 3, NULL);
    jsonb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(response)));

    elems = (Datum *)palloc(7 * sizeof(Datum));
    elems[0] = CStringGetTextDatum("sheets");
    elems[1] = CStringGetTextDatum("0");
    elems[2] = CStringGetTextDatum("data");
    elems[3] = CStringGetTextDatum("0");
    elems[4] = CStringGetTextDatum("rowData");
    elems[5] = CStringGetTextDatum("0");
    elems[6] = CStringGetTextDatum("values");
    values = DatumGetJsonbP(jsonb_get_element(jsonb, elems, 7, &is_null, false));

    it = JsonbIteratorInit(&values->root);
    while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        if (r == WJB_KEY)
        {
            char *key = pnstrdup(v.val.string.val, v.val.string.len);
            if (strcmp(key, "numberValue") == 0)
                types = lappend_oid(types, INT8OID);
            else if (strcmp(key, "boolValue") == 0)
                types = lappend_oid(types, BOOLOID);
            else if (strcmp(key, "stringValue") == 0)
                types = lappend_oid(types, TEXTOID);
        }
        else if (r == WJB_VALUE)
        {
            char *val = pnstrdup(v.val.string.val, v.val.string.len);
            if (strcmp(val, "DATE") == 0)
            {
                // remove the last element
                types = list_truncate(types, list_length(types) - 1);
                types = lappend_oid(types, DATEOID);
            }
        }
    }

    return types;
}

static bool validate_url(const char *url)
{
    if (strstr(url, "https://docs.google.com/spreadsheets/") == NULL)
        return false;
    return true;
}

static void remove_trailing_comma(StringInfoData *buff)
{
    int len = buff->len;

    if (len > 0 && buff->data[len - 1] == ',')
    {
        buff->data[len - 1] = '\0';
        buff->len--;
    }
}

static void initialize_buffer(StringInfoData *buff)
{
    initStringInfo(buff);
    appendStringInfoChar(buff, '{');
    appendStringInfoString(buff, "\"values\": [");
}

static void close_buffer(StringInfoData *buff)
{
    remove_trailing_comma(buff);
    appendStringInfoChar(buff, ']');
    appendStringInfoChar(buff, '}');
}

static void write_to_gsheet(write_state *state)
{
    int start_range;
    struct curl_slist *headers = NULL;
    const char *params[] = {
        "valueInputOption=USER_ENTERED"
    };
    char *response;

    headers = add_header(headers, "Authorization", psprintf("Bearer %s", access_token));
    headers = add_header(headers, "Content-Type", "application/json");

    start_range = state->tcount - state->count + 1;
    response = http_put(SHEET_URL(state->spreadsheet_id, psprintf("%s!A%d", state->sheet_name, start_range)),
                        state->buff.data, params, 1, headers);
    
    free(headers);
    free(response);
}

static char *extract_text_from_jsonb(Jsonb *jb, char *field)
{
    JsonbValue *v;

    if (!JB_ROOT_IS_OBJECT(jb))
		return NULL;

	v = getKeyJsonValueFromContainer(&jb->root,
									 field,
                                     strlen(field),
									 NULL);

    if (v == NULL)
        return NULL;
    else if (v->type == jbvString && v->val.string.len > 0)
        return pnstrdup(v->val.string.val, v->val.string.len);
    else if (v->type == jbvBinary)
        return JsonbToCString(NULL, v->val.binary.data, v->val.binary.len);
    else
        return NULL;
}

static void write_header(Jsonb *jb, write_state *state)
{
    StringInfoData *buff = &state->buff;
    JsonbValue *v;
    
    if (!JB_ROOT_IS_OBJECT(jb))
        return;
    
    v = getKeyJsonValueFromContainer(&jb->root, "header", 6, NULL);
    if (v == NULL)
        return;

    if (v->type == jbvBinary)
        appendStringInfoString(buff, JsonbToCString(NULL, v->val.binary.data, v->val.binary.len));
    else
    {
        appendStringInfoChar(buff, '[');
        for (int i = 0; i < v->val.array.nElems; i++)
        {
            JsonbValue *elem = &v->val.array.elems[i];

            switch (elem->type)
            {
                case jbvString:
                    appendStringInfo(buff, "\"%s\"", pnstrdup(elem->val.string.val, elem->val.string.len));
                    break;
                case jbvNumeric:
                    {
                        char *numstr = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(elem->val.numeric)));
                        appendStringInfoString(buff, numstr);
                        pfree(numstr);
                    }
                    break;
                case jbvBool:
                    appendStringInfoString(buff, elem->val.boolean ? "true" : "false");
                    break;
                case jbvNull:
                    appendStringInfoString(buff, "");
                    break;
                default:
                    break;
            }

            appendStringInfoChar(buff, ',');
        }
        remove_trailing_comma(buff);
        appendStringInfoChar(buff, ']');
    }
    appendStringInfoChar(buff, ',');

    state->count++;
    state->tcount++;
}

static void create_new_sheet(char **spreadsheet_id, char *spreadsheet_name)
{
    char *response;
    Jsonb *jsonb;
    JsonbValue v;
    JsonbIterator *it;
    JsonbIteratorToken r;
    struct curl_slist *headers = NULL;

    headers = add_header(headers, "Authorization", psprintf("Bearer %s", access_token));
    headers = add_header(headers, "Content-Type", "application/json");
    if (spreadsheet_name == NULL)
    {
        time_t t = time(NULL);
        struct tm tm = *localtime(&t);
        spreadsheet_name = psprintf("New Spreadsheet [%d-%d-%d %d:%d:%d]", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    }

    response = http_post(
        BASE_URL, 
        psprintf(
            "{\"properties\": {\"title\": \"%s\"}, \"sheets\": [{\"properties\": {\"title\": \"Sheet1\", \"gridProperties\": {\"rowCount\": 100000, \"columnCount\": 26}}}]}", 
            spreadsheet_name
        ), 
        NULL, 
        0, 
        headers
    );
    jsonb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(response)));

    it = JsonbIteratorInit(&jsonb->root);
    while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        if (r == WJB_KEY)
        {
            char *key = pnstrdup(v.val.string.val, v.val.string.len);
            if (strcmp(key, "spreadsheetId") == 0)
            {
                // Next WJB_VALUE token should be the sheet id
                r = JsonbIteratorNext(&it, &v, false);
                if (r == WJB_VALUE)
                {
                    *spreadsheet_id = pnstrdup(v.val.string.val, v.val.string.len);
                    return;
                }
            }
        }
    }

    free(response);
}
PG_FUNCTION_INFO_V1(gsheets_auth);
Datum gsheets_auth(PG_FUNCTION_ARGS)
{
    const char *client_id = "184409999197-366opgvplluh0bura1n0holvtmvu9i44.apps.googleusercontent.com";
    const char *redirect_uri = "https://auth.pg-gsheets.com";
    const char *auth_url = "https://accounts.google.com/o/oauth2/v2/auth";
    char *full_url = psprintf("%s?client_id=%s&redirect_uri=%s&response_type=token&scope=https://www.googleapis.com/auth/spreadsheets",
                              auth_url, client_id, redirect_uri);
    char *command = palloc0(1024);

    elog(INFO, "Visit the following URL to authenticate: %s", full_url);

    #ifdef __linux__
        command = psprintf("xdg-open \"%s\"", full_url);
    #elif defined(__APPLE__) && defined(__MACH__)
        command = psprintf("open \"%s\"", full_url);
    #elif defined(_WIN32) || defined(_WIN64)
        command = psprintf("start \"%s\"", full_url);
    #endif

    // Open the URL in the default browser
    if (system(command)) {};

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(read_sheet);
Datum read_sheet(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    char *id;
    bool header = PG_GETARG_BOOL(2);
    char *sheet;
    char *response;
    Jsonb *jsonb;
    JsonbValue v;
    JsonbIterator *it;
    JsonbIteratorToken r;
    int nElems = 0;
    bool is_row = false;
    int col = 0;
    Datum *values;
    bool *nulls;
    List *types = NIL;
    struct curl_slist *headers = NULL;

    if (access_token != NULL && strlen(access_token) != 0)
        headers = add_header(headers, "Authorization", psprintf("Bearer %s", access_token));
    else
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Access token is required"),
                 errhint("Set gsheets.access_token")));
    
    if (!PG_ARGISNULL(0))
    {
        char *url = text_to_cstring(PG_GETARG_TEXT_P(0));
        if (validate_url(url))
            id = extract_id(url);
        else if (strlen(url) == 44)
            id = url;
        else
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid URL or sheet id")));
    }
    else
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("URL or sheet id is required")));

    if (PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Sheet name is required")));
    sheet = text_to_cstring(PG_GETARG_TEXT_P(1));

    response = http_get(SHEET_URL(id, header ? sheet : psprintf("%s!A2:Z", sheet)),
                        NULL, 0, headers);
    jsonb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(response)));

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errmsg("SRF called in non-SRF context")));
    if (rsinfo->allowedModes & SFRM_Materialize)
        rsinfo->returnMode = SFRM_Materialize;
    else
        ereport(ERROR, (errmsg("Materialize mode required")));

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = NULL;

    it = JsonbIteratorInit(&jsonb->root);
    while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        if (r == WJB_BEGIN_ARRAY)
        {
            if (!is_row)
                is_row = true;
            else
                nElems = v.val.array.nElems;

            // Set up tuple descriptor once we know the column count
            if (rsinfo->setDesc == NULL && nElems > 0) {
                tupdesc = CreateTemplateTupleDesc(nElems);
                for (int i = 0; i < nElems; i++) {
                    if (enable_infer_types)
                    {
                        Oid typid;

                        types = infer_types(id, sheet, header);
                        typid = list_nth_oid(types, i);
                        TupleDescInitEntry(tupdesc, i + 1, NULL, typid, -1, 0);
                    }
                    else
                        TupleDescInitEntry(tupdesc, i + 1, NULL, TEXTOID, -1, 0);
                }
                BlessTupleDesc(tupdesc);
                rsinfo->setDesc = tupdesc;
            }
        }
        else if (r == WJB_ELEM)
        {
            char *val;
            Oid typid;
            if (col == 0)
            {
                values = (Datum *)palloc(nElems * sizeof(Datum));
                nulls = (bool *)palloc(nElems * sizeof(bool));
            }

            val = pnstrdup(v.val.string.val, v.val.string.len);
            if (enable_infer_types)
            {
                typid = list_nth_oid(types, col);
                if (typid == INT8OID)
                    values[col] = DirectFunctionCall1(int4in, CStringGetDatum(val));
                else if (typid == BOOLOID)
                    values[col] = DirectFunctionCall1(boolin, CStringGetDatum(val));
                else if (typid == DATEOID)
                    values[col] = DirectFunctionCall1(date_in, CStringGetDatum(val));
                else
                    values[col] = CStringGetTextDatum(val);
            }
            else
                values[col] = CStringGetTextDatum(val);

            nulls[col] = false;
            col++;

            if (col == nElems)
            {
                tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
                col = 0;
            }
        }
    }

    MemoryContextSwitchTo(oldcontext);
    free(response);
    PG_RETURN_VOID();
}

/*
 * Transition function to build write data
 * 
 * Arguments (1+2): 
 * - state: the state of the aggregation
 * - data: the record/field to write
 * - options: jsonb object with options
 *      - spreadsheet_id: the id of the spreadsheet
 *      - spreadsheet_name: the name of the spreadsheet
 *      - sheet_name: the name of the sheet
 *      - header: whether to include a header
 *      - will be adding more options in the future
 */
PG_FUNCTION_INFO_V1(write_sheet_transition);
Datum write_sheet_transition(PG_FUNCTION_ARGS)
{
    write_state *state;
    MemoryContext old_mcxt;
    int nargs;
    Datum *args;
    bool *nulls;
    Oid *types;

    nargs = extract_variadic_args(fcinfo, 1, true, &args, &types, &nulls);

    // Initialize the state if it's the first call
    if (PG_ARGISNULL(0))
    {
        char *spreadsheet_name = NULL;

        if (nargs < 1 || nargs > 2)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid number of arguments")));
        
        if (nargs == 2 && (types[1] != JSONBOID || nulls[1]))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Options must be a JSONB object")));

        /* Switch to the memory context of calling function */
        old_mcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

        state = (write_state *) palloc0(sizeof(write_state));
        state->tcount = 0;
        state->count = 0;
        state->spreadsheet_id = NULL;
        state->sheet_name = NULL;

        if (nargs == 2)
        {
            state->spreadsheet_id = extract_text_from_jsonb(DatumGetJsonbP(args[1]), "spreadsheet_id");
            state->sheet_name = extract_text_from_jsonb(DatumGetJsonbP(args[1]), "sheet_name");
            spreadsheet_name = extract_text_from_jsonb(DatumGetJsonbP(args[1]), "spreadsheet_name");
        }
        
        if (state->sheet_name == NULL)
            state->sheet_name = "Sheet1";

        /* Create a new spreadsheet if user does not provide existing spreadsheet id */
        if (state->spreadsheet_id == NULL)
            create_new_sheet(&state->spreadsheet_id, spreadsheet_name);
        else if (state->spreadsheet_id != NULL && strlen(state->spreadsheet_id) != 44)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid sheet id")));

        initialize_buffer(&state->buff);

        // Write the header if available
        if (nargs == 2)
            write_header(DatumGetJsonbP(args[1]), state);

        MemoryContextSwitchTo(old_mcxt);
    }
    else
        /* Get the state from the previous call */
        state = (write_state *) PG_GETARG_POINTER(0);

    appendStringInfoChar(&state->buff, '[');

    if (type_is_rowtype(types[0]))
    {
        Oid			tupType;
        int32		tupTypmod;
        TupleDesc	tupdesc;
        HeapTupleHeader rec;
        HeapTupleData tuple;
        Datum *values;
        bool *_nulls;
        
        rec = DatumGetHeapTupleHeader(args[0]);
        /*
         * Extract type info from the tuple itself -- this will work even for
         * anonymous record types.
         */
        tupType = HeapTupleHeaderGetTypeId(rec);
        tupTypmod = HeapTupleHeaderGetTypMod(rec);

        tupdesc = lookup_rowtype_tupdesc_domain(tupType, tupTypmod, false);

        if (rec)
        {
            /* Build a temporary HeapTuple control structure */
            tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
            ItemPointerSetInvalid(&(tuple.t_self));
            tuple.t_tableOid = InvalidOid;
            tuple.t_data = rec;

            values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
            _nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));

            /* Break down the tuple into fields */
            heap_deform_tuple(&tuple, tupdesc, values, _nulls);

            for (int i = 0; i < tupdesc->natts; i++)
            {
                char *val;
                if (_nulls[i])
                    val = "";
                else
                {
                    Oid typoutput;
                    bool typIsVarlena;
                    getTypeOutputInfo(tupdesc->attrs[i].atttypid, &typoutput, &typIsVarlena);
                    val = OidOutputFunctionCall(typoutput, values[i]);
                }

                if (i < tupdesc->natts - 1)
                    appendStringInfo(&state->buff, "\"%s\",", val);
                else
                    appendStringInfo(&state->buff, "\"%s\"", val);
            }
        }
        else
        {
            values = NULL;
            _nulls = NULL;
        }
        ReleaseTupleDesc(tupdesc);
    }
    else
    {
        char *val;
        if (nulls[0])
            val = "";
        else
        {
            Oid typoutput;
            bool typIsVarlena;

            getTypeOutputInfo(types[0], &typoutput, &typIsVarlena);
            val = OidOutputFunctionCall(typoutput, args[0]);
        }
        appendStringInfo(&state->buff, "\"%s\"", val);
    }

    appendStringInfoChar(&state->buff, ']');

    // Increment the count
    state->tcount++;
    state->count++;

    if (state->count >= 2000)
    {
        close_buffer(&state->buff);
        write_to_gsheet(state);

        /* Swicth to the appropriate memory context */
        old_mcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

        state->count = 0;
        resetStringInfo(&state->buff);
        initialize_buffer(&state->buff);

        MemoryContextSwitchTo(old_mcxt);
    }
    else
        appendStringInfoChar(&state->buff, ',');

    PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(write_sheet_final);
Datum write_sheet_final(PG_FUNCTION_ARGS)
{
    write_state *state = (write_state *) PG_GETARG_POINTER(0);

    close_buffer(&state->buff);
    write_to_gsheet(state);

    elog(INFO, "%d rows written at %s", state->tcount,
         psprintf("https://docs.google.com/spreadsheets/d/%s", state->spreadsheet_id));

    /* cleanup */
    resetStringInfo(&state->buff);
    pfree(state->buff.data);
    pfree(state);

    PG_RETURN_VOID();
}