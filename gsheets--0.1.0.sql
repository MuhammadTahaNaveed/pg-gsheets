-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gsheets" to load this file. \quit

CREATE FUNCTION gsheets_auth()
RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME';

CREATE FUNCTION read_sheet(link text, sheet_name text DEFAULT 'Sheet1', header boolean DEFAULT true)
RETURNS SETOF record
LANGUAGE c
AS 'MODULE_PATHNAME';

CREATE FUNCTION write_sheet_transition(internal, VARIADIC "any")
RETURNS internal
LANGUAGE c
AS 'MODULE_PATHNAME';

CREATE FUNCTION write_sheet_final(internal)
RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME';

CREATE AGGREGATE write_sheet(VARIADIC "any") (
    sfunc = write_sheet_transition,
    stype = internal,
    finalfunc = write_sheet_final
);