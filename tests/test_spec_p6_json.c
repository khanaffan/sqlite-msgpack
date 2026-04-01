/*
** test_spec_p6_json.c — Phase 6: JSON conversion
**
** Covers:
**   msgpack_from_json   — all JSON value types, escape sequences, surrogates
**   msgpack_to_json     — all msgpack types → JSON text
**   msgpack_to_jsonb    — alias of to_json
**   msgpack_pretty      — indented output, configurable indent width
**   NaN / Infinity → null
**   Unicode \\u escapes and surrogate pairs
**
** Mix of SQL text literals and sqlite3_bind_text/blob for inputs.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "sqlite3.h"

static int g_pass = 0, g_fail = 0;
#define CHECK(label, expr) \
  do { if (expr) { printf("PASS  %s\n",(label)); g_pass++; } \
       else      { printf("FAIL  %s\n",(label)); g_fail++; } } while(0)

static char *exec1(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = NULL; char *r = NULL;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return NULL;
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}
static sqlite3_int64 exec1i(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -1;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return -1;
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

/* Build msgpack blob from SQL; caller sqlite3_free() */
static unsigned char *build_blob(sqlite3 *db, const char *sql, int *pn){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pn = 0;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return NULL;
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pn = n; }
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_from_json(text) via bind → returns msgpack blob; caller sqlite3_free() */
static unsigned char *from_json_bind(sqlite3 *db, const char *json, int *pn){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pn = 0;
  sqlite3_prepare_v2(db, "SELECT msgpack_from_json(?)", -1, &s, NULL);
  sqlite3_bind_text(s, 1, json, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pn = n; }
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_to_json(blob) via bind → returns text; caller sqlite3_free() */
static char *to_json_bind(sqlite3 *db, const void *blob, int n){
  sqlite3_stmt *s = NULL; char *r = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_to_json(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_pretty(blob [, indent]) via bind; caller sqlite3_free() */
static char *pretty_bind(sqlite3 *db, const void *blob, int n, int indent){
  sqlite3_stmt *s = NULL; char *r = NULL;
  if(indent < 0){
    sqlite3_prepare_v2(db, "SELECT msgpack_pretty(?)", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  } else {
    sqlite3_prepare_v2(db, "SELECT msgpack_pretty(?,?)", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
    sqlite3_bind_int(s, 2, indent);
  }
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* Round-trip helper: JSON → msgpack → JSON; returns text; caller sqlite3_free() */
static char *json_roundtrip(sqlite3 *db, const char *json){
  int n = 0; unsigned char *blob = from_json_bind(db, json, &n);
  if(!blob) return NULL;
  char *back = to_json_bind(db, blob, n);
  sqlite3_free(blob); return back;
}

/* Round-trip via SQL text (for simple cases) */
static char *sql_roundtrip(sqlite3 *db, const char *json){
  char *sql = sqlite3_mprintf(
    "SELECT msgpack_to_json(msgpack_from_json(%Q))", json);
  char *r = exec1(db, sql);
  sqlite3_free(sql); return r;
}

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── from_json / to_json round-trips ────────────────────────────── */

static void test_roundtrip_scalars(sqlite3 *db){
  struct { const char *json; const char *label; } cases[] = {
    { "null",        "1.1  null roundtrip" },
    { "true",        "1.2  true roundtrip (→ integer 1)" },
    { "false",       "1.3  false roundtrip (→ integer 0)" },
    { "0",           "1.4  int 0" },
    { "42",          "1.5  int 42" },
    { "-7",          "1.6  int -7" },
    { "127",         "1.7  int 127 (posfix boundary)" },
    { "128",         "1.8  int 128 (uint8)" },
    { "255",         "1.9  int 255 (uint8 max)" },
    { "256",         "1.10 int 256 (uint16)" },
    { "-32",         "1.11 int -32 (negfix min)" },
    { "-33",         "1.12 int -33 (int8)" },
    { "-128",        "1.13 int -128 (int8 min)" },
    { "\"hello\"",   "1.14 string" },
    { "\"\"",        "1.15 empty string" },
    { "[1,2,3]",     "1.16 array" },
    { "{\"a\":1}",   "1.17 object" },
    { NULL, NULL }
  };
  int i;
  for(i = 0; cases[i].json; i++){
    /* true/false: JSON true → msgpack true (0xc3) → JSON "true" */
    if(strcmp(cases[i].json,"true")==0){
      char *r = json_roundtrip(db, "true");
      CHECK("1.2 true roundtrip → \"true\"", r && strcmp(r,"true")==0); sqlite3_free(r);
      continue;
    }
    if(strcmp(cases[i].json,"false")==0){
      char *r = json_roundtrip(db, "false");
      CHECK("1.3 false roundtrip → \"false\"", r && strcmp(r,"false")==0); sqlite3_free(r);
      continue;
    }
    char *r = json_roundtrip(db, cases[i].json);
    CHECK(cases[i].label, r && strcmp(r, cases[i].json)==0);
    sqlite3_free(r);
  }
}

static void test_roundtrip_containers(sqlite3 *db){
  /* Nested array */
  {
    char *r = json_roundtrip(db, "[1,[2,3],4]");
    CHECK("2.1 nested array roundtrip", r && strcmp(r,"[1,[2,3],4]")==0); sqlite3_free(r);
  }
  /* Nested object */
  {
    char *r = json_roundtrip(db, "{\"a\":{\"b\":1}}");
    CHECK("2.2 nested object roundtrip", r && strcmp(r,"{\"a\":{\"b\":1}}")==0); sqlite3_free(r);
  }
  /* Mixed array — true/false in JSON round-trip through msgpack bool bytes */
  {
    char *r = json_roundtrip(db, "[null,1,\"hi\",[true]]");
    /* true → msgpack 0xc3 (true) → to_json → "true" */
    CHECK("2.3 mixed array roundtrip (true preserves)", r && strcmp(r,"[null,1,\"hi\",[true]]")==0);
    sqlite3_free(r);
  }
  /* Complex nested */
  {
    char *r = json_roundtrip(db,
      "{\"users\":[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]}");
    CHECK("2.4 complex nested roundtrip",
      r && strcmp(r, "{\"users\":[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]}")==0);
    sqlite3_free(r);
  }
}

/* ── escape sequences ───────────────────────────────────────────── */

static void test_escape_sequences(sqlite3 *db){
  /* \n */
  {
    char *r = json_roundtrip(db, "\"line1\\nline2\"");
    CHECK("3.1 \\n roundtrip", r && strcmp(r,"\"line1\\nline2\"")==0); sqlite3_free(r);
  }
  /* \t */
  {
    char *r = json_roundtrip(db, "\"col1\\tcol2\"");
    CHECK("3.2 \\t roundtrip", r && strcmp(r,"\"col1\\tcol2\"")==0); sqlite3_free(r);
  }
  /* \r */
  {
    char *r = json_roundtrip(db, "\"a\\rb\"");
    CHECK("3.3 \\r roundtrip", r && strcmp(r,"\"a\\rb\"")==0); sqlite3_free(r);
  }
  /* \\ */
  {
    char *r = json_roundtrip(db, "\"back\\\\slash\"");
    CHECK("3.4 \\\\ roundtrip", r && strcmp(r,"\"back\\\\slash\"")==0); sqlite3_free(r);
  }
  /* \" */
  {
    char *r = json_roundtrip(db, "\"say \\\"hi\\\"\"");
    CHECK("3.5 \\\" roundtrip", r && strcmp(r,"\"say \\\"hi\\\"\"")==0); sqlite3_free(r);
  }
  /* \/ (JSON allows escaped slash, stores as literal /) */
  {
    int n = 0; unsigned char *blob = from_json_bind(db, "\"\\/path\"", &n);
    char *r = blob ? to_json_bind(db, blob, n) : NULL;
    CHECK("3.6 \\/ decodes to /", r && strcmp(r,"\"/path\"")==0);
    sqlite3_free(r); sqlite3_free(blob);
  }
  /* \u escape: \u0041 = 'A' */
  {
    int n = 0; unsigned char *blob = from_json_bind(db, "\"\\u0041\"", &n);
    char *r = blob ? to_json_bind(db, blob, n) : NULL;
    CHECK("3.7 \\u0041 = 'A'", r && strcmp(r,"\"A\"")==0);
    sqlite3_free(r); sqlite3_free(blob);
  }
  /* \u escape: \u00e9 = 'é' (UTF-8: c3 a9) */
  {
    int n = 0; unsigned char *blob = from_json_bind(db, "\"caf\\u00e9\"", &n);
    char *r = blob ? to_json_bind(db, blob, n) : NULL;
    CHECK("3.8 \\u00e9 = é", r && strcmp(r,"\"caf\xc3\xa9\"")==0);
    sqlite3_free(r); sqlite3_free(blob);
  }
  /* Surrogate pair: \uD83D\uDE00 = U+1F600 😀 (UTF-8: F0 9F 98 80) */
  {
    int n = 0; unsigned char *blob = from_json_bind(db, "\"\\uD83D\\uDE00\"", &n);
    char *r = blob ? to_json_bind(db, blob, n) : NULL;
    CHECK("3.9 surrogate pair U+1F600 😀",
      r && strcmp(r, "\"\xF0\x9F\x98\x80\"")==0);
    sqlite3_free(r); sqlite3_free(blob);
  }
}

/* ── NaN / Infinity → null ──────────────────────────────────────── */

static void test_special_floats(sqlite3 *db){
  /* NaN encodes as msgpack float64 but to_json converts to null */
  {
    double nan_val = 0.0 / 0.0;
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_to_json(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_double(s, 1, nan_val);
    sqlite3_step(s);
    const char *r = (const char*)sqlite3_column_text(s, 0);
    CHECK("4.1 NaN → JSON null (bind)", r && strcmp(r,"null")==0);
    sqlite3_finalize(s);
  }
  /* +Infinity */
  {
    double inf = 1.0 / 0.0;
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_to_json(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_double(s, 1, inf);
    sqlite3_step(s);
    const char *r = (const char*)sqlite3_column_text(s, 0);
    CHECK("4.2 +Inf → JSON null (bind)", r && strcmp(r,"null")==0);
    sqlite3_finalize(s);
  }
  /* -Infinity */
  {
    double ninf = -1.0 / 0.0;
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_to_json(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_double(s, 1, ninf);
    sqlite3_step(s);
    const char *r = (const char*)sqlite3_column_text(s, 0);
    CHECK("4.3 -Inf → JSON null (bind)", r && strcmp(r,"null")==0);
    sqlite3_finalize(s);
  }
  /* float32 stored manually — should decode correctly */
  {
    /* 1.0f in float32 big-endian: CA 3F 80 00 00 */
    unsigned char f32[] = {0xca, 0x3f, 0x80, 0x00, 0x00};
    char *r = to_json_bind(db, f32, 5);
    CHECK("4.4 float32 1.0 → JSON '1.0'", r && strstr(r,"1")!=NULL); sqlite3_free(r);
  }
}

/* ── to_json for all msgpack types ──────────────────────────────── */

static void test_to_json_types(sqlite3 *db){
  /* nil → null */
  {
    unsigned char nil[] = {0xc0};
    char *r = to_json_bind(db, nil, 1);
    CHECK("5.1 nil → 'null'", r && strcmp(r,"null")==0); sqlite3_free(r);
  }
  /* false (0xc2) → "false" */
  {
    unsigned char fal[] = {0xc2};
    char *r = to_json_bind(db, fal, 1);
    CHECK("5.2 false → 'false'", r && strcmp(r,"false")==0); sqlite3_free(r);
  }
  /* true (0xc3) → "true" */
  {
    unsigned char tru[] = {0xc3};
    char *r = to_json_bind(db, tru, 1);
    CHECK("5.3 true → 'true'", r && strcmp(r,"true")==0); sqlite3_free(r);
  }
  /* positive fixint */
  {
    unsigned char pfi[] = {0x2a}; /* 42 */
    char *r = to_json_bind(db, pfi, 1);
    CHECK("5.4 posfix 42 → '42'", r && strcmp(r,"42")==0); sqlite3_free(r);
  }
  /* negative fixint */
  {
    unsigned char nfi[] = {0xff}; /* -1 */
    char *r = to_json_bind(db, nfi, 1);
    CHECK("5.5 negfix -1 → '-1'", r && strcmp(r,"-1")==0); sqlite3_free(r);
  }
  /* uint64 large */
  {
    unsigned char u64[] = {0xcf, 0x00,0x00,0x00,0x01,0x00,0x00,0x00,0x00}; /* 4294967296 */
    char *r = to_json_bind(db, u64, 9);
    CHECK("5.6 uint64 4294967296 → '4294967296'", r && strcmp(r,"4294967296")==0); sqlite3_free(r);
  }
  /* fixstr */
  {
    unsigned char fstr[] = {0xa3, 0x66, 0x6f, 0x6f}; /* "foo" */
    char *r = to_json_bind(db, fstr, 4);
    CHECK("5.7 fixstr 'foo' → '\"foo\"'", r && strcmp(r,"\"foo\"")==0); sqlite3_free(r);
  }
  /* bin → hex string */
  {
    unsigned char bin[] = {0xc4, 0x02, 0xde, 0xad}; /* bin8 len=2 data=dead */
    char *r = to_json_bind(db, bin, 4);
    CHECK("5.8 bin8 → hex JSON string", r && strcmp(r,"\"dead\"")==0); sqlite3_free(r);
  }
  /* array */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_array(1,2,3)", &n);
    char *r = blob ? to_json_bind(db, blob, n) : NULL;
    CHECK("5.9 array [1,2,3] → '[1,2,3]'", r && strcmp(r,"[1,2,3]")==0);
    sqlite3_free(r); sqlite3_free(blob);
  }
  /* map */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_object('x',1)", &n);
    char *r = blob ? to_json_bind(db, blob, n) : NULL;
    CHECK("5.10 map {x:1} → '{\"x\":1}'", r && strcmp(r,"{\"x\":1}")==0);
    sqlite3_free(r); sqlite3_free(blob);
  }
  /* ext → null (unknown) */
  {
    unsigned char ext1[] = {0xd4, 0x01, 0x42}; /* fixext1 type=1 data=0x42 */
    char *r = to_json_bind(db, ext1, 3);
    CHECK("5.11 ext → 'null'", r && strcmp(r,"null")==0); sqlite3_free(r);
  }
}

/* ── msgpack_pretty ─────────────────────────────────────────────── */

static void test_pretty(sqlite3 *db){
  /* pretty output contains newlines */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_from_json('{\"a\":1}')", &n);
    char *r = blob ? pretty_bind(db, blob, n, -1) : NULL;
    CHECK("6.1 pretty default has newline", r && strchr(r,'\n')!=NULL); sqlite3_free(r);
    sqlite3_free(blob);
  }

  /* pretty with indent=4 */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_from_json('[1,2]')", &n);
    char *r = blob ? pretty_bind(db, blob, n, 4) : NULL;
    CHECK("6.2 pretty indent=4 has newline", r && strchr(r,'\n')!=NULL); sqlite3_free(r);
    sqlite3_free(blob);
  }

  /* pretty with indent=0 should still not have indentation spaces but may have newlines */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_from_json('{\"a\":1}')", &n);
    char *r = blob ? pretty_bind(db, blob, n, 0) : NULL;
    CHECK("6.3 pretty indent=0 produces output", r && strlen(r)>0); sqlite3_free(r);
    sqlite3_free(blob);
  }

  /* SQL text path */
  {
    sqlite3_int64 has_nl = exec1i(db,
      "SELECT instr(msgpack_pretty(msgpack_from_json('{\"a\":1}')), char(10)) > 0");
    CHECK("6.4 pretty (SQL text) has newline", has_nl == 1);
  }

  /* scalar pretty → same as to_json */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_quote(42)", &n);
    char *r = blob ? pretty_bind(db, blob, n, -1) : NULL;
    CHECK("6.5 pretty(scalar)='42'", r && strcmp(r,"42")==0); sqlite3_free(r);
    sqlite3_free(blob);
  }

  /* non-blob → NULL */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_pretty('hello') IS NULL", -1, &s, NULL);
    sqlite3_step(s);
    CHECK("6.6 pretty(non-blob) IS NULL", sqlite3_column_int(s,0) == 1);
    sqlite3_finalize(s);
  }
}

/* ── msgpack_to_jsonb alias ─────────────────────────────────────── */

static void test_to_jsonb_alias(sqlite3 *db){
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_array(1,2,3)", &n);

  /* msgpack_to_json and msgpack_to_jsonb should return identical output */
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db,
    "SELECT msgpack_to_json(?), msgpack_to_jsonb(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  sqlite3_bind_blob(s, 2, blob, n, SQLITE_STATIC);
  sqlite3_step(s);
  const char *tj  = (const char*)sqlite3_column_text(s, 0);
  const char *tjb = (const char*)sqlite3_column_text(s, 1);
  CHECK("7.1 to_jsonb = to_json", tj && tjb && strcmp(tj,tjb)==0);
  sqlite3_finalize(s);

  sqlite3_free(blob);
}

/* ── from_json error handling ───────────────────────────────────── */

static void test_from_json_errors(sqlite3 *db){
  /* invalid JSON → error */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_from_json(?)", -1, &s, NULL);
    sqlite3_bind_text(s, 1, "{invalid}", -1, SQLITE_STATIC);
    int rc = sqlite3_step(s);
    CHECK("8.1 from_json invalid JSON → SQLITE_ERROR", rc == SQLITE_ERROR);
    sqlite3_finalize(s);
  }
  /* NULL input → NULL output */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_from_json(?) IS NULL", -1, &s, NULL);
    sqlite3_bind_null(s, 1);
    sqlite3_step(s);
    CHECK("8.2 from_json(NULL) IS NULL", sqlite3_column_int(s,0)==1);
    sqlite3_finalize(s);
  }
  /* truncated JSON */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_from_json(?)", -1, &s, NULL);
    sqlite3_bind_text(s, 1, "[1,2", -1, SQLITE_STATIC);
    int rc = sqlite3_step(s);
    CHECK("8.3 from_json truncated array → error", rc == SQLITE_ERROR);
    sqlite3_finalize(s);
  }
}

/* ── to_json input validation ───────────────────────────────────── */

static void test_to_json_validation(sqlite3 *db){
  /* non-blob → NULL */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_to_json('hello') IS NULL", -1, &s, NULL);
    sqlite3_step(s);
    CHECK("9.1 to_json(text) IS NULL", sqlite3_column_int(s,0)==1);
    sqlite3_finalize(s);
  }
  /* NULL → NULL */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_to_json(?) IS NULL", -1, &s, NULL);
    sqlite3_bind_null(s, 1);
    sqlite3_step(s);
    CHECK("9.2 to_json(NULL) IS NULL", sqlite3_column_int(s,0)==1);
    sqlite3_finalize(s);
  }
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){ fprintf(stderr,"open failed\n"); return 1; }
#ifdef SQLITE_CORE
  { char *e = NULL; sqlite3_msgpack_init(db, &e, NULL); sqlite3_free(e); }
#endif

  test_roundtrip_scalars(db);
  test_roundtrip_containers(db);
  test_escape_sequences(db);
  test_special_floats(db);
  test_to_json_types(db);
  test_pretty(db);
  test_to_jsonb_alias(db);
  test_from_json_errors(db);
  test_to_json_validation(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
