/*
** test_spec_p4_extract.c — Phase 4: Extraction, type detection, error position
**
** Covers:
**   msgpack_extract  — single path, multi-path, missing, nested
**   msgpack_type     — all 10 type strings
**   msgpack_array_length — array, map, scalar
**   msgpack_error_position — valid, truncated, bad byte, non-blob
**
** Mix of SQL text literals and sqlite3_bind_* for blob/path inputs.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

/* Build a blob from SQL; caller sqlite3_free() */
static unsigned char *build_blob(sqlite3 *db, const char *sql, int *pn){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pn = 0;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return NULL;
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pn = n; }
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_extract(blob, path) → int64 via bind */
static sqlite3_int64 extract_i(sqlite3 *db, const void *b, int n, const char *path){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -999;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

/* msgpack_extract(blob, path) → double via bind */
static double extract_d(sqlite3 *db, const void *b, int n, const char *path){
  sqlite3_stmt *s = NULL; double v = -999.0;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_double(s, 0);
  sqlite3_finalize(s); return v;
}

/* msgpack_extract(blob, path) → text via bind; caller sqlite3_free() */
static char *extract_t(sqlite3 *db, const void *b, int n, const char *path){
  sqlite3_stmt *s = NULL; char *r = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_extract(blob, path) → SQLite type code */
static int extract_coltype(sqlite3 *db, const void *b, int n, const char *path){
  sqlite3_stmt *s = NULL; int ct = SQLITE_NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) ct = sqlite3_column_type(s, 0);
  sqlite3_finalize(s); return ct;
}

/* msgpack_type(blob, path) → type string via bind; caller sqlite3_free() */
static char *type_at(sqlite3 *db, const void *b, int n, const char *path){
  sqlite3_stmt *s = NULL; char *r = NULL;
  const char *sql = path
    ? "SELECT msgpack_type(?,?)"
    : "SELECT msgpack_type(?)";
  sqlite3_prepare_v2(db, sql, -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  if(path) sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_array_length(blob) via bind */
static sqlite3_int64 arr_len(sqlite3 *db, const void *b, int n){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -1;
  sqlite3_prepare_v2(db, "SELECT msgpack_array_length(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

/* msgpack_error_position(blob) via bind */
static sqlite3_int64 err_pos(sqlite3 *db, const void *b, int n){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -1;
  sqlite3_prepare_v2(db, "SELECT msgpack_error_position(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── extract single path ─────────────────────────────────────────── */

static void test_extract_array(sqlite3 *db){
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_array(10, 20, 30, 40, 50)", &n);

  CHECK("1.1 extract $[0]=10",  blob && extract_i(db, blob, n, "$[0]") == 10);
  CHECK("1.2 extract $[4]=50",  blob && extract_i(db, blob, n, "$[4]") == 50);
  CHECK("1.3 extract $[2]=30",  blob && extract_i(db, blob, n, "$[2]") == 30);

  /* out-of-bounds → SQL NULL */
  int ct = blob ? extract_coltype(db, blob, n, "$[9]") : SQLITE_INTEGER;
  CHECK("1.4 extract oob → NULL",  ct == SQLITE_NULL);

  /* root path $ → returns blob (the array itself) */
  ct = blob ? extract_coltype(db, blob, n, "$") : SQLITE_NULL;
  CHECK("1.5 extract '$' on array → BLOB", ct == SQLITE_BLOB);

  sqlite3_free(blob);
}

static void test_extract_map(sqlite3 *db){
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_object('name','Alice','age',30,'score',98.5)", &n);

  char *name = blob ? extract_t(db, blob, n, "$.name") : NULL;
  CHECK("2.1 extract $.name='Alice'", name && strcmp(name,"Alice")==0); sqlite3_free(name);

  CHECK("2.2 extract $.age=30",   blob && extract_i(db, blob, n, "$.age") == 30);

  double score = blob ? extract_d(db, blob, n, "$.score") : 0.0;
  CHECK("2.3 extract $.score=98.5", score == 98.5);

  /* missing key → NULL */
  int ct = blob ? extract_coltype(db, blob, n, "$.missing") : SQLITE_INTEGER;
  CHECK("2.4 extract missing key → NULL", ct == SQLITE_NULL);

  sqlite3_free(blob);
}

static void test_extract_nested(sqlite3 *db){
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_object("
    "  'users', msgpack_array("
    "    msgpack_object('id',1,'name','Alice'),"
    "    msgpack_object('id',2,'name','Bob')"
    "  )"
    ")", &n);

  CHECK("3.1 nested $.users[0].id=1",
    blob && extract_i(db, blob, n, "$.users[0].id") == 1);
  CHECK("3.2 nested $.users[1].id=2",
    blob && extract_i(db, blob, n, "$.users[1].id") == 2);

  char *name = blob ? extract_t(db, blob, n, "$.users[1].name") : NULL;
  CHECK("3.3 nested $.users[1].name='Bob'", name && strcmp(name,"Bob")==0); sqlite3_free(name);

  /* type of nested array */
  char *t = blob ? type_at(db, blob, n, "$.users") : NULL;
  CHECK("3.4 type of $.users='array'", t && strcmp(t,"array")==0); sqlite3_free(t);

  sqlite3_free(blob);
}

static void test_extract_root_scalar(sqlite3 *db){
  /* extract from a scalar blob with path='$' */

  /* integer */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_quote(99)", &n);
    CHECK("4.1 extract '$' from int = 99", blob && extract_i(db, blob, n, "$") == 99);
    sqlite3_free(blob);
  }
  /* text */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_quote('world')", &n);
    char *t = blob ? extract_t(db, blob, n, "$") : NULL;
    CHECK("4.2 extract '$' from str = 'world'", t && strcmp(t,"world")==0); sqlite3_free(t);
    sqlite3_free(blob);
  }
  /* nil */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_quote(NULL)", &n);
    int ct = blob ? extract_coltype(db, blob, n, "$") : SQLITE_INTEGER;
    CHECK("4.3 extract '$' from nil → SQLITE_NULL", ct == SQLITE_NULL);
    sqlite3_free(blob);
  }
}

/* ── msgpack_type for all spec types ─────────────────────────────── */

static void test_type_all(sqlite3 *db){
  struct { const char *sql; const char *expected_type; const char *label; } cases[] = {
    { "SELECT msgpack_quote(NULL)",             "null",    "5.1  type: nil" },
    { "SELECT msgpack_quote(1=1)",              "integer", "5.2  type: true (stored as integer)" },
    { "SELECT msgpack_quote(0=1)",              "integer", "5.3  type: false (stored as integer)" },
    { "SELECT msgpack_quote(0)",                "integer", "5.4  type: pos fixint" },
    { "SELECT msgpack_quote(-1)",               "integer", "5.5  type: neg fixint" },
    { "SELECT msgpack_quote(255)",              "integer", "5.6  type: uint8" },
    { "SELECT msgpack_quote(65535)",            "integer", "5.7  type: uint16" },
    { "SELECT msgpack_quote(4294967295)",       "integer", "5.8  type: uint32" },
    { "SELECT msgpack_quote(4294967296)",       "integer", "5.9  type: uint64" },
    { "SELECT msgpack_quote(-33)",              "integer", "5.10 type: int8" },
    { "SELECT msgpack_quote(-32769)",           "integer", "5.11 type: int32" },
    { "SELECT msgpack_quote(1.5)",              "real",    "5.12 type: float64" },
    { "SELECT msgpack_quote('hi')",             "text",    "5.13 type: fixstr" },
    { "SELECT msgpack_array(1,2)",              "array",   "5.14 type: fixarray" },
    { "SELECT msgpack_object('k','v')",         "map",     "5.15 type: fixmap" },
    { NULL, NULL, NULL }
  };

  int i;
  for(i = 0; cases[i].sql != NULL; i++){
    int n = 0; unsigned char *blob = build_blob(db, cases[i].sql, &n);
    char *t = blob ? type_at(db, blob, n, NULL) : NULL;
    CHECK(cases[i].label, t && strcmp(t, cases[i].expected_type)==0);
    sqlite3_free(t); sqlite3_free(blob);
  }

  /* bin type */
  {
    unsigned char raw[] = {0xDE, 0xAD};
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_type(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, raw, 2, SQLITE_STATIC);
    sqlite3_step(s);
    const char *t = (const char*)sqlite3_column_text(s, 0);
    CHECK("5.16 type: bin", t && strcmp(t,"blob")==0);
    sqlite3_finalize(s);
  }

  /* ext type — manually craft fixext1: D4 type data */
  {
    unsigned char ext1[] = {0xd4, 0x01, 0x42}; /* fixext1, type=1, data=0x42 */
    char *t = type_at(db, ext1, 3, NULL);
    CHECK("5.17 type: fixext1 = 'ext'", t && strcmp(t,"ext")==0); sqlite3_free(t);
  }

  /* ext8 */
  {
    unsigned char ext8[] = {0xc7, 0x02, 0x05, 0xAA, 0xBB}; /* ext8, len=2, type=5, data */
    char *t = type_at(db, ext8, 5, NULL);
    CHECK("5.18 type: ext8 = 'ext'", t && strcmp(t,"ext")==0); sqlite3_free(t);
  }

  /* true/false */
  {
    unsigned char tru[] = {0xc3};
    char *t = type_at(db, tru, 1, NULL);
    CHECK("5.19 type: true byte → 'true'", t && strcmp(t,"true")==0); sqlite3_free(t);
    unsigned char fal[] = {0xc2};
    char *f = type_at(db, fal, 1, NULL);
    CHECK("5.20 type: false byte → 'false'", f && strcmp(f,"false")==0); sqlite3_free(f);
  }
}

/* ── msgpack_type with path ──────────────────────────────────────── */

static void test_type_with_path(sqlite3 *db){
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_object("
    "  'n', NULL, 'i', 42, 'r', 3.14,"
    "  's', 'hello', 'a', msgpack_array(1,2),"
    "  'm', msgpack_object('x',1)"
    ")", &n);

  struct { const char *path; const char *expected; const char *label; } cases[] = {
    { "$.n", "null",    "6.1 type at $.n" },
    { "$.i", "integer", "6.2 type at $.i" },
    { "$.r", "real",    "6.3 type at $.r" },
    { "$.s", "text",    "6.4 type at $.s" },
    { "$.a", "array",   "6.5 type at $.a" },
    { "$.m", "map",     "6.6 type at $.m" },
    { NULL,  NULL,      NULL }
  };

  int i;
  for(i = 0; cases[i].path; i++){
    char *t = blob ? type_at(db, blob, n, cases[i].path) : NULL;
    CHECK(cases[i].label, t && strcmp(t, cases[i].expected)==0); sqlite3_free(t);
  }

  /* missing path → NULL result */
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_type(?, '$.missing') IS NULL", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  sqlite3_step(s);
  int isNull = sqlite3_column_int(s, 0);
  CHECK("6.7 type at missing path IS NULL", isNull == 1);
  sqlite3_finalize(s);

  sqlite3_free(blob);
}

/* ── msgpack_array_length ────────────────────────────────────────── */

static void test_array_length(sqlite3 *db){
  /* fixarray */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_array(1,2,3,4,5)", &n);
    CHECK("7.1 array_length fixarray=5", blob && arr_len(db, blob, n) == 5);
    sqlite3_free(blob);
  }
  /* empty */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_array()", &n);
    CHECK("7.2 array_length empty=0", blob && arr_len(db, blob, n) == 0);
    sqlite3_free(blob);
  }
  /* fixmap pair count */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_object('a',1,'b',2,'c',3)", &n);
    CHECK("7.3 array_length fixmap=3 pairs", blob && arr_len(db, blob, n) == 3);
    sqlite3_free(blob);
  }
  /* scalar → NULL */
  {
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_quote(42)", &n);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_array_length(?) IS NULL", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
    sqlite3_step(s);
    int isNull = sqlite3_column_int(s, 0);
    CHECK("7.4 array_length(scalar) IS NULL", isNull == 1);
    sqlite3_finalize(s); sqlite3_free(blob);
  }
  /* nil → NULL */
  {
    unsigned char nil[] = {0xc0};
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_array_length(?) IS NULL", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, nil, 1, SQLITE_STATIC);
    sqlite3_step(s);
    int isNull = sqlite3_column_int(s, 0);
    CHECK("7.5 array_length(nil) IS NULL", isNull == 1);
    sqlite3_finalize(s);
  }
  /* array_length with path */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_object('items', msgpack_array(10,20,30,40))", &n);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_array_length(?, '$.items')", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
    sqlite3_step(s);
    sqlite3_int64 len = sqlite3_column_int64(s, 0);
    CHECK("7.6 array_length with path '$.items'=4", len == 4);
    sqlite3_finalize(s); sqlite3_free(blob);
  }
}

/* ── msgpack_error_position ──────────────────────────────────────── */

static void test_error_position(sqlite3 *db){
  /* valid blobs → 0 */
  struct { const char *sql; const char *label; } valid_cases[] = {
    { "SELECT msgpack_quote(42)",           "8.1 errpos valid int=0" },
    { "SELECT msgpack_quote('hi')",         "8.2 errpos valid str=0" },
    { "SELECT msgpack_array(1,2,3)",        "8.3 errpos valid array=0" },
    { "SELECT msgpack_object('k','v')",     "8.4 errpos valid map=0" },
    { NULL, NULL }
  };
  int i;
  for(i = 0; valid_cases[i].sql; i++){
    int n = 0; unsigned char *blob = build_blob(db, valid_cases[i].sql, &n);
    sqlite3_int64 ep = blob ? err_pos(db, blob, n) : -1;
    CHECK(valid_cases[i].label, ep == 0);
    sqlite3_free(blob);
  }

  /* empty blob → position 1 */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_error_position(?)", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, "", 0, SQLITE_STATIC);
    sqlite3_step(s);
    sqlite3_int64 ep = sqlite3_column_int64(s, 0);
    CHECK("8.5 errpos empty blob = 1", ep == 1);
    sqlite3_finalize(s);
  }

  /* 0xc1 (never used) at byte 1 → error at position 1 */
  {
    unsigned char bad[] = {0xc1};
    sqlite3_int64 ep = err_pos(db, bad, 1);
    CHECK("8.6 errpos 0xc1 = 1", ep == 1);
  }

  /* truncated fixarray: 0x92 (2 elems) but empty → error */
  {
    unsigned char trunc[] = {0x92};
    sqlite3_int64 ep = err_pos(db, trunc, 1);
    CHECK("8.7 errpos truncated fixarray > 0", ep > 0);
  }

  /* trailing garbage: valid fixint 0x42 + extra byte 0x00 */
  {
    unsigned char trail[] = {0x42, 0x00};
    sqlite3_int64 ep = err_pos(db, trail, 2);
    /* Not zero — trailing bytes make it invalid */
    CHECK("8.8 errpos trailing byte > 0", ep > 0);
  }

  /* non-blob input → NULL */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_error_position('not a blob') IS NULL", -1, &s, NULL);
    sqlite3_step(s);
    int isNull = sqlite3_column_int(s, 0);
    CHECK("8.9 errpos non-blob IS NULL", isNull == 1);
    sqlite3_finalize(s);
  }

  /* SQL text path: use x'' literal */
  {
    sqlite3_int64 ep = exec1i(db,
      "SELECT msgpack_error_position(x'92')");
    CHECK("8.10 errpos x'92' (SQL text) > 0", ep > 0);
  }
}

/* ── extract returns correct SQLite types ───────────────────────── */

static void test_extract_column_types(sqlite3 *db){
  /*
  ** Verify that msgpack_extract returns the correct SQLite affinity:
  **   nil     → SQLITE_NULL
  **   bool    → SQLITE_INTEGER
  **   integer → SQLITE_INTEGER
  **   float   → SQLITE_FLOAT
  **   str     → SQLITE_TEXT
  **   bin     → SQLITE_BLOB
  **   array   → SQLITE_BLOB
  **   map     → SQLITE_BLOB
  */
  unsigned char raw[] = {0xDE, 0xAD};
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db,
    "SELECT msgpack_extract("
    "  msgpack_object("
    "    'n', NULL,"
    "    'b', 0=1,"
    "    'i', 42,"
    "    'r', 3.14,"
    "    't', 'hello',"
    "    'x', ?,"
    "    'a', msgpack_array(1,2),"
    "    'm', msgpack_object('k',1)"
    "  ), ?"
    ")", -1, &s, NULL);
  /* We'll run this 8 times for each path */
  struct { const char *path; int expected_ct; const char *label; } cases[] = {
    {"$.n", SQLITE_NULL,    "9.1 extract nil → SQLITE_NULL"},
    {"$.b", SQLITE_INTEGER, "9.2 extract bool → SQLITE_INTEGER"},
    {"$.i", SQLITE_INTEGER, "9.3 extract int → SQLITE_INTEGER"},
    {"$.r", SQLITE_FLOAT,   "9.4 extract float → SQLITE_FLOAT"},
    {"$.t", SQLITE_TEXT,    "9.5 extract str → SQLITE_TEXT"},
    {"$.a", SQLITE_BLOB,    "9.6 extract array → SQLITE_BLOB"},
    {"$.m", SQLITE_BLOB,    "9.7 extract map → SQLITE_BLOB"},
    {NULL, 0, NULL}
  };
  sqlite3_finalize(s);

  /* Build the container */
  sqlite3_stmt *enc = NULL;
  sqlite3_prepare_v2(db,
    "SELECT msgpack_object("
    "  'n', NULL, 'b', 0=1, 'i', 42, 'r', 3.14, 't', 'hello',"
    "  'x', ?, 'a', msgpack_array(1,2), 'm', msgpack_object('k',1)"
    ")", -1, &enc, NULL);
  sqlite3_bind_blob(enc, 1, raw, 2, SQLITE_STATIC);
  sqlite3_step(enc);
  const void *b = sqlite3_column_blob(enc, 0);
  int n = sqlite3_column_bytes(enc, 0);
  unsigned char *buf = sqlite3_malloc(n); memcpy(buf, b, n);
  sqlite3_finalize(enc);

  int i;
  for(i = 0; cases[i].path; i++){
    int ct = extract_coltype(db, buf, n, cases[i].path);
    CHECK(cases[i].label, ct == cases[i].expected_ct);
  }

  /* bin type */
  int ct = extract_coltype(db, buf, n, "$.x");
  CHECK("9.8 extract bin → SQLITE_BLOB", ct == SQLITE_BLOB);

  sqlite3_free(buf);
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){ fprintf(stderr,"open failed\n"); return 1; }
#ifdef SQLITE_CORE
  { char *e = NULL; sqlite3_msgpack_init(db, &e, NULL); sqlite3_free(e); }
#endif

  test_extract_array(db);
  test_extract_map(db);
  test_extract_nested(db);
  test_extract_root_scalar(db);
  test_type_all(db);
  test_type_with_path(db);
  test_array_length(db);
  test_error_position(db);
  test_extract_column_types(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
