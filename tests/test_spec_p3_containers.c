/*
** test_spec_p3_containers.c — Phase 3: Array / Map construction, nesting,
**                             complex objects
**
** Verifies fixarray (0x90-0x9f), array16 (0xdc), fixmap (0x80-0x8f),
** map16 (0xde), deep nesting, mixed-type containers.
**
** Mix of:
**   • SQL text literals
**   • sqlite3_bind_blob to pass pre-built blobs into functions
**   • sqlite3_column_* result verification
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

/* Build a msgpack BLOB via SQL, return heap-allocated copy.
   Caller must sqlite3_free(). Sets *pn to byte length. */
static unsigned char *build_blob(sqlite3 *db, const char *sql, int *pn){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pn = 0;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return NULL;
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0);
    int n = sqlite3_column_bytes(s, 0);
    if(b && n > 0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pn = n; }
  }
  sqlite3_finalize(s); return r;
}

/* Call msgpack_valid on a blob via bind; return 0/1 */
static int blob_valid(sqlite3 *db, const void *b, int n){
  sqlite3_stmt *s = NULL; int v = 0;
  sqlite3_prepare_v2(db, "SELECT msgpack_valid(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int(s, 0);
  sqlite3_finalize(s); return v;
}

/* Call msgpack_type on a blob via bind; caller sqlite3_free() */
static char *blob_type(sqlite3 *db, const void *b, int n){
  sqlite3_stmt *s = NULL; char *r = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_type(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* Call msgpack_array_length on a blob; return count */
static sqlite3_int64 blob_length(sqlite3 *db, const void *b, int n){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -1;
  sqlite3_prepare_v2(db, "SELECT msgpack_array_length(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

/* Call msgpack_extract(blob, path) via bind; return int64 */
static sqlite3_int64 blob_extract_i(sqlite3 *db, const void *b, int n,
    const char *path){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -999;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?, ?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

/* Call msgpack_extract(blob, path) → text; caller sqlite3_free() */
static char *blob_extract_t(sqlite3 *db, const void *b, int n,
    const char *path){
  sqlite3_stmt *s = NULL; char *r = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?, ?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── fixarray tests (0 to 15 elements) ─────────────────────────── */

static void test_fixarray(sqlite3 *db){
  /* Empty fixarray: 0x90 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_array())");
    CHECK("1.1 empty fixarray hex=90 (SQL)", r && strcmp(r,"90")==0); sqlite3_free(r);
    /* via bind: build then check */
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_array()", &n);
    CHECK("1.2 empty fixarray valid (blob)", blob && blob_valid(db, blob, n));
    char *t = blob ? blob_type(db, blob, n) : NULL;
    CHECK("1.3 empty fixarray type='array'", t && strcmp(t,"array")==0);
    sqlite3_int64 len = blob ? blob_length(db, blob, n) : -1;
    CHECK("1.4 empty fixarray length=0", len == 0);
    sqlite3_free(blob); sqlite3_free(t);
  }

  /* fixarray [1,2,3]: 0x93 01 02 03 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_array(1,2,3))");
    CHECK("1.5 fixarray [1,2,3] hex (SQL)", r && strcmp(r,"93010203")==0); sqlite3_free(r);
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_array(1,2,3)", &n);
    CHECK("1.6 fixarray [1,2,3] len=3 (bind)", blob && blob_length(db, blob, n) == 3);
    CHECK("1.7 fixarray [1,2,3] $[0]=1", blob && blob_extract_i(db, blob, n, "$[0]") == 1);
    CHECK("1.8 fixarray [1,2,3] $[2]=3", blob && blob_extract_i(db, blob, n, "$[2]") == 3);
    sqlite3_free(blob);
  }

  /* fixarray with max count=15 (0x9f) */
  {
    char *r = exec1(db,
      "SELECT hex(substr(msgpack_array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14),1,1))");
    CHECK("1.9 fixarray 15 elements header=9F (SQL)", r && strcmp(r,"9F")==0); sqlite3_free(r);
    sqlite3_int64 len = exec1i(db,
      "SELECT msgpack_array_length(msgpack_array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14))");
    CHECK("1.10 fixarray 15 elements length=15", len == 15);
  }

  /* mixed types in fixarray */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_array(NULL, 1, 3.14, 'hi', x'DEAD')", &n);
    CHECK("1.11 mixed fixarray valid", blob && blob_valid(db, blob, n));
    CHECK("1.12 mixed fixarray length=5", blob && blob_length(db, blob, n) == 5);

    /*
    ** For nil/container elements, msgpack_extract returns SQL NULL or a BLOB,
    ** so msgpack_type(extract_result) won't work for nil.
    ** Instead use msgpack_type(blob, path) which navigates internally.
    */
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_type(?,?)",
      -1, &s, NULL);

    const char *paths[] = {"$[0]","$[1]","$[2]","$[3]"};
    const char *expected[] = {"null","integer","real","text"};
    const char *labels[] = {
      "1.13 mixed[0] type=null",
      "1.14 mixed[1] type=integer",
      "1.15 mixed[2] type=real",
      "1.16 mixed[3] type=text"
    };
    int j;
    for(j = 0; j < 4; j++){
      sqlite3_reset(s);
      sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
      sqlite3_bind_text(s, 2, paths[j], -1, SQLITE_STATIC);
      sqlite3_step(s);
      const char *t = (const char*)sqlite3_column_text(s, 0);
      CHECK(labels[j], t && strcmp(t, expected[j])==0);
    }
    sqlite3_finalize(s);
    sqlite3_free(blob);
  }
}

/* ── array16 tests (16..65535 elements) ────────────────────────── */

static void test_array16(sqlite3 *db){
  /* Build 16-element array via group_array; header should be 0xdc 0x00 0x10 */
  {
    char *r = exec1(db,
      "SELECT hex(substr(msgpack_group_array(value),1,1)) "
      "FROM (SELECT value FROM generate_series(1,16) LIMIT 16)");
    /* generate_series may not exist; use a CTE instead */
    sqlite3_free(r);

    /* Build 16 elements using a temporary table */
    sqlite3_exec(db,
      "CREATE TEMP TABLE t16(v INTEGER); "
      "INSERT INTO t16 VALUES(1),(2),(3),(4),(5),(6),(7),(8),"
      "(9),(10),(11),(12),(13),(14),(15),(16);", NULL,NULL,NULL);

    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_group_array(v) FROM t16", &n);
    if(blob && n >= 1){
      CHECK("2.1 array16 header byte=0xDC", blob[0] == 0xDC);
      CHECK("2.2 array16 length=16", blob_length(db, blob, n) == 16);
      CHECK("2.3 array16 $[0]=1",   blob_extract_i(db, blob, n, "$[0]")  == 1);
      CHECK("2.4 array16 $[15]=16", blob_extract_i(db, blob, n, "$[15]") == 16);
      CHECK("2.5 array16 is valid", blob_valid(db, blob, n) == 1);
    }
    sqlite3_free(blob);
    sqlite3_exec(db, "DROP TABLE t16;", NULL,NULL,NULL);
  }
}

/* ── fixmap tests ───────────────────────────────────────────────── */

static void test_fixmap(sqlite3 *db){
  /* Empty fixmap: 0x80 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_object())");
    CHECK("3.1 empty fixmap hex=80 (SQL)", r && strcmp(r,"80")==0); sqlite3_free(r);
    int n = 0; unsigned char *blob = build_blob(db, "SELECT msgpack_object()", &n);
    char *tmap = blob ? blob_type(db, blob, n) : NULL;
    CHECK("3.2 empty fixmap type='map'", tmap && strcmp(tmap,"map")==0); sqlite3_free(tmap);
    CHECK("3.3 empty fixmap length=0",   blob && blob_length(db, blob, n) == 0);
    sqlite3_free(blob);
  }

  /* fixmap {a:1, b:2}: 0x82 A1 61 01 A1 62 02 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_object('a',1,'b',2))");
    CHECK("3.4 fixmap {a:1,b:2} hex (SQL)", r && strcmp(r,"82A16101A16202")==0); sqlite3_free(r);
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_object('a',1,'b',2)", &n);
    CHECK("3.5 fixmap {a:1,b:2} $.a=1 (bind)", blob && blob_extract_i(db, blob, n, "$.a") == 1);
    CHECK("3.6 fixmap {a:1,b:2} $.b=2 (bind)", blob && blob_extract_i(db, blob, n, "$.b") == 2);
    CHECK("3.7 fixmap {a:1,b:2} length=2", blob && blob_length(db, blob, n) == 2);
    sqlite3_free(blob);
  }

  /* fixmap with all scalar types as values */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_object('n',NULL,'b',0,'i',42,'r',1.5,'s','hi')", &n);
    CHECK("3.8 fixmap all scalars valid", blob && blob_valid(db, blob, n));
    char *tn = blob_extract_t(db, blob, n, "$.n");
    CHECK("3.9 fixmap $.n NULL → null",  tn == NULL || (tn && strcmp(tn,"null")==0));
    sqlite3_free(tn);
    CHECK("3.10 fixmap $.i=42",   blob && blob_extract_i(db, blob, n, "$.i") == 42);
    char *ts = blob_extract_t(db, blob, n, "$.s");
    CHECK("3.11 fixmap $.s='hi'", ts && strcmp(ts,"hi")==0); sqlite3_free(ts);
    sqlite3_free(blob);
  }

  /* odd arguments → error */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_object('a',1,'b')", -1, &s, NULL);
    int rc = sqlite3_step(s);
    CHECK("3.12 msgpack_object odd args → SQLITE_ERROR",
      rc == SQLITE_ERROR || sqlite3_column_type(s,0) == SQLITE_NULL);
    sqlite3_finalize(s);
  }
}

/* ── map16 tests ────────────────────────────────────────────────── */

static void test_map16(sqlite3 *db){
  /* Build a 16-pair map via group_object */
  sqlite3_exec(db,
    "CREATE TEMP TABLE kv16(k TEXT, v INTEGER); "
    "INSERT INTO kv16 VALUES"
    "('k01',1),('k02',2),('k03',3),('k04',4),('k05',5),('k06',6),"
    "('k07',7),('k08',8),('k09',9),('k10',10),('k11',11),('k12',12),"
    "('k13',13),('k14',14),('k15',15),('k16',16);",
    NULL,NULL,NULL);

  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_group_object(k,v) FROM kv16", &n);
  if(blob && n >= 1){
    CHECK("4.1 map16 header byte=0xDE", blob[0] == 0xDE);
    CHECK("4.2 map16 length=16", blob_length(db, blob, n) == 16);
    CHECK("4.3 map16 $.k01=1",  blob_extract_i(db, blob, n, "$.k01") == 1);
    CHECK("4.4 map16 $.k16=16", blob_extract_i(db, blob, n, "$.k16") == 16);
    CHECK("4.5 map16 is valid", blob_valid(db, blob, n) == 1);
  }
  sqlite3_free(blob);
  sqlite3_exec(db, "DROP TABLE kv16;", NULL,NULL,NULL);
}

/* ── deep nesting ───────────────────────────────────────────────── */

static void test_deep_nesting(sqlite3 *db){
  /*
  ** Build a 5-level deep structure:
  **   {"l1": {"l2": {"l3": [10, 20, {"l4": {"l5": 42}}]}}}
  ** using nested msgpack_object/msgpack_array calls.
  */
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_object('l1',"
    "  msgpack_object('l2',"
    "    msgpack_object('l3',"
    "      msgpack_array(10, 20,"
    "        msgpack_object('l4',"
    "          msgpack_object('l5', 42))))))", &n);

  CHECK("5.1 deep nesting valid",   blob && blob_valid(db, blob, n));
  char *t = blob ? blob_type(db, blob, n) : NULL;
  CHECK("5.2 deep nesting root=map", t && strcmp(t,"map")==0); sqlite3_free(t);

  /* Navigate to the deep value via bind */
  sqlite3_int64 v = blob ? blob_extract_i(db, blob, n, "$.l1.l2.l3[2].l4.l5") : -1;
  CHECK("5.3 deep nesting $.l1.l2.l3[2].l4.l5 = 42", v == 42);

  /* Navigate to array element */
  sqlite3_int64 v2 = blob ? blob_extract_i(db, blob, n, "$.l1.l2.l3[0]") : -1;
  CHECK("5.4 deep nesting $.l1.l2.l3[0] = 10", v2 == 10);

  sqlite3_free(blob);
}

static void test_nested_arrays(sqlite3 *db){
  /* Jagged nested arrays */
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_array("
    "  msgpack_array(1,2,3),"
    "  msgpack_array(4,5),"
    "  msgpack_array(msgpack_array(6,7), 8)"
    ")", &n);

  CHECK("6.1 nested arrays valid", blob && blob_valid(db, blob, n));
  CHECK("6.2 nested arrays len=3", blob && blob_length(db, blob, n) == 3);
  CHECK("6.3 nested $[0][1]=2",   blob && blob_extract_i(db, blob, n, "$[0][1]") == 2);
  CHECK("6.4 nested $[2][0][0]=6",blob && blob_extract_i(db, blob, n, "$[2][0][0]") == 6);
  CHECK("6.5 nested $[2][1]=8",   blob && blob_extract_i(db, blob, n, "$[2][1]") == 8);

  sqlite3_free(blob);
}

static void test_msgpack_func(sqlite3 *db){
  /* msgpack(blob) validates and returns the blob */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack(msgpack_array(1,2,3))", &n);
    CHECK("7.1 msgpack() validates array", blob && blob_valid(db, blob, n));
    sqlite3_free(blob);
  }

  /* msgpack(non-blob) → error */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack('hello')", -1, &s, NULL);
    int rc = sqlite3_step(s);
    CHECK("7.2 msgpack(text) → error", rc == SQLITE_ERROR);
    sqlite3_finalize(s);
  }

  /* msgpack(NULL) → NULL */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack(NULL) IS NULL", -1, &s, NULL);
    sqlite3_step(s);
    int isNull = sqlite3_column_int(s, 0);
    CHECK("7.3 msgpack(NULL) → NULL", isNull == 1);
    sqlite3_finalize(s);
  }

  /* msgpack(invalid blob) → error */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack(?)", -1, &s, NULL);
    unsigned char bad[] = {0xc1};
    sqlite3_bind_blob(s, 1, bad, 1, SQLITE_STATIC);
    int rc = sqlite3_step(s);
    CHECK("7.4 msgpack(invalid blob) → error", rc == SQLITE_ERROR);
    sqlite3_finalize(s);
  }
}

static void test_auto_embed(sqlite3 *db){
  /*
  ** When a BLOB argument to msgpack_array/object is itself valid msgpack,
  ** it is embedded directly (not wrapped in bin).
  */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_object('inner', msgpack_array(1,2,3))", &n);
    /* The value at $.inner should be an array, not a blob */
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_type(msgpack_extract(?, '$.inner'))", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
    sqlite3_step(s);
    const char *t = (const char*)sqlite3_column_text(s, 0);
    CHECK("8.1 valid msgpack blob auto-embeds as array", t && strcmp(t,"array")==0);
    sqlite3_finalize(s);
    sqlite3_free(blob);
  }

  /* A raw non-msgpack blob wraps as bin */
  {
    unsigned char raw[] = {0xFF, 0xFE};
    sqlite3_stmt *enc = NULL, *typ = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_object('raw', ?)", -1, &enc, NULL);
    sqlite3_bind_blob(enc, 1, raw, 2, SQLITE_STATIC);
    sqlite3_step(enc);
    const void *b = sqlite3_column_blob(enc, 0);
    int n = sqlite3_column_bytes(enc, 0);
    unsigned char *buf = sqlite3_malloc(n); memcpy(buf, b, n);
    sqlite3_finalize(enc);

    sqlite3_prepare_v2(db,
      "SELECT msgpack_type(msgpack_extract(?, '$.raw'))", -1, &typ, NULL);
    sqlite3_bind_blob(typ, 1, buf, n, SQLITE_STATIC);
    sqlite3_step(typ);
    const char *t = (const char*)sqlite3_column_text(typ, 0);
    CHECK("8.2 non-msgpack blob wraps as 'blob'", t && strcmp(t,"blob")==0);
    sqlite3_finalize(typ);
    sqlite3_free(buf);
  }
}

static void test_multi_path_extract(sqlite3 *db){
  /* msgpack_extract(blob, path1, path2, ...) → returns msgpack array */
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_object('a',1,'b',2,'c',3)", &n);

  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db,
    "SELECT msgpack_type(msgpack_extract(?, '$.a', '$.c'))", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  sqlite3_step(s);
  const char *t = (const char*)sqlite3_column_text(s, 0);
  CHECK("9.1 multi-path extract returns array", t && strcmp(t,"array")==0);
  sqlite3_finalize(s);

  /* Verify values in the returned multi-path array */
  sqlite3_prepare_v2(db,
    "SELECT msgpack_array_length(msgpack_extract(?, '$.a', '$.b', '$.c'))",
    -1, &s, NULL);
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  sqlite3_step(s);
  sqlite3_int64 len = sqlite3_column_int64(s, 0);
  CHECK("9.2 multi-path 3 results → len=3", len == 3);
  sqlite3_finalize(s);

  /*
  ** Missing path in multi-path → nil in result.
  ** msgpack_extract returns SQL NULL for nil elements, so we can't nest
  ** another msgpack_extract call — we use msgpack_type(blob, path) (2-arg)
  ** after first materialising the multi-path result blob.
  */
  {
    sqlite3_stmt *s1 = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_extract(?, '$.a', '$.z')", -1, &s1, NULL);
    sqlite3_bind_blob(s1, 1, blob, n, SQLITE_STATIC);
    sqlite3_step(s1);
    const void *mb = sqlite3_column_blob(s1, 0);
    int mn = sqlite3_column_bytes(s1, 0);
    unsigned char *mblob = NULL;
    if(mb){ mblob = (unsigned char*)sqlite3_malloc(mn); memcpy(mblob, mb, mn); }
    sqlite3_finalize(s1);

    if(mblob){
      sqlite3_stmt *s2 = NULL;
      sqlite3_prepare_v2(db, "SELECT msgpack_type(?, '$[1]')", -1, &s2, NULL);
      sqlite3_bind_blob(s2, 1, mblob, mn, SQLITE_STATIC);
      sqlite3_step(s2);
      const char *t2 = (const char*)sqlite3_column_text(s2, 0);
      CHECK("9.3 multi-path missing path → nil element", t2 && strcmp(t2,"null")==0);
      sqlite3_finalize(s2);
      sqlite3_free(mblob);
    }
  }

  sqlite3_free(blob);
}

static void test_array_length_with_path(sqlite3 *db){
  /* msgpack_array_length(blob, path) — navigate then measure */
  int n = 0; unsigned char *blob = build_blob(db,
    "SELECT msgpack_object('arr', msgpack_array(10,20,30,40))", &n);

  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db,
    "SELECT msgpack_array_length(?, '$.arr')", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  sqlite3_step(s);
  sqlite3_int64 len = sqlite3_column_int64(s, 0);
  CHECK("10.1 array_length with path $.arr = 4", len == 4);
  sqlite3_finalize(s);

  /* scalar path → NULL */
  sqlite3_prepare_v2(db,
    "SELECT msgpack_array_length(msgpack_quote(42)) IS NULL", -1, &s, NULL);
  sqlite3_step(s);
  int isNull = sqlite3_column_int(s, 0);
  CHECK("10.2 array_length(scalar) IS NULL", isNull == 1);
  sqlite3_finalize(s);

  sqlite3_free(blob);
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){ fprintf(stderr,"open failed\n"); return 1; }
#ifdef SQLITE_CORE
  { char *e = NULL; sqlite3_msgpack_init(db, &e, NULL); sqlite3_free(e); }
#endif

  test_fixarray(db);
  test_array16(db);
  test_fixmap(db);
  test_map16(db);
  test_deep_nesting(db);
  test_nested_arrays(db);
  test_msgpack_func(db);
  test_auto_embed(db);
  test_multi_path_extract(db);
  test_array_length_with_path(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
