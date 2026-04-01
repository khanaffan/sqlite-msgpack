/*
** test_spec_p1_ints.c — Phase 1: Nil / Bool / Integer encoding
**
** Verifies every nil, boolean and integer format defined in the MessagePack
** specification using a mix of:
**   • SQL text literals  (exec1 / exec1i helpers)
**   • sqlite3_bind_*     (bind_and_quote / bind_and_hex helpers)
**
** Expected byte sequences come directly from the spec:
**   nil           0xc0
**   false         0xc2
**   true          0xc3
**   positive fixint  0x00–0x7f  (1 byte)
**   negative fixint  0xe0–0xff  (1 byte, -32..−1)
**   uint8         0xcc xx
**   uint16        0xcd xx xx
**   uint32        0xce xx xx xx xx
**   uint64        0xcf xx*8
**   int8          0xd0 xx
**   int16         0xd1 xx xx
**   int32         0xd2 xx xx xx xx
**   int64         0xd3 xx*8
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

/* ── test harness ─────────────────────────────────────────── */
static int g_pass = 0, g_fail = 0;
#define CHECK(label, expr) \
  do { if (expr) { printf("PASS  %s\n",(label)); g_pass++; } \
       else      { printf("FAIL  %s\n",(label)); g_fail++; } } while(0)

/* Run SQL returning a single text value; caller must sqlite3_free() */
static char *exec1(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = NULL;
  char *r = NULL;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return NULL;
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* Run SQL returning a single integer */
static sqlite3_int64 exec1i(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -1;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return -1;
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

/*
** Bind an INTEGER via sqlite3_bind_int64, call msgpack_quote(?), return HEX.
** Uses a prepared statement to demonstrate the bind path.
*/
static char *bind_int_hex(sqlite3 *db, sqlite3_int64 val){
  sqlite3_stmt *s = NULL;
  char *r = NULL;
  const char *sql = "SELECT hex(msgpack_quote(?))";
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return NULL;
  sqlite3_bind_int64(s, 1, val);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/*
** Bind a BLOB (pre-built msgpack bytes) to msgpack_valid(?), return 0/1.
*/
static int bind_blob_valid(sqlite3 *db, const unsigned char *blob, int n){
  sqlite3_stmt *s = NULL; int v = -1;
  if(sqlite3_prepare_v2(db, "SELECT msgpack_valid(?)", -1, &s, NULL) != SQLITE_OK)
    return -1;
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int(s, 0);
  sqlite3_finalize(s); return v;
}

/*
** Bind a BLOB to msgpack_type(?), return type string; caller sqlite3_free().
*/
static char *bind_blob_type(sqlite3 *db, const unsigned char *blob, int n){
  sqlite3_stmt *s = NULL; char *r = NULL;
  if(sqlite3_prepare_v2(db, "SELECT msgpack_type(?)", -1, &s, NULL) != SQLITE_OK)
    return NULL;
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/*
** Bind a BLOB to msgpack_extract(?, path), return int64 value.
*/
static sqlite3_int64 bind_blob_extract_i(sqlite3 *db,
    const unsigned char *blob, int n, const char *path){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -999;
  if(sqlite3_prepare_v2(db, "SELECT msgpack_extract(?,?)", -1, &s, NULL) != SQLITE_OK)
    return -999;
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── helper: verify round-trip for integer via bind ─────────── */
static void check_int_roundtrip(sqlite3 *db, const char *label,
    sqlite3_int64 val, sqlite3_int64 expected_back){
  /* 1. Encode via bind */
  sqlite3_stmt *enc = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_quote(?)", -1, &enc, NULL);
  sqlite3_bind_int64(enc, 1, val);
  sqlite3_step(enc);
  const void *blob  = sqlite3_column_blob(enc, 0);
  int         nblob = sqlite3_column_bytes(enc, 0);

  /* 2. Decode back via bind */
  sqlite3_stmt *dec = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?, '$')", -1, &dec, NULL);
  sqlite3_bind_blob(dec, 1, blob, nblob, SQLITE_TRANSIENT);
  sqlite3_finalize(enc);   /* must finalize enc AFTER copying blob */
  sqlite3_int64 got = -999;
  if(sqlite3_step(dec) == SQLITE_ROW) got = sqlite3_column_int64(dec, 0);
  sqlite3_finalize(dec);

  CHECK(label, got == expected_back);
}

/* ── tests ───────────────────────────────────────────────────── */

static void test_nil(sqlite3 *db){
  char *r;

  /* SQL text */
  r = exec1(db, "SELECT hex(msgpack_quote(NULL))");
  CHECK("1.1 nil hex (SQL text)", r && strcmp(r,"C0")==0);
  sqlite3_free(r);

  /* via bind: bind NULL */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT hex(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_null(s, 1);
    sqlite3_step(s);
    const char *z = (const char*)sqlite3_column_text(s, 0);
    CHECK("1.2 nil hex (bind_null)", z && strcmp(z,"C0")==0);
    sqlite3_finalize(s);
  }

  /* type string */
  unsigned char nil_byte[] = {0xc0};
  char *t = bind_blob_type(db, nil_byte, 1);
  CHECK("1.3 nil type == 'null'", t && strcmp(t,"null")==0);
  sqlite3_free(t);

  /* valid */
  CHECK("1.4 nil blob is valid", bind_blob_valid(db, nil_byte, 1) == 1);

  /* decode nil → SQL NULL */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_extract(?, '$')", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, nil_byte, 1, SQLITE_STATIC);
    sqlite3_step(s);
    CHECK("1.5 nil decode → SQL NULL", sqlite3_column_type(s, 0) == SQLITE_NULL);
    sqlite3_finalize(s);
  }
}

static void test_bool(sqlite3 *db){
  /*
  ** SQLite has no boolean type; SQL expressions like `0=1` yield integer 0.
  ** The extension produces false (0xc2) / true (0xc3) only when those raw bytes
  ** are embedded directly (e.g. via from_json("false"), or manually crafted blobs).
  **
  ** We test the false/true bytes by:
  **   a) crafting the raw blob bytes and calling msgpack_valid / msgpack_type
  **   b) round-tripping through msgpack_from_json → msgpack_to_json
  */

  /* SQL integer 0 encodes as positive fixint 0x00, NOT as false (0xc2) */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(0))");
    CHECK("2.1 integer 0 = positive fixint 0x00 (not false)",
      r && strcmp(r,"00")==0);
    sqlite3_free(r);
  }
  /* SQL integer 1 encodes as positive fixint 0x01, NOT as true (0xc3) */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(1))");
    CHECK("2.2 integer 1 = positive fixint 0x01 (not true)",
      r && strcmp(r,"01")==0);
    sqlite3_free(r);
  }

  /* bind integer 0 → stored as 0x00 (positive fixint), not false */
  {
    char *h = bind_int_hex(db, 0);
    CHECK("2.3 integer 0 encodes as positive fixint 0x00", h && strcmp(h,"00")==0);
    sqlite3_free(h);
  }

  /* false/true round-trip */
  unsigned char fal[] = {0xc2};
  unsigned char tru[] = {0xc3};

  char *tf = bind_blob_type(db, fal, 1);
  CHECK("2.4 false type == 'false'", tf && strcmp(tf,"false")==0);
  sqlite3_free(tf);

  char *tt = bind_blob_type(db, tru, 1);
  CHECK("2.5 true type == 'true'", tt && strcmp(tt,"true")==0);
  sqlite3_free(tt);

  /* Decode false → integer 0, true → integer 1 */
  sqlite3_int64 vf = bind_blob_extract_i(db, fal, 1, "$");
  CHECK("2.6 false decodes → 0", vf == 0);
  sqlite3_int64 vt = bind_blob_extract_i(db, tru, 1, "$");
  CHECK("2.7 true decodes → 1", vt == 1);
}

static void test_positive_fixint(sqlite3 *db){
  /* Spec: positive fixint = 0xxxxxxx = 0x00..0x7f (1 byte) */
  /* Boundary values: 0, 1, 127 */
  struct { sqlite3_int64 v; const char *hex; } cases[] = {
    {0,   "00"},
    {1,   "01"},
    {42,  "2A"},
    {127, "7F"},
  };
  int i;
  for(i = 0; i < 4; i++){
    char label[64]; sqlite3_snprintf(64, label, "3.%d posfix %lld", i+1, cases[i].v);
    /* SQL text path */
    char sql[128]; sqlite3_snprintf(128, sql, "SELECT hex(msgpack_quote(%lld))", cases[i].v);
    char *r = exec1(db, sql);
    CHECK(label, r && strcmp(r, cases[i].hex)==0);
    sqlite3_free(r);
    /* bind path */
    char label2[64]; sqlite3_snprintf(64, label2, "3.%d posfix %lld (bind)", i+1, cases[i].v);
    char *r2 = bind_int_hex(db, cases[i].v);
    CHECK(label2, r2 && strcmp(r2, cases[i].hex)==0);
    sqlite3_free(r2);
  }

  /* round-trip all boundary values */
  check_int_roundtrip(db, "3.9  posfix 0   roundtrip",   0,   0);
  check_int_roundtrip(db, "3.10 posfix 127 roundtrip", 127, 127);
}

static void test_negative_fixint(sqlite3 *db){
  /* Spec: negative fixint = 111xxxxx = 0xe0..0xff = -32..−1 */
  struct { sqlite3_int64 v; const char *hex; } cases[] = {
    {-1,  "FF"},
    {-16, "F0"},
    {-32, "E0"},
  };
  int i;
  for(i = 0; i < 3; i++){
    char label[64]; sqlite3_snprintf(64, label, "4.%d negfix %lld", i+1, cases[i].v);
    char sql[128]; sqlite3_snprintf(128, sql, "SELECT hex(msgpack_quote(%lld))", cases[i].v);
    char *r = exec1(db, sql);
    CHECK(label, r && strcmp(r, cases[i].hex)==0);
    sqlite3_free(r);
    /* bind */
    char label2[64]; sqlite3_snprintf(64, label2, "4.%d negfix %lld (bind)", i+1, cases[i].v);
    char *r2 = bind_int_hex(db, cases[i].v);
    CHECK(label2, r2 && strcmp(r2, cases[i].hex)==0);
    sqlite3_free(r2);
  }
  check_int_roundtrip(db, "4.7 negfix -1  roundtrip",  -1,  -1);
  check_int_roundtrip(db, "4.8 negfix -32 roundtrip", -32, -32);
}

static void test_uint8(sqlite3 *db){
  /* uint8: 0xcc xx — covers 128..255 */
  struct { sqlite3_int64 v; const char *hex; } cases[] = {
    {128, "CC80"},
    {200, "CCC8"},
    {255, "CCFF"},
  };
  int i;
  for(i = 0; i < 3; i++){
    char label[64]; sqlite3_snprintf(64, label, "5.%d uint8 %lld", i+1, cases[i].v);
    char sql[128]; sqlite3_snprintf(128, sql, "SELECT hex(msgpack_quote(%lld))", cases[i].v);
    char *r = exec1(db, sql); CHECK(label, r && strcmp(r, cases[i].hex)==0); sqlite3_free(r);
    char label2[64]; sqlite3_snprintf(64, label2, "5.%d uint8 %lld (bind)", i+1, cases[i].v);
    char *r2 = bind_int_hex(db, cases[i].v); CHECK(label2, r2 && strcmp(r2, cases[i].hex)==0); sqlite3_free(r2);
  }
  check_int_roundtrip(db, "5.7 uint8 255 roundtrip", 255, 255);
}

static void test_uint16(sqlite3 *db){
  /* uint16: 0xcd hi lo — covers 256..65535 */
  /* 256 = 0x0100 → CD 01 00 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(256))");
    CHECK("6.1 uint16 256 SQL", r && strcmp(r,"CD0100")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, 256);
    CHECK("6.2 uint16 256 bind", r2 && strcmp(r2,"CD0100")==0); sqlite3_free(r2);
  }
  /* 65535 = 0xffff → CD FF FF */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(65535))");
    CHECK("6.3 uint16 65535 SQL", r && strcmp(r,"CDFFFF")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, 65535);
    CHECK("6.4 uint16 65535 bind", r2 && strcmp(r2,"CDFFFF")==0); sqlite3_free(r2);
  }
  check_int_roundtrip(db, "6.5 uint16 1000 roundtrip", 1000, 1000);
}

static void test_uint32(sqlite3 *db){
  /* uint32: 0xce a b c d — covers 65536..4294967295 */
  /* 65536 = 0x00010000 → CE 00 01 00 00 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(65536))");
    CHECK("7.1 uint32 65536 SQL", r && strcmp(r,"CE00010000")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, 65536);
    CHECK("7.2 uint32 65536 bind", r2 && strcmp(r2,"CE00010000")==0); sqlite3_free(r2);
  }
  /* 4294967295 = 0xFFFFFFFF → CE FF FF FF FF */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(4294967295))");
    CHECK("7.3 uint32 0xFFFFFFFF SQL", r && strcmp(r,"CEFFFFFFFF")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, 4294967295LL);
    CHECK("7.4 uint32 0xFFFFFFFF bind", r2 && strcmp(r2,"CEFFFFFFFF")==0); sqlite3_free(r2);
  }
  check_int_roundtrip(db, "7.5 uint32 1000000 roundtrip", 1000000, 1000000);
}

static void test_uint64(sqlite3 *db){
  /* uint64: 0xcf a b c d e f g h — covers 4294967296..MAX */
  /* 4294967296 = 0x100000000 → CF 00 00 00 01 00 00 00 00 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(4294967296))");
    CHECK("8.1 uint64 4294967296 SQL", r && strcmp(r,"CF0000000100000000")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, 4294967296LL);
    CHECK("8.2 uint64 4294967296 bind", r2 && strcmp(r2,"CF0000000100000000")==0); sqlite3_free(r2);
  }
  check_int_roundtrip(db, "8.3 uint64 4294967296 roundtrip", 4294967296LL, 4294967296LL);
}

static void test_int8(sqlite3 *db){
  /* int8: 0xd0 xx — covers -128..−33 */
  /* -33 = 0xDF → D0 DF */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(-33))");
    CHECK("9.1 int8 -33 SQL", r && strcmp(r,"D0DF")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, -33);
    CHECK("9.2 int8 -33 bind", r2 && strcmp(r2,"D0DF")==0); sqlite3_free(r2);
  }
  /* -128 = 0x80 → D0 80 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(-128))");
    CHECK("9.3 int8 -128 SQL", r && strcmp(r,"D080")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, -128);
    CHECK("9.4 int8 -128 bind", r2 && strcmp(r2,"D080")==0); sqlite3_free(r2);
  }
  check_int_roundtrip(db, "9.5 int8 -100 roundtrip", -100, -100);
}

static void test_int16(sqlite3 *db){
  /* int16: 0xd1 hi lo — covers -32768..−129 */
  /* -200 in two's complement 16-bit = 0xFF38 → D1 FF 38 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(-200))");
    CHECK("10.1 int16 -200 SQL", r && strcmp(r,"D1FF38")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, -200);
    CHECK("10.2 int16 -200 bind", r2 && strcmp(r2,"D1FF38")==0); sqlite3_free(r2);
  }
  /* -32768 = 0x8000 → D1 80 00 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(-32768))");
    CHECK("10.3 int16 -32768 SQL", r && strcmp(r,"D18000")==0); sqlite3_free(r);
  }
  check_int_roundtrip(db, "10.4 int16 -200 roundtrip", -200, -200);
}

static void test_int32(sqlite3 *db){
  /* int32: 0xd2 — covers -2147483648..−32769 */
  /* -32769 → D2 FF FF 7F FF */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(-32769))");
    CHECK("11.1 int32 -32769 SQL", r && strcmp(r,"D2FFFF7FFF")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, -32769);
    CHECK("11.2 int32 -32769 bind", r2 && strcmp(r2,"D2FFFF7FFF")==0); sqlite3_free(r2);
  }
  /* -2147483648 = 0x80000000 → D2 80 00 00 00 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(-2147483648))");
    CHECK("11.3 int32 -2147483648 SQL", r && strcmp(r,"D280000000")==0); sqlite3_free(r);
  }
  check_int_roundtrip(db, "11.4 int32 -1000000 roundtrip", -1000000, -1000000);
}

static void test_int64(sqlite3 *db){
  /* int64: 0xd3 — covers < -2147483648 */
  /* -2147483649 = 0xFFFFFFFF7FFFFFFF → D3 FF FF FF FF 7F FF FF FF */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(-2147483649))");
    CHECK("12.1 int64 -2147483649 SQL", r && strcmp(r,"D3FFFFFFFF7FFFFFFF")==0); sqlite3_free(r);
    char *r2 = bind_int_hex(db, -2147483649LL);
    CHECK("12.2 int64 -2147483649 bind", r2 && strcmp(r2,"D3FFFFFFFF7FFFFFFF")==0); sqlite3_free(r2);
  }
  check_int_roundtrip(db, "12.3 int64 -2147483649 roundtrip", -2147483649LL, -2147483649LL);
  check_int_roundtrip(db, "12.4 int64 big neg roundtrip", -9000000000LL, -9000000000LL);
}

static void test_type_string_integers(sqlite3 *db){
  /* All integer formats must return type 'integer' */
  struct { sqlite3_int64 v; } cases[] = {
    {0},{1},{127},{128},{255},{256},{65535},{65536},
    {4294967295LL},{-1},{-32},{-33},{-128},{-129},{-32768},{-32769},{-2147483648LL}
  };
  int i;
  for(i = 0; i < 17; i++){
    unsigned char blob[12]; int n = 0;
    /* Encode via bind into a blob */
    sqlite3_stmt *enc = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_quote(?)", -1, &enc, NULL);
    sqlite3_bind_int64(enc, 1, cases[i].v);
    sqlite3_step(enc);
    const void *b = sqlite3_column_blob(enc, 0);
    n = sqlite3_column_bytes(enc, 0);
    memcpy(blob, b, n);
    sqlite3_finalize(enc);

    char *t = bind_blob_type(db, blob, n);
    char label[64]; sqlite3_snprintf(64, label, "13.%d type(int %lld)='integer'", i+1, cases[i].v);
    CHECK(label, t && strcmp(t,"integer")==0);
    sqlite3_free(t);
  }
}

static void test_smallest_encoding_rule(sqlite3 *db){
  /*
  ** The spec says: serializers SHOULD use the format representing the
  ** data in the smallest number of bytes.  Verify our encoder does.
  */
  struct { sqlite3_int64 v; int expected_bytes; } cases[] = {
    /* positive fixint */    {0, 1},{127, 1},
    /* uint8  */             {128, 2},{255, 2},
    /* uint16 */             {256, 3},{65535, 3},
    /* uint32 */             {65536, 5},{4294967295LL, 5},
    /* uint64 */             {4294967296LL, 9},
    /* negative fixint */    {-1, 1},{-32, 1},
    /* int8  */              {-33, 2},{-128, 2},
    /* int16 */              {-129, 3},{-32768, 3},
    /* int32 */              {-32769, 5},{-2147483648LL, 5},
    /* int64 */              {-2147483649LL, 9}
  };
  int i;
  for(i = 0; i < 18; i++){
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT length(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_int64(s, 1, cases[i].v);
    sqlite3_step(s);
    int got = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);
    char label[80];
    sqlite3_snprintf(80, label, "14.%d smallest encoding v=%lld len=%d",
        i+1, cases[i].v, cases[i].expected_bytes);
    CHECK(label, got == cases[i].expected_bytes);
  }
}

static void test_invalid_blobs(sqlite3 *db){
  /* Spec: 0xc1 is "never used" → invalid */
  unsigned char bad1[] = {0xc1};
  CHECK("15.1 0xc1 is invalid", bind_blob_valid(db, bad1, 1) == 0);

  /* Empty blob is invalid */
  CHECK("15.2 empty blob invalid", bind_blob_valid(db, (const unsigned char*)"", 0) == 0);

  /* Truncated uint16 (needs 2 payload bytes, only 1 present) */
  unsigned char trunc[] = {0xcd, 0x01};
  CHECK("15.3 truncated uint16 invalid", bind_blob_valid(db, trunc, 2) == 0);

  /* Trailing garbage after a valid fixint */
  unsigned char trail[] = {0x42, 0x00};
  CHECK("15.4 trailing byte invalid", bind_blob_valid(db, trail, 2) == 0);

  /* Non-blob input to msgpack_valid → 0 */
  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid('hello')");
  CHECK("15.5 text input → valid=0", v == 0);

  /* NULL input to msgpack_valid → NULL (not 0) */
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_valid(?) IS NULL", -1, &s, NULL);
  sqlite3_bind_null(s, 1);
  sqlite3_step(s);
  int isNull = sqlite3_column_int(s, 0);
  sqlite3_finalize(s);
  CHECK("15.6 NULL input → msgpack_valid returns NULL", isNull == 1);
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){
    fprintf(stderr, "Cannot open db\n"); return 1;
  }
#ifdef SQLITE_CORE
  {
    char *zErr = NULL;
    if(sqlite3_msgpack_init(db, &zErr, NULL) != SQLITE_OK){
      fprintf(stderr, "init failed: %s\n", zErr ? zErr : "?");
      sqlite3_free(zErr); sqlite3_close(db); return 1;
    }
  }
#endif

  test_nil(db);
  test_bool(db);
  test_positive_fixint(db);
  test_negative_fixint(db);
  test_uint8(db);
  test_uint16(db);
  test_uint32(db);
  test_uint64(db);
  test_int8(db);
  test_int16(db);
  test_int32(db);
  test_int64(db);
  test_type_string_integers(db);
  test_smallest_encoding_rule(db);
  test_invalid_blobs(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
