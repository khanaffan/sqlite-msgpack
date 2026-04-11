/*
** test_spec_p9_typed_primitives.c — Phase 9: Typed primitive constructors
**
** Verifies all new helper functions that produce specific msgpack wire types
** not otherwise reachable from SQLite's native type system:
**
**   msgpack_nil()             → 0xc0
**   msgpack_true()            → 0xc3
**   msgpack_false()           → 0xc2
**   msgpack_bool(v)           → 0xc2 or 0xc3
**   msgpack_float32(v)        → 0xca + 4 bytes IEEE-754
**   msgpack_int8(v)           → 0xd0 xx
**   msgpack_int16(v)          → 0xd1 xx xx
**   msgpack_int32(v)          → 0xd2 xx xx xx xx
**   msgpack_uint8(v)          → 0xcc xx
**   msgpack_uint16(v)         → 0xcd xx xx
**   msgpack_uint32(v)         → 0xce xx xx xx xx
**   msgpack_uint64(v)         → 0xcf xx*8
**   msgpack_bin(blob)         → always bin8/16/32 (no auto-embed)
**   msgpack_ext(type, blob)   → fixext or ext8/16/32
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

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── nil ──────────────────────────────────────────────────────────── */
static void test_nil(sqlite3 *db){
  char *r = exec1(db, "SELECT hex(msgpack_nil())");
  CHECK("1.1 msgpack_nil() = C0",     r && strcmp(r,"C0")==0);
  sqlite3_free(r);

  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid(msgpack_nil())");
  CHECK("1.2 msgpack_nil() is valid",  v == 1);

  char *t = exec1(db, "SELECT msgpack_type(msgpack_nil())");
  CHECK("1.3 msgpack_nil() type='null'", t && strcmp(t,"null")==0);
  sqlite3_free(t);
}

/* ── bool ─────────────────────────────────────────────────────────── */
static void test_bool(sqlite3 *db){
  char *rt = exec1(db, "SELECT hex(msgpack_true())");
  CHECK("2.1 msgpack_true() = C3",    rt && strcmp(rt,"C3")==0);
  sqlite3_free(rt);

  char *rf = exec1(db, "SELECT hex(msgpack_false())");
  CHECK("2.2 msgpack_false() = C2",   rf && strcmp(rf,"C2")==0);
  sqlite3_free(rf);

  char *bt = exec1(db, "SELECT hex(msgpack_bool(1))");
  CHECK("2.3 msgpack_bool(1) = C3",   bt && strcmp(bt,"C3")==0);
  sqlite3_free(bt);

  char *bf = exec1(db, "SELECT hex(msgpack_bool(0))");
  CHECK("2.4 msgpack_bool(0) = C2",   bf && strcmp(bf,"C2")==0);
  sqlite3_free(bf);

  char *bn = exec1(db, "SELECT hex(msgpack_bool(42))");
  CHECK("2.5 msgpack_bool(42) = C3",  bn && strcmp(bn,"C3")==0);
  sqlite3_free(bn);

  char *tt = exec1(db, "SELECT msgpack_type(msgpack_true())");
  CHECK("2.6 msgpack_true() type='true'",  tt && strcmp(tt,"true")==0);
  sqlite3_free(tt);

  char *ft = exec1(db, "SELECT msgpack_type(msgpack_false())");
  CHECK("2.7 msgpack_false() type='false'", ft && strcmp(ft,"false")==0);
  sqlite3_free(ft);

  sqlite3_int64 vt = exec1i(db, "SELECT msgpack_valid(msgpack_true())");
  CHECK("2.8 msgpack_true() is valid",  vt == 1);
  sqlite3_int64 vf = exec1i(db, "SELECT msgpack_valid(msgpack_false())");
  CHECK("2.9 msgpack_false() is valid", vf == 1);
}

/* ── float32 ─────────────────────────────────────────────────────── */
static void test_float32(sqlite3 *db){
  /* Header byte must be CA */
  char *r = exec1(db, "SELECT hex(msgpack_float32(3.14))");
  CHECK("3.1 msgpack_float32 header CA", r && r[0]=='C' && r[1]=='A');
  /* always 5 bytes → hex length = 10 */
  CHECK("3.2 msgpack_float32 length=5 bytes", r && strlen(r)==10);
  sqlite3_free(r);

  /* type = 'real' */
  char *t = exec1(db, "SELECT msgpack_type(msgpack_float32(1.5))");
  CHECK("3.3 msgpack_float32 type='real'", t && strcmp(t,"real")==0);
  sqlite3_free(t);

  /* valid */
  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid(msgpack_float32(0.0))");
  CHECK("3.4 msgpack_float32(0.0) is valid", v == 1);

  /* round-trip: decode back to REAL and check within float32 precision */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_extract(msgpack_float32(?), '$')", -1, &s, NULL);
    sqlite3_bind_double(s, 1, 1.0);
    sqlite3_step(s);
    double got = sqlite3_column_double(s, 0);
    sqlite3_finalize(s);
    CHECK("3.5 msgpack_float32(1.0) round-trip", got == 1.0);
  }

  /* msgpack_float32 vs msgpack_quote: different wire sizes */
  sqlite3_int64 len32 = exec1i(db, "SELECT length(msgpack_float32(1.0))");
  sqlite3_int64 len64 = exec1i(db, "SELECT length(msgpack_quote(1.0))");
  CHECK("3.6 float32 is 5 bytes", len32 == 5);
  CHECK("3.7 float64 is 9 bytes", len64 == 9);
}

/* ── signed fixed-width integers ─────────────────────────────────── */
static void test_int8(sqlite3 *db){
  char *r = exec1(db, "SELECT hex(msgpack_int8(42))");
  CHECK("4.1 msgpack_int8(42) = D02A", r && strcmp(r,"D02A")==0);
  sqlite3_free(r);

  char *rn = exec1(db, "SELECT hex(msgpack_int8(-1))");
  CHECK("4.2 msgpack_int8(-1) = D0FF", rn && strcmp(rn,"D0FF")==0);
  sqlite3_free(rn);

  char *rm = exec1(db, "SELECT hex(msgpack_int8(-128))");
  CHECK("4.3 msgpack_int8(-128) = D080", rm && strcmp(rm,"D080")==0);
  sqlite3_free(rm);

  char *rp = exec1(db, "SELECT hex(msgpack_int8(127))");
  CHECK("4.4 msgpack_int8(127) = D07F", rp && strcmp(rp,"D07F")==0);
  sqlite3_free(rp);

  /* out of range returns error */
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_int8(?)", -1, &s, NULL);
  sqlite3_bind_int(s, 1, 200);
  int rc = sqlite3_step(s);
  CHECK("4.5 msgpack_int8(200) is error", rc == SQLITE_ERROR);
  sqlite3_finalize(s);

  char *t = exec1(db, "SELECT msgpack_type(msgpack_int8(0))");
  CHECK("4.6 msgpack_int8 type='integer'", t && strcmp(t,"integer")==0);
  sqlite3_free(t);
}

static void test_int16(sqlite3 *db){
  /* 1000 = 0x03E8 → D1 03 E8 */
  char *r = exec1(db, "SELECT hex(msgpack_int16(1000))");
  CHECK("5.1 msgpack_int16(1000) = D103E8", r && strcmp(r,"D103E8")==0);
  sqlite3_free(r);

  char *rn = exec1(db, "SELECT hex(msgpack_int16(-32768))");
  CHECK("5.2 msgpack_int16(-32768) header D1", rn && rn[0]=='D' && rn[1]=='1');
  sqlite3_free(rn);

  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_int16(?)", -1, &s, NULL);
  sqlite3_bind_int(s, 1, 40000);
  int rc = sqlite3_step(s);
  CHECK("5.3 msgpack_int16(40000) is error", rc == SQLITE_ERROR);
  sqlite3_finalize(s);

  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid(msgpack_int16(0))");
  CHECK("5.4 msgpack_int16 is valid", v == 1);
}

static void test_int32(sqlite3 *db){
  /* 70000 = 0x00011170 → D2 00 01 11 70 */
  char *r = exec1(db, "SELECT hex(msgpack_int32(70000))");
  CHECK("6.1 msgpack_int32(70000) = D200011170", r && strcmp(r,"D200011170")==0);
  sqlite3_free(r);

  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_int32(?)", -1, &s, NULL);
  sqlite3_bind_int64(s, 1, 3000000000LL);
  int rc = sqlite3_step(s);
  CHECK("6.2 msgpack_int32(3e9) is error", rc == SQLITE_ERROR);
  sqlite3_finalize(s);

  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid(msgpack_int32(-1))");
  CHECK("6.3 msgpack_int32(-1) is valid", v == 1);
}

/* ── unsigned fixed-width integers ──────────────────────────────── */
static void test_uint8(sqlite3 *db){
  char *r = exec1(db, "SELECT hex(msgpack_uint8(200))");
  CHECK("7.1 msgpack_uint8(200) = CCC8", r && strcmp(r,"CCC8")==0);
  sqlite3_free(r);

  char *r0 = exec1(db, "SELECT hex(msgpack_uint8(0))");
  CHECK("7.2 msgpack_uint8(0) = CC00", r0 && strcmp(r0,"CC00")==0);
  sqlite3_free(r0);

  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_uint8(?)", -1, &s, NULL);
  sqlite3_bind_int(s, 1, 256);
  int rc = sqlite3_step(s);
  CHECK("7.3 msgpack_uint8(256) is error", rc == SQLITE_ERROR);
  sqlite3_finalize(s);

  sqlite3_stmt *s2 = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_uint8(?)", -1, &s2, NULL);
  sqlite3_bind_int(s2, 1, -1);
  int rc2 = sqlite3_step(s2);
  CHECK("7.4 msgpack_uint8(-1) is error", rc2 == SQLITE_ERROR);
  sqlite3_finalize(s2);
}

static void test_uint16(sqlite3 *db){
  /* 1000 = 0x03E8 → CD 03 E8 */
  char *r = exec1(db, "SELECT hex(msgpack_uint16(1000))");
  CHECK("8.1 msgpack_uint16(1000) = CD03E8", r && strcmp(r,"CD03E8")==0);
  sqlite3_free(r);

  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_uint16(?)", -1, &s, NULL);
  sqlite3_bind_int(s, 1, 70000);
  int rc = sqlite3_step(s);
  CHECK("8.2 msgpack_uint16(70000) is error", rc == SQLITE_ERROR);
  sqlite3_finalize(s);

  char *t = exec1(db, "SELECT msgpack_type(msgpack_uint16(100))");
  CHECK("8.3 msgpack_uint16 type='integer'", t && strcmp(t,"integer")==0);
  sqlite3_free(t);
}

static void test_uint32(sqlite3 *db){
  /* 4294967295 = 0xFFFFFFFF → CE FF FF FF FF */
  char *r = exec1(db, "SELECT hex(msgpack_uint32(4294967295))");
  CHECK("9.1 msgpack_uint32(UINT32_MAX) = CEFFFFFFFF",
        r && strcmp(r,"CEFFFFFFFF")==0);
  sqlite3_free(r);

  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_uint32(?)", -1, &s, NULL);
  sqlite3_bind_int64(s, 1, 5000000000LL);
  int rc = sqlite3_step(s);
  CHECK("9.2 msgpack_uint32(5e9) is error", rc == SQLITE_ERROR);
  sqlite3_finalize(s);
}

static void test_uint64(sqlite3 *db){
  /* 0 → CF 00 00 00 00 00 00 00 00 */
  char *r = exec1(db, "SELECT hex(msgpack_uint64(0))");
  CHECK("10.1 msgpack_uint64(0) header CF", r && r[0]=='C' && r[1]=='F');
  CHECK("10.2 msgpack_uint64(0) length=9 bytes", r && strlen(r)==18);
  sqlite3_free(r);

  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid(msgpack_uint64(1))");
  CHECK("10.3 msgpack_uint64(1) is valid", v == 1);

  char *t = exec1(db, "SELECT msgpack_type(msgpack_uint64(0))");
  CHECK("10.4 msgpack_uint64 type='integer'", t && strcmp(t,"integer")==0);
  sqlite3_free(t);

  /* large value round-trip (within i64 range: 2^62) */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_extract(msgpack_uint64(?), '$')", -1, &s, NULL);
    sqlite3_bind_int64(s, 1, (sqlite3_int64)4611686018427387904LL); /* 2^62 */
    sqlite3_step(s);
    sqlite3_int64 got = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    CHECK("10.5 msgpack_uint64(2^62) round-trip",
          got == (sqlite3_int64)4611686018427387904LL);
  }
}

/* ── msgpack_bin ─────────────────────────────────────────────────── */
static void test_bin(sqlite3 *db){
  /* Force-wrap valid msgpack nil (0xc0) as bin — must NOT auto-embed */
  unsigned char nil_blob[] = {0xc0};
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT hex(msgpack_bin(?))", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, nil_blob, 1, SQLITE_STATIC);
  sqlite3_step(s);
  const char *hex = (const char*)sqlite3_column_text(s, 0);
  /* bin8 header: C4 01, then data C0 */
  CHECK("11.1 msgpack_bin wraps valid msgpack as bin8",
        hex && strcmp(hex,"C401C0")==0);
  sqlite3_finalize(s);

  /* empty blob → bin8 C4 00 */
  char *r = exec1(db, "SELECT hex(msgpack_bin(x''))");
  CHECK("11.2 msgpack_bin(empty) = C400", r && strcmp(r,"C400")==0);
  sqlite3_free(r);

  /* type of result is 'blob' */
  {
    sqlite3_stmt *s2 = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_type(msgpack_bin(?))", -1, &s2, NULL);
    sqlite3_bind_blob(s2, 1, nil_blob, 1, SQLITE_STATIC);
    sqlite3_step(s2);
    const char *t = (const char*)sqlite3_column_text(s2, 0);
    CHECK("11.3 msgpack_bin result type='blob'", t && strcmp(t,"blob")==0);
    sqlite3_finalize(s2);
  }

  /* compare with msgpack_quote: quote auto-embeds valid msgpack, bin does not */
  {
    sqlite3_stmt *q = NULL;
    sqlite3_prepare_v2(db, "SELECT hex(msgpack_quote(?))", -1, &q, NULL);
    sqlite3_bind_blob(q, 1, nil_blob, 1, SQLITE_STATIC);
    sqlite3_step(q);
    const char *qhex = (const char*)sqlite3_column_text(q, 0);
    /* quote auto-embeds → just "C0"; bin wraps → "C401C0" */
    CHECK("11.4 msgpack_quote auto-embeds nil, msgpack_bin does not",
          qhex && strcmp(qhex,"C0")==0);
    sqlite3_finalize(q);
  }
}

/* ── msgpack_ext ─────────────────────────────────────────────────── */
static void test_ext(sqlite3 *db){
  /* fixext1: 1-byte payload → D4 <type> <data> */
  {
    unsigned char data1[] = {0xAB};
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT hex(msgpack_ext(?,?))", -1, &s, NULL);
    sqlite3_bind_int(s, 1, 5);
    sqlite3_bind_blob(s, 2, data1, 1, SQLITE_STATIC);
    sqlite3_step(s);
    const char *h = (const char*)sqlite3_column_text(s, 0);
    CHECK("12.1 msgpack_ext fixext1 header D4", h && h[0]=='D' && h[1]=='4');
    CHECK("12.2 msgpack_ext fixext1 = D405AB", h && strcmp(h,"D405AB")==0);
    sqlite3_finalize(s);
  }

  /* fixext2: 2-byte payload → D5 <type> <data*2> */
  {
    unsigned char data2[] = {0x01, 0x02};
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT hex(msgpack_ext(?,?))", -1, &s, NULL);
    sqlite3_bind_int(s, 1, 1);
    sqlite3_bind_blob(s, 2, data2, 2, SQLITE_STATIC);
    sqlite3_step(s);
    const char *h = (const char*)sqlite3_column_text(s, 0);
    CHECK("12.3 msgpack_ext fixext2 = D5010102", h && strcmp(h,"D5010102")==0);
    sqlite3_finalize(s);
  }

  /* ext8: 3-byte payload → C7 03 <type> <data*3> */
  {
    unsigned char data3[] = {0x0A, 0x0B, 0x0C};
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT hex(msgpack_ext(?,?))", -1, &s, NULL);
    sqlite3_bind_int(s, 1, 7);
    sqlite3_bind_blob(s, 2, data3, 3, SQLITE_STATIC);
    sqlite3_step(s);
    const char *h = (const char*)sqlite3_column_text(s, 0);
    CHECK("12.4 msgpack_ext ext8 = C703070A0B0C", h && strcmp(h,"C703070A0B0C")==0);
    sqlite3_finalize(s);
  }

  /* all ext results are valid msgpack */
  {
    unsigned char data4[] = {0xFF};
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_valid(msgpack_ext(?,?))", -1, &s, NULL);
    sqlite3_bind_int(s, 1, 0);
    sqlite3_bind_blob(s, 2, data4, 1, SQLITE_STATIC);
    sqlite3_step(s);
    int v = sqlite3_column_int(s, 0);
    CHECK("12.5 msgpack_ext result is valid", v == 1);
    sqlite3_finalize(s);
  }

  /* type_code out of range */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_ext(?,x'01')", -1, &s, NULL);
    sqlite3_bind_int(s, 1, 200);
    int rc = sqlite3_step(s);
    CHECK("12.6 msgpack_ext type_code=200 is error", rc == SQLITE_ERROR);
    sqlite3_finalize(s);
  }

  /* type = 'ext' */
  {
    unsigned char data1[] = {0x00};
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_type(msgpack_ext(?,?))", -1, &s, NULL);
    sqlite3_bind_int(s, 1, 0);
    sqlite3_bind_blob(s, 2, data1, 1, SQLITE_STATIC);
    sqlite3_step(s);
    const char *t = (const char*)sqlite3_column_text(s, 0);
    CHECK("12.7 msgpack_ext type='ext'", t && strcmp(t,"ext")==0);
    sqlite3_finalize(s);
  }
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){
    fprintf(stderr, "open failed\n"); return 1;
  }
#ifdef SQLITE_CORE
  { char *e = NULL; sqlite3_msgpack_init(db, &e, NULL); sqlite3_free(e); }
#endif

  test_nil(db);
  test_bool(db);
  test_float32(db);
  test_int8(db);
  test_int16(db);
  test_int32(db);
  test_uint8(db);
  test_uint16(db);
  test_uint32(db);
  test_uint64(db);
  test_bin(db);
  test_ext(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
