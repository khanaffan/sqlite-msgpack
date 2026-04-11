/*
** test_spec_p10_timestamp.c — Phase 10: Timestamp (ext type -1)
**
** Verifies msgpack_timestamp(), msgpack_timestamp_s(), msgpack_timestamp_ns(),
** and the automatic decoding/type-detection of all three timestamp formats:
**
**   ts32  fixext4  0xFF  4-byte uint32 seconds          (D6 FF xxxxxxxx)
**   ts64  fixext8  0xFF  nsec(30)|sec(34) packed u64    (D7 FF xxxxxxxxxxxxxxxx)
**   ts96  ext8/12  0xFF  4-byte nsec + 8-byte int64 sec (C7 0C FF nnnnnnnn ssssssssssssssss)
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
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -999;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return -999;
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}
static double exec1d(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = NULL; double v = -1.0;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return -1.0;
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_double(s, 0);
  sqlite3_finalize(s); return v;
}

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── ts32 encoding ─────────────────────────────────────────────── */
static void test_ts32(sqlite3 *db){
  /* Unix epoch (sec=0, nsec=0) → D6 FF 00 00 00 00 */
  char *r = exec1(db, "SELECT hex(msgpack_timestamp(0))");
  CHECK("1.1 ts32(0) = D6FF00000000", r && strcmp(r,"D6FF00000000")==0);
  sqlite3_free(r);

  /* sec=1 → D6 FF 00 00 00 01 */
  char *r1 = exec1(db, "SELECT hex(msgpack_timestamp(1))");
  CHECK("1.2 ts32(1) = D6FF00000001", r1 && strcmp(r1,"D6FF00000001")==0);
  sqlite3_free(r1);

  /* UINT32_MAX = 4294967295 → D6 FF FF FF FF FF */
  char *rmax = exec1(db, "SELECT hex(msgpack_timestamp(4294967295))");
  CHECK("1.3 ts32(UINT32_MAX) = D6FFFFFFFFFF",
        rmax && strcmp(rmax,"D6FFFFFFFFFF")==0);
  sqlite3_free(rmax);

  /* 6 bytes total */
  sqlite3_int64 len = exec1i(db, "SELECT length(msgpack_timestamp(0))");
  CHECK("1.4 ts32 is 6 bytes", len == 6);

  /* valid msgpack */
  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid(msgpack_timestamp(0))");
  CHECK("1.5 ts32 is valid", v == 1);

  /* type = 'timestamp' */
  char *t = exec1(db, "SELECT msgpack_type(msgpack_timestamp(0))");
  CHECK("1.6 ts32 type='timestamp'", t && strcmp(t,"timestamp")==0);
  sqlite3_free(t);
}

/* ── ts64 encoding ─────────────────────────────────────────────── */
static void test_ts64(sqlite3 *db){
  /* UINT32_MAX+1 = 4294967296 → must use ts64 (fixext8) */
  char *r = exec1(db, "SELECT hex(msgpack_timestamp(4294967296))");
  CHECK("2.1 ts64(2^32) header D7FF", r && r[0]=='D' && r[1]=='7');
  CHECK("2.2 ts64(2^32) length=20 hex chars (10 bytes)", r && strlen(r)==20);
  sqlite3_free(r);

  /* REAL with sub-second → ts64 */
  char *rf = exec1(db, "SELECT hex(msgpack_timestamp(1.5))");
  CHECK("2.3 ts64(1.5) header D7FF", rf && rf[0]=='D' && rf[1]=='7');
  sqlite3_free(rf);

  /* valid */
  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid(msgpack_timestamp(1.5))");
  CHECK("2.4 ts64(1.5) is valid", v == 1);

  /* type = 'timestamp' */
  char *t = exec1(db, "SELECT msgpack_type(msgpack_timestamp(1.5))");
  CHECK("2.5 ts64 type='timestamp'", t && strcmp(t,"timestamp")==0);
  sqlite3_free(t);
}

/* ── ts96 encoding ─────────────────────────────────────────────── */
static void test_ts96(sqlite3 *db){
  /* Negative timestamp (pre-epoch: -1 second) → ts96 (ext8/12) */
  char *r = exec1(db, "SELECT hex(msgpack_timestamp(-1))");
  /* ts96: C7 0C FF + 4-byte nsec(0) + 8-byte sec(-1=0xFFFFFFFFFFFFFFFF) */
  CHECK("3.1 ts96(-1) starts C70CFF", r && strncmp(r,"C70CFF",6)==0);
  CHECK("3.2 ts96(-1) is 15 bytes (30 hex)", r && strlen(r)==30);
  sqlite3_free(r);

  /* valid */
  sqlite3_int64 v = exec1i(db, "SELECT msgpack_valid(msgpack_timestamp(-1))");
  CHECK("3.3 ts96(-1) is valid", v == 1);

  /* type = 'timestamp' */
  char *t = exec1(db, "SELECT msgpack_type(msgpack_timestamp(-1))");
  CHECK("3.4 ts96 type='timestamp'", t && strcmp(t,"timestamp")==0);
  sqlite3_free(t);
}

/* ── msgpack_timestamp_s and _ns ───────────────────────────────── */
static void test_ts_accessors(sqlite3 *db){
  /* ts32 seconds round-trip */
  sqlite3_int64 s0 = exec1i(db,
    "SELECT msgpack_timestamp_s(msgpack_timestamp(1000))");
  CHECK("4.1 ts32 _s round-trip", s0 == 1000);

  /* ts32 nanoseconds = 0 */
  sqlite3_int64 ns0 = exec1i(db,
    "SELECT msgpack_timestamp_ns(msgpack_timestamp(1000))");
  CHECK("4.2 ts32 _ns = 0", ns0 == 0);

  /* ts64 seconds round-trip */
  sqlite3_int64 s64 = exec1i(db,
    "SELECT msgpack_timestamp_s(msgpack_timestamp(1.5))");
  CHECK("4.3 ts64 _s = 1", s64 == 1);

  /* ts64 nanoseconds round-trip (1.5 s → 500000000 ns) */
  sqlite3_int64 ns64 = exec1i(db,
    "SELECT msgpack_timestamp_ns(msgpack_timestamp(1.5))");
  CHECK("4.4 ts64 _ns = 500000000", ns64 == 500000000);

  /* ts96 negative seconds */
  sqlite3_int64 sneg = exec1i(db,
    "SELECT msgpack_timestamp_s(msgpack_timestamp(-1))");
  CHECK("4.5 ts96 _s = -1", sneg == -1);

  /* _s/_ns on non-timestamp returns error */
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db,
    "SELECT msgpack_timestamp_s(msgpack_quote(42))", -1, &s, NULL);
  int rc = sqlite3_step(s);
  CHECK("4.6 _s on non-timestamp is error", rc == SQLITE_ERROR);
  sqlite3_finalize(s);
}

/* ── msgpack_extract decoding ──────────────────────────────────── */
static void test_extract(sqlite3 *db){
  /* ts32: extract returns INTEGER */
  sqlite3_int64 ex32 = exec1i(db,
    "SELECT msgpack_extract(msgpack_timestamp(42), '$')");
  CHECK("5.1 extract ts32 = 42", ex32 == 42);

  /* ts64 with sub-second: extract returns REAL */
  double ex64 = exec1d(db,
    "SELECT msgpack_extract(msgpack_timestamp(1.25), '$')");
  CHECK("5.2 extract ts64 ≈ 1.25", fabs(ex64 - 1.25) < 1e-6);

  /* ts96 negative: extract returns INTEGER */
  sqlite3_int64 ex96 = exec1i(db,
    "SELECT msgpack_extract(msgpack_timestamp(-100), '$')");
  CHECK("5.3 extract ts96 = -100", ex96 == -100);
}

/* ── msgpack_to_json ISO 8601 output ───────────────────────────── */
static void test_to_json(sqlite3 *db){
  /* Unix epoch → "1970-01-01T00:00:00Z" */
  char *j = exec1(db,
    "SELECT msgpack_to_json(msgpack_timestamp(0))");
  CHECK("6.1 to_json ts32(0) = \"1970-01-01T00:00:00Z\"",
        j && strcmp(j,"\"1970-01-01T00:00:00Z\"")==0);
  sqlite3_free(j);

  /* 2001-09-09T01:46:40Z = 1000000000 seconds */
  char *j2 = exec1(db,
    "SELECT msgpack_to_json(msgpack_timestamp(1000000000))");
  CHECK("6.2 to_json ts32(1e9) contains T and Z",
        j2 && strchr(j2,'T') && strchr(j2,'Z'));
  sqlite3_free(j2);

  /* ts64 with nanoseconds includes fractional part */
  char *j3 = exec1(db,
    "SELECT msgpack_to_json(msgpack_timestamp(1.5))");
  CHECK("6.3 to_json ts64(1.5) contains fractional seconds",
        j3 && strchr(j3,'.'));
  sqlite3_free(j3);
}

/* ── error handling ────────────────────────────────────────────── */
static void test_errors(sqlite3 *db){
  /* TEXT input is not accepted */
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_timestamp('2024-01-01')", -1, &s, NULL);
  int rc = sqlite3_step(s);
  CHECK("7.1 timestamp(text) is error", rc == SQLITE_ERROR);
  sqlite3_finalize(s);
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){
    fprintf(stderr, "open failed\n"); return 1;
  }
#ifdef SQLITE_CORE
  { char *e = NULL; sqlite3_msgpack_init(db, &e, NULL); sqlite3_free(e); }
#endif

  test_ts32(db);
  test_ts64(db);
  test_ts96(db);
  test_ts_accessors(db);
  test_extract(db);
  test_to_json(db);
  test_errors(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
