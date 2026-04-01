/*
** test_spec_p2_scalars.c — Phase 2: Float / String / Binary encoding
**
** Verifies float32 (0xca), float64 (0xcb), fixstr, str8, str16,
** bin8, bin16 per the MessagePack specification.
**
** Uses a mix of:
**   • SQL text literals  (exec1 helper)
**   • sqlite3_bind_double / bind_text / bind_blob
**   • sqlite3_column_* for result verification
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

/* Bind a double → msgpack_quote → hex string; caller sqlite3_free() */
static char *bind_dbl_hex(sqlite3 *db, double val){
  sqlite3_stmt *s = NULL; char *r = NULL;
  sqlite3_prepare_v2(db, "SELECT hex(msgpack_quote(?))", -1, &s, NULL);
  sqlite3_bind_double(s, 1, val);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* Bind a text value → msgpack_quote → hex string; caller sqlite3_free() */
static char *bind_text_hex(sqlite3 *db, const char *text, int n){
  sqlite3_stmt *s = NULL; char *r = NULL;
  sqlite3_prepare_v2(db, "SELECT hex(msgpack_quote(?))", -1, &s, NULL);
  sqlite3_bind_text(s, 1, text, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* Bind a blob → msgpack_quote → hex string; caller sqlite3_free() */
static char *bind_blob_hex(sqlite3 *db, const void *blob, int n){
  sqlite3_stmt *s = NULL; char *r = NULL;
  sqlite3_prepare_v2(db, "SELECT hex(msgpack_quote(?))", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* Bind a blob to msgpack_type(?); caller sqlite3_free() */
static char *bind_blob_type(sqlite3 *db, const void *blob, int n){
  sqlite3_stmt *s = NULL; char *r = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_type(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const char *z = (const char*)sqlite3_column_text(s, 0);
    if(z) r = sqlite3_mprintf("%s", z);
  }
  sqlite3_finalize(s); return r;
}

/* Encode val via bind; decode via msgpack_extract('$'); return double */
static double bind_dbl_roundtrip(sqlite3 *db, double val){
  /* encode */
  sqlite3_stmt *enc = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_quote(?)", -1, &enc, NULL);
  sqlite3_bind_double(enc, 1, val);
  sqlite3_step(enc);
  const void *blob = sqlite3_column_blob(enc, 0);
  int n = sqlite3_column_bytes(enc, 0);
  unsigned char buf[16]; memcpy(buf, blob, n); sqlite3_finalize(enc);

  /* decode */
  sqlite3_stmt *dec = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?, '$')", -1, &dec, NULL);
  sqlite3_bind_blob(dec, 1, buf, n, SQLITE_STATIC);
  double got = 0.0;
  if(sqlite3_step(dec) == SQLITE_ROW) got = sqlite3_column_double(dec, 0);
  sqlite3_finalize(dec); return got;
}

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── float tests ──────────────────────────────────────────────── */

static void test_float64(sqlite3 *db){
  /*
  ** Spec: float 64 = 0xcb + 8 bytes IEEE-754 double, big-endian.
  ** The extension always encodes REAL as float64.
  */

  /* 0.0 → CB 00 00 00 00 00 00 00 00 */
  {
    char *r = bind_dbl_hex(db, 0.0);
    CHECK("1.1 float64 0.0 header (bind)", r && r[0]=='C' && r[1]=='B');
    if(r){ CHECK("1.2 float64 0.0 length=9 bytes → hex len=18", r && strlen(r)==18); }
    sqlite3_free(r);
  }

  /* SQL text path for 3.14 */
  {
    char *r = exec1(db, "SELECT hex(msgpack_quote(3.14))");
    CHECK("1.3 float64 3.14 header (SQL text)", r && r[0]=='C' && r[1]=='B');
    sqlite3_free(r);
  }

  /* type == 'real' */
  {
    char *r = exec1(db, "SELECT msgpack_type(msgpack_quote(1.5))");
    CHECK("1.4 float64 type='real' (SQL text)", r && strcmp(r,"real")==0);
    sqlite3_free(r);
  }

  /* bind double → type via blob */
  {
    sqlite3_stmt *enc = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_quote(?)", -1, &enc, NULL);
    sqlite3_bind_double(enc, 1, 2.718281828);
    sqlite3_step(enc);
    const void *b = sqlite3_column_blob(enc, 0);
    int n = sqlite3_column_bytes(enc, 0);
    char *t = bind_blob_type(db, b, n);
    CHECK("1.5 float64 type via bind='real'", t && strcmp(t,"real")==0);
    sqlite3_free(t); sqlite3_finalize(enc);
  }

  /* round-trips */
  double vals[] = {0.0, 1.0, -1.0, 3.14159265358979, 1e300, -1e-300};
  int i;
  for(i = 0; i < 6; i++){
    double back = bind_dbl_roundtrip(db, vals[i]);
    char label[64]; sqlite3_snprintf(64, label, "1.%d float64 roundtrip %.6g", 6+i, vals[i]);
    CHECK(label, back == vals[i]);
  }

  /* length always 9 bytes */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT length(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_double(s, 1, 42.5);
    sqlite3_step(s);
    int n = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);
    CHECK("1.13 float64 always 9 bytes", n == 9);
  }
}

static void test_float_special(sqlite3 *db){
  /* NaN and Infinity encode as float64 blobs (valid msgpack) but convert
     to JSON null per the extension's design */
  double nan_val  = 0.0 / 0.0;
  double inf_val  = 1.0 / 0.0;
  double ninf_val = -1.0 / 0.0;

  /* They produce valid msgpack */
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_valid(msgpack_quote(?))", -1, &s, NULL);
  sqlite3_bind_double(s, 1, nan_val);
  sqlite3_step(s); int vn = sqlite3_column_int(s, 0); sqlite3_finalize(s);
  CHECK("2.1 NaN encodes to valid msgpack", vn == 1);

  /* to_json converts NaN → "null" */
  {
    char *r = exec1(db, "SELECT msgpack_to_json(msgpack_from_json('1e999'))");
    /* 1e999 parses as Infinity in JSON, should come back as null */
    CHECK("2.2 Infinity → JSON null", r && strcmp(r,"null")==0);
    sqlite3_free(r);
  }
  (void)inf_val; (void)ninf_val;
}

/* ── string tests ──────────────────────────────────────────────── */

static void test_fixstr(sqlite3 *db){
  /* fixstr: 0xa0 | len, followed by len bytes (0–31 bytes) */

  /* empty string: 0xa0 */
  {
    char *r = bind_text_hex(db, "", 0);
    CHECK("3.1 fixstr empty hex=A0 (bind)", r && strcmp(r,"A0")==0); sqlite3_free(r);
    char *r2 = exec1(db, "SELECT hex(msgpack_quote(''))");
    CHECK("3.2 fixstr empty hex=A0 (SQL)", r2 && strcmp(r2,"A0")==0); sqlite3_free(r2);
  }

  /* "hello" = 5 chars → 0xa5 + 68656c6c6f */
  {
    char *r = bind_text_hex(db, "hello", -1);
    CHECK("3.3 fixstr 'hello' (bind)", r && strcmp(r,"A568656C6C6F")==0); sqlite3_free(r);
  }

  /* 31-byte string → 0xbf + data */
  {
    char buf32[32]; memset(buf32, 'x', 31); buf32[31] = '\0';
    char *r = bind_text_hex(db, buf32, 31);
    CHECK("3.4 fixstr 31 bytes header BF (bind)", r && r[0]=='B' && r[1]=='F');
    sqlite3_free(r);
  }

  /* type = 'text' */
  {
    char *r = exec1(db, "SELECT msgpack_type(msgpack_quote('hi'))");
    CHECK("3.5 fixstr type='text'", r && strcmp(r,"text")==0); sqlite3_free(r);
  }

  /* round-trip text */
  {
    sqlite3_stmt *enc = NULL, *dec = NULL;
    const char *orig = "round-trip me";
    sqlite3_prepare_v2(db, "SELECT msgpack_quote(?)", -1, &enc, NULL);
    sqlite3_bind_text(enc, 1, orig, -1, SQLITE_STATIC);
    sqlite3_step(enc);
    const void *b = sqlite3_column_blob(enc, 0); int n = sqlite3_column_bytes(enc, 0);
    unsigned char buf[64]; memcpy(buf, b, n); sqlite3_finalize(enc);

    sqlite3_prepare_v2(db, "SELECT msgpack_extract(?, '$')", -1, &dec, NULL);
    sqlite3_bind_blob(dec, 1, buf, n, SQLITE_STATIC);
    sqlite3_step(dec);
    const char *got = (const char*)sqlite3_column_text(dec, 0);
    CHECK("3.6 fixstr round-trip (bind enc+dec)", got && strcmp(got, orig)==0);
    sqlite3_finalize(dec);
  }
}

static void test_str8(sqlite3 *db){
  /* str8: 0xd9 + 1-byte-len + data — covers 32..255 byte strings */

  /* 32-byte string → str8 header D9 20 */
  {
    char buf[33]; memset(buf, 'a', 32); buf[32] = '\0';
    char *r = bind_text_hex(db, buf, 32);
    /* First 4 hex chars should be "D920" */
    CHECK("4.1 str8 32-byte header D920 (bind)", r && strncmp(r,"D920",4)==0);
    sqlite3_free(r);
  }

  /* 255-byte string → str8 header D9 FF */
  {
    char *buf = sqlite3_malloc(256); memset(buf, 'b', 255); buf[255] = '\0';
    char *r = bind_text_hex(db, buf, 255);
    CHECK("4.2 str8 255-byte header D9FF (bind)", r && strncmp(r,"D9FF",4)==0);
    sqlite3_free(r); sqlite3_free(buf);
  }

  /* type = 'text' */
  {
    char *buf = sqlite3_malloc(100); memset(buf, 'c', 99); buf[99] = '\0';
    sqlite3_stmt *enc = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_type(msgpack_quote(?))", -1, &enc, NULL);
    sqlite3_bind_text(enc, 1, buf, 99, SQLITE_STATIC);
    sqlite3_step(enc);
    const char *t = (const char*)sqlite3_column_text(enc, 0);
    CHECK("4.3 str8 type='text'", t && strcmp(t,"text")==0);
    sqlite3_finalize(enc); sqlite3_free(buf);
  }
}

static void test_str16(sqlite3 *db){
  /* str16: 0xda + 2-byte-len + data — covers 256..65535 bytes */

  /* 256-byte string → DA 01 00 + data */
  {
    char *buf = sqlite3_malloc(257); memset(buf, 'd', 256); buf[256] = '\0';
    char *r = bind_text_hex(db, buf, 256);
    CHECK("5.1 str16 256-byte header DA0100 (bind)", r && strncmp(r,"DA0100",6)==0);
    sqlite3_free(r); sqlite3_free(buf);
  }

  /* valid check */
  {
    char *buf = sqlite3_malloc(300); memset(buf, 'e', 299); buf[299] = '\0';
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_valid(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_text(s, 1, buf, 299, SQLITE_STATIC);
    sqlite3_step(s);
    int v = sqlite3_column_int(s, 0);
    CHECK("5.2 str16 is valid msgpack", v == 1);
    sqlite3_finalize(s); sqlite3_free(buf);
  }
}

/* ── binary tests ──────────────────────────────────────────────── */

static void test_bin8(sqlite3 *db){
  /* bin8: 0xc4 + 1-byte-len + data — raw BLOB not itself valid msgpack */
  /* A raw byte like 0xDE is not valid msgpack on its own */
  unsigned char raw[] = {0xDE, 0xAD, 0xBE, 0xEF};

  {
    char *r = bind_blob_hex(db, raw, 4);
    /* bin8 header = C4 04 then DEADBEEF */
    CHECK("6.1 bin8 header C404 (bind)", r && strncmp(r,"C404",4)==0);
    CHECK("6.2 bin8 data DEADBEEF", r && strncmp(r+4,"DEADBEEF",8)==0);
    sqlite3_free(r);
  }

  /* type = 'blob' */
  {
    char *t = bind_blob_type(db, raw, 4);
    /* The raw bytes are invalid msgpack, so they get wrapped in bin8;
       msgpack_type of that bin8 blob is 'blob' */
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_type(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, raw, 4, SQLITE_STATIC);
    sqlite3_step(s);
    const char *tt = (const char*)sqlite3_column_text(s, 0);
    CHECK("6.3 bin8 type='blob'", tt && strcmp(tt,"blob")==0);
    sqlite3_finalize(s);
    sqlite3_free(t);
  }

  /* valid msgpack BLOB auto-embeds (not re-wrapped in bin) */
  {
    unsigned char valid_nil[] = {0xc0}; /* nil — valid msgpack */
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_type(msgpack_quote(?))", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, valid_nil, 1, SQLITE_STATIC);
    sqlite3_step(s);
    const char *tt = (const char*)sqlite3_column_text(s, 0);
    CHECK("6.4 valid msgpack blob auto-embeds → type='null'", tt && strcmp(tt,"null")==0);
    sqlite3_finalize(s);
  }

  /* empty binary blob → bin8 header C4 00 */
  {
    char *r = bind_blob_hex(db, "", 0);
    CHECK("6.5 bin8 empty header C400", r && strncmp(r,"C400",4)==0);
    sqlite3_free(r);
  }

  /* bin8 boundary: 255-byte raw blob */
  {
    unsigned char *buf = sqlite3_malloc(255);
    int i; for(i=0; i<255; i++) buf[i] = (unsigned char)(i & 0xff);
    char *r = bind_blob_hex(db, buf, 255);
    /* header = C4 FF */
    CHECK("6.6 bin8 255-byte header C4FF", r && strncmp(r,"C4FF",4)==0);
    sqlite3_free(r); sqlite3_free(buf);
  }
}

static void test_bin16(sqlite3 *db){
  /* bin16: 0xc5 + 2-byte-len + data — covers 256..65535 bytes of raw binary */
  unsigned char *buf = sqlite3_malloc(256);
  int i; for(i=0; i<256; i++) buf[i] = (unsigned char)(i & 0xff);

  char *r = bind_blob_hex(db, buf, 256);
  /* bin16 header = C5 01 00 */
  CHECK("7.1 bin16 256-byte header C50100", r && strncmp(r,"C50100",6)==0);
  sqlite3_free(r);

  /* valid */
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_valid(msgpack_quote(?))", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, buf, 256, SQLITE_STATIC);
  sqlite3_step(s);
  int v = sqlite3_column_int(s, 0);
  CHECK("7.2 bin16 is valid msgpack", v == 1);
  sqlite3_finalize(s);

  sqlite3_free(buf);
}

static void test_msgpack_valid_2arg(sqlite3 *db){
  /* msgpack_valid accepts an optional second flags argument (reserved) */
  sqlite3_int64 v = exec1i(db,
      "SELECT msgpack_valid(msgpack_quote(99), 0)");
  CHECK("8.1 msgpack_valid 2-arg (flags=0)", v == 1);
}

static void test_error_position(sqlite3 *db){
  /* valid → 0 */
  sqlite3_int64 ep = exec1i(db,
      "SELECT msgpack_error_position(msgpack_quote(42))");
  CHECK("9.1 error_position valid=0", ep == 0);

  /* truncated array header: 0x92 says 2 elements but has 0 */
  unsigned char trunc[] = {0x92};
  sqlite3_stmt *s = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_error_position(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, trunc, 1, SQLITE_STATIC);
  sqlite3_step(s);
  sqlite3_int64 ep2 = sqlite3_column_int64(s, 0);
  CHECK("9.2 error_position truncated array > 0", ep2 > 0);
  sqlite3_finalize(s);

  /* empty blob → error at position 1 */
  sqlite3_stmt *s2 = NULL;
  sqlite3_prepare_v2(db, "SELECT msgpack_error_position(?)", -1, &s2, NULL);
  sqlite3_bind_blob(s2, 1, "", 0, SQLITE_STATIC);
  sqlite3_step(s2);
  sqlite3_int64 ep3 = sqlite3_column_int64(s2, 0);
  CHECK("9.3 error_position empty blob = 1", ep3 == 1);
  sqlite3_finalize(s2);
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){ fprintf(stderr,"open failed\n"); return 1; }
#ifdef SQLITE_CORE
  { char *e = NULL; sqlite3_msgpack_init(db, &e, NULL); sqlite3_free(e); }
#endif

  test_float64(db);
  test_float_special(db);
  test_fixstr(db);
  test_str8(db);
  test_str16(db);
  test_bin8(db);
  test_bin16(db);
  test_msgpack_valid_2arg(db);
  test_error_position(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
