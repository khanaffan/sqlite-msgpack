/*
** test_spec_p5_mutation.c — Phase 5: Mutation functions
**
** Covers all 6 mutation functions:
**   msgpack_set         — create or overwrite
**   msgpack_insert      — create only if absent
**   msgpack_replace     — overwrite only if present
**   msgpack_remove      — remove one or many paths
**   msgpack_array_insert — insert before array index
**   msgpack_patch       — RFC-7386 merge-patch
**
** Mix of SQL text literals and sqlite3_bind_blob/text/int for inputs.
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

/* Build blob from SQL; caller sqlite3_free() */
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

/* msgpack_array_length(blob) via bind */
static sqlite3_int64 arr_len(sqlite3 *db, const void *b, int n){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -1;
  sqlite3_prepare_v2(db, "SELECT msgpack_array_length(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

/* msgpack_type(blob) via bind; caller sqlite3_free() */
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

/*
** Apply a single msgpack_set(blob, path, val) where val is an integer.
** Returns new blob; caller sqlite3_free().
*/
static unsigned char *apply_set_i(sqlite3 *db,
    const void *src, int nsrc, const char *path, sqlite3_int64 val, int *pnOut){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pnOut = 0;
  sqlite3_prepare_v2(db, "SELECT msgpack_set(?,?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, src, nsrc, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  sqlite3_bind_int64(s, 3, val);
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pnOut = n; }
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_insert(blob, path, int_val) → new blob; caller sqlite3_free() */
static unsigned char *apply_insert_i(sqlite3 *db,
    const void *src, int nsrc, const char *path, sqlite3_int64 val, int *pnOut){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pnOut = 0;
  sqlite3_prepare_v2(db, "SELECT msgpack_insert(?,?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, src, nsrc, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  sqlite3_bind_int64(s, 3, val);
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pnOut = n; }
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_replace(blob, path, int_val) → new blob; caller sqlite3_free() */
static unsigned char *apply_replace_i(sqlite3 *db,
    const void *src, int nsrc, const char *path, sqlite3_int64 val, int *pnOut){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pnOut = 0;
  sqlite3_prepare_v2(db, "SELECT msgpack_replace(?,?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, src, nsrc, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  sqlite3_bind_int64(s, 3, val);
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pnOut = n; }
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_remove(blob, path) → new blob; caller sqlite3_free() */
static unsigned char *apply_remove(sqlite3 *db,
    const void *src, int nsrc, const char *path, int *pnOut){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pnOut = 0;
  sqlite3_prepare_v2(db, "SELECT msgpack_remove(?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, src, nsrc, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pnOut = n; }
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_array_insert(blob, path, val) → new blob; caller sqlite3_free() */
static unsigned char *apply_array_insert_i(sqlite3 *db,
    const void *src, int nsrc, const char *path, sqlite3_int64 val, int *pnOut){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pnOut = 0;
  sqlite3_prepare_v2(db, "SELECT msgpack_array_insert(?,?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, src, nsrc, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC);
  sqlite3_bind_int64(s, 3, val);
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pnOut = n; }
  }
  sqlite3_finalize(s); return r;
}

/* msgpack_patch(target, patch) → new blob; caller sqlite3_free() */
static unsigned char *apply_patch(sqlite3 *db,
    const void *target, int ntarget,
    const void *patch, int npatch, int *pnOut){
  sqlite3_stmt *s = NULL; unsigned char *r = NULL; *pnOut = 0;
  sqlite3_prepare_v2(db, "SELECT msgpack_patch(?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, target, ntarget, SQLITE_STATIC);
  sqlite3_bind_blob(s, 2, patch,  npatch,  SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW){
    const void *b = sqlite3_column_blob(s, 0); int n = sqlite3_column_bytes(s, 0);
    if(b && n>0){ r = sqlite3_malloc(n); memcpy(r, b, n); *pnOut = n; }
  }
  sqlite3_finalize(s); return r;
}

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── msgpack_set ─────────────────────────────────────────────────── */

static void test_set(sqlite3 *db){
  /* Overwrite existing map key */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('x',1,'y',2)", &n);
    int nr = 0; unsigned char *result = apply_set_i(db, src, n, "$.x", 99, &nr);
    CHECK("1.1 set overwrites existing map key (bind)", result && extract_i(db, result, nr, "$.x") == 99);
    CHECK("1.2 set: other key preserved", result && extract_i(db, result, nr, "$.y") == 2);
    CHECK("1.3 set: map length unchanged", result && arr_len(db, result, nr) == 2);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Create new map key */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('a',1)", &n);
    int nr = 0; unsigned char *result = apply_set_i(db, src, n, "$.b", 42, &nr);
    CHECK("1.4 set creates new key", result && extract_i(db, result, nr, "$.b") == 42);
    CHECK("1.5 set: old key preserved", result && extract_i(db, result, nr, "$.a") == 1);
    CHECK("1.6 set: map length = 2", result && arr_len(db, result, nr) == 2);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Overwrite array element */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_array(10,20,30)", &n);
    int nr = 0; unsigned char *result = apply_set_i(db, src, n, "$[1]", 99, &nr);
    CHECK("1.7 set overwrites array element", result && extract_i(db, result, nr, "$[1]") == 99);
    CHECK("1.8 set: array length preserved", result && arr_len(db, result, nr) == 3);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Chained set via SQL text */
  {
    char *r = exec1(db,
      "SELECT msgpack_extract("
      "  msgpack_set(msgpack_object('a',1,'b',2), '$.a',10,'$.b',20),"
      "  '$.b')");
    CHECK("1.9 chained set $.b=20 (SQL text)", r && strcmp(r,"20")==0); sqlite3_free(r);
  }

  /* set with text value via bind */
  {
    int n = 0; unsigned char *src = build_blob(db, "SELECT msgpack_object('k','old')", &n);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_set(?,?,?)", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, src, n, SQLITE_STATIC);
    sqlite3_bind_text(s, 2, "$.k", -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 3, "new", -1, SQLITE_STATIC);
    sqlite3_step(s);
    const void *b = sqlite3_column_blob(s, 0); int nr = sqlite3_column_bytes(s, 0);
    unsigned char *res = sqlite3_malloc(nr); memcpy(res, b, nr);
    sqlite3_finalize(s);
    char *t = NULL;
    sqlite3_stmt *ex = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_extract(?, '$.k')", -1, &ex, NULL);
    sqlite3_bind_blob(ex, 1, res, nr, SQLITE_STATIC);
    sqlite3_step(ex);
    const char *z = (const char*)sqlite3_column_text(ex, 0);
    if(z) t = sqlite3_mprintf("%s", z);
    sqlite3_finalize(ex);
    CHECK("1.10 set text value via bind", t && strcmp(t,"new")==0);
    sqlite3_free(t); sqlite3_free(res); sqlite3_free(src);
  }
}

/* ── msgpack_insert ──────────────────────────────────────────────── */

static void test_insert(sqlite3 *db){
  /* Insert absent key → creates it */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('a',1)", &n);
    int nr = 0; unsigned char *result = apply_insert_i(db, src, n, "$.b", 99, &nr);
    CHECK("2.1 insert creates absent key", result && extract_i(db, result, nr, "$.b") == 99);
    CHECK("2.2 insert: map length = 2", result && arr_len(db, result, nr) == 2);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Insert existing key → no-op */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('a',1)", &n);
    int nr = 0; unsigned char *result = apply_insert_i(db, src, n, "$.a", 99, &nr);
    CHECK("2.3 insert existing key is no-op", result && extract_i(db, result, nr, "$.a") == 1);
    CHECK("2.4 insert no-op: map length = 1", result && arr_len(db, result, nr) == 1);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* SQL text verification */
  {
    char *r = exec1(db,
      "SELECT msgpack_extract(msgpack_insert(msgpack_object('x',5), '$.x', 99), '$.x')");
    CHECK("2.5 insert existing (SQL text) → original value=5", r && strcmp(r,"5")==0);
    sqlite3_free(r);
  }
}

/* ── msgpack_replace ─────────────────────────────────────────────── */

static void test_replace(sqlite3 *db){
  /* Replace existing key */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('x',5)", &n);
    int nr = 0; unsigned char *result = apply_replace_i(db, src, n, "$.x", 50, &nr);
    CHECK("3.1 replace existing key", result && extract_i(db, result, nr, "$.x") == 50);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Replace missing key → no-op (length unchanged) */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('x',5)", &n);
    int nr = 0; unsigned char *result = apply_replace_i(db, src, n, "$.y", 50, &nr);
    CHECK("3.2 replace missing key is no-op", result && arr_len(db, result, nr) == 1);
    CHECK("3.3 replace no-op: $.x unchanged", result && extract_i(db, result, nr, "$.x") == 5);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Replace array element */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_array(10,20,30)", &n);
    int nr = 0; unsigned char *result = apply_replace_i(db, src, n, "$[0]", 99, &nr);
    CHECK("3.4 replace array element $[0]", result && extract_i(db, result, nr, "$[0]") == 99);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* SQL text verification */
  {
    sqlite3_int64 v = exec1i(db,
      "SELECT msgpack_array_length(msgpack_replace(msgpack_object('x',5), '$.y', 50))");
    CHECK("3.5 replace missing (SQL text) map length unchanged=1", v == 1);
  }
}

/* ── msgpack_remove ──────────────────────────────────────────────── */

static void test_remove(sqlite3 *db){
  /* Remove single map key */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('a',1,'b',2,'c',3)", &n);
    int nr = 0; unsigned char *result = apply_remove(db, src, n, "$.b", &nr);
    CHECK("4.1 remove map key: length=2", result && arr_len(db, result, nr) == 2);
    /* $.b should be gone */
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_extract(?, '$.b') IS NULL", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, result, nr, SQLITE_STATIC);
    sqlite3_step(s);
    int isNull = sqlite3_column_int(s, 0);
    CHECK("4.2 remove: $.b IS NULL", isNull == 1);
    sqlite3_finalize(s);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Remove array element */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_array(10,20,30)", &n);
    int nr = 0; unsigned char *result = apply_remove(db, src, n, "$[0]", &nr);
    CHECK("4.3 remove array element: length=2", result && arr_len(db, result, nr) == 2);
    CHECK("4.4 remove $[0]: new $[0]=20", result && extract_i(db, result, nr, "$[0]") == 20);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Remove multiple paths (SQL text) */
  {
    sqlite3_int64 len = exec1i(db,
      "SELECT msgpack_array_length("
      "  msgpack_remove(msgpack_object('a',1,'b',2,'c',3), '$.a','$.b'))");
    CHECK("4.5 remove 2 paths → length=1 (SQL text)", len == 1);
  }

  /* Remove non-existent key → no-op */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('x',1)", &n);
    int nr = 0; unsigned char *result = apply_remove(db, src, n, "$.z", &nr);
    CHECK("4.6 remove missing key is no-op: length=1", result && arr_len(db, result, nr) == 1);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Remove with bind for path */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_object('a',1,'b',2)", &n);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_remove(?, ?)", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, src, n, SQLITE_STATIC);
    sqlite3_bind_text(s, 2, "$.a", -1, SQLITE_STATIC);
    sqlite3_step(s);
    const void *b = sqlite3_column_blob(s, 0); int nr = sqlite3_column_bytes(s, 0);
    unsigned char *res = sqlite3_malloc(nr); memcpy(res, b, nr);
    sqlite3_finalize(s);
    CHECK("4.7 remove via bind: length=1", arr_len(db, res, nr) == 1);
    sqlite3_free(src); sqlite3_free(res);
  }
}

/* ── msgpack_array_insert ────────────────────────────────────────── */

static void test_array_insert(sqlite3 *db){
  /* Insert before index 1 */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_array(10,20,30)", &n);
    int nr = 0; unsigned char *result = apply_array_insert_i(db, src, n, "$[1]", 99, &nr);
    CHECK("5.1 array_insert before $[1]: length=4", result && arr_len(db, result, nr) == 4);
    CHECK("5.2 array_insert: new $[1]=99",  result && extract_i(db, result, nr, "$[1]") == 99);
    CHECK("5.3 array_insert: old $[2]=20",  result && extract_i(db, result, nr, "$[2]") == 20);
    CHECK("5.4 array_insert: $[0] unchanged=10", result && extract_i(db, result, nr, "$[0]") == 10);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Insert at index 0 (prepend) */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_array(1,2,3)", &n);
    int nr = 0; unsigned char *result = apply_array_insert_i(db, src, n, "$[0]", 0, &nr);
    CHECK("5.5 array_insert prepend: $[0]=0", result && extract_i(db, result, nr, "$[0]") == 0);
    CHECK("5.6 array_insert prepend: $[1]=1", result && extract_i(db, result, nr, "$[1]") == 1);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* Insert beyond end → appends */
  {
    int n = 0; unsigned char *src = build_blob(db,
      "SELECT msgpack_array(1,2,3)", &n);
    int nr = 0; unsigned char *result = apply_array_insert_i(db, src, n, "$[100]", 999, &nr);
    CHECK("5.7 array_insert beyond end → appended", result && arr_len(db, result, nr) == 4);
    CHECK("5.8 array_insert append: $[3]=999", result && extract_i(db, result, nr, "$[3]") == 999);
    sqlite3_free(src); sqlite3_free(result);
  }

  /* SQL text verification */
  {
    sqlite3_int64 len = exec1i(db,
      "SELECT msgpack_array_length(msgpack_array_insert(msgpack_array(10,20,30),'$[1]',99))");
    CHECK("5.9 array_insert length=4 (SQL text)", len == 4);
  }
}

/* ── msgpack_patch (RFC-7386) ────────────────────────────────────── */

static void test_patch(sqlite3 *db){
  /* Add and update keys */
  {
    int n_target = 0, n_patch = 0, n_result = 0;
    unsigned char *target = build_blob(db,
      "SELECT msgpack_object('a',1,'b',2)", &n_target);
    unsigned char *patch = build_blob(db,
      "SELECT msgpack_object('b',20,'c',30)", &n_patch);
    unsigned char *result = apply_patch(db, target, n_target, patch, n_patch, &n_result);

    CHECK("6.1 patch: $.b updated to 20",   result && extract_i(db, result, n_result, "$.b") == 20);
    CHECK("6.2 patch: $.c added = 30",       result && extract_i(db, result, n_result, "$.c") == 30);
    CHECK("6.3 patch: $.a preserved = 1",    result && extract_i(db, result, n_result, "$.a") == 1);
    CHECK("6.4 patch: result length = 3",    result && arr_len(db, result, n_result) == 3);

    sqlite3_free(target); sqlite3_free(patch); sqlite3_free(result);
  }

  /* Nil value in patch removes key */
  {
    int n_target = 0, n_patch = 0, n_result = 0;
    unsigned char *target = build_blob(db,
      "SELECT msgpack_object('a',1,'b',2)", &n_target);
    unsigned char *patch = build_blob(db,
      "SELECT msgpack_object('b',NULL)", &n_patch);
    unsigned char *result = apply_patch(db, target, n_target, patch, n_patch, &n_result);

    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_extract(?, '$.b') IS NULL", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, result, n_result, SQLITE_STATIC);
    sqlite3_step(s);
    int isNull = sqlite3_column_int(s, 0);
    CHECK("6.5 patch: nil removes $.b", isNull == 1);
    sqlite3_finalize(s);
    CHECK("6.6 patch: length=1 after remove", result && arr_len(db, result, n_result) == 1);

    sqlite3_free(target); sqlite3_free(patch); sqlite3_free(result);
  }

  /* Patch with non-map value → replace target entirely */
  {
    int n_target = 0, n_patch = 0, n_result = 0;
    unsigned char *target = build_blob(db,
      "SELECT msgpack_object('x',1)", &n_target);
    unsigned char *patch = build_blob(db,
      "SELECT msgpack_array(1,2,3)", &n_patch);
    unsigned char *result = apply_patch(db, target, n_target, patch, n_patch, &n_result);
    char *t = result ? NULL : NULL;
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db, "SELECT msgpack_type(?)", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, result, n_result, SQLITE_STATIC);
    sqlite3_step(s);
    const char *tt = (const char*)sqlite3_column_text(s, 0);
    CHECK("6.7 patch with non-map replaces target (type=array)",
      tt && strcmp(tt,"array")==0);
    sqlite3_finalize(s);
    sqlite3_free(t); sqlite3_free(target); sqlite3_free(patch); sqlite3_free(result);
  }

  /* Deep merge */
  {
    char *r = exec1(db,
      "SELECT msgpack_extract("
      "  msgpack_patch("
      "    msgpack_object('a', msgpack_object('x',1,'y',2)),"
      "    msgpack_object('a', msgpack_object('y',99,'z',3))"
      "  ),"
      "  '$.a.y')");
    CHECK("6.8 patch deep merge $.a.y=99 (SQL text)", r && strcmp(r,"99")==0); sqlite3_free(r);
  }

  /* SQL text: add only new key */
  {
    char *r = exec1(db,
      "SELECT msgpack_extract("
      "  msgpack_patch("
      "    msgpack_object('a',1),"
      "    msgpack_object('b',2)"
      "  ),"
      "  '$.b')");
    CHECK("6.9 patch adds new key $.b=2 (SQL text)", r && strcmp(r,"2")==0); sqlite3_free(r);
  }
}

/* ── immutability / original unmodified ─────────────────────────── */

static void test_immutability(sqlite3 *db){
  /*
  ** After a mutation, the original blob should be unchanged.
  ** We verify by inspecting the original after passing to a mutation.
  */
  int n = 0; unsigned char *original = build_blob(db,
    "SELECT msgpack_object('x',1,'y',2)", &n);

  /* take a hex snapshot of original */
  sqlite3_stmt *hex_s = NULL;
  sqlite3_prepare_v2(db, "SELECT hex(?)", -1, &hex_s, NULL);
  sqlite3_bind_blob(hex_s, 1, original, n, SQLITE_STATIC);
  sqlite3_step(hex_s);
  char *orig_hex = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(hex_s, 0));
  sqlite3_finalize(hex_s);

  /* perform mutation */
  int nr = 0; unsigned char *result = apply_set_i(db, original, n, "$.x", 999, &nr);
  (void)result; sqlite3_free(result);

  /* re-check original hex */
  sqlite3_prepare_v2(db, "SELECT hex(?)", -1, &hex_s, NULL);
  sqlite3_bind_blob(hex_s, 1, original, n, SQLITE_STATIC);
  sqlite3_step(hex_s);
  const char *new_hex = (const char*)sqlite3_column_text(hex_s, 0);
  CHECK("7.1 original blob unmodified after set", new_hex && strcmp(new_hex, orig_hex)==0);
  sqlite3_finalize(hex_s);

  sqlite3_free(orig_hex); sqlite3_free(original);
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){ fprintf(stderr,"open failed\n"); return 1; }
#ifdef SQLITE_CORE
  { char *e = NULL; sqlite3_msgpack_init(db, &e, NULL); sqlite3_free(e); }
#endif

  test_set(db);
  test_insert(db);
  test_replace(db);
  test_remove(db);
  test_array_insert(db);
  test_patch(db);
  test_immutability(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
