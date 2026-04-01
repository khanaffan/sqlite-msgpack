/*
** test_spec_p7_agg_vtab.c — Phase 7: Aggregates and table-valued functions
**
** Covers:
**   msgpack_group_array  — 0/1/N rows, GROUP BY, window OVER
**   msgpack_group_object — 0/1/N rows, GROUP BY
**   msgpack_each         — array + map + root path parameter
**   msgpack_tree         — all 8 columns, depth, parent chain
**
** Mix of SQL text literals and sqlite3_bind_blob for result verification.
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
static int exec_rows(sqlite3 *db, const char *sql){
  sqlite3_stmt *s = NULL; int n = 0;
  if(sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return -1;
  while(sqlite3_step(s) == SQLITE_ROW) n++;
  sqlite3_finalize(s); return n;
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

/* msgpack_array_length(blob) via bind */
static sqlite3_int64 arr_len(sqlite3 *db, const void *b, int n){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -1;
  sqlite3_prepare_v2(db, "SELECT msgpack_array_length(?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

/* msgpack_type(blob) via bind */
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

/* extract int64 */
static sqlite3_int64 extract_i(sqlite3 *db, const void *b, int n, const char *p){
  sqlite3_stmt *s = NULL; sqlite3_int64 v = -999;
  sqlite3_prepare_v2(db, "SELECT msgpack_extract(?,?)", -1, &s, NULL);
  sqlite3_bind_blob(s, 1, b, n, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, p, -1, SQLITE_STATIC);
  if(sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
  sqlite3_finalize(s); return v;
}

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3*, char**, const sqlite3_api_routines*);
#endif

/* ── msgpack_group_array ─────────────────────────────────────────── */

static void test_group_array(sqlite3 *db){
  /* 0 rows → NULL result from aggregate */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE empty_t(v INTEGER);", NULL,NULL,NULL);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT msgpack_group_array(v) FROM empty_t", -1, &s, NULL);
    sqlite3_step(s);
    int ct = sqlite3_column_type(s, 0);
    /* SQLite aggregate over empty set → NULL */
    CHECK("1.1 group_array empty set → NULL", ct == SQLITE_NULL);
    sqlite3_finalize(s);
    sqlite3_exec(db, "DROP TABLE empty_t;", NULL,NULL,NULL);
  }

  /* 1 row */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE one_t(v INTEGER); INSERT INTO one_t VALUES(42);",
      NULL,NULL,NULL);
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_group_array(v) FROM one_t", &n);
    CHECK("1.2 group_array 1 row length=1", blob && arr_len(db, blob, n) == 1);
    CHECK("1.3 group_array 1 row $[0]=42", blob && extract_i(db, blob, n, "$[0]") == 42);
    char *t = blob ? blob_type(db, blob, n) : NULL;
    CHECK("1.4 group_array type='array'", t && strcmp(t,"array")==0);
    sqlite3_free(t); sqlite3_free(blob);
    sqlite3_exec(db, "DROP TABLE one_t;", NULL,NULL,NULL);
  }

  /* N rows, verify values and order */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE nums(n INTEGER); "
      "INSERT INTO nums VALUES(10),(20),(30);",
      NULL,NULL,NULL);
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_group_array(n) FROM nums", &n);
    CHECK("1.5 group_array N=3 length=3", blob && arr_len(db, blob, n) == 3);
    CHECK("1.6 group_array $[0]=10", blob && extract_i(db, blob, n, "$[0]") == 10);
    CHECK("1.7 group_array $[2]=30", blob && extract_i(db, blob, n, "$[2]") == 30);
    {
      int valid = 0;
      if(blob){
        sqlite3_stmt *s2 = NULL;
        sqlite3_prepare_v2(db, "SELECT msgpack_valid(?)", -1, &s2, NULL);
        sqlite3_bind_blob(s2, 1, blob, n, SQLITE_STATIC);
        sqlite3_step(s2); valid = sqlite3_column_int(s2, 0); sqlite3_finalize(s2);
      }
      CHECK("1.8 group_array is valid msgpack", valid == 1);
    }
    sqlite3_free(blob);
    sqlite3_exec(db, "DROP TABLE nums;", NULL,NULL,NULL);
  }

  /* Group BY: different groups */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE sensor(grp TEXT, val INTEGER); "
      "INSERT INTO sensor VALUES('A',1),('A',2),('B',3),('B',4),('B',5);",
      NULL,NULL,NULL);

    /* Group A: [1,2] */
    int n = 0; unsigned char *blobA = build_blob(db,
      "SELECT msgpack_group_array(val) FROM sensor WHERE grp='A'", &n);
    CHECK("1.9  group_array grp=A length=2", blobA && arr_len(db, blobA, n) == 2);
    sqlite3_free(blobA);

    /* Group B: [3,4,5] */
    int n2 = 0; unsigned char *blobB = build_blob(db,
      "SELECT msgpack_group_array(val) FROM sensor WHERE grp='B'", &n2);
    CHECK("1.10 group_array grp=B length=3", blobB && arr_len(db, blobB, n2) == 3);
    sqlite3_free(blobB);

    sqlite3_exec(db, "DROP TABLE sensor;", NULL,NULL,NULL);
  }

  /* Group array with mixed types */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE mix(v); "
      "INSERT INTO mix VALUES(NULL),(1),(3.14),('hi');",
      NULL,NULL,NULL);
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_group_array(v) FROM mix", &n);
    CHECK("1.11 group_array mixed types length=4", blob && arr_len(db, blob, n) == 4);
    sqlite3_free(blob);
    sqlite3_exec(db, "DROP TABLE mix;", NULL,NULL,NULL);
  }

  /* Window function OVER() — each row gets running array */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE win(v INTEGER); "
      "INSERT INTO win VALUES(1),(2),(3);",
      NULL,NULL,NULL);
    int rows = exec_rows(db,
      "SELECT msgpack_group_array(v) OVER () FROM win");
    CHECK("1.12 group_array OVER() produces 3 rows", rows == 3);
    sqlite3_exec(db, "DROP TABLE win;", NULL,NULL,NULL);
  }

  /* SQL text path */
  {
    char *r = exec1(db,
      "SELECT msgpack_to_json(msgpack_group_array(x)) "
      "FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3)");
    CHECK("1.13 group_array SQL text → '[1,2,3]'", r && strcmp(r,"[1,2,3]")==0);
    sqlite3_free(r);
  }
}

/* ── msgpack_group_object ────────────────────────────────────────── */

static void test_group_object(sqlite3 *db){
  /* 1 pair */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE kv1(k TEXT, v INTEGER); "
      "INSERT INTO kv1 VALUES('x',42);",
      NULL,NULL,NULL);
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_group_object(k,v) FROM kv1", &n);
    char *t = blob ? blob_type(db, blob, n) : NULL;
    CHECK("2.1 group_object type='map'", t && strcmp(t,"map")==0); sqlite3_free(t);
    CHECK("2.2 group_object $.x=42", blob && extract_i(db, blob, n, "$.x") == 42);
    sqlite3_free(blob);
    sqlite3_exec(db, "DROP TABLE kv1;", NULL,NULL,NULL);
  }

  /* N pairs */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE kv3(k TEXT, v INTEGER); "
      "INSERT INTO kv3 VALUES('a',1),('b',2),('c',3);",
      NULL,NULL,NULL);
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_group_object(k,v) FROM kv3", &n);
    CHECK("2.3 group_object N=3 length=3", blob && arr_len(db, blob, n) == 3);
    CHECK("2.4 group_object $.c=3",        blob && extract_i(db, blob, n, "$.c") == 3);
    sqlite3_free(blob);
    sqlite3_exec(db, "DROP TABLE kv3;", NULL,NULL,NULL);
  }

  /* SQL text path */
  {
    char *r = exec1(db,
      "SELECT msgpack_to_json(msgpack_group_object(k,v)) "
      "FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2)");
    CHECK("2.5 group_object SQL text → '{\"a\":1,\"b\":2}'",
      r && strcmp(r,"{\"a\":1,\"b\":2}")==0);
    sqlite3_free(r);
  }

  /* Window OVER() */
  {
    sqlite3_exec(db,
      "CREATE TEMP TABLE kv_win(k TEXT, v INTEGER); "
      "INSERT INTO kv_win VALUES('a',1),('b',2);",
      NULL,NULL,NULL);
    int rows = exec_rows(db,
      "SELECT msgpack_group_object(k,v) OVER () FROM kv_win");
    CHECK("2.6 group_object OVER() produces 2 rows", rows == 2);
    sqlite3_exec(db, "DROP TABLE kv_win;", NULL,NULL,NULL);
  }
}

/* ── msgpack_each (array) ────────────────────────────────────────── */

static void test_each_array(sqlite3 *db){
  /* row count */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_array(10,20,30))");
    CHECK("3.1 each array: 3 rows", rows == 3);
  }

  /* key column is integer index */
  {
    sqlite3_int64 minKey = exec1i(db,
      "SELECT min(key) FROM msgpack_each(msgpack_array(10,20,30))");
    CHECK("3.2 each array: min(key)=0", minKey == 0);
    sqlite3_int64 maxKey = exec1i(db,
      "SELECT max(key) FROM msgpack_each(msgpack_array(10,20,30))");
    CHECK("3.3 each array: max(key)=2", maxKey == 2);
  }

  /* value column */
  {
    sqlite3_int64 sum = exec1i(db,
      "SELECT sum(value) FROM msgpack_each(msgpack_array(10,20,30))");
    CHECK("3.4 each array: sum(value)=60", sum == 60);
  }

  /* type column */
  {
    char *r = exec1(db,
      "SELECT type FROM msgpack_each(msgpack_array(1,'hi',NULL)) WHERE key=1");
    CHECK("3.5 each array type at key=1 = 'text'", r && strcmp(r,"text")==0); sqlite3_free(r);
  }

  /* atom column: scalars have atom = value; containers have atom IS NULL */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_array(1,2)) WHERE atom IS NOT NULL");
    CHECK("3.6 each scalar atom IS NOT NULL", rows == 2);
    int rows2 = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_array(msgpack_array(1,2))) WHERE atom IS NULL");
    CHECK("3.7 each container atom IS NULL", rows2 == 1);
  }

  /* fullkey column */
  {
    char *r = exec1(db,
      "SELECT fullkey FROM msgpack_each(msgpack_array(1,2)) WHERE key=0");
    CHECK("3.8 each array fullkey='$[0]'", r && strcmp(r,"$[0]")==0); sqlite3_free(r);
  }

  /* path column */
  {
    char *r = exec1(db,
      "SELECT path FROM msgpack_each(msgpack_array(1,2)) LIMIT 1");
    CHECK("3.9 each path='$'", r && strcmp(r,"$")==0); sqlite3_free(r);
  }

  /* id column: offset in blob */
  {
    sqlite3_int64 id0 = exec1i(db,
      "SELECT id FROM msgpack_each(msgpack_array(1,2,3)) WHERE key=0");
    CHECK("3.10 each id(key=0) >= 0", id0 >= 0);
  }

  /* parent column: IS NULL (direct children of root have root as parent) */
  {
    /* parent should equal the offset of the array start (id of root=0 in tree) */
    sqlite3_int64 parent0 = exec1i(db,
      "SELECT parent FROM msgpack_each(msgpack_array(1,2)) LIMIT 1");
    CHECK("3.11 each: parent is not NULL (points to root)", parent0 >= 0);
  }

  /* each does NOT recurse into nested */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_array(1,msgpack_array(2,3)))");
    CHECK("3.12 each does not recurse: 2 rows", rows == 2);
  }

  /* bind blob to each via a prepared statement selecting from it */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_array(100,200,300)", &n);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT sum(value) FROM msgpack_each(?)", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
    sqlite3_step(s);
    sqlite3_int64 sum = sqlite3_column_int64(s, 0);
    CHECK("3.13 each via bind: sum=600", sum == 600);
    sqlite3_finalize(s);
    sqlite3_free(blob);
  }
}

/* ── msgpack_each (map) ──────────────────────────────────────────── */

static void test_each_map(sqlite3 *db){
  /* row count */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_object('a',1,'b',2))");
    CHECK("4.1 each map: 2 rows", rows == 2);
  }

  /* key column is text */
  {
    char *r = exec1(db,
      "SELECT group_concat(fullkey ORDER BY fullkey) "
      "FROM msgpack_each(msgpack_object('a',1,'b',2))");
    CHECK("4.2 each map fullkeys='$.a,$.b'", r && strcmp(r,"$.a,$.b")==0); sqlite3_free(r);
  }

  /* value */
  {
    sqlite3_int64 v = exec1i(db,
      "SELECT value FROM msgpack_each(msgpack_object('x',99)) WHERE key='x'");
    CHECK("4.3 each map value at key='x'=99", v == 99);
  }

  /* each map does NOT recurse */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_object('x',1,'y',msgpack_array(1,2,3)))");
    CHECK("4.4 each map does not recurse: 2 rows", rows == 2);
  }

  /* root path parameter */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_object('tags', msgpack_array('a','b','c'))", &n);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT count(*) FROM msgpack_each(?, '$.tags')", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
    sqlite3_step(s);
    sqlite3_int64 cnt = sqlite3_column_int64(s, 0);
    CHECK("4.5 each with root path '$.tags': 3 rows (bind)", cnt == 3);
    sqlite3_finalize(s);
    sqlite3_free(blob);
  }
}

/* ── msgpack_tree ────────────────────────────────────────────────── */

static void test_tree(sqlite3 *db){
  /* Simple array [10, [20,30]]: 5 nodes total */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_tree(msgpack_array(10,msgpack_array(20,30)))");
    CHECK("5.1 tree [10,[20,30]]: 5 nodes", rows == 5);
  }

  /* Root node has key IS NULL */
  {
    sqlite3_int64 isNull = exec1i(db,
      "SELECT key IS NULL FROM msgpack_tree(msgpack_array(1,2)) WHERE id=0");
    CHECK("5.2 tree root key IS NULL", isNull == 1);
  }

  /* Root node has parent IS NULL */
  {
    sqlite3_int64 parNull = exec1i(db,
      "SELECT parent IS NULL FROM msgpack_tree(msgpack_array(1,2)) WHERE id=0");
    CHECK("5.3 tree root parent IS NULL", parNull == 1);
  }

  /* fullkeys for [10,[20,30]] */
  {
    char *r = exec1(db,
      "SELECT group_concat(fullkey ORDER BY fullkey) "
      "FROM msgpack_tree(msgpack_array(10,msgpack_array(20,30)))");
    CHECK("5.4 tree fullkeys",
      r && strcmp(r,"$,$[0],$[1],$[1][0],$[1][1]")==0); sqlite3_free(r);
  }

  /* Child's parent = root's id */
  {
    sqlite3_int64 parent_of_child = exec1i(db,
      "SELECT parent FROM msgpack_tree(msgpack_array(1,2)) WHERE key=0");
    sqlite3_int64 root_id = exec1i(db,
      "SELECT id FROM msgpack_tree(msgpack_array(1,2)) WHERE id=0");
    CHECK("5.5 child parent = root id", parent_of_child == root_id);
  }

  /* atom column: NULL for arrays, non-NULL for scalars */
  {
    int arr_nodes = exec_rows(db,
      "SELECT * FROM msgpack_tree(msgpack_array(1,msgpack_array(2,3))) "
      "WHERE type='array' AND key IS NOT NULL AND atom IS NULL");
    CHECK("5.6 tree nested array atom IS NULL", arr_nodes == 1);
    int scalar_nodes = exec_rows(db,
      "SELECT * FROM msgpack_tree(msgpack_array(1,2,3)) "
      "WHERE type='integer' AND atom IS NOT NULL");
    CHECK("5.7 tree scalar atom IS NOT NULL", scalar_nodes == 3);
  }

  /* tree on a map */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_tree(msgpack_object('a',1,'b',msgpack_array(2,3)))");
    /* root + 'a':1 + 'b':[2,3] + 2 + 3 = 5 nodes */
    CHECK("5.8 tree on map: 5 nodes", rows == 5);
  }

  /* Deep nesting: 5-level structure */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_object('l1',"
      "  msgpack_object('l2',"
      "    msgpack_array(1, 2, msgpack_object('l3', 42))))", &n);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT count(*) FROM msgpack_tree(?)", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
    sqlite3_step(s);
    sqlite3_int64 cnt = sqlite3_column_int64(s, 0);
    /* root({l1}) + {l2} + [1,2,{l3}] + 1 + 2 + {l3:42} + 42 = 7 nodes */
    CHECK("5.9 tree deep structure node count (bind)", cnt == 7);
    sqlite3_finalize(s);
    sqlite3_free(blob);
  }

  /* All 8 columns accessible */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT key, value, type, atom, id, parent, fullkey, path "
      "FROM msgpack_tree(msgpack_array(42)) WHERE key=0",
      -1, &s, NULL);
    sqlite3_step(s);
    int key_val   = sqlite3_column_int(s, 0);    /* key=0 */
    int val_val   = sqlite3_column_int(s, 1);    /* value=42 */
    const char *type_val = (const char*)sqlite3_column_text(s, 2); /* type='integer' */
    int atom_val  = sqlite3_column_int(s, 3);    /* atom=42 */
    /* id, parent, fullkey, path */
    const char *fk = (const char*)sqlite3_column_text(s, 6);
    const char *pa = (const char*)sqlite3_column_text(s, 7);
    CHECK("5.10 tree columns: key=0",    key_val == 0);
    CHECK("5.11 tree columns: value=42", val_val == 42);
    CHECK("5.12 tree columns: type='integer'", type_val && strcmp(type_val,"integer")==0);
    CHECK("5.13 tree columns: atom=42",  atom_val == 42);
    CHECK("5.14 tree columns: fullkey=$[0]", fk && strcmp(fk,"$[0]")==0);
    CHECK("5.15 tree columns: path=$",   pa && strcmp(pa,"$")==0);
    sqlite3_finalize(s);
  }

  /* tree with root path parameter via bind */
  {
    int n = 0; unsigned char *blob = build_blob(db,
      "SELECT msgpack_object('data', msgpack_array(10,20,30))", &n);
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT count(*) FROM msgpack_tree(?, '$.data')", -1, &s, NULL);
    sqlite3_bind_blob(s, 1, blob, n, SQLITE_STATIC);
    sqlite3_step(s);
    sqlite3_int64 cnt = sqlite3_column_int64(s, 0);
    /* root [10,20,30] + 3 elements = 4 nodes */
    CHECK("5.16 tree with root path '$.data' (bind)", cnt == 4);
    sqlite3_finalize(s);
    sqlite3_free(blob);
  }
}

/* ── edge cases ──────────────────────────────────────────────────── */

static void test_vtab_edge_cases(sqlite3 *db){
  /* each on scalar → 0 rows */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_quote(42))");
    CHECK("6.1 each on scalar: 0 rows", rows == 0);
  }

  /* each on nil → 0 rows */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_quote(NULL))");
    CHECK("6.2 each on nil: 0 rows", rows == 0);
  }

  /* tree on scalar → 1 row (root only) */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_tree(msgpack_quote(42))");
    CHECK("6.3 tree on scalar: 1 row (root only)", rows == 1);
  }

  /* each on empty array → 0 rows */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_array())");
    CHECK("6.4 each on empty array: 0 rows", rows == 0);
  }

  /* each on empty map → 0 rows */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_each(msgpack_object())");
    CHECK("6.5 each on empty map: 0 rows", rows == 0);
  }

  /* tree on empty array → 1 row (root) */
  {
    int rows = exec_rows(db,
      "SELECT * FROM msgpack_tree(msgpack_array())");
    CHECK("6.6 tree on empty array: 1 row", rows == 1);
  }

  /* each with NULL input → 0 rows (no error) */
  {
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(db,
      "SELECT count(*) FROM msgpack_each(?)", -1, &s, NULL);
    sqlite3_bind_null(s, 1);
    sqlite3_step(s);
    sqlite3_int64 cnt = sqlite3_column_int64(s, 0);
    CHECK("6.7 each with NULL input: 0 rows", cnt == 0);
    sqlite3_finalize(s);
  }
}

int main(void){
  sqlite3 *db = NULL;
  if(sqlite3_open(":memory:", &db) != SQLITE_OK){ fprintf(stderr,"open failed\n"); return 1; }
#ifdef SQLITE_CORE
  { char *e = NULL; sqlite3_msgpack_init(db, &e, NULL); sqlite3_free(e); }
#endif

  test_group_array(db);
  test_group_object(db);
  test_each_array(db);
  test_each_map(db);
  test_tree(db);
  test_vtab_edge_cases(db);

  sqlite3_close(db);
  printf("\n%d passed, %d failed\n", g_pass, g_fail);
  return g_fail ? 1 : 0;
}
