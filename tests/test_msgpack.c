/*
** test_msgpack.c — CTest C unit tests for the SQLite msgpack extension.
**
** Compiled with SQLITE_CORE so msgpack.c registers via sqlite3_msgpack_init()
** which is called explicitly below (it is also the auto-extension entry point
** when loaded as a .so, but in static mode we call it ourselves).
**
** Build via CMake (see CMakeLists.txt).  Run: ctest --verbose
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Pull in the amalgamation first (provides sqlite3_api_routines stubs). */
#include "sqlite3.h"

/* ---- Minimal test harness ------------------------------------------------ */
static int g_pass = 0;
static int g_fail = 0;

#define CHECK(label, expr)                                          \
    do {                                                            \
        if (expr) { printf("PASS  %s\n", (label)); g_pass++; }     \
        else { printf("FAIL  %s\n", (label)); g_fail++; }           \
    } while (0)

/* Execute a single-value SQL query and return the result as a heap string.
   Caller must sqlite3_free() the result. Returns NULL on error/NULL result. */
static char *exec1(sqlite3 *db, const char *sql) {
    sqlite3_stmt *pStmt = NULL;
    char *zResult = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &pStmt, NULL) != SQLITE_OK)
        return NULL;
    if (sqlite3_step(pStmt) == SQLITE_ROW) {
        const char *z = (const char *)sqlite3_column_text(pStmt, 0);
        if (z) zResult = sqlite3_mprintf("%s", z);
    }
    sqlite3_finalize(pStmt);
    return zResult;
}

/* Return the integer result of a query, or -1 on error. */
static sqlite3_int64 exec1i(sqlite3 *db, const char *sql) {
    sqlite3_stmt *pStmt = NULL;
    sqlite3_int64 v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &pStmt, NULL) != SQLITE_OK)
        return -1;
    if (sqlite3_step(pStmt) == SQLITE_ROW)
        v = sqlite3_column_int64(pStmt, 0);
    sqlite3_finalize(pStmt);
    return v;
}

/* Check that a query returns exactly nExpect rows. */
static int exec_rowcount(sqlite3 *db, const char *sql) {
    sqlite3_stmt *pStmt = NULL;
    int n = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &pStmt, NULL) != SQLITE_OK)
        return -1;
    while (sqlite3_step(pStmt) == SQLITE_ROW) n++;
    sqlite3_finalize(pStmt);
    return n;
}

/* ---- Forward-declare the extension entry point (defined in msgpack.c) ----- */
#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3 *db, char **pzErr,
                         const sqlite3_api_routines *pApi);
#endif

/* ---- Test groups ---------------------------------------------------------- */

static void test_encoding(sqlite3 *db) {
    char *r;

    r = exec1(db, "SELECT hex(msgpack_quote(NULL))");
    CHECK("1.1 nil", r && strcmp(r,"C0")==0);
    sqlite3_free(r);

    r = exec1(db, "SELECT hex(msgpack_quote(42))");
    CHECK("1.2 positive fixint", r && strcmp(r,"2A")==0);
    sqlite3_free(r);

    r = exec1(db, "SELECT hex(msgpack_quote(-1))");
    CHECK("1.3 negative fixint", r && strcmp(r,"FF")==0);
    sqlite3_free(r);

    r = exec1(db, "SELECT hex(msgpack_quote(255))");
    CHECK("1.4 uint8", r && strcmp(r,"CCFF")==0);
    sqlite3_free(r);

    r = exec1(db, "SELECT hex(msgpack_quote('hello'))");
    CHECK("1.5 fixstr hello", r && strcmp(r,"A568656C6C6F")==0);
    sqlite3_free(r);

    r = exec1(db, "SELECT msgpack_valid(msgpack_quote(42))");
    CHECK("1.6 valid round-trip", r && strcmp(r,"1")==0);
    sqlite3_free(r);
}

static void test_construction(sqlite3 *db) {
    char *r;

    r = exec1(db, "SELECT hex(msgpack_array(1,2,3))");
    CHECK("2.1 array 3 elements", r && strcmp(r,"93010203")==0);
    sqlite3_free(r);

    r = exec1(db, "SELECT hex(msgpack_object('k','v'))");
    /* fixmap{1}, fixstr "k", fixstr "v" */
    CHECK("2.2 object 1 pair", r && r[0]!='\0');
    sqlite3_free(r);

    r = exec1(db,
        "SELECT msgpack_valid(msgpack_object('a',1,'b',msgpack_array(2,3)))");
    CHECK("2.3 nested object valid", r && strcmp(r,"1")==0);
    sqlite3_free(r);
}

static void test_extraction(sqlite3 *db) {
    sqlite3_int64 v;
    char *r;

    r = exec1(db, "SELECT msgpack_extract(msgpack_array(10,20,30), '$[1]')");
    CHECK("3.1 array index extract", r && strcmp(r,"20")==0);
    sqlite3_free(r);

    r = exec1(db,
        "SELECT msgpack_extract(msgpack_object('x',99), '$.x')");
    CHECK("3.2 map key extract", r && strcmp(r,"99")==0);
    sqlite3_free(r);

    r = exec1(db, "SELECT msgpack_type(msgpack_quote(3.14), '$')");
    CHECK("3.3 float type", r && strcmp(r,"real")==0);
    sqlite3_free(r);

    v = exec1i(db, "SELECT msgpack_array_length(msgpack_array(1,2,3,4))");
    CHECK("3.4 array length", v == 4);

    r = exec1(db, "SELECT msgpack_type(msgpack_array(1,2), '$')");
    CHECK("3.5 array type", r && strcmp(r,"array")==0);
    sqlite3_free(r);
}

static void test_mutation(sqlite3 *db) {
    char *r;

    r = exec1(db,
        "SELECT msgpack_extract("
        "  msgpack_set(msgpack_object('a',1), '$.a', 99),"
        "  '$.a')");
    CHECK("4.1 set existing key", r && strcmp(r,"99")==0);
    sqlite3_free(r);

    r = exec1(db,
        "SELECT msgpack_type("
        "  msgpack_remove(msgpack_object('a',1,'b',2), '$.a'),"
        "  '$')");
    CHECK("4.2 remove key leaves map", r && strcmp(r,"map")==0);
    sqlite3_free(r);

    r = exec1(db,
        "SELECT msgpack_extract("
        "  msgpack_array_insert(msgpack_array(1,2,3), '$[1]', 99),"
        "  '$[1]')");
    CHECK("4.3 array insert", r && strcmp(r,"99")==0);
    sqlite3_free(r);
}

static void test_json(sqlite3 *db) {
    char *r;

    r = exec1(db, "SELECT msgpack_to_json(msgpack_array(1,2,3))");
    CHECK("5.1 array to json", r && strcmp(r,"[1,2,3]")==0);
    sqlite3_free(r);

    r = exec1(db, "SELECT msgpack_valid(msgpack_from_json('[1,2,3]'))");
    CHECK("5.2 from_json valid", r && strcmp(r,"1")==0);
    sqlite3_free(r);

    r = exec1(db,
        "SELECT msgpack_to_json(msgpack_from_json('{\"x\":1}'))");
    CHECK("5.3 json round-trip map", r && strcmp(r,"{\"x\":1}")==0);
    sqlite3_free(r);
}

static void test_aggregates(sqlite3 *db) {
    sqlite3_int64 v;
    char *r;

    sqlite3_exec(db,
        "CREATE TEMP TABLE nums(n INTEGER); "
        "INSERT INTO nums VALUES(1),(2),(3);",
        NULL, NULL, NULL);

    v = exec1i(db, "SELECT msgpack_array_length(msgpack_group_array(n)) FROM nums");
    CHECK("6.1 group_array length", v == 3);

    r = exec1(db,
        "SELECT msgpack_type(msgpack_group_object(n, n*10), '$') FROM nums");
    CHECK("6.2 group_object type", r && strcmp(r,"map")==0);
    sqlite3_free(r);
}

static void test_vtab(sqlite3 *db) {
    int n;

    n = exec_rowcount(db,
        "SELECT * FROM msgpack_each(msgpack_array(10,20,30))");
    CHECK("7.1 each 3 rows", n == 3);

    /* msgpack_tree on [10, [20,30]] = 5 nodes: root + 10 + [20,30] + 20 + 30 */
    n = exec_rowcount(db,
        "SELECT * FROM msgpack_tree(msgpack_array(10,msgpack_array(20,30)))");
    CHECK("7.2 tree 5 nodes", n == 5);

    char *r = exec1(db,
        "SELECT type FROM msgpack_each(msgpack_array(1,'hi',NULL)) WHERE key=1");
    CHECK("7.3 each type column", r && strcmp(r,"text")==0);
    sqlite3_free(r);
}

/* ---- Main ----------------------------------------------------------------- */
int main(void) {
    sqlite3 *db = NULL;

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "Cannot open in-memory database\n");
        return 1;
    }

#ifdef SQLITE_CORE
    {
        char *zErr = NULL;
        if (sqlite3_msgpack_init(db, &zErr, NULL) != SQLITE_OK) {
            fprintf(stderr, "Failed to init msgpack: %s\n", zErr ? zErr : "?");
            sqlite3_free(zErr);
            sqlite3_close(db);
            return 1;
        }
    }
#else
    /* When not SQLITE_CORE, the extension must be loaded via sqlite3_load_extension */
    fprintf(stderr, "Recompile with -DSQLITE_CORE\n");
    sqlite3_close(db);
    return 1;
#endif

    test_encoding(db);
    test_construction(db);
    test_extraction(db);
    test_mutation(db);
    test_json(db);
    test_aggregates(db);
    test_vtab(db);

    sqlite3_close(db);

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
