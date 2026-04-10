/*
** test_spec_p8_schema.c — Tests for msgpack_schema_validate() (Phase 1 MVP).
**
** Covers: type, const, enum, numeric constraints, text constraints,
**         array constraints, map constraints, nested schemas.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

/* ---- Minimal test harness ------------------------------------------------ */
static int g_pass = 0;
static int g_fail = 0;

#define CHECK(label, expr)                                          \
    do {                                                            \
        if (expr) { printf("PASS  %s\n", (label)); g_pass++; }     \
        else { printf("FAIL  %s\n", (label)); g_fail++; }          \
    } while (0)

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

/* Forward-declare the extension entry point */
#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3 *db, char **pzErr,
                         const sqlite3_api_routines *pApi);
#endif

/* ---- Type validation ----------------------------------------------------- */
static void test_type(sqlite3 *db) {
    /* Integer type */
    CHECK("type.1 integer match",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":\"integer\"}')")==1);
    CHECK("type.2 integer mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('hello'),"
                  "'{\"type\":\"integer\"}')")==0);

    /* Text type */
    CHECK("type.3 text match",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('hello'),"
                  "'{\"type\":\"text\"}')")==1);
    CHECK("type.4 text mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":\"text\"}')")==0);

    /* Null type */
    CHECK("type.5 null match",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(NULL),"
                  "'{\"type\":\"null\"}')")==1);
    CHECK("type.6 null mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":\"null\"}')")==0);

    /* Real type */
    CHECK("type.7 real match",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(3.14),"
                  "'{\"type\":\"real\"}')")==1);
    CHECK("type.8 real mismatch integer",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":\"real\"}')")==0);

    /* Bool type */
    CHECK("type.9 bool match true",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_from_json('true'),'{\"type\":\"bool\"}')")==1);
    CHECK("type.10 bool match false",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_from_json('false'),'{\"type\":\"bool\"}')")==1);
    CHECK("type.11 bool mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(1),"
                  "'{\"type\":\"bool\"}')")==0);

    /* Array type */
    CHECK("type.12 array match",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(1,2,3),"
                  "'{\"type\":\"array\"}')")==1);
    CHECK("type.13 array mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":\"array\"}')")==0);

    /* Map type */
    CHECK("type.14 map match",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('a',1),'{\"type\":\"map\"}')")==1);
    CHECK("type.15 map mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":\"map\"}')")==0);

    /* Any type */
    CHECK("type.16 any matches integer",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":\"any\"}')")==1);
    CHECK("type.17 any matches text",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('x'),"
                  "'{\"type\":\"any\"}')")==1);

    /* Union type */
    CHECK("type.18 union integer|null int",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":[\"integer\",\"null\"]}')")==1);
    CHECK("type.19 union integer|null null",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(NULL),"
                  "'{\"type\":[\"integer\",\"null\"]}')")==1);
    CHECK("type.20 union integer|null text fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('x'),"
                  "'{\"type\":[\"integer\",\"null\"]}')")==0);
}

/* ---- Const and enum ------------------------------------------------------ */
static void test_const_enum(sqlite3 *db) {
    CHECK("const.1 match",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"const\":42}')")==1);
    CHECK("const.2 mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(43),"
                  "'{\"const\":42}')")==0);
    CHECK("const.3 string match",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('hello'),"
                  "'{\"const\":\"hello\"}')")==1);
    CHECK("const.4 string mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('world'),"
                  "'{\"const\":\"hello\"}')")==0);

    CHECK("enum.1 match",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('active'),"
                  "'{\"enum\":[\"active\",\"inactive\",\"pending\"]}')")==1);
    CHECK("enum.2 mismatch",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('deleted'),"
                  "'{\"enum\":[\"active\",\"inactive\",\"pending\"]}')")==0);
    CHECK("enum.3 int enum",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(2),"
                  "'{\"enum\":[1,2,3]}')")==1);
    CHECK("enum.4 int enum fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(5),"
                  "'{\"enum\":[1,2,3]}')")==0);
}

/* ---- Numeric constraints ------------------------------------------------- */
static void test_numeric(sqlite3 *db) {
    CHECK("num.1 minimum pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(10),"
                  "'{\"type\":\"integer\",\"minimum\":0}')")==1);
    CHECK("num.2 minimum fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(-1),"
                  "'{\"type\":\"integer\",\"minimum\":0}')")==0);
    CHECK("num.3 minimum boundary",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(0),"
                  "'{\"type\":\"integer\",\"minimum\":0}')")==1);

    CHECK("num.4 maximum pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(100),"
                  "'{\"type\":\"integer\",\"maximum\":150}')")==1);
    CHECK("num.5 maximum fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(200),"
                  "'{\"type\":\"integer\",\"maximum\":150}')")==0);
    CHECK("num.6 maximum boundary",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(150),"
                  "'{\"type\":\"integer\",\"maximum\":150}')")==1);

    CHECK("num.7 range pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(25),"
                  "'{\"type\":\"integer\",\"minimum\":0,\"maximum\":150}')")==1);

    CHECK("num.8 exclusiveMinimum pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(1),"
                  "'{\"type\":\"integer\",\"exclusiveMinimum\":0}')")==1);
    CHECK("num.9 exclusiveMinimum fail boundary",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(0),"
                  "'{\"type\":\"integer\",\"exclusiveMinimum\":0}')")==0);

    CHECK("num.10 exclusiveMaximum pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(9),"
                  "'{\"type\":\"integer\",\"exclusiveMaximum\":10}')")==1);
    CHECK("num.11 exclusiveMaximum fail boundary",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(10),"
                  "'{\"type\":\"integer\",\"exclusiveMaximum\":10}')")==0);

    /* Real number constraints */
    CHECK("num.12 real minimum pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(3.14),"
                  "'{\"type\":\"real\",\"minimum\":0.0}')")==1);
    CHECK("num.13 real maximum fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(3.14),"
                  "'{\"type\":\"real\",\"maximum\":3.0}')")==0);
}

/* ---- Text constraints ---------------------------------------------------- */
static void test_text(sqlite3 *db) {
    CHECK("text.1 minLength pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('hello'),"
                  "'{\"type\":\"text\",\"minLength\":1}')")==1);
    CHECK("text.2 minLength fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(''),"
                  "'{\"type\":\"text\",\"minLength\":1}')")==0);
    CHECK("text.3 maxLength pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('hi'),"
                  "'{\"type\":\"text\",\"maxLength\":5}')")==1);
    CHECK("text.4 maxLength fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('hello world'),"
                  "'{\"type\":\"text\",\"maxLength\":5}')")==0);
    CHECK("text.5 range pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('abc'),"
                  "'{\"type\":\"text\",\"minLength\":1,\"maxLength\":100}')")==1);
}

/* ---- Array constraints --------------------------------------------------- */
static void test_array(sqlite3 *db) {
    /* items */
    CHECK("arr.1 items all int pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(1,2,3),"
                  "'{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}')")==1);
    CHECK("arr.2 items type mismatch",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_array(1,'two',3),"
                  "'{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}')")==0);

    /* minItems / maxItems */
    CHECK("arr.3 minItems pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(1,2,3),"
                  "'{\"type\":\"array\",\"minItems\":1}')")==1);
    CHECK("arr.4 minItems fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(),"
                  "'{\"type\":\"array\",\"minItems\":1}')")==0);
    CHECK("arr.5 maxItems pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(1,2),"
                  "'{\"type\":\"array\",\"maxItems\":5}')")==1);
    CHECK("arr.6 maxItems fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(1,2,3,4,5,6),"
                  "'{\"type\":\"array\",\"maxItems\":5}')")==0);

    /* items: false → no elements allowed */
    CHECK("arr.7 items false empty ok",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(),"
                  "'{\"type\":\"array\",\"items\":false}')")==1);
    CHECK("arr.8 items false non-empty fail",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(1),"
                  "'{\"type\":\"array\",\"items\":false}')")==0);

    /* Combined */
    CHECK("arr.9 items + range pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(1,2,3),"
                  "'{\"type\":\"array\",\"items\":{\"type\":\"integer\"},"
                  "\"minItems\":1,\"maxItems\":10}')")==1);
}

/* ---- Map constraints ----------------------------------------------------- */
static void test_map(sqlite3 *db) {
    /* required */
    CHECK("map.1 required present",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice','age',30),"
                  "'{\"type\":\"map\",\"required\":[\"name\",\"age\"]}')")==1);
    CHECK("map.2 required missing",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice'),"
                  "'{\"type\":\"map\",\"required\":[\"name\",\"age\"]}')")==0);

    /* properties */
    CHECK("map.3 properties valid",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice','age',30),"
                  "'{\"type\":\"map\",\"properties\":{"
                  "\"name\":{\"type\":\"text\"},"
                  "\"age\":{\"type\":\"integer\"}}}')")==1);
    CHECK("map.4 properties type mismatch",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice','age','thirty'),"
                  "'{\"type\":\"map\",\"properties\":{"
                  "\"name\":{\"type\":\"text\"},"
                  "\"age\":{\"type\":\"integer\"}}}')")==0);

    /* additionalProperties: false */
    CHECK("map.5 additionalProperties false ok",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice'),"
                  "'{\"type\":\"map\",\"properties\":{"
                  "\"name\":{\"type\":\"text\"}},"
                  "\"additionalProperties\":false}')")==1);
    CHECK("map.6 additionalProperties false reject",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice','extra',1),"
                  "'{\"type\":\"map\",\"properties\":{"
                  "\"name\":{\"type\":\"text\"}},"
                  "\"additionalProperties\":false}')")==0);

    /* additionalProperties: schema */
    CHECK("map.7 additionalProperties schema pass",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice','tag','vip'),"
                  "'{\"type\":\"map\",\"properties\":{"
                  "\"name\":{\"type\":\"text\"}},"
                  "\"additionalProperties\":{\"type\":\"text\"}}')")==1);
    CHECK("map.8 additionalProperties schema fail",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice','tag',123),"
                  "'{\"type\":\"map\",\"properties\":{"
                  "\"name\":{\"type\":\"text\"}},"
                  "\"additionalProperties\":{\"type\":\"text\"}}')")==0);

    /* Full user object from proposal */
    CHECK("map.9 user object pass",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice','age',30),"
                  "'{\"type\":\"map\","
                  "\"required\":[\"name\",\"age\"],"
                  "\"properties\":{"
                  "\"name\":{\"type\":\"text\",\"minLength\":1},"
                  "\"age\":{\"type\":\"integer\",\"minimum\":0}},"
                  "\"additionalProperties\":false}')")==1);
    CHECK("map.10 user object missing required",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice'),"
                  "'{\"type\":\"map\","
                  "\"required\":[\"name\",\"age\"],"
                  "\"properties\":{"
                  "\"name\":{\"type\":\"text\",\"minLength\":1},"
                  "\"age\":{\"type\":\"integer\",\"minimum\":0}},"
                  "\"additionalProperties\":false}')")==0);
    CHECK("map.11 user object age out of range",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('name','Alice','age',-5),"
                  "'{\"type\":\"map\","
                  "\"required\":[\"name\",\"age\"],"
                  "\"properties\":{"
                  "\"name\":{\"type\":\"text\",\"minLength\":1},"
                  "\"age\":{\"type\":\"integer\",\"minimum\":0}},"
                  "\"additionalProperties\":false}')")==0);
}

/* ---- Nested schemas ------------------------------------------------------ */
static void test_nested(sqlite3 *db) {
    /* Array of maps */
    CHECK("nest.1 array of maps pass",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_array("
                  "  msgpack_object('x',1),"
                  "  msgpack_object('x',2)),"
                  "'{\"type\":\"array\",\"items\":{"
                  "\"type\":\"map\",\"required\":[\"x\"],"
                  "\"properties\":{\"x\":{\"type\":\"integer\"}}}}')")==1);
    CHECK("nest.2 array of maps fail",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_array("
                  "  msgpack_object('x',1),"
                  "  msgpack_object('y',2)),"
                  "'{\"type\":\"array\",\"items\":{"
                  "\"type\":\"map\",\"required\":[\"x\"],"
                  "\"properties\":{\"x\":{\"type\":\"integer\"}}}}')")==0);

    /* Map with nested map */
    CHECK("nest.3 nested map pass",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object('user',"
                  "  msgpack_object('name','Alice','age',30)),"
                  "'{\"type\":\"map\",\"properties\":{"
                  "\"user\":{\"type\":\"map\",\"required\":[\"name\"],"
                  "\"properties\":{\"name\":{\"type\":\"text\"},"
                  "\"age\":{\"type\":\"integer\"}}}}}')")==1);
}

/* ---- Edge cases ---------------------------------------------------------- */
static void test_edge_cases(sqlite3 *db) {
    /* Empty schema validates anything */
    CHECK("edge.1 empty schema",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{}')")==1);

    /* NULL data returns NULL */
    CHECK("edge.2 NULL data",
      exec1i(db, "SELECT msgpack_schema_validate(NULL,"
                  "'{\"type\":\"integer\"}') IS NULL")==1);

    /* Non-blob data returns 0 */
    CHECK("edge.3 non-blob data",
      exec1i(db, "SELECT msgpack_schema_validate(42,"
                  "'{\"type\":\"integer\"}')")==0);

    /* Boolean schema true */
    CHECK("edge.4 schema true",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'true')")==1);

    /* Boolean schema false */
    CHECK("edge.5 schema false",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'false')")==0);

    /* Schema as msgpack BLOB */
    CHECK("edge.6 schema as blob",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "msgpack_from_json('{\"type\":\"integer\"}'))")==1);

    /* Negative integer */
    CHECK("edge.7 negative int",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(-10),"
                  "'{\"type\":\"integer\"}')")==1);

    /* Large integer range */
    CHECK("edge.8 large int pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(1000000),"
                  "'{\"type\":\"integer\",\"minimum\":0,\"maximum\":9999999}')")==1);

    /* Empty array */
    CHECK("edge.9 empty array",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_array(),"
                  "'{\"type\":\"array\"}')")==1);

    /* Empty map */
    CHECK("edge.10 empty map",
      exec1i(db, "SELECT msgpack_schema_validate("
                  "msgpack_object(),"
                  "'{\"type\":\"map\"}')")==1);

    /* type + const combined */
    CHECK("edge.11 type+const pass",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote(42),"
                  "'{\"type\":\"integer\",\"const\":42}')")==1);
    CHECK("edge.12 type+const fail type",
      exec1i(db, "SELECT msgpack_schema_validate(msgpack_quote('42'),"
                  "'{\"type\":\"integer\",\"const\":42}')")==0);
}

/* ---- Schema caching (ensures repeated calls work) ------------------------ */
static void test_caching(sqlite3 *db) {
    int rc;
    sqlite3_stmt *pStmt = NULL;

    rc = sqlite3_exec(db,
      "CREATE TABLE test_data(id INTEGER PRIMARY KEY, mp BLOB);"
      "INSERT INTO test_data VALUES(1, msgpack_quote(10));"
      "INSERT INTO test_data VALUES(2, msgpack_quote(20));"
      "INSERT INTO test_data VALUES(3, msgpack_quote('bad'));"
      "INSERT INTO test_data VALUES(4, msgpack_quote(40));",
      0, 0, 0);
    CHECK("cache.0 setup", rc==SQLITE_OK);

    /* Count valid rows — schema should be cached across rows */
    rc = sqlite3_prepare_v2(db,
      "SELECT count(*) FROM test_data "
      "WHERE msgpack_schema_validate(mp, '{\"type\":\"integer\",\"minimum\":0}')",
      -1, &pStmt, NULL);
    CHECK("cache.1 prepare", rc==SQLITE_OK);
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      CHECK("cache.2 count valid", sqlite3_column_int(pStmt, 0)==3);
    }
    sqlite3_finalize(pStmt);

    /* Use in CHECK constraint */
    rc = sqlite3_exec(db,
      "CREATE TABLE validated("
      "  id INTEGER PRIMARY KEY,"
      "  data BLOB CHECK(msgpack_schema_validate(data,"
      "    '{\"type\":\"map\",\"required\":[\"name\"],"
      "    \"properties\":{\"name\":{\"type\":\"text\"}}}'))"
      ");",
      0, 0, 0);
    CHECK("cache.3 create checked table", rc==SQLITE_OK);

    rc = sqlite3_exec(db,
      "INSERT INTO validated VALUES(1, msgpack_object('name','Alice'))",
      0, 0, 0);
    CHECK("cache.4 valid insert", rc==SQLITE_OK);

    rc = sqlite3_exec(db,
      "INSERT INTO validated VALUES(2, msgpack_object('age',30))",
      0, 0, 0);
    CHECK("cache.5 invalid insert rejected", rc!=SQLITE_OK);

    sqlite3_exec(db, "DROP TABLE test_data; DROP TABLE validated;", 0, 0, 0);
}

/* ---- Main ---------------------------------------------------------------- */
int main(void) {
    sqlite3 *db = NULL;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open db: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_msgpack_init(db, 0, 0);

    test_type(db);
    test_const_enum(db);
    test_numeric(db);
    test_text(db);
    test_array(db);
    test_map(db);
    test_nested(db);
    test_edge_cases(db);
    test_caching(db);

    sqlite3_close(db);
    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
