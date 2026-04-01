/*
** bench_size_comparison.c
**
** Measures the serialised byte-size of the same logical data encoded as:
**   • msgpack  — compact binary (this extension)
**   • JSON     — canonical text (SQLite json() function)
**   • JSONB    — SQLite's internal binary JSON (jsonb() function, SQLite ≥ 3.45)
**
** ~25 test cases covering every significant payload category:
**   scalars · strings (various lengths) · arrays · maps · complex/nested
**
** Output is a GitHub-flavoured Markdown table between
**   <!-- SIZE_START --> and <!-- SIZE_END -->
** markers, suitable for README injection by the run_size_readme CMake target.
**
** Build (requires MSGPACK_BUILD_BENCH=ON):
**   cmake -B build -DMSGPACK_BUILD_BENCH=ON
**   cmake --build build --target bench_size_comparison
**
** Update README automatically:
**   cmake --build build --target run_size_readme
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3 *, char **, const sqlite3_api_routines *);
#endif

static sqlite3 *g_db = NULL;

/* ── Measure helpers ─────────────────────────────────────────────────────── */

/* Return the byte-length of the first column of sql, or -1 on error.
** Handles both direct BLOB/TEXT columns and length() → INTEGER results. */
static int measure(const char *sql){
    sqlite3_stmt *s = NULL;
    if(sqlite3_prepare_v2(g_db, sql, -1, &s, NULL) != SQLITE_OK){
        fprintf(stderr, "prepare error: %s\n  SQL: %s\n",
                sqlite3_errmsg(g_db), sql);
        return -1;
    }
    int bytes = -1;
    if(sqlite3_step(s) == SQLITE_ROW){
        int type = sqlite3_column_type(s, 0);
        if(type == SQLITE_INTEGER || type == SQLITE_FLOAT)
            bytes = sqlite3_column_int(s, 0);   /* length() returns INTEGER */
        else if(type == SQLITE_BLOB || type == SQLITE_TEXT)
            bytes = sqlite3_column_bytes(s, 0);
        else if(type == SQLITE_NULL)
            bytes = -1;  /* NULL result = unsupported / error */
    }
    sqlite3_finalize(s);
    return bytes;
}

/* ── Output helpers ──────────────────────────────────────────────────────── */

static void print_header(void){
    printf("| %-38s | %12s | %10s | %11s | %10s | %11s |\n",
           "Payload", "msgpack (B)", "json (B)", "jsonb (B)",
           "mp/json %", "mp/jsonb %");
    printf("|%-40s|%14s|%12s|%13s|%12s|%13s|\n",
           "---------------------------------------",
           "--------------",
           "------------",
           "-------------",
           "------------",
           "-------------");
}

/* pct: percentage of reference, printed as e.g. "74%". -1 → "n/a" */
static void fmt_pct(char *buf, int sz, int num, int ref){
    if(num < 0 || ref <= 0) { snprintf(buf, sz, "       n/a"); return; }
    snprintf(buf, sz, "%9d%%", (int)((double)num / ref * 100 + 0.5));
}

static void print_row(const char *label, int mp, int js, int jb){
    char pct_js[16], pct_jb[16];
    fmt_pct(pct_js, sizeof(pct_js), mp, js);
    fmt_pct(pct_jb, sizeof(pct_jb), mp, jb);

    char jb_s[16];
    if(jb < 0) snprintf(jb_s, sizeof(jb_s), "        n/a");
    else        snprintf(jb_s, sizeof(jb_s), "%11d", jb);

    printf("| %-38s | %12d | %10d | %11s | %10s | %11s |\n",
           label, mp, js, jb_s, pct_js, pct_jb);
}

/*
** Measure all three formats from the same JSON source string.
** mp_sql    : SQL that produces a msgpack BLOB (use msgpack_from_json or msgpack_*)
** json_src  : canonical JSON string — we measure length(json('<json_src>'))
** jsonb_sql : SQL that produces a JSONB blob  (use jsonb(...))
*/
static void row_from_json(const char *label,
                          const char *mp_sql,
                          const char *json_src,
                          const char *jsonb_sql)
{
    int mp = measure(mp_sql);

    /* JSON: canonicalise via json() to strip whitespace, measure text bytes */
    char *js_sql = sqlite3_mprintf("SELECT length(json(%Q))", json_src);
    int js = measure(js_sql);
    sqlite3_free(js_sql);

    int jb = jsonb_sql ? measure(jsonb_sql) : -1;

    print_row(label, mp, js, jb);
}

/* ── main ─────────────────────────────────────────────────────────────────── */

int main(void){
    if(sqlite3_open(":memory:", &g_db) != SQLITE_OK){
        fprintf(stderr, "sqlite3_open failed\n");
        return 1;
    }

#ifdef SQLITE_CORE
    { char *e = NULL; sqlite3_msgpack_init(g_db, &e, NULL); sqlite3_free(e); }
#endif

    printf("<!-- SIZE_START -->\n");
    printf("## Serialised-size comparison\n\n");
    printf("Byte sizes for identical logical data encoded as msgpack, JSON (text),\n");
    printf("and SQLite JSONB (binary).  `mp/json %%` and `mp/jsonb %%` show msgpack\n");
    printf("size as a percentage of the other format — below 100 %% means msgpack\n");
    printf("is more compact.\n\n");

    printf("> Platform: ");
#if defined(_WIN32)
    printf("Windows");
#elif defined(__APPLE__)
    printf("macOS");
#else
    printf("Linux");
#endif
    printf(" · SQLite %s\n\n", sqlite3_libversion());

    print_header();

    /* ── Scalars ──────────────────────────────────────────────────────────── */

    print_row("null",
        measure("SELECT length(msgpack_quote(NULL))"),
        measure("SELECT length(json('null'))"),
        measure("SELECT length(jsonb('null'))"));

    print_row("true  (via from_json)",
        measure("SELECT length(msgpack_from_json('true'))"),
        measure("SELECT length(json('true'))"),
        measure("SELECT length(jsonb('true'))"));

    print_row("false (via from_json)",
        measure("SELECT length(msgpack_from_json('false'))"),
        measure("SELECT length(json('false'))"),
        measure("SELECT length(jsonb('false'))"));

    print_row("integer 0",
        measure("SELECT length(msgpack_quote(0))"),
        measure("SELECT length(json('0'))"),
        measure("SELECT length(jsonb('0'))"));

    print_row("integer 42",
        measure("SELECT length(msgpack_quote(42))"),
        measure("SELECT length(json('42'))"),
        measure("SELECT length(jsonb('42'))"));

    print_row("integer 128  (uint8 boundary)",
        measure("SELECT length(msgpack_quote(128))"),
        measure("SELECT length(json('128'))"),
        measure("SELECT length(jsonb('128'))"));

    print_row("integer 65536  (uint32 range)",
        measure("SELECT length(msgpack_quote(65536))"),
        measure("SELECT length(json('65536'))"),
        measure("SELECT length(jsonb('65536'))"));

    print_row("integer 1 000 000 000",
        measure("SELECT length(msgpack_quote(1000000000))"),
        measure("SELECT length(json('1000000000'))"),
        measure("SELECT length(jsonb('1000000000'))"));

    print_row("integer -1",
        measure("SELECT length(msgpack_quote(-1))"),
        measure("SELECT length(json('-1'))"),
        measure("SELECT length(jsonb('-1'))"));

    print_row("integer -128  (int8 boundary)",
        measure("SELECT length(msgpack_quote(-128))"),
        measure("SELECT length(json('-128'))"),
        measure("SELECT length(jsonb('-128'))"));

    print_row("float 3.14",
        measure("SELECT length(msgpack_quote(3.14))"),
        measure("SELECT length(json('3.14'))"),
        measure("SELECT length(jsonb('3.14'))"));

    /* ── Strings ──────────────────────────────────────────────────────────── */

    print_row("string  2 chars  (\"hi\")",
        measure("SELECT length(msgpack_quote('hi'))"),
        measure("SELECT length(json('\"hi\"'))"),
        measure("SELECT length(jsonb('\"hi\"'))"));

    print_row("string 11 chars  (\"hello world\")",
        measure("SELECT length(msgpack_quote('hello world'))"),
        measure("SELECT length(json('\"hello world\"'))"),
        measure("SELECT length(jsonb('\"hello world\"'))"));

    /* 32-char string — fixstr boundary */
    print_row("string 31 chars  (fixstr max)",
        measure("SELECT length(msgpack_quote('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))"),
        measure("SELECT length(json('\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"'))"),
        measure("SELECT length(jsonb('\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"'))"));

    /* 32-char string — str8 territory */
    print_row("string 32 chars  (str8 range)",
        measure("SELECT length(msgpack_quote('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))"),
        measure("SELECT length(json('\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"'))"),
        measure("SELECT length(jsonb('\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"'))"));

    /* ── Arrays ───────────────────────────────────────────────────────────── */

    row_from_json("array  3 ints   [1,2,3]",
        "SELECT length(msgpack_from_json('[1,2,3]'))",
        "[1,2,3]",
        "SELECT length(jsonb('[1,2,3]'))");

    row_from_json("array 10 ints   [1..10]",
        "SELECT length(msgpack_from_json('[1,2,3,4,5,6,7,8,9,10]'))",
        "[1,2,3,4,5,6,7,8,9,10]",
        "SELECT length(jsonb('[1,2,3,4,5,6,7,8,9,10]'))");

    row_from_json("array 10 floats  [1.1..10.1]",
        "SELECT length(msgpack_from_json('[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8,9.9,10.1]'))",
        "[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8,9.9,10.1]",
        "SELECT length(jsonb('[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8,9.9,10.1]'))");

    /* ── Maps ─────────────────────────────────────────────────────────────── */

    row_from_json("map  2 fields   {a:1,b:2}",
        "SELECT length(msgpack_from_json('{\"a\":1,\"b\":2}'))",
        "{\"a\":1,\"b\":2}",
        "SELECT length(jsonb('{\"a\":1,\"b\":2}'))");

    row_from_json("user record (5 fields)",
        "SELECT length(msgpack_from_json('{\"id\":1,\"name\":\"Alice\",\"score\":9.5,\"active\":true,\"tag\":\"admin\"}'))",
        "{\"id\":1,\"name\":\"Alice\",\"score\":9.5,\"active\":true,\"tag\":\"admin\"}",
        "SELECT length(jsonb('{\"id\":1,\"name\":\"Alice\",\"score\":9.5,\"active\":true,\"tag\":\"admin\"}'))");

    row_from_json("full user record (8 fields)",
        "SELECT length(msgpack_from_json("
        "'{\"id\":1001,\"username\":\"alice\",\"email\":\"alice@example.com\","
        "\"age\":30,\"score\":98.6,\"active\":true,\"role\":\"admin\",\"dept\":\"engineering\"}'))",
        "{\"id\":1001,\"username\":\"alice\",\"email\":\"alice@example.com\","
        "\"age\":30,\"score\":98.6,\"active\":true,\"role\":\"admin\",\"dept\":\"engineering\"}",
        "SELECT length(jsonb("
        "'{\"id\":1001,\"username\":\"alice\",\"email\":\"alice@example.com\","
        "\"age\":30,\"score\":98.6,\"active\":true,\"role\":\"admin\",\"dept\":\"engineering\"}'))");

    /* ── Nested / complex ─────────────────────────────────────────────────── */

    row_from_json("nested  3 levels  {a:{b:{c:42}}}",
        "SELECT length(msgpack_from_json('{\"a\":{\"b\":{\"c\":42}}}'))",
        "{\"a\":{\"b\":{\"c\":42}}}",
        "SELECT length(jsonb('{\"a\":{\"b\":{\"c\":42}}}'))");

    row_from_json("array of 3 user objects",
        "SELECT length(msgpack_from_json("
        "'[{\"id\":1,\"name\":\"Alice\",\"age\":30},"
        "{\"id\":2,\"name\":\"Bob\",\"age\":25},"
        "{\"id\":3,\"name\":\"Carol\",\"age\":28}]'))",
        "[{\"id\":1,\"name\":\"Alice\",\"age\":30},"
        "{\"id\":2,\"name\":\"Bob\",\"age\":25},"
        "{\"id\":3,\"name\":\"Carol\",\"age\":28}]",
        "SELECT length(jsonb("
        "'[{\"id\":1,\"name\":\"Alice\",\"age\":30},"
        "{\"id\":2,\"name\":\"Bob\",\"age\":25},"
        "{\"id\":3,\"name\":\"Carol\",\"age\":28}]'))");

    row_from_json("config object (mixed types)",
        "SELECT length(msgpack_from_json("
        "'{\"host\":\"localhost\",\"port\":5432,\"tls\":true,"
        "\"timeout\":30.5,\"tags\":[\"db\",\"primary\"],\"meta\":{\"version\":2,\"env\":\"prod\"}}'))",
        "{\"host\":\"localhost\",\"port\":5432,\"tls\":true,"
        "\"timeout\":30.5,\"tags\":[\"db\",\"primary\"],\"meta\":{\"version\":2,\"env\":\"prod\"}}",
        "SELECT length(jsonb("
        "'{\"host\":\"localhost\",\"port\":5432,\"tls\":true,"
        "\"timeout\":30.5,\"tags\":[\"db\",\"primary\"],\"meta\":{\"version\":2,\"env\":\"prod\"}}'))");

    /* Large array via WITH RECURSIVE */
    {
        int mp_100 = measure(
            "SELECT length(msgpack_group_array(value)) FROM "
            "(WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<100)"
            " SELECT i AS value FROM n)");
        int js_100 = measure(
            "SELECT length(json_group_array(value)) FROM "
            "(WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<100)"
            " SELECT i AS value FROM n)");
        int jb_100 = measure(
            "SELECT length(jsonb_group_array(value)) FROM "
            "(WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<100)"
            " SELECT i AS value FROM n)");
        print_row("array 100 integers [1..100]", mp_100, js_100, jb_100);
    }

    printf("\n<!-- SIZE_END -->\n");

    sqlite3_close(g_db);
    return 0;
}
