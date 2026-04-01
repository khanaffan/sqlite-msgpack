/*
** bench_msgpack_vs_json.c
**
** Comparative throughput benchmark: msgpack vs JSON text vs JSONB binary.
**
** Each scenario runs the same logical operation using all three serialisation
** formats and reports nanoseconds-per-operation so results are directly
** comparable.  A brief warmup phase runs before each timed loop to avoid
** cold-start skew.
**
** Output is a GitHub-flavoured Markdown table, suitable for pasting directly
** into README.md (the cmake run_bench_readme target does this automatically).
**
** Build (optional, default OFF):
**   cmake -B build -DMSGPACK_BUILD_BENCH=ON
**   cmake --build build --target bench_msgpack_vs_json
**
** Run manually:
**   ./build/bench_msgpack_vs_json
**
** Update README automatically:
**   cmake --build build --target run_bench_readme
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

#ifdef SQLITE_CORE
int sqlite3_msgpack_init(sqlite3 *, char **, const sqlite3_api_routines *);
#endif

/* ── Cross-platform high-resolution timer ────────────────────────────────── */

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
static double now_sec(void){
    LARGE_INTEGER t, f;
    QueryPerformanceCounter(&t);
    QueryPerformanceFrequency(&f);
    return (double)t.QuadPart / (double)f.QuadPart;
}
#else
#  include <time.h>
static double now_sec(void){
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}
#endif

/* ── Benchmark helpers ────────────────────────────────────────────────────── */

static sqlite3 *g_db = NULL;

/*
** Run a read-only SQL expression N times (with warmup = N/10).
** Prepares once, steps+resets in a tight loop.
** Returns ns/op, or -1 on prepare error.
*/
static double bench_expr(const char *sql, int n){
    sqlite3_stmt *s = NULL;
    if(sqlite3_prepare_v2(g_db, sql, -1, &s, NULL) != SQLITE_OK){
        fprintf(stderr, "bench_expr prepare failed: %s\n  SQL: %s\n",
                sqlite3_errmsg(g_db), sql);
        return -1.0;
    }
    /* Warmup */
    int warm = n / 10;
    if(warm < 50) warm = 50;
    for(int i = 0; i < warm; i++){ sqlite3_step(s); sqlite3_reset(s); }

    double t0 = now_sec();
    for(int i = 0; i < n; i++){ sqlite3_step(s); sqlite3_reset(s); }
    double ns = (now_sec() - t0) / n * 1e9;

    sqlite3_finalize(s);
    return ns;
}

/*
** Run a full query (prepare+step+finalize) N times.
** Used for aggregates that re-scan a table on every call.
** Returns ns/op, or -1 on error.
*/
static double bench_query(const char *sql, int n){
    /* Warmup */
    int warm = n / 5;
    if(warm < 3) warm = 3;
    for(int i = 0; i < warm; i++){
        sqlite3_stmt *s = NULL;
        if(sqlite3_prepare_v2(g_db, sql, -1, &s, NULL) == SQLITE_OK){
            sqlite3_step(s); sqlite3_finalize(s);
        }
    }
    double t0 = now_sec();
    for(int i = 0; i < n; i++){
        sqlite3_stmt *s = NULL;
        sqlite3_prepare_v2(g_db, sql, -1, &s, NULL);
        sqlite3_step(s);
        sqlite3_finalize(s);
    }
    return (now_sec() - t0) / n * 1e9;
}

/* ── Output helpers ───────────────────────────────────────────────────────── */

static void print_header(void){
    printf("| %-34s | %13s | %10s | %11s | %8s | %9s |\n",
           "Operation", "msgpack ns/op", "json ns/op", "jsonb ns/op",
           "json/mp", "jsonb/mp");
    printf("|%-36s|%15s|%12s|%13s|%10s|%11s|\n",
           "-----------------------------------",
           "---------------",
           "------------",
           "-------------",
           "----------",
           "-----------");
}

static void print_row(const char *name, double mp, double js, double jb){
    /* ratio > 1.0 means the other format is slower than msgpack */
    char jr[16], jbr[16];
    if(js  > 0 && mp > 0) snprintf(jr,  sizeof(jr),  "%.2fx", js  / mp);
    else snprintf(jr,  sizeof(jr),  "n/a");
    if(jb  > 0 && mp > 0) snprintf(jbr, sizeof(jbr), "%.2fx", jb  / mp);
    else snprintf(jbr, sizeof(jbr), "n/a");

    char jss[16], jbs[16];
    if(js > 0) snprintf(jss, sizeof(jss), "%10.1f", js);
    else snprintf(jss, sizeof(jss), "       n/a");
    if(jb > 0) snprintf(jbs, sizeof(jbs), "%11.1f", jb);
    else snprintf(jbs, sizeof(jbs), "        n/a");

    printf("| %-34s | %13.1f | %10s | %11s | %8s | %9s |\n",
           name, mp, jss, jbs, jr, jbr);
}

/* ── Main ─────────────────────────────────────────────────────────────────── */

int main(void){
    if(sqlite3_open(":memory:", &g_db) != SQLITE_OK){
        fprintf(stderr, "sqlite3_open failed\n");
        return 1;
    }

#ifdef SQLITE_CORE
    { char *e = NULL; sqlite3_msgpack_init(g_db, &e, NULL); sqlite3_free(e); }
#endif

    /* ── Setup: 1 000-row table for aggregation benchmarks ── */
    sqlite3_exec(g_db,
        "CREATE TEMP TABLE bench_rows(id INTEGER PRIMARY KEY, v INTEGER, s TEXT);"
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1000)"
        "  INSERT INTO bench_rows(v,s) SELECT i, 'r'||i FROM n;",
        NULL, NULL, NULL);

    /* ── Setup: pre-built blobs/strings for extraction benchmarks ── */
    sqlite3_exec(g_db,
        "CREATE TEMP TABLE src AS SELECT"
        "  msgpack_object('id',1,'name','Alice','score',9.5,'active',1) AS mp,"
        "  json_object('id',1,'name','Alice','score',9.5,'active',1)    AS js,"
        "  jsonb_object('id',1,'name','Alice','score',9.5,'active',1)   AS jb;",
        NULL, NULL, NULL);

    /* Iteration counts tuned so each scenario takes ~0.5–2 s on a modern machine */
    const int N_BUILD   = 200000;
    const int N_EXTRACT = 400000;
    const int N_MUTATE  = 200000;
    const int N_AGG     =    600;
    const int N_EACH    =  30000;

    printf("<!-- BENCH_START -->\n");
    printf("## Performance benchmarks\n\n");
    printf("> Nanoseconds per operation — lower is better.  \n");
    printf("> `json/mp` and `jsonb/mp`: ratio relative to msgpack (>1 means msgpack is faster).  \n");
    printf("> Platform: ");
#if   defined(_WIN32)
    printf("Windows");
#elif defined(__APPLE__)
    printf("macOS");
#else
    printf("Linux");
#endif
    printf(" · SQLite %s\n\n", sqlite3_libversion());

    print_header();

    /* ── Construction ──────────────────────────────────────────────────────── */

    print_row("map build (4 fields)",
        bench_expr("SELECT msgpack_object('id',1,'name','Alice','score',9.5,'active',1)", N_BUILD),
        bench_expr("SELECT json_object  ('id',1,'name','Alice','score',9.5,'active',1)", N_BUILD),
        bench_expr("SELECT jsonb_object ('id',1,'name','Alice','score',9.5,'active',1)", N_BUILD));

    print_row("array build (8 integers)",
        bench_expr("SELECT msgpack_array(1,2,3,4,5,6,7,8)", N_BUILD),
        bench_expr("SELECT json_array   (1,2,3,4,5,6,7,8)", N_BUILD),
        bench_expr("SELECT jsonb_array  (1,2,3,4,5,6,7,8)", N_BUILD));

    print_row("nested build (map + array)",
        bench_expr("SELECT msgpack_object('u',msgpack_object('id',1,'name','Alice'),'t',msgpack_array('a','b','c'))", N_BUILD),
        bench_expr("SELECT json_object   ('u',json_object   ('id',1,'name','Alice'),'t',json_array   ('a','b','c'))", N_BUILD),
        bench_expr("SELECT jsonb_object  ('u',jsonb_object  ('id',1,'name','Alice'),'t',jsonb_array  ('a','b','c'))", N_BUILD));

    /* ── Extraction ────────────────────────────────────────────────────────── */

    print_row("extract text field ($.name)",
        bench_expr("SELECT msgpack_extract(mp,'$.name') FROM src", N_EXTRACT),
        bench_expr("SELECT json_extract   (js,'$.name') FROM src", N_EXTRACT),
        bench_expr("SELECT jsonb_extract  (jb,'$.name') FROM src", N_EXTRACT));

    print_row("extract numeric field ($.score)",
        bench_expr("SELECT msgpack_extract(mp,'$.score') FROM src", N_EXTRACT),
        bench_expr("SELECT json_extract   (js,'$.score') FROM src", N_EXTRACT),
        bench_expr("SELECT jsonb_extract  (jb,'$.score') FROM src", N_EXTRACT));

    print_row("type check ($.name)",
        bench_expr("SELECT msgpack_type(mp,'$.name') FROM src", N_EXTRACT),
        bench_expr("SELECT json_type   (js,'$.name') FROM src", N_EXTRACT),
        -1.0 /* no jsonb_type */);

    /* ── Mutation ──────────────────────────────────────────────────────────── */

    print_row("set field ($.extra = 42)",
        bench_expr("SELECT msgpack_set   (mp,'$.extra',42) FROM src", N_MUTATE),
        bench_expr("SELECT json_set      (js,'$.extra',42) FROM src", N_MUTATE),
        bench_expr("SELECT jsonb_set     (jb,'$.extra',42) FROM src", N_MUTATE));

    print_row("remove field ($.active)",
        bench_expr("SELECT msgpack_remove(mp,'$.active') FROM src", N_MUTATE),
        bench_expr("SELECT json_remove   (js,'$.active') FROM src", N_MUTATE),
        bench_expr("SELECT jsonb_remove  (jb,'$.active') FROM src", N_MUTATE));

    /* ── Aggregation ───────────────────────────────────────────────────────── */

    print_row("group_array (1 000 rows)",
        bench_query("SELECT msgpack_group_array(v)    FROM bench_rows", N_AGG),
        bench_query("SELECT json_group_array(v)       FROM bench_rows", N_AGG),
        bench_query("SELECT jsonb_group_array(v)      FROM bench_rows", N_AGG));

    print_row("group_object (1 000 rows)",
        bench_query("SELECT msgpack_group_object(s,v) FROM bench_rows", N_AGG),
        bench_query("SELECT json_group_object(s,v)    FROM bench_rows", N_AGG),
        bench_query("SELECT jsonb_group_object(s,v)   FROM bench_rows", N_AGG));

    /* ── Table-valued iteration ─────────────────────────────────────────────── */

    print_row("each — iterate 4-field map",
        bench_expr("SELECT count(*) FROM msgpack_each((SELECT mp FROM src))", N_EACH),
        bench_expr("SELECT count(*) FROM json_each   ((SELECT js FROM src))", N_EACH),
        bench_expr("SELECT count(*) FROM jsonb_each  ((SELECT jb FROM src))", N_EACH));

    /* ── Validation ────────────────────────────────────────────────────────── */

    print_row("valid check",
        bench_expr("SELECT msgpack_valid(mp) FROM src", N_EXTRACT),
        bench_expr("SELECT json_valid   (js) FROM src", N_EXTRACT),
        -1.0 /* no jsonb_valid */);

    /* ── JSON ↔ msgpack conversion ──────────────────────────────────────────── */

    print_row("from_json (parse JSON text → blob)",
        bench_expr("SELECT msgpack_from_json(js) FROM src", N_EXTRACT),
        -1.0 /* baseline */, -1.0);

    print_row("to_json (serialise blob → JSON text)",
        bench_expr("SELECT msgpack_to_json(mp) FROM src", N_EXTRACT),
        -1.0 /* baseline */, -1.0);

    printf("\n<!-- BENCH_END -->\n");

    sqlite3_close(g_db);
    return 0;
}
