/*
** fuzz_msgpack.c — libFuzzer harness for the SQLite msgpack extension.
**
** Exercises all major SQL-facing entry points with arbitrary byte sequences.
** Build with: -fsanitize=fuzzer,address -DSQLITE_CORE
** Link against: sqlite3_amalg + msgpack_static
**
** Every function tested here must handle arbitrary (malformed) input without
** crashing, leaking memory, or reading out of bounds.
*/
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include "sqlite3.h"

/* Provided by msgpack_static when compiled with SQLITE_CORE */
extern int sqlite3_msgpack_init(sqlite3*, char**, const void*);

/*
** Persistent database connection — reused across fuzzer iterations for speed.
** Opened once in LLVMFuzzerInitialize().
*/
static sqlite3 *g_db = NULL;

/*
** Prepared statements — each exercises a different code path through the
** extension.  Prepared once, rebound each iteration.
*/
static sqlite3_stmt *g_valid     = NULL;
static sqlite3_stmt *g_to_json  = NULL;
static sqlite3_stmt *g_pretty   = NULL;
static sqlite3_stmt *g_type     = NULL;
static sqlite3_stmt *g_arrlen   = NULL;
static sqlite3_stmt *g_extract  = NULL;
static sqlite3_stmt *g_set      = NULL;
static sqlite3_stmt *g_remove   = NULL;
static sqlite3_stmt *g_insert   = NULL;
static sqlite3_stmt *g_replace  = NULL;
static sqlite3_stmt *g_patch    = NULL;
static sqlite3_stmt *g_each     = NULL;
static sqlite3_stmt *g_tree     = NULL;
static sqlite3_stmt *g_errpos   = NULL;
static sqlite3_stmt *g_from_json = NULL;

static void prep(sqlite3_stmt **pp, const char *sql) {
  if (sqlite3_prepare_v2(g_db, sql, -1, pp, NULL) != SQLITE_OK) {
    abort();
  }
}

static void drain(sqlite3_stmt *s) {
  while (sqlite3_step(s) == SQLITE_ROW) { /* consume all rows */ }
  sqlite3_reset(s);
}

static void run_blob(sqlite3_stmt *s, const uint8_t *data, size_t size) {
  sqlite3_bind_blob(s, 1, data, (int)size, SQLITE_STATIC);
  drain(s);
}

int LLVMFuzzerInitialize(int *argc, char ***argv) {
  (void)argc; (void)argv;

  if (sqlite3_open(":memory:", &g_db) != SQLITE_OK) abort();
  if (sqlite3_msgpack_init(g_db, NULL, NULL) != SQLITE_OK) abort();

  /* Scalar functions: bind the blob as ?1 */
  prep(&g_valid,    "SELECT msgpack_valid(?1)");
  prep(&g_to_json,  "SELECT msgpack_to_json(?1)");
  prep(&g_pretty,   "SELECT msgpack_pretty(?1)");
  prep(&g_type,     "SELECT msgpack_type(?1)");
  prep(&g_arrlen,   "SELECT msgpack_array_length(?1)");
  prep(&g_extract,  "SELECT msgpack_extract(?1, '$')");
  prep(&g_set,      "SELECT msgpack_set(?1, '$[0]', 99)");
  prep(&g_remove,   "SELECT msgpack_remove(?1, '$[0]')");
  prep(&g_insert,   "SELECT msgpack_insert(?1, '$[0]', 99)");
  prep(&g_replace,  "SELECT msgpack_replace(?1, '$[0]', 99)");
  prep(&g_patch,    "SELECT msgpack_patch(?1, ?1)");
  prep(&g_errpos,   "SELECT msgpack_error_position(?1)");

  /* Virtual tables: consume all rows */
  prep(&g_each, "SELECT * FROM msgpack_each(?1)");
  prep(&g_tree, "SELECT * FROM msgpack_tree(?1)");

  /* JSON→msgpack path: treat input as text */
  prep(&g_from_json, "SELECT hex(msgpack_from_json(?1))");

  return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  /* Cap input size to avoid spending too long on pathological inputs */
  if (size > 4096) return 0;

  /* --- Scalar functions with raw blob input --- */
  run_blob(g_valid,   data, size);
  run_blob(g_to_json, data, size);
  run_blob(g_type,    data, size);
  run_blob(g_arrlen,  data, size);
  run_blob(g_extract, data, size);
  run_blob(g_errpos,  data, size);

  /* pretty uses same code as to_json; only test on small inputs */
  if (size <= 256) {
    run_blob(g_pretty, data, size);
  }

  /* --- Mutation functions --- */
  run_blob(g_set,     data, size);
  run_blob(g_remove,  data, size);
  run_blob(g_insert,  data, size);
  run_blob(g_replace, data, size);
  run_blob(g_patch,   data, size);

  /* --- Virtual tables --- */
  run_blob(g_each, data, size);
  run_blob(g_tree, data, size);

  /* --- JSON→msgpack: reinterpret bytes as text --- */
  if (size > 0 && size < 4096) {
    sqlite3_bind_text(g_from_json, 1, (const char *)data, (int)size,
                      SQLITE_STATIC);
    drain(g_from_json);
  }

  return 0;
}
