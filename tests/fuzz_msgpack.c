/*
** fuzz_msgpack.c — libFuzzer harness for the SQLite msgpack extension.
**
** Exercises ALL SQL-facing entry points with arbitrary byte sequences,
** including scalar functions, path-based variants, constructors, aggregate
** window functions, virtual tables, and round-trip conversions.
**
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

/* --- Prepared statements (one per entry point) --- */

/* 1-arg scalar functions taking a blob */
static sqlite3_stmt *g_valid        = NULL;
static sqlite3_stmt *g_to_json      = NULL;
static sqlite3_stmt *g_to_jsonb     = NULL;
static sqlite3_stmt *g_pretty       = NULL;
static sqlite3_stmt *g_type         = NULL;
static sqlite3_stmt *g_arrlen       = NULL;
static sqlite3_stmt *g_errpos       = NULL;
static sqlite3_stmt *g_quote        = NULL;
static sqlite3_stmt *g_msgpack      = NULL;

/* 2-arg path variants: (?1 = blob, ?2 = path text) */
static sqlite3_stmt *g_valid_path   = NULL;
static sqlite3_stmt *g_type_path    = NULL;
static sqlite3_stmt *g_arrlen_path  = NULL;
static sqlite3_stmt *g_pretty_ind   = NULL;

/* Extract / mutation with static path */
static sqlite3_stmt *g_extract      = NULL;
static sqlite3_stmt *g_set          = NULL;
static sqlite3_stmt *g_remove       = NULL;
static sqlite3_stmt *g_insert       = NULL;
static sqlite3_stmt *g_replace      = NULL;
static sqlite3_stmt *g_patch        = NULL;
static sqlite3_stmt *g_arr_insert   = NULL;

/* Extract / mutation with fuzzed path */
static sqlite3_stmt *g_extract_fp   = NULL;
static sqlite3_stmt *g_set_fp       = NULL;
static sqlite3_stmt *g_remove_fp    = NULL;
static sqlite3_stmt *g_insert_fp    = NULL;
static sqlite3_stmt *g_replace_fp   = NULL;

/* Multi-path extract */
static sqlite3_stmt *g_extract_multi = NULL;

/* Constructors */
static sqlite3_stmt *g_array_ctor   = NULL;
static sqlite3_stmt *g_object_ctor  = NULL;

/* Virtual tables */
static sqlite3_stmt *g_each         = NULL;
static sqlite3_stmt *g_tree         = NULL;
static sqlite3_stmt *g_each_path    = NULL;
static sqlite3_stmt *g_tree_path    = NULL;

/* JSON → msgpack and round-trip */
static sqlite3_stmt *g_from_json    = NULL;
static sqlite3_stmt *g_roundtrip    = NULL;

/* Aggregate / window functions */
static sqlite3_stmt *g_group_array  = NULL;
static sqlite3_stmt *g_group_object = NULL;

/* Schema validation: blob data + static schemas */
static sqlite3_stmt *g_sv_type_int  = NULL;
static sqlite3_stmt *g_sv_type_text = NULL;
static sqlite3_stmt *g_sv_type_arr  = NULL;
static sqlite3_stmt *g_sv_type_map  = NULL;
static sqlite3_stmt *g_sv_numeric   = NULL;
static sqlite3_stmt *g_sv_text_len  = NULL;
static sqlite3_stmt *g_sv_arr_items = NULL;
static sqlite3_stmt *g_sv_map_full  = NULL;
static sqlite3_stmt *g_sv_nested    = NULL;
static sqlite3_stmt *g_sv_enum      = NULL;
static sqlite3_stmt *g_sv_union     = NULL;

/* Schema validation: blob data + fuzzed schema text */
static sqlite3_stmt *g_sv_fuzz_text = NULL;

/* Schema validation: blob data + fuzzed schema blob */
static sqlite3_stmt *g_sv_fuzz_blob = NULL;

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

/* Bind blob as ?1 and a NUL-terminated text path as ?2 */
static void run_blob_text(sqlite3_stmt *s,
                          const uint8_t *blob, size_t blobsz,
                          const char *text, size_t textsz) {
  sqlite3_bind_blob(s, 1, blob, (int)blobsz, SQLITE_STATIC);
  sqlite3_bind_text(s, 2, text, (int)textsz, SQLITE_STATIC);
  drain(s);
}

int LLVMFuzzerInitialize(int *argc, char ***argv) {
  (void)argc; (void)argv;

  if (sqlite3_open(":memory:", &g_db) != SQLITE_OK) abort();
  if (sqlite3_msgpack_init(g_db, NULL, NULL) != SQLITE_OK) abort();

  /* ── 1-arg scalar functions ─────────────────────────────────────── */
  prep(&g_valid,    "SELECT msgpack_valid(?1)");
  prep(&g_to_json,  "SELECT msgpack_to_json(?1)");
  prep(&g_to_jsonb, "SELECT msgpack_to_jsonb(?1)");
  prep(&g_pretty,   "SELECT msgpack_pretty(?1)");
  prep(&g_type,     "SELECT msgpack_type(?1)");
  prep(&g_arrlen,   "SELECT msgpack_array_length(?1)");
  prep(&g_errpos,   "SELECT msgpack_error_position(?1)");
  prep(&g_quote,    "SELECT msgpack_quote(?1)");
  prep(&g_msgpack,  "SELECT hex(msgpack(?1))");

  /* ── 2-arg path variants ────────────────────────────────────────── */
  prep(&g_valid_path,  "SELECT msgpack_valid(?1, ?2)");
  prep(&g_type_path,   "SELECT msgpack_type(?1, ?2)");
  prep(&g_arrlen_path, "SELECT msgpack_array_length(?1, ?2)");
  prep(&g_pretty_ind,  "SELECT msgpack_pretty(?1, ?2)");

  /* ── Extract / mutation with static paths ───────────────────────── */
  prep(&g_extract,     "SELECT msgpack_extract(?1, '$')");
  prep(&g_set,         "SELECT msgpack_set(?1, '$[0]', 99)");
  prep(&g_remove,      "SELECT msgpack_remove(?1, '$[0]')");
  prep(&g_insert,      "SELECT msgpack_insert(?1, '$[0]', 99)");
  prep(&g_replace,     "SELECT msgpack_replace(?1, '$[0]', 99)");
  prep(&g_patch,       "SELECT msgpack_patch(?1, ?1)");
  prep(&g_arr_insert,  "SELECT msgpack_array_insert(?1, '$[0]', 99)");

  /* ── Extract / mutation with fuzzed path (?2) ───────────────────── */
  prep(&g_extract_fp,  "SELECT msgpack_extract(?1, ?2)");
  prep(&g_set_fp,      "SELECT msgpack_set(?1, ?2, 99)");
  prep(&g_remove_fp,   "SELECT msgpack_remove(?1, ?2)");
  prep(&g_insert_fp,   "SELECT msgpack_insert(?1, ?2, 99)");
  prep(&g_replace_fp,  "SELECT msgpack_replace(?1, ?2, 99)");

  /* ── Multi-path extract ─────────────────────────────────────────── */
  prep(&g_extract_multi, "SELECT msgpack_extract(?1, '$[0]', '$.a', '$')");

  /* ── Constructors: feed fuzz blob as if it were a SQL value ─────── */
  prep(&g_array_ctor,  "SELECT hex(msgpack_array(?1, ?1, ?1))");
  prep(&g_object_ctor, "SELECT hex(msgpack_object('k', ?1))");

  /* ── Virtual tables ─────────────────────────────────────────────── */
  prep(&g_each,      "SELECT * FROM msgpack_each(?1)");
  prep(&g_tree,      "SELECT * FROM msgpack_tree(?1)");
  prep(&g_each_path, "SELECT * FROM msgpack_each(?1, ?2)");
  prep(&g_tree_path, "SELECT * FROM msgpack_tree(?1, ?2)");

  /* ── JSON→msgpack and round-trip ────────────────────────────────── */
  prep(&g_from_json, "SELECT hex(msgpack_from_json(?1))");
  prep(&g_roundtrip, "SELECT msgpack_to_json(msgpack_from_json(?1))");

  /* ── Aggregate / window functions via sub-select on json_each ──── */
  prep(&g_group_array,
    "SELECT hex(msgpack_group_array(value)) "
    "FROM (SELECT value FROM json_each('[1,2,3]'))");
  prep(&g_group_object,
    "SELECT hex(msgpack_group_object(key, value)) "
    "FROM (SELECT key, value FROM json_each('{\"a\":1,\"b\":2}'))");

  /* ── Schema validation: blob data + static schemas ─────────────── */
  prep(&g_sv_type_int,
    "SELECT msgpack_schema_validate(?1, '{\"type\":\"integer\"}')");
  prep(&g_sv_type_text,
    "SELECT msgpack_schema_validate(?1, '{\"type\":\"text\"}')");
  prep(&g_sv_type_arr,
    "SELECT msgpack_schema_validate(?1, '{\"type\":\"array\","
    "\"items\":{\"type\":\"integer\"},\"minItems\":0,\"maxItems\":100}')");
  prep(&g_sv_type_map,
    "SELECT msgpack_schema_validate(?1, '{\"type\":\"map\","
    "\"required\":[\"id\",\"name\"],"
    "\"properties\":{\"id\":{\"type\":\"integer\",\"minimum\":0},"
    "\"name\":{\"type\":\"text\",\"minLength\":1,\"maxLength\":255}},"
    "\"additionalProperties\":false}')");
  prep(&g_sv_numeric,
    "SELECT msgpack_schema_validate(?1, "
    "'{\"type\":\"integer\",\"minimum\":-100,\"maximum\":100,"
    "\"exclusiveMinimum\":-101,\"exclusiveMaximum\":101}')");
  prep(&g_sv_text_len,
    "SELECT msgpack_schema_validate(?1, "
    "'{\"type\":\"text\",\"minLength\":1,\"maxLength\":1000}')");
  prep(&g_sv_arr_items,
    "SELECT msgpack_schema_validate(?1, "
    "'{\"type\":\"array\",\"items\":{\"type\":\"map\","
    "\"properties\":{\"x\":{\"type\":\"integer\"}}}}')");
  prep(&g_sv_map_full,
    "SELECT msgpack_schema_validate(?1, "
    "'{\"type\":\"map\",\"properties\":{\"a\":{\"type\":\"integer\"},"
    "\"b\":{\"type\":\"text\"},\"c\":{\"type\":\"array\"}},"
    "\"additionalProperties\":{\"type\":\"text\"}}')");
  prep(&g_sv_nested,
    "SELECT msgpack_schema_validate(?1, "
    "'{\"type\":\"map\",\"properties\":{\"inner\":{"
    "\"type\":\"map\",\"properties\":{\"deep\":{"
    "\"type\":\"array\",\"items\":{\"type\":\"integer\"}}}}}}')");
  prep(&g_sv_enum,
    "SELECT msgpack_schema_validate(?1, "
    "'{\"enum\":[1,2,3,\"active\",\"inactive\",null,true,false]}')");
  prep(&g_sv_union,
    "SELECT msgpack_schema_validate(?1, "
    "'{\"type\":[\"integer\",\"text\",\"null\"]}')");

  /* ── Schema validation: fuzzed schema as text or blob ──────────── */
  prep(&g_sv_fuzz_text,
    "SELECT msgpack_schema_validate(?1, ?2)");
  prep(&g_sv_fuzz_blob,
    "SELECT msgpack_schema_validate(?1, ?2)");
  return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  if (size > 4096) return 0;

  /* --- Split input: first half = blob, second half = path text --- */
  size_t split = size / 2;
  const uint8_t *blob = data;
  size_t blobsz = split > 0 ? split : size;
  /* Ensure NUL-terminated path from the second half */
  char pathbuf[2048];
  size_t pathsz = size - split;
  if (pathsz >= sizeof(pathbuf)) pathsz = sizeof(pathbuf) - 1;
  if (pathsz > 0) memcpy(pathbuf, data + split, pathsz);
  pathbuf[pathsz] = '\0';

  /* ── 1-arg scalar functions with raw blob input ─────────────────── */
  run_blob(g_valid,   data, size);
  run_blob(g_to_json, data, size);
  run_blob(g_to_jsonb, data, size);
  run_blob(g_type,    data, size);
  run_blob(g_arrlen,  data, size);
  run_blob(g_errpos,  data, size);
  run_blob(g_quote,   data, size);
  run_blob(g_msgpack, data, size);

  if (size <= 256) {
    run_blob(g_pretty, data, size);
  }

  /* ── 2-arg path variants (blob + fuzzed path) ──────────────────── */
  if (pathsz > 0) {
    run_blob_text(g_valid_path,  blob, blobsz, pathbuf, pathsz);
    run_blob_text(g_type_path,   blob, blobsz, pathbuf, pathsz);
    run_blob_text(g_arrlen_path, blob, blobsz, pathbuf, pathsz);
    run_blob_text(g_pretty_ind,  blob, blobsz, pathbuf, pathsz);
  }

  /* ── Extract / mutation with static paths ───────────────────────── */
  run_blob(g_extract,    data, size);
  run_blob(g_set,        data, size);
  run_blob(g_remove,     data, size);
  run_blob(g_insert,     data, size);
  run_blob(g_replace,    data, size);
  run_blob(g_patch,      data, size);
  run_blob(g_arr_insert, data, size);

  /* ── Extract / mutation with fuzzed path ────────────────────────── */
  if (pathsz > 0) {
    run_blob_text(g_extract_fp,  blob, blobsz, pathbuf, pathsz);
    run_blob_text(g_set_fp,      blob, blobsz, pathbuf, pathsz);
    run_blob_text(g_remove_fp,   blob, blobsz, pathbuf, pathsz);
    run_blob_text(g_insert_fp,   blob, blobsz, pathbuf, pathsz);
    run_blob_text(g_replace_fp,  blob, blobsz, pathbuf, pathsz);
  }

  /* ── Multi-path extract ─────────────────────────────────────────── */
  run_blob(g_extract_multi, data, size);

  /* ── Constructors: use blob as a SQL value argument ─────────────── */
  run_blob(g_array_ctor,  data, size);
  run_blob(g_object_ctor, data, size);

  /* ── Virtual tables ─────────────────────────────────────────────── */
  run_blob(g_each, data, size);
  run_blob(g_tree, data, size);
  if (pathsz > 0) {
    run_blob_text(g_each_path, blob, blobsz, pathbuf, pathsz);
    run_blob_text(g_tree_path, blob, blobsz, pathbuf, pathsz);
  }

  /* ── JSON→msgpack: reinterpret bytes as text ────────────────────── */
  if (size > 0) {
    sqlite3_bind_text(g_from_json, 1, (const char *)data, (int)size,
                      SQLITE_STATIC);
    drain(g_from_json);
  }

  /* ── Round-trip: JSON text → msgpack → JSON text ────────────────── */
  if (size > 0 && size <= 1024) {
    sqlite3_bind_text(g_roundtrip, 1, (const char *)data, (int)size,
                      SQLITE_STATIC);
    drain(g_roundtrip);
  }

  /* ── Aggregate / window functions (fixed data, exercises code paths) */
  drain(g_group_array);
  drain(g_group_object);

  /* ── Schema validation: fuzzed blob against static schemas ─────── */
  run_blob(g_sv_type_int,  data, size);
  run_blob(g_sv_type_text, data, size);
  run_blob(g_sv_type_arr,  data, size);
  run_blob(g_sv_type_map,  data, size);
  run_blob(g_sv_numeric,   data, size);
  run_blob(g_sv_text_len,  data, size);
  run_blob(g_sv_arr_items, data, size);
  run_blob(g_sv_map_full,  data, size);
  run_blob(g_sv_nested,    data, size);
  run_blob(g_sv_enum,      data, size);
  run_blob(g_sv_union,     data, size);

  /* ── Schema validation: fuzzed blob + fuzzed schema as text ────── */
  if (pathsz > 0) {
    run_blob_text(g_sv_fuzz_text, blob, blobsz, pathbuf, pathsz);
  }

  /* ── Schema validation: fuzzed blob + fuzzed schema as blob ────── */
  if (split > 0 && size > split) {
    sqlite3_bind_blob(g_sv_fuzz_blob, 1, data, (int)split, SQLITE_STATIC);
    sqlite3_bind_blob(g_sv_fuzz_blob, 2, data + split,
                      (int)(size - split), SQLITE_STATIC);
    drain(g_sv_fuzz_blob);
  }

  return 0;
}
