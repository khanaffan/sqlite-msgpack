# msgpack_blob (Rust)

A **pure-Rust**, zero-dependency port of the standalone [C++ MessagePack Blob
API](../cpp/README.md) from [sqlite-msgpack](../README.md). It creates,
queries, mutates and iterates [MessagePack](https://msgpack.org/) binary blobs
and produces **byte-identical** output to the C++ library and the
`sqlite-msgpack` SQLite extension, so blobs are fully interchangeable across all
of them.

- Zero dependencies (no `serde`, no `num-bigint` — std only)
- Same `Blob` / `Builder` / `Value` / `Iterator` API as the C++ library
- All msgpack primitive types: fixed-width ints, float32/64, ext, timestamp, binary
- Native `i64` / `u64`, byte-preserving strings (`string_bytes` / `as_bytes`)
- JSON conversion modelled on SQLite's JSON1 extension

## Add to your project

```toml
[dependencies]
msgpack_blob = { path = "rust" }   # or a published version
```

## Quick start

```rust
use msgpack_blob::{Blob, Builder, Value, Iterator};

// Build from JSON
let blob = Blob::from_json(r#"{"name":"Alice","scores":[95,87,91]}"#);
assert_eq!(blob.extract("$.name").as_string(), "Alice");
assert_eq!(blob.array_length_at("$.scores"), 3);
assert_eq!(blob.to_json(), r#"{"name":"Alice","scores":[95,87,91]}"#);

// Mutate (copy-on-write — original is unchanged)
let updated = blob.set("$.age", &Value::integer(30));
assert_eq!(updated.to_json(), r#"{"name":"Alice","scores":[95,87,91],"age":30}"#);

// Build with the streaming Builder
let b = Builder::new()
    .map_header(2)
    .string("temp").real32(23.5)
    .string("ts").timestamp_ns(1_700_000_000, 500_000_000)
    .build();

// Iterate (flat "each" or recursive "tree")
for row in Iterator::new(&blob, "$", true) {
    println!("{} {}", row.fullkey, msgpack_blob::type_str(row.ty));
}
```

## API overview

| Type | Purpose |
|---|---|
| `Value` | A decoded scalar / sub-blob. Constructors: `Value::integer`, `Value::real32`, `Value::string` / `Value::string_bytes`, `Value::binary`, `Value::ext`, `Value::timestamp` / `Value::timestamp_ns`, fixed-width `Value::int8`…`Value::uint64`. |
| `Blob` | Owning byte buffer. `from_json`, `to_json` / `to_json_bytes`, `to_json_pretty`, `extract`, `type_at`, `array_length`, `valid`, and copy-on-write `set` / `insert` / `replace` / `remove` / `array_insert` / `patch`. |
| `Builder` | Streaming encoder. Chainable `nil`/`boolean`/`integer`/`real`/`string`/`binary`/`ext`/`timestamp`/`array_header`/`map_header`/`value`, plus fixed-width integer methods. `build()` → `Blob`. |
| `Iterator` | Cursor over container children (`each` / `tree`). Use the `next()`/`current()` cursor, `for row in iter`, or `.rows()`. |
| `Type`, `IntWidth`, `type_str` | Type enum, integer-width hint, and label helper. |

Methods come in path-aware pairs where the C++ API overloads: `type_at(path)` /
`type_str_at(path)` / `array_length_at(path)` versus the root-level
`root_type()` / `type_str()` / `array_length()`. Non-UTF-8 string payloads are
preserved byte-exactly via `to_json_bytes()` and `Value::string_bytes` /
`Value::as_bytes`. Paths use the same `$`-rooted syntax as the SQLite extension:
`$`, `$.key`, `$[0]`, `$.users[0].email`.

## Tests

```bash
cd rust
cargo test
```

The suite includes `tests/vectors.rs`, which replays
[`tests/vectors/blob_vectors.json`](../tests/vectors/blob_vectors.json) — vectors
generated from the C++ reference implementation — to prove byte-identical output.
A tiny std-only JSON reader (`tests/common/mod.rs`) keeps the crate itself
dependency-free.
