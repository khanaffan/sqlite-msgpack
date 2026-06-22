# msgpack-blob (TypeScript / JavaScript)

A **pure-TypeScript** port of the standalone [C++ MessagePack Blob API](../cpp/README.md)
from [sqlite-msgpack](../README.md). It creates, queries, mutates and iterates
[MessagePack](https://msgpack.org/) binary blobs and produces **byte-identical**
output to the C++ library and the `sqlite-msgpack` SQLite extension, so blobs are
fully interchangeable across all three.

- Zero runtime dependencies
- Same `Blob` / `Builder` / `Value` / `Iterator` API as the C++ library
- All msgpack primitive types: fixed-width ints, float32/64, ext, timestamp, binary
- Full signed/unsigned **64-bit integers** via `bigint`
- JSON conversion modelled on SQLite's JSON1 extension
- Ships ESM + `.d.ts` type declarations

## Install

```bash
cd js
npm install      # dev only (TypeScript, for building/type-checking)
npm run build    # emit dist/ (ESM + .d.ts)
```

## Quick start

```ts
import { Blob, Builder, Value, Iterator } from "msgpack-blob";

// Build from JSON
const blob = Blob.fromJson('{"name":"Alice","scores":[95,87,91]}');
blob.extract("$.name").asString();        // 'Alice'
blob.arrayLength("$.scores");             // 3
blob.toJson();                            // '{"name":"Alice","scores":[95,87,91]}'

// Mutate (copy-on-write — original is unchanged)
const updated = blob.set("$.age", Value.integer(30));
updated.toJson();                         // '{"name":"Alice","scores":[95,87,91],"age":30}'

// Build with the streaming Builder
const b = new Builder()
  .mapHeader(2)
  .string("temp").real32(23.5)
  .string("ts").timestamp(1700000000, 500000000)
  .build();

// Iterate (flat "each" or recursive "tree")
for (const row of new Iterator(blob, "$", true)) {
  console.log(row.fullkey, row.type);
}

// 64-bit integers use bigint
Builder.quote(Value.uint64(18446744073709551615n)).hex(); // 'cfffffffffffffffff'
```

## API overview

| Class | Purpose |
|---|---|
| `Value` | A decoded scalar / sub-blob. Factories: `Value.integer`, `Value.real32`, `Value.string`, `Value.binary`, `Value.ext`, `Value.timestamp`, fixed-width `Value.int8`…`Value.uint64`. Integer accessors return `bigint`. |
| `Blob` | Owning byte buffer. `fromJson`, `toJson`, `toJsonPretty`, `extract`, `type`, `arrayLength`, `valid`, and copy-on-write `set` / `insert` / `replace` / `remove` / `arrayInsert` / `patch`. |
| `Builder` | Streaming encoder. Chainable `nil`/`boolean`/`integer`/`real`/`string`/`binary`/`ext`/`timestamp`/`arrayHeader`/`mapHeader`/`value`, plus fixed-width integer methods. `build()` → `Blob`. |
| `Iterator` | Cursor over container children (`each` / `tree`). Use `for…of` or the C++-style `next()`/`current()` cursor. |
| `Type`, `IntWidth`, `typeStr` | Type labels, integer-width hint, and label helper. |

Integer factories and `Builder.integer` accept `number | bigint`; values are
stored as `bigint` so the full 64-bit range round-trips exactly. Paths use the
same `$`-rooted syntax as the SQLite extension: `$`, `$.key`, `$[0]`,
`$.users[0].email`.

## Tests

```bash
cd js
npm test         # runs test/*.test.ts via Node's built-in test runner
```

Node ≥ 22 runs the TypeScript test files directly (type stripping); no build step
is required for testing. `test/vectors.test.ts` replays
[`tests/vectors/blob_vectors.json`](../tests/vectors/blob_vectors.json) — vectors
generated from the C++ reference implementation — to prove byte-identical output.
