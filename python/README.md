# msgpack-blob (Python)

A **pure-Python** port of the standalone [C++ MessagePack Blob API](../cpp/README.md)
from [sqlite-msgpack](../README.md). It creates, queries, mutates and iterates
[MessagePack](https://msgpack.org/) binary blobs and produces **byte-identical**
output to the C++ library and the `sqlite-msgpack` SQLite extension, so blobs are
fully interchangeable across all three.

- Zero dependencies (standard library only)
- Same `Blob` / `Builder` / `Value` / `Iterator` API as the C++ library
- All msgpack primitive types: fixed-width ints, float32/64, ext, timestamp, binary
- JSON conversion modelled on SQLite's JSON1 extension

## Install

```bash
cd python
pip install -e .
```

## Quick start

```python
from msgpack_blob import Blob, Builder, Value, Iterator

# Build from JSON
blob = Blob.from_json('{"name":"Alice","scores":[95,87,91]}')
blob.extract("$.name").as_string()        # 'Alice'
blob.array_length("$.scores")             # 3
blob.to_json()                            # '{"name":"Alice","scores":[95,87,91]}'

# Mutate (copy-on-write — original is unchanged)
updated = blob.set("$.age", Value.integer(30))
updated.to_json()                         # '{"name":"Alice","scores":[95,87,91],"age":30}'

# Build with the streaming Builder
b = (Builder()
     .map_header(2)
     .string("temp").real32(23.5)
     .string("ts").timestamp(1700000000, 500000000)
     .build())

# Iterate (flat "each" or recursive "tree")
for row in Iterator(blob, "$", recursive=True):
    print(row.fullkey, row.type.value)
```

## API overview

| Class | Purpose |
|---|---|
| `Value` | A decoded scalar / sub-blob. Factories: `Value.integer`, `Value.real32`, `Value.string`, `Value.binary`, `Value.ext`, `Value.timestamp`, fixed-width `Value.int8`…`Value.uint64`. |
| `Blob` | Owning byte buffer. `from_json`, `to_json`, `to_json_pretty`, `extract`, `type`, `array_length`, `valid`, and copy-on-write `set` / `insert` / `replace` / `remove` / `array_insert` / `patch`. |
| `Builder` | Streaming encoder. Chainable `nil`/`boolean`/`integer`/`real`/`string`/`binary`/`ext`/`timestamp`/`array_header`/`map_header`/`value`, plus fixed-width integer methods. `build()` → `Blob`. |
| `Iterator` | Cursor over container children (`each` / `tree`). Pythonic iteration (`for row in ...`) or C++-style `next()`/`current()`. |
| `Type`, `IntWidth`, `type_str` | Type enum, integer-width hint enum, and label helper. |

Paths use the same `$`-rooted syntax as the SQLite extension: `$`, `$.key`,
`$[0]`, `$.users[0].email`.

## Tests

```bash
cd python
python -m unittest discover -s tests
```

The suite includes `test_vectors.py`, which replays
[`tests/vectors/blob_vectors.json`](../tests/vectors/blob_vectors.json) — vectors
generated from the C++ reference implementation — to prove byte-identical output.
