# sqlite-msgpack

[![CI](https://github.com/khanaffan/sqlite-msgpack/actions/workflows/ci.yml/badge.svg)](https://github.com/khanaffan/sqlite-msgpack/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**sqlite-msgpack** is a SQLite extension that adds functions for creating, querying, and
mutating [MessagePack](https://msgpack.org/) BLOBs directly inside SQL queries. The API is
modeled after SQLite's built-in
[JSON1 extension](https://www.sqlite.org/json1.html) so it is immediately familiar to anyone
who has used `json_extract`, `json_set`, or `json_each`.

MessagePack values are stored as ordinary SQLite `BLOB` columns. All functions are
deterministic and side-effect free (copy-on-write mutation).

---

## Table of contents

1. [Loading the extension](#loading-the-extension)
2. [Building from source](#building-from-source)
3. [Quick start](#quick-start)
4. [Path syntax](#path-syntax)
5. [Type system](#type-system)
6. [Function reference](#function-reference)
   - [Encoding & validation](#encoding--validation)
   - [Construction](#construction)
   - [Extraction](#extraction)
   - [Mutation](#mutation)
   - [JSON conversion](#json-conversion)
   - [Aggregates](#aggregates)
   - [Table-valued functions](#table-valued-functions)
7. [BLOB auto-embedding](#blob-auto-embedding)
8. [MessagePack spec compliance](#messagepack-spec-compliance)
9. [Performance benchmarks](#performance-benchmarks)
10. [Serialised-size comparison](#serialised-size-comparison)

---

## Loading the extension

```sql
-- SQLite shell
.load ./msgpack

-- Application code (C)
sqlite3_load_extension(db, "./msgpack", NULL, &zErr);
```

Once loaded, all `msgpack_*` functions and the two table-valued functions
(`msgpack_each`, `msgpack_tree`) are available in every database connection.

---

## Building from source

Requires **CMake ≥ 3.16** and a C99 compiler (GCC, Clang, or MSVC).

```bash
cmake -B build
cmake --build build
ctest --test-dir build
```

The default build produces:

| Artifact | Description |
|----------|-------------|
| `msgpack.so` / `msgpack.dll` / `msgpack.dylib` | Loadable extension |
| `sqlite3_cli` | SQLite shell with extension-loading enabled |

### Build options

| CMake option | Default | Effect |
|---|---|---|
| `BUILD_SHARED_LIBS` | `ON` | Build the loadable extension |
| `MSGPACK_BUILD_TESTS` | `ON` | Build and register CTest test targets |

---

## Quick start

```sql
-- Encode a scalar value
SELECT hex(msgpack_quote(42));         -- 2A
SELECT hex(msgpack_quote('hello'));    -- A568656C6C6F
SELECT hex(msgpack_quote(NULL));       -- C0  (nil)

-- Build a map and extract from it
SELECT msgpack_extract(
  msgpack_object('name', 'Alice', 'age', 30),
  '$.name'
);  -- Alice

-- Build an array and query its length
SELECT msgpack_array_length(msgpack_array(10, 20, 30));  -- 3

-- Update a value (returns a new BLOB, original is unchanged)
SELECT msgpack_to_json(
  msgpack_set(msgpack_object('a', 1), '$.b', 2)
);  -- {"a":1,"b":2}

-- Convert to/from JSON
SELECT msgpack_to_json(msgpack_from_json('[1,true,"hi"]'));  -- [1,true,"hi"]

-- Aggregate rows into a msgpack array
CREATE TABLE t(v INTEGER);
INSERT INTO t VALUES (1),(2),(3);
SELECT msgpack_to_json(msgpack_group_array(v)) FROM t;  -- [1,2,3]
```

---

## Path syntax

Path expressions follow the same conventions as SQLite's JSON1:

| Expression | Meaning |
|------------|---------|
| `$` | The root element |
| `$.key` | Value stored under `key` in a map |
| `$[N]` | Element at zero-based index `N` in an array |
| `$.a.b[2].c` | Chained navigation |

A path that does not exist returns `NULL` from scalar functions and is
treated as a missing element in multi-path and mutation operations.

---

## Type system

Each MessagePack element has a type string returned by `msgpack_type()`:

| Type string | MessagePack formats | SQL affinity |
|-------------|---------------------|--------------|
| `null`      | nil (`0xc0`) | NULL |
| `bool`      | false (`0xc2`), true (`0xc3`) | INTEGER (0 or 1) when extracted |
| `integer`   | positive fixint, negative fixint, uint8–uint64, int8–int64 | INTEGER |
| `real`      | float32, float64 | REAL |
| `text`      | fixstr, str8, str16, str32 | TEXT |
| `blob`      | bin8, bin16, bin32 | BLOB |
| `array`     | fixarray, array16, array32 | BLOB |
| `map`       | fixmap, map16, map32 | BLOB |

> **Note on SQL booleans.** SQLite has no boolean type; `1=1` evaluates to
> integer `1`. The `bool` type is only produced by `msgpack_from_json` when it
> parses JSON `true` or `false` literals, or by passing a manually crafted
> BLOB containing `0xc2`/`0xc3`. When a bool element is *extracted* with
> `msgpack_extract` it becomes SQL integer `0` or `1`.

---

## Function reference

### Encoding & validation

#### `msgpack_quote(value)`

Encodes a single SQL value as a msgpack BLOB using the smallest valid format:

```sql
SELECT hex(msgpack_quote(NULL));   -- C0
SELECT hex(msgpack_quote(0));      -- 00
SELECT hex(msgpack_quote(127));    -- 7F  (positive fixint)
SELECT hex(msgpack_quote(128));    -- CC80  (uint8)
SELECT hex(msgpack_quote(-32));    -- E0  (negative fixint)
SELECT hex(msgpack_quote(-33));    -- D0DF  (int8)
SELECT hex(msgpack_quote(3.14));   -- CB400...  (float64)
SELECT hex(msgpack_quote('hi'));   -- A268 69  (fixstr)
SELECT hex(msgpack_quote(x'DEAD'));-- C402DEAD  (bin8)
```

#### `msgpack_valid(mp)`  
#### `msgpack_valid(mp, path)`

Returns `1` if `mp` is a well-formed msgpack BLOB, `0` otherwise. With a
`path` argument, returns `1` if the element at that path is well-formed.

```sql
SELECT msgpack_valid(msgpack_quote(42));          -- 1
SELECT msgpack_valid(x'FF');                      -- 0  (0xFF is reserved)
SELECT msgpack_valid(msgpack_array(1,2,3), '$[1]'); -- 1
```

#### `msgpack_error_position(mp)`

Returns the byte offset (1-based) of the first encoding error in `mp`, or
`0` if the BLOB is valid. Useful for diagnosing corrupt data.

```sql
SELECT msgpack_error_position(msgpack_quote(42));  -- 0  (valid)
SELECT msgpack_error_position(x'C1');              -- 1  (0xC1 is never-used byte)
```

---

### Construction

#### `msgpack(mp)`

Validates `mp` and returns it unchanged. Raises an error if `mp` is not a
well-formed msgpack BLOB. Passing `NULL` returns `NULL`.

```sql
SELECT hex(msgpack(msgpack_array(1,2)));  -- same bytes
SELECT msgpack('not a blob');             -- error
```

#### `msgpack_array(v1, v2, ...)`

Returns a msgpack array containing the encoded values. Any number of
arguments is accepted (including zero for an empty array). BLOB arguments
that are themselves valid msgpack are embedded directly as nested elements;
raw BLOBs are stored as `bin` type.

```sql
SELECT msgpack_to_json(msgpack_array(1, 'hello', NULL, 3.14));
-- [1,"hello",null,3.14]

SELECT msgpack_to_json(msgpack_array());
-- []

-- Nested array
SELECT msgpack_to_json(
  msgpack_array(msgpack_array(1,2), msgpack_array(3,4))
);
-- [[1,2],[3,4]]
```

#### `msgpack_object(key1, val1, key2, val2, ...)`

Returns a msgpack map. Arguments must appear in key/value pairs; keys must
be TEXT. Duplicate keys are allowed (last writer wins on extraction, matching
JSON1 behaviour). Raises an error if an odd number of arguments is given.

```sql
SELECT msgpack_to_json(msgpack_object('x', 1, 'y', 2));
-- {"x":1,"y":2}

-- Nested map
SELECT msgpack_to_json(
  msgpack_object('user', msgpack_object('name', 'Bob', 'age', 25))
);
-- {"user":{"name":"Bob","age":25}}
```

---

### Extraction

#### `msgpack_extract(mp, path)`  
#### `msgpack_extract(mp, path1, path2, ...)`

Returns the element at `path` as a SQL value. Arrays and maps are returned
as BLOBs. A missing path returns `NULL`.

With two or more path arguments, returns a new msgpack array whose elements
correspond to each path in order. Missing paths produce `nil` elements in
the result array.

```sql
SELECT msgpack_extract(msgpack_object('a',1,'b',2), '$.a');  -- 1
SELECT msgpack_extract(msgpack_array(10,20,30), '$[2]');     -- 30

-- Multi-path → array
SELECT msgpack_to_json(
  msgpack_extract(msgpack_object('a',1,'b',2,'c',3), '$.a','$.c')
);
-- [1,3]
```

#### `msgpack_type(mp)`  
#### `msgpack_type(mp, path)`

Returns the type string of the root element or the element at `path`. See
[Type system](#type-system) for the full list. Returns `NULL` if the path
does not exist.

```sql
SELECT msgpack_type(msgpack_quote(42));                       -- integer
SELECT msgpack_type(msgpack_quote('hi'));                     -- text
SELECT msgpack_type(msgpack_array(1,2), '$[0]');              -- integer
SELECT msgpack_type(msgpack_object('a', msgpack_array(1)));   -- map
```

#### `msgpack_array_length(mp)`  
#### `msgpack_array_length(mp, path)`

Returns the number of elements in the array or map at `mp` (or at `path`
inside `mp`). Returns `NULL` for scalar values.

```sql
SELECT msgpack_array_length(msgpack_array(1,2,3));    -- 3
SELECT msgpack_array_length(msgpack_object('a',1));   -- 1
SELECT msgpack_array_length(msgpack_quote(99));        -- NULL
SELECT msgpack_array_length(
  msgpack_object('arr', msgpack_array(10,20,30)), '$.arr'
);                                                    -- 3
```

---

### Mutation

All mutation functions are **copy-on-write**: they return a new msgpack BLOB
and leave the original unchanged.

Each function accepts multiple `path, value` pairs in a single call,
applied left to right.

#### `msgpack_set(mp, path, val, ...)`

Inserts or replaces the element at each `path`. If the path does not exist,
it is created (if the parent exists). Equivalent to JSON1's `json_set`.

```sql
SELECT msgpack_to_json(msgpack_set(msgpack_object('a',1), '$.b', 2));
-- {"a":1,"b":2}

SELECT msgpack_to_json(msgpack_set(msgpack_object('a',1), '$.a', 99));
-- {"a":99}
```

#### `msgpack_insert(mp, path, val, ...)`

Inserts only — no-op when the path already exists.

```sql
SELECT msgpack_to_json(msgpack_insert(msgpack_object('a',1), '$.a', 99));
-- {"a":1}  (unchanged — 'a' exists)

SELECT msgpack_to_json(msgpack_insert(msgpack_object('a',1), '$.b', 2));
-- {"a":1,"b":2}
```

#### `msgpack_replace(mp, path, val, ...)`

Replaces only — no-op when the path does not exist.

```sql
SELECT msgpack_to_json(msgpack_replace(msgpack_object('a',1), '$.b', 2));
-- {"a":1}  (unchanged — 'b' missing)

SELECT msgpack_to_json(msgpack_replace(msgpack_object('a',1), '$.a', 99));
-- {"a":99}
```

#### `msgpack_remove(mp, path, ...)`

Removes each element at `path`. Missing paths are silently ignored.

```sql
SELECT msgpack_to_json(msgpack_remove(msgpack_object('a',1,'b',2), '$.a'));
-- {"b":2}

SELECT msgpack_to_json(msgpack_remove(msgpack_array(10,20,30), '$[1]'));
-- [10,30]
```

#### `msgpack_array_insert(mp, path, val, ...)`

Inserts `val` *before* the element at the array index specified in `path`.
Use `$[#]` to append to the end of the array.

```sql
SELECT msgpack_to_json(msgpack_array_insert(msgpack_array(1,3), '$[1]', 2));
-- [1,2,3]

SELECT msgpack_to_json(msgpack_array_insert(msgpack_array(1,2), '$[#]', 3));
-- [1,2,3]
```

#### `msgpack_patch(mp, patch)`

Applies an [RFC 7386 merge-patch](https://www.rfc-editor.org/rfc/rfc7386)
to `mp`. `patch` must be a msgpack map. Keys in `patch` whose values are
`nil` remove the corresponding key from `mp`; all other keys are set.

```sql
SELECT msgpack_to_json(
  msgpack_patch(
    msgpack_from_json('{"a":1,"b":2}'),
    msgpack_from_json('{"b":null,"c":3}')
  )
);
-- {"a":1,"c":3}
```

---

### JSON conversion

#### `msgpack_from_json(json_text)`

Parses a JSON text string and returns the equivalent msgpack BLOB.
Supports all JSON value types: `null`, `true`, `false`, numbers, strings,
arrays, and objects.

```sql
SELECT hex(msgpack_from_json('null'));        -- C0
SELECT hex(msgpack_from_json('true'));        -- C3
SELECT hex(msgpack_from_json('false'));       -- C2
SELECT hex(msgpack_from_json('42'));          -- 2A
SELECT hex(msgpack_from_json('"hello"'));     -- A568656C6C6F
SELECT msgpack_to_json(msgpack_from_json('[1,2,3]'));       -- [1,2,3]
SELECT msgpack_to_json(msgpack_from_json('{"a":1}'));       -- {"a":1}
```

#### `msgpack_to_json(mp)`  
#### `msgpack_to_jsonb(mp)` *(alias)*

Serializes a msgpack BLOB to a JSON text string. Type mapping:

| msgpack type | JSON output |
|---|---|
| nil | `null` |
| false | `false` |
| true | `true` |
| integer | number |
| float32 / float64 | number (`null` for NaN/Infinity) |
| text (UTF-8) | `"string"` with JSON escaping |
| bin | lowercase hex string (e.g. `"deadbeef"`) |
| array | `[...]` |
| map | `{...}` |
| ext | `null` |

```sql
SELECT msgpack_to_json(msgpack_array(1, 'hi', NULL, true));
-- [1,"hi",null,true]   -- note: SQL TRUE is integer 1, not msgpack true
```

#### `msgpack_pretty(mp)`  
#### `msgpack_pretty(mp, indent)`

Returns a multi-line, indented JSON string. `indent` controls the number of
spaces per level (default `2`). Useful for debugging stored BLOBs.

```sql
SELECT msgpack_pretty(msgpack_from_json('{"a":1,"b":[2,3]}'));
-- {
--   "a": 1,
--   "b": [
--     2,
--     3
--   ]
-- }
```

---

### Aggregates

Both aggregate functions also work as **window functions** with an `OVER`
clause.

#### `msgpack_group_array(value)`

Accumulates values from every row in the group into a single msgpack array.

```sql
CREATE TABLE scores(player TEXT, score INTEGER);
INSERT INTO scores VALUES ('Alice',10),('Bob',20),('Carol',30);

SELECT msgpack_to_json(msgpack_group_array(score)) FROM scores;
-- [10,20,30]

-- As a window function
SELECT player,
       msgpack_to_json(msgpack_group_array(score) OVER ()) AS all_scores
FROM scores;
```

#### `msgpack_group_object(key, value)`

Accumulates key/value pairs into a single msgpack map. Later rows with a
duplicate key overwrite earlier ones.

```sql
SELECT msgpack_to_json(msgpack_group_object(player, score)) FROM scores;
-- {"Alice":10,"Bob":20,"Carol":30}
```

---

### Table-valued functions

Table-valued functions expand a msgpack BLOB into a result set. Both
functions emit **one row per element** and share the same column schema:

| Column | Type | Description |
|--------|------|-------------|
| `key` | TEXT or INTEGER | Map key (text) or array index (integer) |
| `value` | any | The element value as a SQL scalar; arrays/maps as BLOBs |
| `type` | TEXT | Type string (see [Type system](#type-system)) |
| `atom` | any | Scalar value; `NULL` for arrays and maps |
| `id` | INTEGER | Unique node identifier within this traversal |
| `parent` | INTEGER | `id` of the parent node; `NULL` for the root |
| `fullkey` | TEXT | Full path expression to this element (e.g. `$.a[2]`) |
| `path` | TEXT | Path to the *parent* container |

#### `msgpack_each(mp)`  
#### `msgpack_each(mp, path)`

Iterates the **direct children** of the root element (or of the element at
`path`). Does not recurse into nested arrays or maps.

```sql
SELECT key, value, type
FROM msgpack_each(msgpack_object('a', 1, 'b', 'hello', 'c', NULL));
```

| key | value | type |
|-----|-------|------|
| a | 1 | integer |
| b | hello | text |
| c | NULL | null |

```sql
-- Iterate an array
SELECT key, value FROM msgpack_each(msgpack_array(10, 20, 30));
-- 0 | 10
-- 1 | 20
-- 2 | 30

-- Start at a nested path
SELECT key, value
FROM msgpack_each(msgpack_object('arr', msgpack_array(1,2,3)), '$.arr');
-- 0 | 1
-- 1 | 2
-- 2 | 3
```

#### `msgpack_tree(mp)`  
#### `msgpack_tree(mp, path)`

Recursively traverses **all nodes** in the subtree rooted at `mp` (or at
`path`), in depth-first pre-order. Unlike `msgpack_each`, it descends into
nested containers.

```sql
SELECT fullkey, type, atom
FROM msgpack_tree(msgpack_from_json('{"x":[1,2],"y":3}'));
```

| fullkey | type | atom |
|---------|------|------|
| `$` | map | NULL |
| `$.x` | array | NULL |
| `$.x[0]` | integer | 1 |
| `$.x[1]` | integer | 2 |
| `$.y` | integer | 3 |

```sql
-- Count all leaf nodes (non-containers) in a nested structure
SELECT count(*)
FROM msgpack_tree(msgpack_from_json('{"a":{"b":[1,2,3]}}'))
WHERE type NOT IN ('array','map');
-- 3
```

---

## BLOB auto-embedding

When a BLOB value is passed to any construction or mutation function, the
extension checks whether it is valid msgpack:

- **Valid msgpack BLOB** → embedded directly as a nested element (transparent nesting)
- **Invalid / raw BLOB** → stored as msgpack `bin` type

This makes composition natural without extra wrapping:

```sql
-- Works just like JSON1's json() wrapper for nested values
SELECT msgpack_to_json(
  msgpack_object(
    'tags',   msgpack_array('sqlite', 'msgpack'),
    'meta',   msgpack_object('version', 1)
  )
);
-- {"tags":["sqlite","msgpack"],"meta":{"version":1}}
```

---

## MessagePack spec compliance

This extension implements the
[MessagePack specification](https://github.com/msgpack/msgpack/blob/master/spec.md)
in full:

- **Smallest encoding rule** — values are always encoded in the most compact
  valid format (e.g., `42` uses positive fixint `0x2a`, not uint8 `0xcc 0x2a`).
- **All 36 format families** — nil, bool, positive fixint, negative fixint,
  uint8/16/32/64, int8/16/32/64, float32/64, fixstr, str8/16/32,
  bin8/16/32, fixarray, array16/32, fixmap, map16/32, fixext1/2/4/8/16,
  ext8/16/32.
- **Copy-on-write mutation** — no in-place modification; every mutation
  returns a new BLOB.
- **Never-used byte** — `0xc1` is treated as an error by `msgpack_valid`
  and `msgpack_error_position`.

---

## License

[MIT](LICENSE) — see the `LICENSE` file for details.

SQLite itself is in the [public domain](https://www.sqlite.org/copyright.html).

---

<!-- BENCH_START -->
## Performance benchmarks

> Nanoseconds per operation — lower is better.  
> `json/mp` and `jsonb/mp`: ratio relative to msgpack (>1 means msgpack is faster).  
> Platform: macOS · SQLite 3.53.0

| Operation                          | msgpack ns/op | json ns/op | jsonb ns/op |  json/mp |  jsonb/mp |
|----------------------------------- |---------------|------------|-------------|----------|-----------|
| map build (4 fields)               |         138.1 |      186.5 |       254.9 |    1.35x |     1.85x |
| array build (8 integers)           |         113.1 |      158.9 |       202.4 |    1.40x |     1.79x |
| nested build (map + array)         |         254.7 |      167.6 |       324.1 |    0.66x |     1.27x |
| extract text field ($.name)        |         130.0 |      252.6 |       143.9 |    1.94x |     1.11x |
| extract numeric field ($.score)    |         128.9 |      263.6 |       144.7 |    2.04x |     1.12x |
| type check ($.name)                |         127.4 |      254.0 |         n/a |    1.99x |       n/a |
| set field ($.extra = 42)           |         300.0 |      391.4 |       197.5 |    1.30x |     0.66x |
| remove field ($.active)            |         240.5 |      328.7 |       165.1 |    1.37x |     0.69x |
| group_array (1 000 rows)           |       22665.0 |    24658.3 |     30725.0 |    1.09x |     1.36x |
| group_object (1 000 rows)          |       35120.0 |    36185.0 |     48598.3 |    1.03x |     1.38x |
| each — iterate 4-field map       |        1108.8 |      292.3 |       234.6 |    0.26x |     0.21x |
| valid check                        |         125.3 |      234.2 |         n/a |    1.87x |       n/a |
| from_json (parse JSON text → blob) |         293.9 |        n/a |         n/a |      n/a |       n/a |
| to_json (serialise blob → JSON text) |         305.2 |        n/a |         n/a |      n/a |       n/a |



![sqlite-msgpack vs JSON vs JSONB performance](docs/bench.png)

<!-- BENCH_END -->



---

<!-- SIZE_START -->
## Serialised-size comparison

Byte sizes for identical logical data encoded as msgpack, JSON (text),
and SQLite JSONB (binary).  `mp/json %` and `mp/jsonb %` show msgpack
size as a percentage of the other format — below 100 % means msgpack
is more compact.

> Platform: macOS · SQLite 3.53.0

| Payload                                |  msgpack (B) |   json (B) |   jsonb (B) |  mp/json % |  mp/jsonb % |
|--------------------------------------- |--------------|------------|-------------|------------|-------------|
| null                                   |            1 |          4 |           1 |        25% |        100% |
| true  (via from_json)                  |            1 |          4 |           1 |        25% |        100% |
| false (via from_json)                  |            1 |          5 |           1 |        20% |        100% |
| integer 0                              |            1 |          1 |           2 |       100% |         50% |
| integer 42                             |            1 |          2 |           3 |        50% |         33% |
| integer 128  (uint8 boundary)          |            2 |          3 |           4 |        67% |         50% |
| integer 65536  (uint32 range)          |            5 |          5 |           6 |       100% |         83% |
| integer 1 000 000 000                  |            5 |         10 |          11 |        50% |         45% |
| integer -1                             |            1 |          2 |           3 |        50% |         33% |
| integer -128  (int8 boundary)          |            2 |          4 |           5 |        50% |         40% |
| float 3.14                             |            9 |          4 |           5 |       225% |        180% |
| string  2 chars  ("hi")                |            3 |          4 |           3 |        75% |        100% |
| string 11 chars  ("hello world")       |           12 |         13 |          12 |        92% |        100% |
| string 31 chars  (fixstr max)          |           32 |         33 |          33 |        97% |         97% |
| string 32 chars  (str8 range)          |           34 |         34 |          34 |       100% |        100% |
| array  3 ints   [1,2,3]                |            4 |          7 |           7 |        57% |         57% |
| array 10 ints   [1..10]                |           11 |         22 |          23 |        50% |         48% |
| array 10 floats  [1.1..10.1]           |           91 |         42 |          43 |       217% |        212% |
| map  2 fields   {a:1,b:2}              |            7 |         13 |           9 |        54% |         78% |
| user record (5 fields)                 |           49 |         63 |          46 |        78% |        107% |
| full user record (8 fields)            |          102 |        130 |         104 |        78% |         98% |
| nested  3 levels  {a:{b:{c:42}}}       |           10 |         20 |          12 |        50% |         83% |
| array of 3 user objects                |           62 |         98 |          75 |        63% |         83% |
| config object (mixed types)            |           87 |        116 |          88 |        75% |         99% |
| array 100 integers [1..100]            |          103 |        293 |         295 |        35% |         35% |



![Serialised-size comparison](docs/size_comparison.png)

<!-- SIZE_END -->
