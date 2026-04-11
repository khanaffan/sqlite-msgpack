# C++ MessagePack Blob API

A standalone, zero-dependency C++ library for creating, querying, mutating, and
iterating MessagePack binary blobs.  Produces byte-identical output to the
`sqlite-msgpack` SQL extension so blobs are fully interchangeable between the two
APIs.

**Header:** `#include "msgpack_blob.hpp"`  
**Namespace:** `msgpack`  
**Requires:** C++17 (`std::string_view`)

---

## Table of contents

1. [Primitive types](#primitive-types)
2. [Type enum](#type-enum)
3. [Value](#value)
4. [Blob](#blob)
5. [Builder](#builder)
6. [Iterator](#iterator)
7. [Usage examples](#usage-examples)

---

## Primitive types

The library supports every [MessagePack format](https://github.com/msgpack/msgpack/blob/master/spec.md):

| MsgPack Format | C++ Type | Builder method | Value factory | Value accessor |
|---|---|---|---|---|
| nil | — | `nil()` | `Value::nil()` | `is_nil()` |
| true / false | `bool` | `boolean(bool)` | `Value::boolean(bool)` | `as_bool()` |
| positive fixint / negative fixint | `int64_t` | `integer(int64_t)` | `Value::integer(int64_t)` | `as_int64()` |
| uint8 | `uint8_t` | `uint8(uint8_t)` | `Value::uint8(uint8_t)` | `as_int64()` / `as_uint64()` |
| uint16 | `uint16_t` | `uint16(uint16_t)` | `Value::uint16(uint16_t)` | `as_int64()` / `as_uint64()` |
| uint32 | `uint32_t` | `uint32(uint32_t)` | `Value::uint32(uint32_t)` | `as_int64()` / `as_uint64()` |
| uint64 | `uint64_t` | `uint64(uint64_t)` | `Value::uint64(uint64_t)` | `as_uint64()` |
| int8 | `int8_t` | `int8(int8_t)` | `Value::int8(int8_t)` | `as_int64()` |
| int16 | `int16_t` | `int16(int16_t)` | `Value::int16(int16_t)` | `as_int64()` |
| int32 | `int32_t` | `int32(int32_t)` | `Value::int32(int32_t)` | `as_int64()` |
| int64 | `int64_t` | `int64(int64_t)` | `Value::int64(int64_t)` | `as_int64()` |
| float 32 | `float` | `real32(float)` | `Value::real32(float)` | `as_float()` / `as_double()` |
| float 64 | `double` | `real(double)` | `Value::real(double)` | `as_double()` |
| fixstr / str 8 / str 16 / str 32 | `std::string_view` | `string(string_view)` | `Value::string(string_view)` | `as_string()` |
| bin 8 / bin 16 / bin 32 | `const uint8_t*` | `binary(ptr, len)` | `Value::binary(ptr, len)` | `blob_data()` / `blob_size()` |
| fixext / ext 8 / ext 16 / ext 32 | `const uint8_t*` | `ext(type, ptr, len)` | `Value::ext(type, ptr, len)` | `ext_type()` / `blob_data()` / `blob_size()` |
| timestamp 32 / 64 / 96 | `int64_t` + `uint32_t` | `timestamp(sec)` / `timestamp(sec, nsec)` | `Value::timestamp(sec)` / `Value::timestamp(sec, nsec)` | `timestamp_seconds()` / `timestamp_nanoseconds()` |
| fixarray / array 16 / array 32 | — | `array_header(count)` | — | — |
| fixmap / map 16 / map 32 | — | `map_header(count)` | — | — |

### Compact vs fixed-width integers

`Builder::integer()` and `Value::integer()` use **compact encoding** — they
automatically pick the smallest wire format that fits the value (fixint, uint8,
int16, etc.).  This is the default and matches how JSON integers are stored.

The **fixed-width** methods (`int8`, `int16`, `int32`, `int64`, `uint8`,
`uint16`, `uint32`, `uint64`) force a specific wire encoding regardless of value.
Use these when you need exact byte layout control or SQLite extension
interoperability with `msgpack_int8()`, `msgpack_uint32()`, etc.

```cpp
b.integer(42);   // → fixint (1 byte): most compact
b.int32(42);     // → MP_INT32 (5 bytes): always 32-bit signed
b.uint64(42);    // → MP_UINT64 (9 bytes): always 64-bit unsigned
```

When a fixed-width `Value` is used in mutation (set/insert/replace), the forced
encoding is preserved in the output blob.  The `int_width()` accessor returns the
`IntWidth` hint:

```cpp
auto v = Value::int16(500);
v.int_width();  // IntWidth::Int16
v.as_int64();   // 500
v.type();       // Type::Integer
```

---

## Type enum

```cpp
enum class Type {
    Nil, True, False,
    Integer,            // all integer widths (fixint through int64/uint64)
    Real,               // float 64
    Float32,            // float 32
    String,             // fixstr, str8, str16, str32
    Binary,             // bin8, bin16, bin32
    Array,              // fixarray, array16, array32
    Map,                // fixmap, map16, map32
    Ext,                // ext types (non-timestamp)
    Timestamp           // ext type -1 (msgpack timestamp)
};

const char* type_str(Type t) noexcept;
```

`type_str()` returns a human-readable label: `"null"`, `"true"`, `"false"`,
`"integer"`, `"real"`, `"float32"`, `"text"`, `"binary"`, `"array"`, `"map"`,
`"ext"`, `"timestamp"`.

### Integer width hint

```cpp
enum class IntWidth {
    Auto,               // compact encoding (default)
    Int8, Int16, Int32, Int64,
    Uint8, Uint16, Uint32, Uint64
};
```

---

## Value

A decoded scalar or sub-blob.  Values are cheap to copy and can be used to both
read from and write to blobs.

### Accessors

| Method | Returns | Notes |
|---|---|---|
| `type()` | `Type` | Semantic type |
| `is_nil()` | `bool` | True if Nil |
| `as_bool()` | `bool` | True only for `Type::True` |
| `as_int64()` | `int64_t` | Works for Integer, Real, Float32, Timestamp |
| `as_uint64()` | `uint64_t` | Raw unsigned bits (Integer only) |
| `as_double()` | `double` | Works for Real, Float32, Integer |
| `as_float()` | `float` | Works for Float32, Real |
| `as_string()` | `string_view` | String payload |
| `blob_data()` | `const uint8_t*` | Binary/Ext payload (no header) |
| `blob_size()` | `size_t` | Payload byte count |
| `ext_type()` | `int8_t` | Ext type code (-128..127) |
| `timestamp_seconds()` | `int64_t` | Seconds since epoch |
| `timestamp_nanoseconds()` | `uint32_t` | Sub-second nanoseconds |
| `int_width()` | `IntWidth` | Encoding width hint |

### Static constructors

```cpp
Value::nil()
Value::boolean(bool)
Value::integer(int64_t)             // compact encoding
Value::unsigned_integer(uint64_t)   // compact encoding
Value::real(double)                 // float64
Value::real32(float)                // float32
Value::string(std::string_view)
Value::binary(const uint8_t*, size_t)
Value::ext(int8_t type, const uint8_t* data, size_t len)
Value::timestamp(int64_t sec)
Value::timestamp(int64_t sec, uint32_t nsec)

// Fixed-width integers (force specific wire encoding)
Value::int8(int8_t)       Value::uint8(uint8_t)
Value::int16(int16_t)     Value::uint16(uint16_t)
Value::int32(int32_t)     Value::uint32(uint32_t)
Value::int64(int64_t)     Value::uint64(uint64_t)
```

---

## Blob

An owning byte buffer wrapping a msgpack-encoded value.  Supports read, write, mutation (copy-on-write), and JSON conversion.

### Construction

```cpp
Blob();                                // empty
Blob(const uint8_t* data, size_t n);   // copy from buffer
Blob(std::vector<uint8_t> data);       // move-in
Blob::from_json(const char* json);     // parse JSON → msgpack
```

### Raw access

```cpp
const uint8_t* data() const;
size_t size() const;
bool empty() const;
```

### Validation

```cpp
bool valid() const;
size_t error_position() const;  // byte offset of first error
```

### Type inspection

```cpp
Type type() const;
Type type(const char* path) const;
const char* type_str() const;
const char* type_str(const char* path) const;
```

### Extraction

```cpp
Value extract(const char* path) const;
int64_t array_length() const;
int64_t array_length(const char* path) const;
```

### Mutation (copy-on-write)

All mutation methods return a **new** `Blob`; the original is unchanged.

```cpp
Blob set(const char* path, const Value& val) const;
Blob set(const char* path, const Blob& sub) const;
Blob insert(const char* path, const Value& val) const;
Blob replace(const char* path, const Value& val) const;
Blob remove(const char* path) const;
Blob array_insert(const char* path, const Value& val) const;
Blob patch(const Blob& merge_patch) const;
```

### JSON conversion

```cpp
std::string to_json() const;
std::string to_json_pretty(int indent = 2) const;
static Blob from_json(const char* json);
static Blob from_json(const std::string& json);
```

### Path syntax

Paths follow the same `$`-rooted syntax as the SQLite extension:

| Token | Meaning | Example |
|---|---|---|
| `$` | root element | `$` |
| `.key` | map key lookup | `$.name` |
| `[N]` | array index | `$[0]` |
| chained | nested access | `$.users[0].email` |

---

## Builder

Streaming encoder that produces a `Blob`.  Call methods in order to append
msgpack elements, then finalize with `build()`.

```cpp
Builder b;
b.map_header(3)
  .string("name").string("Alice")
  .string("age").integer(30)
  .string("scores").array_header(3)
    .real(95.5).real(87.3).real(91.0);
Blob blob = b.build();
```

### Scalar methods

| Method | Wire format |
|---|---|
| `nil()` | `0xc0` |
| `boolean(bool)` | `0xc2` / `0xc3` |
| `integer(int64_t)` | compact (fixint → int64) |
| `unsigned_integer(uint64_t)` | compact (fixint → uint64) |
| `real(double)` | `0xcb` (float 64) |
| `real32(float)` | `0xca` (float 32) |
| `string(string_view)` | fixstr / str8 / str16 / str32 |
| `binary(ptr, len)` | bin8 / bin16 / bin32 |
| `ext(type, ptr, len)` | fixext / ext8 / ext16 / ext32 |

### Fixed-width integer methods

| Method | Wire format | Size |
|---|---|---|
| `int8(int8_t)` | `0xd0` | 2 bytes |
| `int16(int16_t)` | `0xd1` | 3 bytes |
| `int32(int32_t)` | `0xd2` | 5 bytes |
| `int64(int64_t)` | `0xd3` | 9 bytes |
| `uint8(uint8_t)` | `0xcc` | 2 bytes |
| `uint16(uint16_t)` | `0xcd` | 3 bytes |
| `uint32(uint32_t)` | `0xce` | 5 bytes |
| `uint64(uint64_t)` | `0xcf` | 9 bytes |

### Container, timestamp & embedding methods

| Method | Description |
|---|---|
| `array_header(uint32_t n)` | Start array with `n` elements |
| `map_header(uint32_t n)` | Start map with `n` key-value pairs |
| `timestamp(int64_t sec)` | Encode timestamp (nsec = 0); picks ts32/ts64/ts96 automatically |
| `timestamp(int64_t sec, uint32_t nsec)` | Encode timestamp with nanoseconds |
| `raw(ptr, len)` | Embed raw msgpack bytes |
| `raw(const Blob&)` | Embed existing Blob |
| `value(const Value&)` | Encode a Value (respects int_width hint) |

### Low-level accessors

| Method | Description |
|---|---|
| `buf_data() → const uint8_t*` | Pointer to bytes accumulated so far |
| `buf_size() → size_t` | Number of bytes accumulated so far |

### Finalize

```cpp
Blob build();                   // consume builder, return Blob
static Blob quote(const Value&); // one-shot: Value → Blob
```

---

## Iterator

Cursor for iterating over container children.  Supports flat (`each`) and
recursive (`tree`) modes, mirroring the SQLite extension's `msgpack_each` and
`msgpack_tree` table-valued functions.

```cpp
Iterator(const Blob& blob, const char* path = "$", bool recursive = false);
bool next();
const EachRow& current() const;
void reset();
```

### EachRow

```cpp
struct EachRow {
    std::string key;       // map key ("" for arrays)
    int64_t     index;     // array index or pair index
    Value       value;     // element value
    Type        type;      // element type
    std::string fullkey;   // e.g. "$.users[0].name"
    std::string path;      // parent path
    size_t      id;        // byte offset in blob
};
```

### Example: flat iteration

```cpp
Blob blob = Blob::from_json(R"({"a":1,"b":2,"c":3})");
Iterator it(blob, "$", false);  // each
while (it.next()) {
    auto& row = it.current();
    printf("%s = %lld\n", row.key.c_str(), row.value.as_int64());
}
```

### Example: recursive tree walk

```cpp
Blob blob = Blob::from_json(R"({"x":{"y":[1,2,3]}})");
Iterator it(blob, "$", true);   // tree
while (it.next()) {
    printf("%s : %s\n", it.current().fullkey.c_str(),
           type_str(it.current().type));
}
```

---

## Usage examples

### Build and extract

```cpp
#include "msgpack_blob.hpp"
using namespace msgpack;

Builder b;
b.map_header(2)
  .string("temp").real32(23.5f)
  .string("ts").value(Value::timestamp(1700000000, 500000000));
Blob blob = b.build();

Value temp = blob.extract("$.temp");
// temp.type()     == Type::Float32
// temp.as_float() == 23.5f

Value ts = blob.extract("$.ts");
// ts.timestamp_seconds()     == 1700000000
// ts.timestamp_nanoseconds() == 500000000
```

### Mutate with typed values

```cpp
Blob blob = Blob::from_json(R"({"x":0})");

// Replace with fixed-width int16
Blob b2 = blob.set("$.x", Value::int16(1000));
// Wire format: 0xd1 0x03 0xe8 (MP_INT16 + big-endian 1000)

// Insert a timestamp
Blob b3 = b2.insert("$.created", Value::timestamp(1700000000));

// Insert binary data
uint8_t raw[] = {0xDE, 0xAD};
Blob b4 = b3.insert("$.data", Value::binary(raw, 2));
```

### Read binary and ext payloads

```cpp
Builder b;
b.map_header(2)
  .string("bin").binary(payload, 4)
  .string("ext").ext(42, extdata, 2);
Blob blob = b.build();

Value bin = blob.extract("$.bin");
// bin.type()      == Type::Binary
// bin.blob_size() == 4       (payload only, no msgpack header)

Value ext = blob.extract("$.ext");
// ext.type()      == Type::Ext
// ext.ext_type()  == 42
// ext.blob_size() == 2       (payload only)
```

### JSON round-trip

```cpp
Blob blob = Blob::from_json(R"({"name":"Alice","scores":[95,87,91]})");
std::string json = blob.to_json();
// json == {"name":"Alice","scores":[95,87,91]}
```

---

## Build integration

The library is built as part of the CMake project:

```cmake
cmake -B build -DMSGPACK_BUILD_TESTS=ON
cmake --build build
```

This produces:
- `libmsgpack_blob_static.a` — static library
- `msgpack_blob_unit` — unit test executable (289 tests, standalone C++ API)
- `msgpack_interop` — integration test executable (197 tests, C++ ↔ SQLite interop)
- `fuzz_blob_corpus_runner` — corpus-based fuzz runner (83+ corpus files)

Link against `msgpack_blob_static` and add `include/` to your include path.

### Testing

```bash
# Run all tests (unit + integration + fuzz corpus)
cd build && ctest --output-on-failure

# Run only C++ API unit tests
./build/msgpack_blob_unit

# Run C++ ↔ SQLite interop tests
./build/msgpack_interop

# Run C++ fuzz corpus
./build/fuzz_blob_corpus_runner tests/fuzz_corpus
```

### Fuzz testing

The C++ API has a dedicated libFuzzer harness (`tests/fuzz_msgpack_blob.cpp`) that
exercises every public entry point with arbitrary byte sequences:

- **Blob**: validation, type inspection, extraction, all mutation modes, patch,
  JSON round-trip — with both static and fuzz-derived paths
- **Builder**: structured fuzzing that interprets random bytes as type tags to
  build arbitrary blobs (all scalar types, fixed-width integers, timestamps, ext,
  binary, containers)
- **Iterator**: flat and recursive iteration with fuzzed paths
- **Value**: round-trip through `extract → quote → extract`
- **Cross-API**: build → mutate → iterate → patch → JSON

To run with libFuzzer (requires Clang with libFuzzer support):

```bash
cmake -B build-fuzz -DMSGPACK_BUILD_FUZZ=ON \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
cmake --build build-fuzz --target fuzz_msgpack_blob
./build-fuzz/fuzz_msgpack_blob tests/fuzz_corpus -max_total_time=300
```

Without libFuzzer, the corpus runner (`fuzz_blob_corpus_runner`) runs all corpus
files through the same code paths as part of the normal CTest suite.

### SQLite interoperability

Blobs created by the C++ API are byte-identical to those produced by the SQLite
extension and vice versa.  The `msgpack_interop` test verifies this by:

1. Creating blobs via SQL (`msgpack_object`, `msgpack_array`, `msgpack_timestamp`,
   typed primitives) and reading them with the C++ API
2. Building blobs with `Builder` and verifying them via SQL (`msgpack_extract`,
   `msgpack_type`, `msgpack_valid`, `msgpack_to_json`)
3. Comparing hex encoding to confirm byte-identical output
4. Mutating in one API and verifying in the other
5. Cross-checking `msgpack_each`/`msgpack_tree` row counts against `Iterator`
