# msgpackblob (Go)

A **pure-Go**, zero-dependency port of the standalone [C++ MessagePack Blob
API](../cpp/README.md) from [sqlite-msgpack](../README.md). It creates,
queries, mutates and iterates [MessagePack](https://msgpack.org/) binary blobs
and produces **byte-identical** output to the C++ library and the
`sqlite-msgpack` SQLite extension, so blobs are fully interchangeable across all
of them.

- Standard library only (no third-party dependencies)
- Same `Blob` / `Builder` / `Value` / `Iterator` API as the C++ library
- All msgpack primitive types: fixed-width ints, float32/64, ext, timestamp, binary
- Native `int64` / `uint64`; Go strings naturally preserve non-UTF-8 bytes
- JSON conversion modelled on SQLite's JSON1 extension

## Install

```bash
go get github.com/khanaffan/sqlite-msgpack/go
```

```go
import mb "github.com/khanaffan/sqlite-msgpack/go"
```

## Quick start

```go
package main

import (
	"fmt"

	mb "github.com/khanaffan/sqlite-msgpack/go"
)

func main() {
	// Build from JSON
	blob := mb.FromJSON(`{"name":"Alice","scores":[95,87,91]}`)
	fmt.Println(blob.Extract("$.name").AsString()) // Alice
	fmt.Println(blob.ArrayLengthAt("$.scores"))    // 3
	fmt.Println(blob.ToJSON())                     // {"name":"Alice","scores":[95,87,91]}

	// Mutate (copy-on-write — original is unchanged)
	updated := blob.Set("$.age", mb.Int(30))
	fmt.Println(updated.ToJSON()) // {"name":"Alice","scores":[95,87,91],"age":30}

	// Build with the streaming Builder
	b := mb.NewBuilder().
		MapHeader(2).
		String("temp").Real32(23.5).
		String("ts").TimestampNs(1700000000, 500000000).
		Build()
	_ = b

	// Iterate (flat "each" or recursive "tree")
	for _, row := range mb.NewIterator(blob, "$", true).Rows() {
		fmt.Println(row.Fullkey, row.Type)
	}
}
```

## API overview

| Type | Purpose |
|---|---|
| `Value` | A decoded scalar / sub-blob. Constructors are package functions: `mb.Int`, `mb.Uint`, `mb.Real32`, `mb.Str` / `mb.StrBytes`, `mb.Bin`, `mb.Ext`, `mb.Timestamp` / `mb.TimestampNs`, fixed-width `mb.Int8`…`mb.Uint64`. |
| `Blob` | Owning byte buffer. `FromJSON`, `ToJSON` / `ToJSONBytes`, `ToJSONPretty`, `Extract`, `TypeAt`, `ArrayLength`, `Valid`, and copy-on-write `Set` / `Insert` / `Replace` / `Remove` / `ArrayInsert` / `Patch`. |
| `Builder` | Streaming encoder. Chainable `Nil`/`Boolean`/`Integer`/`Real`/`String`/`Binary`/`Ext`/`Timestamp`/`ArrayHeader`/`MapHeader`/`Value`, plus fixed-width integer methods. `Build()` → `Blob`. |
| `Iterator` | Cursor over container children (`each` / `tree`). Use the `Next()`/`Current()` cursor or `Rows()`. |
| `Type`, `IntWidth`, `TypeStr` | Type enum (with `String()`), integer-width hint, and label helper. |

Methods come in path-aware pairs where the C++ API overloads: `TypeAt(path)` /
`TypeStrAt(path)` / `ArrayLengthAt(path)` versus the root-level `Type()` /
`TypeStr()` / `ArrayLength()`. Non-UTF-8 string payloads are preserved
byte-exactly via `ToJSONBytes()` and `mb.StrBytes` / `Value.AsBytes`. Paths use
the same `$`-rooted syntax as the SQLite extension: `$`, `$.key`, `$[0]`,
`$.users[0].email`.

## Tests

```bash
cd go
go test ./...
```

`vectors_test.go` replays
[`tests/vectors/blob_vectors.json`](../tests/vectors/blob_vectors.json) — vectors
generated from the C++ reference implementation — to prove byte-identical output.
