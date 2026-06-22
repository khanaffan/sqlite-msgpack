package msgpackblob_test

// API behaviour and round-trip tests for the Go port.

import (
	"bytes"
	"testing"

	mb "github.com/khanaffan/sqlite-msgpack/go"
)

func TestBuilderMatchesFromJSON(t *testing.T) {
	built := mb.NewBuilder().
		MapHeader(3).
		String("name").String("Alice").
		String("age").Integer(30).
		String("scores").ArrayHeader(3).
		Real(95.5).Real(87.5).Real(91.0).
		Build()
	ref := mb.FromJSON(`{"name":"Alice","age":30,"scores":[95.5,87.5,91.0]}`)
	if built.Hex() != ref.Hex() {
		t.Errorf("got %s want %s", built.Hex(), ref.Hex())
	}
}

func TestQuoteRoundtripsType(t *testing.T) {
	values := []mb.Value{
		mb.Nil(),
		mb.Bool(true),
		mb.Int(-12345),
		mb.Real(3.25),
		mb.Real32(1.5),
		mb.Str("hello"),
		mb.Bin([]byte{0xde, 0xad}),
		mb.Ext(7, []byte{1, 2}),
		mb.TimestampNs(1700000000, 123456789),
	}
	for _, v := range values {
		blob := mb.Quote(v)
		if !blob.Valid() {
			t.Errorf("quote(%v) invalid", v.Type())
		}
		if blob.Extract("$").Type() != v.Type() {
			t.Errorf("roundtrip type: got %v want %v", blob.Extract("$").Type(), v.Type())
		}
	}
}

func TestJSONBytesStable(t *testing.T) {
	cases := []string{
		"null", "true", "false", "0", "-1", "127", "128", "65536",
		"1.5", "0.1", "1e10", `"hi"`, "[]", "{}", "[1,2,3]",
		`{"a":1,"b":[2,3],"c":{"d":true}}`,
		`{"u":"caf\u00e9","emoji":"\ud83d\ude00"}`,
	}
	for _, c := range cases {
		once := mb.FromJSON(c)
		twice := mb.FromJSON(once.ToJSON())
		if once.Hex() != twice.Hex() {
			t.Errorf("%s: %s != %s", c, once.Hex(), twice.Hex())
		}
	}
}

func TestIntegers64Bit(t *testing.T) {
	blob := mb.Quote(mb.Uint64(^uint64(0)))
	if blob.Hex() != "cfffffffffffffffff" {
		t.Errorf("uint64 max hex: %s", blob.Hex())
	}
	if blob.Extract("$").AsUint64() != ^uint64(0) {
		t.Errorf("uint64 max roundtrip")
	}
	if blob.ToJSON() != "18446744073709551615" {
		t.Errorf("uint64 json: %s", blob.ToJSON())
	}
	const minI64 = -9223372036854775808
	neg := mb.Quote(mb.Int64(minI64))
	if neg.Extract("$").AsInt64() != minI64 {
		t.Errorf("int64 min roundtrip")
	}
}

func TestExtraction(t *testing.T) {
	blob := mb.FromJSON(`{"name":"Alice","age":30,"tall":true,"pets":["cat","dog"],"addr":{"city":"NYC"}}`)
	if blob.Extract("$.name").AsString() != "Alice" {
		t.Error("name")
	}
	if blob.Extract("$.age").AsInt64() != 30 {
		t.Error("age")
	}
	if !blob.Extract("$.tall").AsBool() {
		t.Error("tall")
	}
	if blob.Extract("$.pets[1]").AsString() != "dog" {
		t.Error("pets[1]")
	}
	if blob.Extract("$.addr.city").AsString() != "NYC" {
		t.Error("addr.city")
	}
	if !blob.Extract("$.nope").IsNil() {
		t.Error("missing should be nil")
	}
	if blob.TypeStrAt("$.pets") != "array" {
		t.Error("pets type")
	}
	if blob.ArrayLengthAt("$.pets") != 2 {
		t.Error("pets length")
	}
	if blob.ArrayLengthAt("$.name") != -1 {
		t.Error("name length should be -1")
	}
}

func TestBinaryExtTimestamp(t *testing.T) {
	bin := mb.NewBuilder().Binary([]byte{1, 2, 3, 4}).Build()
	if bin.Extract("$").Type() != mb.TypeBinary {
		t.Error("binary type")
	}
	if !bytes.Equal(bin.Extract("$").BlobData(), []byte{1, 2, 3, 4}) {
		t.Error("binary data")
	}
	if bin.ToJSON() != `"01020304"` {
		t.Errorf("binary json: %s", bin.ToJSON())
	}

	ext := mb.NewBuilder().Ext(42, []byte{0xaa, 0xbb}).Build()
	if ext.Extract("$").ExtType() != 42 {
		t.Error("ext type")
	}

	ts := mb.NewBuilder().TimestampNs(1700000000, 500000000).Build()
	if ts.Extract("$").TimestampSeconds() != 1700000000 {
		t.Error("ts seconds")
	}
	if ts.Extract("$").TimestampNanoseconds() != 500000000 {
		t.Error("ts nanos")
	}
}

func TestCopyOnWriteMutation(t *testing.T) {
	orig := mb.FromJSON(`{"a":1}`)
	updated := orig.Set("$.b", mb.Int(2))
	if orig.ToJSON() != `{"a":1}` {
		t.Errorf("orig mutated: %s", orig.ToJSON())
	}
	if updated.ToJSON() != `{"a":1,"b":2}` {
		t.Errorf("set: %s", updated.ToJSON())
	}

	b := mb.FromJSON(`{"a":1,"b":2,"c":3}`)
	if b.Remove("$.b").ToJSON() != `{"a":1,"c":3}` {
		t.Errorf("remove: %s", b.Remove("$.b").ToJSON())
	}
	patched := b.Patch(mb.FromJSON(`{"b":null,"d":4}`))
	if patched.ToJSON() != `{"a":1,"c":3,"d":4}` {
		t.Errorf("patch: %s", patched.ToJSON())
	}

	arr := mb.FromJSON("[1,2,3]")
	if arr.ArrayInsert("$[1]", mb.Int(9)).ToJSON() != "[1,9,2,3]" {
		t.Errorf("array_insert: %s", arr.ArrayInsert("$[1]", mb.Int(9)).ToJSON())
	}
	if arr.Set("$[3]", mb.Int(4)).ToJSON() != "[1,2,3,4]" {
		t.Errorf("append: %s", arr.Set("$[3]", mb.Int(4)).ToJSON())
	}
}

func TestIterator(t *testing.T) {
	m := mb.FromJSON(`{"a":1,"b":2,"c":3}`)
	var keys []string
	for _, r := range mb.NewIterator(m, "$", false).Rows() {
		keys = append(keys, r.Key)
	}
	if len(keys) != 3 || keys[0] != "a" || keys[2] != "c" {
		t.Errorf("each keys: %v", keys)
	}

	nested := mb.FromJSON(`{"x":{"y":[1,2]}}`)
	var fullkeys []string
	for _, r := range mb.NewIterator(nested, "$", true).Rows() {
		fullkeys = append(fullkeys, r.Fullkey)
	}
	want := []string{"$", "$.x", "$.x.y", "$.x.y[0]", "$.x.y[1]"}
	if len(fullkeys) != len(want) {
		t.Fatalf("tree fullkeys: %v", fullkeys)
	}
	for i := range want {
		if fullkeys[i] != want[i] {
			t.Errorf("tree[%d]: %s != %s", i, fullkeys[i], want[i])
		}
	}

	it := mb.NewIterator(mb.FromJSON("[1,2]"), "$", false)
	var seen []int64
	for it.Next() {
		seen = append(seen, it.Current().Value.AsInt64())
	}
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Errorf("cursor: %v", seen)
	}
}

func TestValidity(t *testing.T) {
	if !mb.FromJSON("[1,2,3]").Valid() {
		t.Error("valid array")
	}
	if mb.NewBlob(nil).Valid() {
		t.Error("empty should be invalid")
	}
	if mb.NewBlob([]byte{0x91}).Valid() {
		t.Error("truncated array should be invalid")
	}
}

func TestTypeStr(t *testing.T) {
	if mb.TypeStr(mb.TypeNil) != "null" || mb.TypeStr(mb.TypeString) != "text" ||
		mb.TypeStr(mb.TypeFloat32) != "float32" || mb.TypeStr(mb.TypeTimestamp) != "timestamp" {
		t.Error("type labels")
	}
}

func TestTruncatedMapKeyNoPanic(t *testing.T) {
	// One-entry map whose str8 key declares length 10 but supplies only 2 bytes.
	// The C++ reference bails via skipOne; the port must not panic.
	blob := mb.NewBlob([]byte{0x81, 0xd9, 0x0a, 0x61, 0x62})
	if !bytes.Equal(blob.Set("$.x", mb.Int(1)).Data(), blob.Data()) {
		t.Error("set should return original on truncated key")
	}
	if !bytes.Equal(blob.Remove("$.x").Data(), blob.Data()) {
		t.Error("remove should return original on truncated key")
	}
	if !bytes.Equal(blob.Patch(mb.FromJSON(`{"a":1}`)).Data(), blob.Data()) {
		t.Error("patch should return original on truncated key")
	}
	if len(mb.NewIterator(blob, "$", false).Rows()) != 0 {
		t.Error("flat iterate should yield 0 rows on truncated key")
	}
	_ = mb.NewIterator(blob, "$", true).Rows()
	// Truncated fixstr key as well.
	fix := mb.NewBlob([]byte{0x81, 0xa5, 0x68, 0x69})
	if !bytes.Equal(fix.Set("$.x", mb.Int(1)).Data(), fix.Data()) {
		t.Error("set should return original on truncated fixstr key")
	}
	if len(mb.NewIterator(fix, "$", false).Rows()) != 0 {
		t.Error("flat iterate should yield 0 rows on truncated fixstr key")
	}
}

func TestNonUTF8Preserved(t *testing.T) {
	// {"k": <0xff 0x80 0xfe 0xc0>} — a str with non-UTF-8 payload, as a foreign
	// encoder (C++/SQLite) may produce. C++ passes raw bytes through verbatim.
	blob := mb.NewBlob([]byte{0x81, 0xa1, 0x6b, 0xa4, 0xff, 0x80, 0xfe, 0xc0})
	want := []byte{0x7b, 0x22, 0x6b, 0x22, 0x3a, 0x22, 0xff, 0x80, 0xfe, 0xc0, 0x22, 0x7d}
	if !bytes.Equal(blob.ToJSONBytes(), want) {
		t.Errorf("to_json bytes: %x want %x", blob.ToJSONBytes(), want)
	}
	v := blob.Extract("$.k")
	if !bytes.Equal(v.AsBytes(), []byte{0xff, 0x80, 0xfe, 0xc0}) {
		t.Errorf("as_bytes: %x", v.AsBytes())
	}
	rebuilt := mb.NewBuilder().StringBytes(v.AsBytes()).Build()
	if rebuilt.Hex() != "a4ff80fec0" {
		t.Errorf("rebuilt: %s", rebuilt.Hex())
	}
}
