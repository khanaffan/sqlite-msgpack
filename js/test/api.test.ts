/* API behaviour and round-trip tests for the TypeScript port. */

import { test } from "node:test";
import assert from "node:assert/strict";

import { Blob, Builder, Iterator, Type, Value, typeStr } from "../src/index.ts";
import { utf8Encode } from "../src/format.ts";

test("Builder matches fromJson", () => {
  const built = new Builder()
    .mapHeader(3)
    .string("name").string("Alice")
    .string("age").integer(30)
    .string("scores").arrayHeader(3)
    .real(95.5).real(87.5).real(91.0)
    .build();
  const ref = Blob.fromJson('{"name":"Alice","age":30,"scores":[95.5,87.5,91.0]}');
  assert.equal(built.hex(), ref.hex());
});

test("quote round-trips type", () => {
  const values = [
    Value.nil(),
    Value.boolean(true),
    Value.integer(-12345),
    Value.real(3.25),
    Value.real32(1.5),
    Value.string("hello"),
    Value.binary(Uint8Array.from([0xde, 0xad])),
    Value.ext(7, Uint8Array.from([1, 2])),
    Value.timestamp(1700000000n, 123456789),
  ];
  for (const v of values) {
    const blob = Builder.quote(v);
    assert.ok(blob.valid());
    assert.equal(blob.extract("$").type(), v.type());
  }
});

const ROUND_TRIP = [
  "null", "true", "false", "0", "-1", "127", "128", "65536",
  "1.5", "0.1", "1e10", '"hi"', "[]", "{}", "[1,2,3]",
  '{"a":1,"b":[2,3],"c":{"d":true}}',
  '{"u":"caf\\u00e9","emoji":"\\ud83d\\ude00"}',
];

test("json bytes stable across round-trip", () => {
  for (const c of ROUND_TRIP) {
    const once = Blob.fromJson(c);
    const twice = Blob.fromJson(once.toJson());
    assert.equal(once.hex(), twice.hex(), c);
  }
});

test("64-bit integers round-trip via bigint", () => {
  const big = 18446744073709551615n;
  const blob = Builder.quote(Value.uint64(big));
  assert.equal(blob.hex(), "cfffffffffffffffff");
  assert.equal(blob.extract("$").asUint64(), big);
  assert.equal(blob.toJson(), "18446744073709551615");

  const negMax = -9223372036854775808n;
  const b2 = Builder.quote(Value.int64(negMax));
  assert.equal(b2.extract("$").asInt64(), negMax);
});

test("extraction", () => {
  const blob = Blob.fromJson('{"name":"Alice","age":30,"tall":true,"pets":["cat","dog"],"addr":{"city":"NYC"}}');
  assert.equal(blob.extract("$.name").asString(), "Alice");
  assert.equal(blob.extract("$.age").asInt64(), 30n);
  assert.equal(blob.extract("$.tall").asBool(), true);
  assert.equal(blob.extract("$.pets[1]").asString(), "dog");
  assert.equal(blob.extract("$.addr.city").asString(), "NYC");
  assert.ok(blob.extract("$.nope").isNil());
  assert.equal(blob.typeStr("$.pets"), "array");
  assert.equal(blob.arrayLength("$.pets"), 2);
  assert.equal(blob.arrayLength("$.name"), -1);
});

test("binary, ext and timestamp", () => {
  const bin = new Builder().binary(Uint8Array.from([1, 2, 3, 4])).build();
  assert.equal(bin.extract("$").type(), Type.Binary);
  assert.deepEqual([...bin.extract("$").blobData()], [1, 2, 3, 4]);
  assert.equal(bin.toJson(), '"01020304"');

  const ext = new Builder().ext(42, Uint8Array.from([0xaa, 0xbb])).build();
  assert.equal(ext.extract("$").extType(), 42);

  const ts = new Builder().timestamp(1700000000, 500000000).build();
  assert.equal(ts.extract("$").timestampSeconds(), 1700000000n);
  assert.equal(ts.extract("$").timestampNanoseconds(), 500000000);
});

test("copy-on-write mutation", () => {
  const orig = Blob.fromJson('{"a":1}');
  const updated = orig.set("$.b", Value.integer(2));
  assert.equal(orig.toJson(), '{"a":1}');
  assert.equal(updated.toJson(), '{"a":1,"b":2}');

  const b = Blob.fromJson('{"a":1,"b":2,"c":3}');
  assert.equal(b.remove("$.b").toJson(), '{"a":1,"c":3}');
  assert.equal(b.patch(Blob.fromJson('{"b":null,"d":4}')).toJson(), '{"a":1,"c":3,"d":4}');

  const arr = Blob.fromJson("[1,2,3]");
  assert.equal(arr.arrayInsert("$[1]", Value.integer(9)).toJson(), "[1,9,2,3]");
  assert.equal(arr.set("$[3]", Value.integer(4)).toJson(), "[1,2,3,4]");
});

test("iterator each and tree", () => {
  const map = Blob.fromJson('{"a":1,"b":2,"c":3}');
  assert.deepEqual(new Iterator(map).rows().map((r) => r.key), ["a", "b", "c"]);
  assert.deepEqual([...new Iterator(map)].map((r) => Number(r.value.asInt64())), [1, 2, 3]);

  const nested = Blob.fromJson('{"x":{"y":[1,2]}}');
  assert.deepEqual(
    [...new Iterator(nested, "$", true)].map((r) => r.fullkey),
    ["$", "$.x", "$.x.y", "$.x.y[0]", "$.x.y[1]"],
  );

  const it = new Iterator(Blob.fromJson("[1,2]"));
  const seen: number[] = [];
  while (it.next()) seen.push(Number(it.current().value.asInt64()));
  assert.deepEqual(seen, [1, 2]);
});

test("validity", () => {
  assert.ok(Blob.fromJson("[1,2,3]").valid());
  assert.equal(new Blob().valid(), false);
  assert.equal(new Blob(Uint8Array.from([0x91])).valid(), false);
});

test("typeStr labels", () => {
  assert.equal(typeStr(Type.Nil), "null");
  assert.equal(typeStr(Type.String), "text");
  assert.equal(typeStr(Type.Float32), "float32");
  assert.equal(typeStr(Type.Timestamp), "timestamp");
});

test("non-UTF-8 str bytes preserved (byte-identical to C++)", () => {
  // {"k": <0xff 0x80 0xfe 0xc0>} — a str with non-UTF-8 payload, as a foreign
  // encoder (C++/SQLite) may produce. C++ passes raw bytes through verbatim.
  const blob = new Blob(Uint8Array.from([0x81, 0xa1, 0x6b, 0xa4, 0xff, 0x80, 0xfe, 0xc0]));
  // Re-encoding the JSON text reproduces the C++ raw output bytes exactly.
  assert.deepEqual(
    [...utf8Encode(blob.toJson())],
    [0x7b, 0x22, 0x6b, 0x22, 0x3a, 0x22, 0xff, 0x80, 0xfe, 0xc0, 0x22, 0x7d],
  );
  const v = blob.extract("$.k");
  assert.deepEqual([...utf8Encode(v.asString())], [0xff, 0x80, 0xfe, 0xc0]);
  // Value.string round-trips a surrogateescape string back to identical bytes.
  const rebuilt = new Builder().string(v.asString()).build();
  assert.deepEqual([...rebuilt.data()], [0xa4, 0xff, 0x80, 0xfe, 0xc0]);
});
