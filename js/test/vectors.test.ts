/*
 * Replay the shared cross-language vectors (tests/vectors/blob_vectors.json).
 * Generated from the C++ reference implementation, so passing them proves the
 * TypeScript port is byte-identical.
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { Blob, Builder, Iterator, Value } from "../src/index.ts";

const vectors = JSON.parse(
  readFileSync(new URL("../../tests/vectors/blob_vectors.json", import.meta.url), "utf8"),
);

function hexToBytes(h: string): Uint8Array {
  const out = new Uint8Array(h.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(h.substr(i * 2, 2), 16);
  return out;
}

function buildValue(spec: any): Value {
  switch (spec.k) {
    case "nil":
      return Value.nil();
    case "bool":
      return Value.boolean(Boolean(spec.v));
    case "int":
      return Value.integer(BigInt(spec.v));
    case "uint":
      return Value.unsignedInteger(BigInt(spec.v));
    case "int8":
      return Value.int8(BigInt(spec.v));
    case "int16":
      return Value.int16(BigInt(spec.v));
    case "int32":
      return Value.int32(BigInt(spec.v));
    case "int64":
      return Value.int64(BigInt(spec.v));
    case "uint8":
      return Value.uint8(BigInt(spec.v));
    case "uint16":
      return Value.uint16(BigInt(spec.v));
    case "uint32":
      return Value.uint32(BigInt(spec.v));
    case "uint64":
      return Value.uint64(BigInt(spec.v));
    case "real":
      return Value.real(spec.v);
    case "real32":
      return Value.real32(spec.v);
    case "str":
      return Value.string(spec.v);
    case "binary":
      return Value.binary(hexToBytes(spec.hex));
    case "ext":
      return Value.ext(spec.type, hexToBytes(spec.hex));
    case "timestamp":
      return Value.timestamp(BigInt(spec.sec), spec.nsec);
    default:
      throw new Error("unknown spec kind: " + spec.k);
  }
}

test("from_json → byte-identical msgpack", () => {
  for (const v of vectors.from_json) {
    assert.equal(Blob.fromJson(v.json).hex(), v.hex, v.json);
  }
});

test("to_json", () => {
  for (const v of vectors.to_json) {
    assert.equal(new Blob(hexToBytes(v.hex)).toJson(), v.json, v.hex);
  }
});

test("to_json_pretty", () => {
  for (const v of vectors.to_json_pretty) {
    assert.equal(new Blob(hexToBytes(v.hex)).toJsonPretty(v.indent), v.json, v.hex);
  }
});

test("typed Value → quote", () => {
  for (const v of vectors.typed) {
    assert.equal(Builder.quote(buildValue(v.spec)).hex(), v.hex, JSON.stringify(v.spec));
  }
});

test("mutate", () => {
  for (const v of vectors.mutate) {
    const base = Blob.fromJson(v.base);
    let r: Blob;
    switch (v.op) {
      case "set":
        r = base.set(v.path, buildValue(v.spec));
        break;
      case "insert":
        r = base.insert(v.path, buildValue(v.spec));
        break;
      case "replace":
        r = base.replace(v.path, buildValue(v.spec));
        break;
      case "array_insert":
        r = base.arrayInsert(v.path, buildValue(v.spec));
        break;
      case "remove":
        r = base.remove(v.path);
        break;
      case "set_blob":
        r = base.set(v.path, Blob.fromJson(v.spec.json));
        break;
      case "patch":
        r = base.patch(Blob.fromJson(v.patch));
        break;
      default:
        throw new Error("unknown op " + v.op);
    }
    assert.equal(r.hex(), v.hex, `${v.op} ${v.base} ${v.path ?? ""}`);
  }
});

test("extract type + value", () => {
  for (const v of vectors.extract) {
    const blob = Blob.fromJson(v.base);
    assert.equal(blob.typeStr(v.path), v.type, `${v.base} ${v.path}`);
    assert.equal(Builder.quote(blob.extract(v.path)).toJson(), v.vjson, `${v.base} ${v.path}`);
  }
});

test("array_length", () => {
  for (const v of vectors.array_length) {
    const blob = Blob.fromJson(v.base);
    const got = v.path === "$" ? blob.arrayLength() : blob.arrayLength(v.path);
    assert.equal(got, v.len, `${v.base} ${v.path}`);
  }
});

test("iterate each + tree", () => {
  for (const v of vectors.iterate) {
    const blob = Blob.fromJson(v.base);
    const rows = new Iterator(blob, v.path, v.recursive).rows();
    assert.equal(rows.length, v.rows.length, `${v.base} ${v.path}`);
    for (let i = 0; i < rows.length; i++) {
      const got = rows[i];
      const exp = v.rows[i];
      assert.equal(got.fullkey, exp.fullkey);
      assert.equal(got.path, exp.path);
      assert.equal(got.id, exp.id);
      assert.equal(got.type, exp.type);
      if ("key" in exp) {
        assert.equal(got.key, exp.key);
        assert.equal(got.index, exp.index);
      }
    }
  }
});
