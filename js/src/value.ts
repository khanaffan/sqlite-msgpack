/*
 * Value — a decoded scalar or sub-blob MessagePack value.
 *
 * Mirrors msgpack::Value from the C++ Blob library. Integer values use bigint
 * so the full signed/unsigned 64-bit range round-trips exactly.
 */

import { utf8Decode, utf8Encode } from "./format.ts";

export const Type = {
  Nil: "null",
  True: "true",
  False: "false",
  Integer: "integer",
  Real: "real",
  Float32: "float32",
  String: "text",
  Binary: "binary",
  Array: "array",
  Map: "map",
  Ext: "ext",
  Timestamp: "timestamp",
} as const;
export type Type = (typeof Type)[keyof typeof Type];

export const IntWidth = {
  Auto: 0,
  Int8: 1,
  Int16: 2,
  Int32: 3,
  Int64: 4,
  Uint8: 5,
  Uint16: 6,
  Uint32: 7,
  Uint64: 8,
} as const;
export type IntWidth = (typeof IntWidth)[keyof typeof IntWidth];

export function typeStr(t: Type): string {
  return t;
}

const I64_MAX = (1n << 63n) - 1n;

export class Value {
  _type: Type;
  _int: bigint;
  _float: number;
  _str: Uint8Array;
  _blob: Uint8Array;
  _extType: number;
  _tsSec: bigint;
  _tsNsec: number;
  _intWidth: IntWidth;

  constructor() {
    this._type = Type.Nil;
    this._int = 0n;
    this._float = 0;
    this._str = new Uint8Array(0);
    this._blob = new Uint8Array(0);
    this._extType = 0;
    this._tsSec = 0n;
    this._tsNsec = 0;
    this._intWidth = IntWidth.Auto;
  }

  // ── accessors ─────────────────────────────────────────────────────
  type(): Type {
    return this._type;
  }
  isNil(): boolean {
    return this._type === Type.Nil;
  }
  asBool(): boolean {
    return this._type === Type.True;
  }
  asInt64(): bigint {
    if (this._type === Type.Integer) return BigInt.asIntN(64, this._int);
    if (this._type === Type.Real || this._type === Type.Float32) {
      return BigInt(Math.trunc(this._float));
    }
    if (this._type === Type.Timestamp) return this._tsSec;
    if (this._type === Type.True) return 1n;
    return 0n;
  }
  asUint64(): bigint {
    if (this._type === Type.Integer) return BigInt.asUintN(64, this._int);
    return 0n;
  }
  asDouble(): number {
    if (this._type === Type.Real || this._type === Type.Float32) return this._float;
    if (this._type === Type.Integer) return Number(this.asInt64());
    return 0;
  }
  asFloat(): number {
    if (this._type === Type.Float32) return this._float;
    if (this._type === Type.Real) return Math.fround(this._float);
    return 0;
  }
  asString(): string {
    if (this._type === Type.String) return utf8Decode(this._str);
    return "";
  }
  asBytes(): Uint8Array {
    return this._type === Type.String ? this._str : new Uint8Array(0);
  }
  blobData(): Uint8Array {
    return this._blob;
  }
  blobSize(): number {
    return this._blob.length;
  }
  extType(): number {
    return this._extType;
  }
  timestampSeconds(): bigint {
    return this._type === Type.Timestamp ? this._tsSec : 0n;
  }
  timestampNanoseconds(): number {
    return this._type === Type.Timestamp ? this._tsNsec : 0;
  }
  intWidth(): IntWidth {
    return this._intWidth;
  }

  // ── static constructors ───────────────────────────────────────────
  static nil(): Value {
    return new Value();
  }
  static boolean(b: boolean): Value {
    const v = new Value();
    v._type = b ? Type.True : Type.False;
    return v;
  }
  static integer(x: number | bigint): Value {
    const v = new Value();
    v._type = Type.Integer;
    v._int = BigInt(x);
    return v;
  }
  static unsignedInteger(x: number | bigint): Value {
    const v = new Value();
    v._type = Type.Integer;
    v._int = BigInt.asUintN(64, BigInt(x));
    if (v._int > I64_MAX) v._intWidth = IntWidth.Uint64;
    return v;
  }
  static real(d: number): Value {
    const v = new Value();
    v._type = Type.Real;
    v._float = d;
    return v;
  }
  static real32(f: number): Value {
    const v = new Value();
    v._type = Type.Float32;
    v._float = Math.fround(f);
    return v;
  }
  static string(s: string | Uint8Array): Value {
    const v = new Value();
    v._type = Type.String;
    v._str = typeof s === "string" ? utf8Encode(s) : s;
    return v;
  }
  static binary(data: Uint8Array): Value {
    const v = new Value();
    v._type = Type.Binary;
    v._blob = data;
    return v;
  }
  static ext(typeCode: number, data: Uint8Array): Value {
    const v = new Value();
    v._type = Type.Ext;
    v._extType = typeCode | 0;
    v._blob = data;
    return v;
  }
  static timestamp(seconds: number | bigint, nanoseconds: number = 0): Value {
    const v = new Value();
    v._type = Type.Timestamp;
    v._tsSec = BigInt(seconds);
    v._tsNsec = nanoseconds | 0;
    return v;
  }

  static _fixed(width: IntWidth, x: number | bigint): Value {
    const v = new Value();
    v._type = Type.Integer;
    v._int = BigInt(x);
    v._intWidth = width;
    return v;
  }
  static int8(x: number | bigint): Value {
    return Value._fixed(IntWidth.Int8, x);
  }
  static int16(x: number | bigint): Value {
    return Value._fixed(IntWidth.Int16, x);
  }
  static int32(x: number | bigint): Value {
    return Value._fixed(IntWidth.Int32, x);
  }
  static int64(x: number | bigint): Value {
    return Value._fixed(IntWidth.Int64, x);
  }
  static uint8(x: number | bigint): Value {
    return Value._fixed(IntWidth.Uint8, x);
  }
  static uint16(x: number | bigint): Value {
    return Value._fixed(IntWidth.Uint16, x);
  }
  static uint32(x: number | bigint): Value {
    return Value._fixed(IntWidth.Uint32, x);
  }
  static uint64(x: number | bigint): Value {
    return Value._fixed(IntWidth.Uint64, x);
  }
}
