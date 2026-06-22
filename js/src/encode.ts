/* Internal: encoding primitives (mirrors msgpack_blob_encode.cpp). */

import * as F from "./format.ts";
import { Buf } from "./format.ts";
import { IntWidth, Type, Value } from "./value.ts";

export function encNil(out: Buf): void {
  out.push(F.MP_NIL);
}

export function encBool(out: Buf, v: boolean): void {
  out.push(v ? F.MP_TRUE : F.MP_FALSE);
}

export function encInteger(out: Buf, x: bigint): void {
  if (x >= 0n) {
    if (x <= 0x7fn) out.push(Number(x));
    else if (x <= 0xffn) {
      out.push(F.MP_UINT8);
      out.push(Number(x));
    } else if (x <= 0xffffn) {
      out.push(F.MP_UINT16);
      out.pushU16(Number(x));
    } else if (x <= 0xffffffffn) {
      out.push(F.MP_UINT32);
      out.pushU32(Number(x));
    } else {
      out.push(F.MP_UINT64);
      out.pushU64(x);
    }
  } else {
    if (x >= -32n) out.push(Number(x & 0xffn));
    else if (x >= -128n) {
      out.push(F.MP_INT8);
      out.push(Number(x & 0xffn));
    } else if (x >= -32768n) {
      out.push(F.MP_INT16);
      out.pushU16(Number(x & 0xffffn));
    } else if (x >= -2147483648n) {
      out.push(F.MP_INT32);
      out.pushU32(Number(x & 0xffffffffn));
    } else {
      out.push(F.MP_INT64);
      out.pushU64(x);
    }
  }
}

export function encUnsigned(out: Buf, x: bigint): void {
  x = BigInt.asUintN(64, x);
  if (x <= 0x7fn) out.push(Number(x));
  else if (x <= 0xffn) {
    out.push(F.MP_UINT8);
    out.push(Number(x));
  } else if (x <= 0xffffn) {
    out.push(F.MP_UINT16);
    out.pushU16(Number(x));
  } else if (x <= 0xffffffffn) {
    out.push(F.MP_UINT32);
    out.pushU32(Number(x));
  } else {
    out.push(F.MP_UINT64);
    out.pushU64(x);
  }
}

export function encReal(out: Buf, d: number): void {
  out.push(F.MP_FLOAT64);
  out.pushF64(d);
}

export function encReal32(out: Buf, f: number): void {
  out.push(F.MP_FLOAT32);
  out.pushF32(f);
}

export function encString(out: Buf, s: Uint8Array): void {
  const n = s.length;
  if (n <= 31) out.push(F.MP_FIXSTR_MASK | n);
  else if (n <= 0xff) {
    out.push(F.MP_STR8);
    out.push(n);
  } else if (n <= 0xffff) {
    out.push(F.MP_STR16);
    out.pushU16(n);
  } else {
    out.push(F.MP_STR32);
    out.pushU32(n);
  }
  out.pushBytes(s);
}

export function encBinary(out: Buf, data: Uint8Array): void {
  const n = data.length;
  if (n <= 0xff) {
    out.push(F.MP_BIN8);
    out.push(n);
  } else if (n <= 0xffff) {
    out.push(F.MP_BIN16);
    out.pushU16(n);
  } else {
    out.push(F.MP_BIN32);
    out.pushU32(n);
  }
  out.pushBytes(data);
}

export function encExt(out: Buf, typeCode: number, data: Uint8Array): void {
  const n = data.length;
  if (n === 1) out.push(F.MP_FIXEXT1);
  else if (n === 2) out.push(F.MP_FIXEXT2);
  else if (n === 4) out.push(F.MP_FIXEXT4);
  else if (n === 8) out.push(F.MP_FIXEXT8);
  else if (n === 16) out.push(F.MP_FIXEXT16);
  else if (n <= 0xff) {
    out.push(F.MP_EXT8);
    out.push(n);
  } else if (n <= 0xffff) {
    out.push(F.MP_EXT16);
    out.pushU16(n);
  } else {
    out.push(F.MP_EXT32);
    out.pushU32(n);
  }
  out.push(typeCode & 0xff);
  out.pushBytes(data);
}

export function encInt8(out: Buf, x: bigint): void {
  out.push(F.MP_INT8);
  out.push(Number(BigInt.asUintN(8, x)));
}
export function encInt16(out: Buf, x: bigint): void {
  out.push(F.MP_INT16);
  out.pushU16(Number(BigInt.asUintN(16, x)));
}
export function encInt32(out: Buf, x: bigint): void {
  out.push(F.MP_INT32);
  out.pushU32(Number(BigInt.asUintN(32, x)));
}
export function encInt64(out: Buf, x: bigint): void {
  out.push(F.MP_INT64);
  out.pushU64(x);
}
export function encUint8(out: Buf, x: bigint): void {
  out.push(F.MP_UINT8);
  out.push(Number(BigInt.asUintN(8, x)));
}
export function encUint16(out: Buf, x: bigint): void {
  out.push(F.MP_UINT16);
  out.pushU16(Number(BigInt.asUintN(16, x)));
}
export function encUint32(out: Buf, x: bigint): void {
  out.push(F.MP_UINT32);
  out.pushU32(Number(BigInt.asUintN(32, x)));
}
export function encUint64(out: Buf, x: bigint): void {
  out.push(F.MP_UINT64);
  out.pushU64(x);
}

export function encArrayHeader(out: Buf, count: number): void {
  if (count <= 15) out.push(F.MP_FIXARRAY_MASK | count);
  else if (count <= 0xffff) {
    out.push(F.MP_ARRAY16);
    out.pushU16(count);
  } else {
    out.push(F.MP_ARRAY32);
    out.pushU32(count);
  }
}

export function encMapHeader(out: Buf, count: number): void {
  if (count <= 15) out.push(F.MP_FIXMAP_MASK | count);
  else if (count <= 0xffff) {
    out.push(F.MP_MAP16);
    out.pushU16(count);
  } else {
    out.push(F.MP_MAP32);
    out.pushU32(count);
  }
}

export function encTimestamp(out: Buf, sec: bigint, nsec: number): void {
  if (nsec === 0 && sec >= 0n && sec <= 0xffffffffn) {
    out.push(F.MP_FIXEXT4);
    out.push(0xff);
    out.pushU32(Number(sec));
  } else if (sec >= 0n && sec <= 0x3ffffffffn) {
    out.push(F.MP_FIXEXT8);
    out.push(0xff);
    out.pushU64((BigInt(nsec) << 34n) | sec);
  } else {
    out.push(F.MP_EXT8);
    out.push(12);
    out.push(0xff);
    out.pushU32(nsec >>> 0);
    out.pushU64(sec);
  }
}

export function encodeValue(out: Buf, v: Value): void {
  const t = v.type();
  if (t === Type.Nil) encNil(out);
  else if (t === Type.True) encBool(out, true);
  else if (t === Type.False) encBool(out, false);
  else if (t === Type.Integer) {
    const w = v.intWidth();
    if (w === IntWidth.Int8) encInt8(out, v.asInt64());
    else if (w === IntWidth.Int16) encInt16(out, v.asInt64());
    else if (w === IntWidth.Int32) encInt32(out, v.asInt64());
    else if (w === IntWidth.Int64) encInt64(out, v.asInt64());
    else if (w === IntWidth.Uint8) encUint8(out, v.asUint64());
    else if (w === IntWidth.Uint16) encUint16(out, v.asUint64());
    else if (w === IntWidth.Uint32) encUint32(out, v.asUint64());
    else if (w === IntWidth.Uint64) encUint64(out, v.asUint64());
    else encInteger(out, v.asInt64());
  } else if (t === Type.Real) encReal(out, v.asDouble());
  else if (t === Type.Float32) encReal32(out, v.asFloat());
  else if (t === Type.String) encString(out, v.asBytes());
  else if (t === Type.Binary) encBinary(out, v.blobData());
  else if (t === Type.Ext) encExt(out, v.extType(), v.blobData());
  else if (t === Type.Timestamp) encTimestamp(out, v.timestampSeconds(), v.timestampNanoseconds());
  else encNil(out);
}
