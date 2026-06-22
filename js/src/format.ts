/*
 * Internal: MessagePack format constants, byte-order helpers, the growable
 * output buffer, and skip_one. Private to the implementation; mirrors
 * cpp/src/msgpack_blob_detail.hpp and the skip routine from the decode module.
 */

export const MAX_DEPTH = 200;
export const MAX_OUTPUT = 64 * 1024 * 1024;

export const MP_NIL = 0xc0;
export const MP_FALSE = 0xc2;
export const MP_TRUE = 0xc3;
export const MP_BIN8 = 0xc4;
export const MP_BIN16 = 0xc5;
export const MP_BIN32 = 0xc6;
export const MP_EXT8 = 0xc7;
export const MP_EXT16 = 0xc8;
export const MP_EXT32 = 0xc9;
export const MP_FLOAT32 = 0xca;
export const MP_FLOAT64 = 0xcb;
export const MP_UINT8 = 0xcc;
export const MP_UINT16 = 0xcd;
export const MP_UINT32 = 0xce;
export const MP_UINT64 = 0xcf;
export const MP_INT8 = 0xd0;
export const MP_INT16 = 0xd1;
export const MP_INT32 = 0xd2;
export const MP_INT64 = 0xd3;
export const MP_FIXEXT1 = 0xd4;
export const MP_FIXEXT2 = 0xd5;
export const MP_FIXEXT4 = 0xd6;
export const MP_FIXEXT8 = 0xd7;
export const MP_FIXEXT16 = 0xd8;
export const MP_STR8 = 0xd9;
export const MP_STR16 = 0xda;
export const MP_STR32 = 0xdb;
export const MP_ARRAY16 = 0xdc;
export const MP_ARRAY32 = 0xdd;
export const MP_MAP16 = 0xde;
export const MP_MAP32 = 0xdf;

export const MP_FIXMAP_MASK = 0x80;
export const MP_FIXARRAY_MASK = 0x90;
export const MP_FIXSTR_MASK = 0xa0;

export const MP_TIMESTAMP_TYPE = 0xff;

export const U64_MASK = 0xffffffffffffffffn;

// ── big-endian read helpers ─────────────────────────────────────────
export function read16(a: Uint8Array, i: number): number {
  return (a[i] << 8) | a[i + 1];
}
export function read32(a: Uint8Array, i: number): number {
  return ((a[i] << 24) | (a[i + 1] << 16) | (a[i + 2] << 8) | a[i + 3]) >>> 0;
}
export function read64(a: Uint8Array, i: number): bigint {
  let v = 0n;
  for (let k = 0; k < 8; k++) v = (v << 8n) | BigInt(a[i + k]);
  return v;
}

const _scratch = new DataView(new ArrayBuffer(8));
export function readF32(a: Uint8Array, i: number): number {
  for (let k = 0; k < 4; k++) _scratch.setUint8(k, a[i + k]);
  return _scratch.getFloat32(0, false);
}
export function readF64(a: Uint8Array, i: number): number {
  for (let k = 0; k < 8; k++) _scratch.setUint8(k, a[i + k]);
  return _scratch.getFloat64(0, false);
}

// ── growable output buffer ──────────────────────────────────────────
export class Buf {
  bytes: number[];
  constructor() {
    this.bytes = [];
  }
  get length(): number {
    return this.bytes.length;
  }
  push(b: number): void {
    this.bytes.push(b & 0xff);
  }
  pushU16(v: number): void {
    this.bytes.push((v >>> 8) & 0xff, v & 0xff);
  }
  pushU32(v: number): void {
    this.bytes.push((v >>> 24) & 0xff, (v >>> 16) & 0xff, (v >>> 8) & 0xff, v & 0xff);
  }
  pushU64(v: bigint): void {
    const b = BigInt.asUintN(64, v);
    for (let s = 56n; s >= 0n; s -= 8n) this.bytes.push(Number((b >> s) & 0xffn));
  }
  pushF32(f: number): void {
    _scratch.setFloat32(0, f, false);
    for (let k = 0; k < 4; k++) this.bytes.push(_scratch.getUint8(k));
  }
  pushF64(d: number): void {
    _scratch.setFloat64(0, d, false);
    for (let k = 0; k < 8; k++) this.bytes.push(_scratch.getUint8(k));
  }
  pushBytes(src: Uint8Array | number[]): void {
    for (let k = 0; k < src.length; k++) this.bytes.push(src[k] & 0xff);
  }
  pushSlice(a: Uint8Array, start: number, end: number): void {
    for (let i = start; i < end; i++) this.bytes.push(a[i]);
  }
  toBytes(): Uint8Array {
    return Uint8Array.from(this.bytes);
  }
}

// ── skip_one — offset just past one element, or 0 on malformed input ─
export function skipOne(a: Uint8Array, n: number, i: number): number {
  return skipOneD(a, n, i, 0);
}

function skipOneD(a: Uint8Array, n: number, i: number, depth: number): number {
  if (depth > MAX_DEPTH) return 0;
  if (i >= n) return 0;
  const b = a[i];
  i += 1;

  if (b <= 0x7f) return i;
  if (b >= 0xe0) return i;

  switch (b) {
    case MP_NIL:
    case MP_FALSE:
    case MP_TRUE:
      return i;
    case MP_FLOAT32:
      return i + 4 <= n ? i + 4 : 0;
    case MP_FLOAT64:
    case MP_INT64:
    case MP_UINT64:
      return i + 8 <= n ? i + 8 : 0;
    case MP_UINT8:
    case MP_INT8:
      return i + 1 <= n ? i + 1 : 0;
    case MP_UINT16:
    case MP_INT16:
      return i + 2 <= n ? i + 2 : 0;
    case MP_UINT32:
    case MP_INT32:
      return i + 4 <= n ? i + 4 : 0;
    case MP_BIN8:
    case MP_STR8: {
      if (i + 1 > n) return 0;
      const sz = a[i];
      i += 1;
      return sz <= n - i ? i + sz : 0;
    }
    case MP_BIN16:
    case MP_STR16: {
      if (i + 2 > n) return 0;
      const sz = read16(a, i);
      i += 2;
      return sz <= n - i ? i + sz : 0;
    }
    case MP_BIN32:
    case MP_STR32: {
      if (i + 4 > n) return 0;
      const sz = read32(a, i);
      i += 4;
      return sz <= n - i ? i + sz : 0;
    }
    case MP_FIXEXT1:
      return i + 2 <= n ? i + 2 : 0;
    case MP_FIXEXT2:
      return i + 3 <= n ? i + 3 : 0;
    case MP_FIXEXT4:
      return i + 5 <= n ? i + 5 : 0;
    case MP_FIXEXT8:
      return i + 9 <= n ? i + 9 : 0;
    case MP_FIXEXT16:
      return i + 17 <= n ? i + 17 : 0;
    case MP_EXT8: {
      if (i + 2 > n) return 0;
      const sz = a[i];
      i += 2;
      return sz <= n - i ? i + sz : 0;
    }
    case MP_EXT16: {
      if (i + 3 > n) return 0;
      const sz = read16(a, i);
      i += 3;
      return sz <= n - i ? i + sz : 0;
    }
    case MP_EXT32: {
      if (i + 5 > n) return 0;
      const sz = read32(a, i);
      i += 5;
      return sz <= n - i ? i + sz : 0;
    }
    default:
      break;
  }

  if (b >= 0xa0 && b <= 0xbf) {
    const sz = b & 0x1f;
    return sz <= n - i ? i + sz : 0;
  }

  if (b >= 0x90 && b <= 0x9f) {
    const count = b & 0x0f;
    for (let j = 0; j < count; j++) {
      i = skipOneD(a, n, i, depth + 1);
      if (!i) return 0;
    }
    return i;
  }

  if (b >= 0x80 && b <= 0x8f) {
    const count = b & 0x0f;
    for (let j = 0; j < count; j++) {
      i = skipOneD(a, n, i, depth + 1);
      if (!i) return 0;
      i = skipOneD(a, n, i, depth + 1);
      if (!i) return 0;
    }
    return i;
  }

  if (b === MP_ARRAY16 || b === MP_ARRAY32) {
    let count: number;
    if (b === MP_ARRAY16) {
      if (i + 2 > n) return 0;
      count = read16(a, i);
      i += 2;
    } else {
      if (i + 4 > n) return 0;
      count = read32(a, i);
      i += 4;
    }
    for (let j = 0; j < count; j++) {
      i = skipOneD(a, n, i, depth + 1);
      if (!i) return 0;
    }
    return i;
  }

  if (b === MP_MAP16 || b === MP_MAP32) {
    let count: number;
    if (b === MP_MAP16) {
      if (i + 2 > n) return 0;
      count = read16(a, i);
      i += 2;
    } else {
      if (i + 4 > n) return 0;
      count = read32(a, i);
      i += 4;
    }
    for (let j = 0; j < count; j++) {
      i = skipOneD(a, n, i, depth + 1);
      if (!i) return 0;
      i = skipOneD(a, n, i, depth + 1);
      if (!i) return 0;
    }
    return i;
  }

  return 0;
}

// ── UTF-8 helpers ───────────────────────────────────────────────────
// Byte-preserving (lossless) UTF-8 codec matching Python's
// `bytes.decode("utf-8", "surrogateescape")` / `str.encode(...)`. The C++
// reference passes raw `str` bytes through verbatim, so a plain replacing
// TextDecoder (which substitutes U+FFFD) would diverge on non-UTF-8 input.
// Invalid bytes round-trip through lone low surrogates U+DC80..U+DCFF.
export function utf8Decode(bytes: Uint8Array): string {
  let out = "";
  let i = 0;
  const n = bytes.length;
  while (i < n) {
    const b0 = bytes[i];
    if (b0 < 0x80) {
      out += String.fromCharCode(b0);
      i++;
      continue;
    }
    let len: number;
    let cp: number;
    if (b0 >= 0xc2 && b0 <= 0xdf) {
      len = 2;
      cp = b0 & 0x1f;
    } else if (b0 >= 0xe0 && b0 <= 0xef) {
      len = 3;
      cp = b0 & 0x0f;
    } else if (b0 >= 0xf0 && b0 <= 0xf4) {
      len = 4;
      cp = b0 & 0x07;
    } else {
      out += String.fromCharCode(0xdc00 + b0);
      i++;
      continue;
    }
    if (i + len > n) {
      out += String.fromCharCode(0xdc00 + b0);
      i++;
      continue;
    }
    const b1 = bytes[i + 1];
    let valid: boolean;
    if (len === 2) valid = b1 >= 0x80 && b1 <= 0xbf;
    else if (len === 3) {
      if (b0 === 0xe0) valid = b1 >= 0xa0 && b1 <= 0xbf;
      else if (b0 === 0xed) valid = b1 >= 0x80 && b1 <= 0x9f; // exclude surrogates
      else valid = b1 >= 0x80 && b1 <= 0xbf;
    } else {
      if (b0 === 0xf0) valid = b1 >= 0x90 && b1 <= 0xbf;
      else if (b0 === 0xf4) valid = b1 >= 0x80 && b1 <= 0x8f;
      else valid = b1 >= 0x80 && b1 <= 0xbf;
    }
    if (!valid) {
      out += String.fromCharCode(0xdc00 + b0);
      i++;
      continue;
    }
    cp = (cp << 6) | (b1 & 0x3f);
    let ok = true;
    for (let k = 2; k < len; k++) {
      const bk = bytes[i + k];
      if (bk < 0x80 || bk > 0xbf) {
        ok = false;
        break;
      }
      cp = (cp << 6) | (bk & 0x3f);
    }
    if (!ok) {
      out += String.fromCharCode(0xdc00 + b0);
      i++;
      continue;
    }
    if (cp <= 0xffff) out += String.fromCharCode(cp);
    else {
      cp -= 0x10000;
      out += String.fromCharCode(0xd800 + (cp >> 10), 0xdc00 + (cp & 0x3ff));
    }
    i += len;
  }
  return out;
}

export function utf8Encode(s: string): Uint8Array {
  const out: number[] = [];
  for (let i = 0; i < s.length; i++) {
    let cp = s.charCodeAt(i);
    if (cp >= 0xdc80 && cp <= 0xdcff) {
      out.push(cp & 0xff); // surrogateescape byte
      continue;
    }
    if (cp >= 0xd800 && cp <= 0xdbff && i + 1 < s.length) {
      const lo = s.charCodeAt(i + 1);
      if (lo >= 0xdc00 && lo <= 0xdfff) {
        cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
        i++;
      }
    }
    if (cp < 0x80) out.push(cp);
    else if (cp < 0x800) out.push(0xc0 | (cp >> 6), 0x80 | (cp & 0x3f));
    else if (cp < 0x10000) out.push(0xe0 | (cp >> 12), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
    else out.push(0xf0 | (cp >> 18), 0x80 | ((cp >> 12) & 0x3f), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
  }
  return Uint8Array.from(out);
}
