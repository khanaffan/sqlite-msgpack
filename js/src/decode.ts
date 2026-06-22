/* Internal: decoding & inspection (mirrors msgpack_blob_decode.cpp). */

import * as F from "./format.ts";
import { Type, Value } from "./value.ts";

export const RC_OK = 0;
export const RC_ERROR = 1;
export const RC_NOTFOUND = 2;

export interface LookupResult {
  rc: number;
  iStart: number;
  iEnd: number;
}

export interface PathStep {
  kind: number | string; // 0 | -1 | 'k' | 'i'
  pi: number;
  key: string;
  idx: number;
}

export function isValid(a: Uint8Array, n: number): boolean {
  if (n === 0) return false;
  return F.skipOne(a, n, 0) === n;
}

export function errorPosition(a: Uint8Array, n: number): number {
  if (n === 0) return 0;
  if (F.skipOne(a, n, 0) === n) return 0;
  let i = 0;
  while (i < n) {
    const nxt = F.skipOne(a, n, i);
    if (!nxt) return i;
    i = nxt;
  }
  return 0;
}

function isTimestampExt(a: Uint8Array, n: number, i: number): boolean {
  if (i >= n) return false;
  const b = a[i];
  if (b === F.MP_FIXEXT4 && i + 6 <= n && a[i + 1] === F.MP_TIMESTAMP_TYPE) return true;
  if (b === F.MP_FIXEXT8 && i + 10 <= n && a[i + 1] === F.MP_TIMESTAMP_TYPE) return true;
  if (b === F.MP_EXT8 && i + 3 <= n && a[i + 1] === 12 && a[i + 2] === F.MP_TIMESTAMP_TYPE) return true;
  return false;
}

function decodeTimestamp(a: Uint8Array, n: number, i: number): [bigint, number] | null {
  if (i >= n) return null;
  const b = a[i];
  if (b === F.MP_FIXEXT4 && i + 6 <= n && a[i + 1] === F.MP_TIMESTAMP_TYPE) {
    return [BigInt(F.read32(a, i + 2)), 0];
  }
  if (b === F.MP_FIXEXT8 && i + 10 <= n && a[i + 1] === F.MP_TIMESTAMP_TYPE) {
    const v = F.read64(a, i + 2);
    return [v & 0x3ffffffffn, Number(v >> 34n)];
  }
  if (b === F.MP_EXT8 && i + 15 <= n && a[i + 1] === 12 && a[i + 2] === F.MP_TIMESTAMP_TYPE) {
    const nsec = F.read32(a, i + 3);
    const sec = BigInt.asIntN(64, F.read64(a, i + 7));
    return [sec, nsec];
  }
  return null;
}

export function getType(a: Uint8Array, n: number, i: number): Type {
  if (i >= n) return Type.Nil;
  const b = a[i];
  if (b === F.MP_NIL) return Type.Nil;
  if (b === F.MP_TRUE) return Type.True;
  if (b === F.MP_FALSE) return Type.False;
  if (b <= 0x7f || b >= 0xe0) return Type.Integer;
  if (b >= 0xa0 && b <= 0xbf) return Type.String;
  if (b >= 0x90 && b <= 0x9f) return Type.Array;
  if (b >= 0x80 && b <= 0x8f) return Type.Map;
  switch (b) {
    case F.MP_UINT8:
    case F.MP_UINT16:
    case F.MP_UINT32:
    case F.MP_UINT64:
    case F.MP_INT8:
    case F.MP_INT16:
    case F.MP_INT32:
    case F.MP_INT64:
      return Type.Integer;
    case F.MP_FLOAT32:
      return Type.Float32;
    case F.MP_FLOAT64:
      return Type.Real;
    case F.MP_STR8:
    case F.MP_STR16:
    case F.MP_STR32:
      return Type.String;
    case F.MP_BIN8:
    case F.MP_BIN16:
    case F.MP_BIN32:
      return Type.Binary;
    case F.MP_ARRAY16:
    case F.MP_ARRAY32:
      return Type.Array;
    case F.MP_MAP16:
    case F.MP_MAP32:
      return Type.Map;
    case F.MP_EXT8:
    case F.MP_EXT16:
    case F.MP_EXT32:
    case F.MP_FIXEXT1:
    case F.MP_FIXEXT2:
    case F.MP_FIXEXT4:
    case F.MP_FIXEXT8:
    case F.MP_FIXEXT16:
      return isTimestampExt(a, n, i) ? Type.Timestamp : Type.Ext;
    default:
      return Type.Nil;
  }
}

export function getTypeStr(a: Uint8Array, n: number, i: number): string {
  return getType(a, n, i);
}

export function getContainerCount(a: Uint8Array, n: number, i: number): number {
  if (i >= n) return -1;
  const b = a[i];
  if (b >= 0x90 && b <= 0x9f) return b & 0x0f;
  if (b >= 0x80 && b <= 0x8f) return b & 0x0f;
  if (b === F.MP_ARRAY16 && i + 3 <= n) return F.read16(a, i + 1);
  if (b === F.MP_ARRAY32 && i + 5 <= n) return F.read32(a, i + 1);
  if (b === F.MP_MAP16 && i + 3 <= n) return F.read16(a, i + 1);
  if (b === F.MP_MAP32 && i + 5 <= n) return F.read32(a, i + 1);
  return -1;
}

export function pathStep(zpath: string, pi: number): PathStep {
  let i = pi;
  if (i >= zpath.length) return { kind: 0, pi: i, key: "", idx: 0 };
  const c = zpath[i];
  if (c === ".") {
    i += 1;
    const start = i;
    while (i < zpath.length && zpath[i] !== "." && zpath[i] !== "[") i += 1;
    return { kind: "k", pi: i, key: zpath.slice(start, i), idx: 0 };
  }
  if (c === "[") {
    let idx = 0;
    let hasDigit = false;
    i += 1;
    while (i < zpath.length && zpath[i] >= "0" && zpath[i] <= "9") {
      idx = idx * 10 + (zpath.charCodeAt(i) - 48);
      i += 1;
      hasDigit = true;
    }
    if (!hasDigit || i >= zpath.length || zpath[i] !== "]") return { kind: -1, pi: i, key: "", idx: 0 };
    i += 1;
    return { kind: "i", pi: i, key: "", idx };
  }
  return { kind: -1, pi: i, key: "", idx: 0 };
}

// Returns the key bytes for a map key at offset i, or null.
function keyAt(a: Uint8Array, n: number, i: number): Uint8Array | null {
  const kb = a[i];
  let klen: number;
  let koff: number;
  if (kb >= 0xa0 && kb <= 0xbf) {
    klen = kb & 0x1f;
    koff = i + 1;
  } else if (kb === F.MP_STR8 && i + 2 <= n) {
    klen = a[i + 1];
    koff = i + 2;
  } else if (kb === F.MP_STR16 && i + 3 <= n) {
    klen = F.read16(a, i + 1);
    koff = i + 3;
  } else if (kb === F.MP_STR32 && i + 5 <= n) {
    klen = F.read32(a, i + 1);
    koff = i + 5;
  } else {
    return null;
  }
  if (klen > n - koff) return null;
  return a.subarray(koff, koff + klen);
}

function bytesEqual(x: Uint8Array, y: Uint8Array): boolean {
  if (x.length !== y.length) return false;
  for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return false;
  return true;
}

export function lookup(a: Uint8Array, n: number, iroot: number, zpath: string): LookupResult {
  if (!zpath || zpath[0] !== "$") return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
  let icur = iroot;
  let pi = 1;

  for (;;) {
    const st = pathStep(zpath, pi);
    pi = st.pi;

    if (st.kind === 0) {
      const inext = F.skipOne(a, n, icur);
      const iEnd = inext ? inext : n;
      return { rc: inext || icur === n ? RC_OK : RC_ERROR, iStart: icur, iEnd };
    }
    if (st.kind === -1) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
    if (icur >= n) return { rc: RC_NOTFOUND, iStart: 0, iEnd: 0 };

    if (st.kind === "i") {
      const b = a[icur];
      let count: number;
      let elemOff: number;
      if (b >= 0x90 && b <= 0x9f) {
        count = b & 0x0f;
        elemOff = icur + 1;
      } else if (b === F.MP_ARRAY16) {
        if (icur + 3 > n) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
        count = F.read16(a, icur + 1);
        elemOff = icur + 3;
      } else if (b === F.MP_ARRAY32) {
        if (icur + 5 > n) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
        count = F.read32(a, icur + 1);
        elemOff = icur + 5;
      } else {
        return { rc: RC_NOTFOUND, iStart: 0, iEnd: 0 };
      }
      if (st.idx < 0 || st.idx >= count) return { rc: RC_NOTFOUND, iStart: 0, iEnd: 0 };
      icur = elemOff;
      for (let j = 0; j < st.idx; j++) {
        icur = F.skipOne(a, n, icur);
        if (!icur) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
      }
    } else {
      const b = a[icur];
      let count: number;
      let elemOff: number;
      if (b >= 0x80 && b <= 0x8f) {
        count = b & 0x0f;
        elemOff = icur + 1;
      } else if (b === F.MP_MAP16) {
        if (icur + 3 > n) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
        count = F.read16(a, icur + 1);
        elemOff = icur + 3;
      } else if (b === F.MP_MAP32) {
        if (icur + 5 > n) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
        count = F.read32(a, icur + 1);
        elemOff = icur + 5;
      } else {
        return { rc: RC_NOTFOUND, iStart: 0, iEnd: 0 };
      }
      const keyBytes = F.utf8Encode(st.key);
      icur = elemOff;
      let found = false;
      let j = 0;
      while (j < count && !found) {
        if (icur >= n) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
        const kstr = keyAt(a, n, icur);
        const valOff = F.skipOne(a, n, icur);
        if (!valOff) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
        if (kstr !== null && bytesEqual(kstr, keyBytes)) {
          icur = valOff;
          found = true;
        } else {
          icur = F.skipOne(a, n, valOff);
          if (!icur) return { rc: RC_ERROR, iStart: 0, iEnd: 0 };
        }
        j += 1;
      }
      if (!found) return { rc: RC_NOTFOUND, iStart: 0, iEnd: 0 };
    }
  }
}

export function decodeElement(a: Uint8Array, n: number, iStart: number, iEnd: number): Value {
  if (iStart >= n || iStart >= iEnd) return Value.nil();
  const b = a[iStart];

  if (b === F.MP_NIL) return Value.nil();
  if (b === F.MP_FALSE) return Value.boolean(false);
  if (b === F.MP_TRUE) return Value.boolean(true);
  if (b <= 0x7f) return Value.integer(b);
  if (b >= 0xe0) return Value.integer(b - 256);

  switch (b) {
    case F.MP_UINT8:
      if (iStart + 2 <= n) return Value.integer(a[iStart + 1]);
      break;
    case F.MP_UINT16:
      if (iStart + 3 <= n) return Value.integer(F.read16(a, iStart + 1));
      break;
    case F.MP_UINT32:
      if (iStart + 5 <= n) return Value.integer(F.read32(a, iStart + 1));
      break;
    case F.MP_UINT64:
      if (iStart + 9 <= n) return Value.unsignedInteger(F.read64(a, iStart + 1));
      break;
    case F.MP_INT8:
      if (iStart + 2 <= n) {
        const v = a[iStart + 1];
        return Value.integer(v >= 128 ? v - 256 : v);
      }
      break;
    case F.MP_INT16:
      if (iStart + 3 <= n) {
        const v = F.read16(a, iStart + 1);
        return Value.integer(v >= 0x8000 ? v - 0x10000 : v);
      }
      break;
    case F.MP_INT32:
      if (iStart + 5 <= n) {
        const v = F.read32(a, iStart + 1);
        return Value.integer(v >= 0x80000000 ? v - 0x100000000 : v);
      }
      break;
    case F.MP_INT64:
      if (iStart + 9 <= n) return Value.integer(BigInt.asIntN(64, F.read64(a, iStart + 1)));
      break;
    case F.MP_FLOAT32:
      if (iStart + 5 <= n) return Value.real32(F.readF32(a, iStart + 1));
      break;
    case F.MP_FLOAT64:
      if (iStart + 9 <= n) return Value.real(F.readF64(a, iStart + 1));
      break;
    default:
      break;
  }

  // str
  let soff = 0;
  let slen = 0;
  if (b >= 0xa0 && b <= 0xbf) {
    slen = b & 0x1f;
    soff = iStart + 1;
  } else if (b === F.MP_STR8 && iStart + 2 <= n) {
    slen = a[iStart + 1];
    soff = iStart + 2;
  } else if (b === F.MP_STR16 && iStart + 3 <= n) {
    slen = F.read16(a, iStart + 1);
    soff = iStart + 3;
  } else if (b === F.MP_STR32 && iStart + 5 <= n) {
    slen = F.read32(a, iStart + 1);
    soff = iStart + 5;
  }
  if (soff) {
    if (slen > n - soff) slen = n - soff;
    return Value.string(a.slice(soff, soff + slen));
  }

  // bin
  let boff = 0;
  let blen = 0;
  if (b === F.MP_BIN8 && iStart + 2 <= n) {
    blen = a[iStart + 1];
    boff = iStart + 2;
  } else if (b === F.MP_BIN16 && iStart + 3 <= n) {
    blen = F.read16(a, iStart + 1);
    boff = iStart + 3;
  } else if (b === F.MP_BIN32 && iStart + 5 <= n) {
    blen = F.read32(a, iStart + 1);
    boff = iStart + 5;
  }
  if (boff) {
    if (blen > n - boff) blen = n - boff;
    return Value.binary(a.slice(boff, boff + blen));
  }

  // timestamp
  const ts = decodeTimestamp(a, n, iStart);
  if (ts !== null) return Value.timestamp(ts[0], ts[1]);

  // ext
  let tc = 0;
  let elen = 0;
  let eoff = 0;
  if (b === F.MP_FIXEXT1 && iStart + 3 <= n) {
    tc = a[iStart + 1];
    elen = 1;
    eoff = iStart + 2;
  } else if (b === F.MP_FIXEXT2 && iStart + 4 <= n) {
    tc = a[iStart + 1];
    elen = 2;
    eoff = iStart + 2;
  } else if (b === F.MP_FIXEXT4 && iStart + 6 <= n) {
    tc = a[iStart + 1];
    elen = 4;
    eoff = iStart + 2;
  } else if (b === F.MP_FIXEXT8 && iStart + 10 <= n) {
    tc = a[iStart + 1];
    elen = 8;
    eoff = iStart + 2;
  } else if (b === F.MP_FIXEXT16 && iStart + 18 <= n) {
    tc = a[iStart + 1];
    elen = 16;
    eoff = iStart + 2;
  } else if (b === F.MP_EXT8 && iStart + 3 <= n) {
    elen = a[iStart + 1];
    tc = a[iStart + 2];
    eoff = iStart + 3;
  } else if (b === F.MP_EXT16 && iStart + 4 <= n) {
    elen = F.read16(a, iStart + 1);
    tc = a[iStart + 3];
    eoff = iStart + 4;
  } else if (b === F.MP_EXT32 && iStart + 6 <= n) {
    elen = F.read32(a, iStart + 1);
    tc = a[iStart + 5];
    eoff = iStart + 6;
  }
  if (eoff) {
    if (elen > n - eoff) elen = n - eoff;
    const tcSigned = tc >= 128 ? tc - 256 : tc;
    return Value.ext(tcSigned, a.slice(eoff, eoff + elen));
  }

  // containers → raw binary blob (includes header)
  return Value.binary(a.slice(iStart, iEnd));
}
