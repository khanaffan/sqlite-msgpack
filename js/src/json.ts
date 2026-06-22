/*
 * Internal: JSON conversion (mirrors msgpack_blob_json.cpp).
 *
 * to_json builds a byte buffer exactly like the C++ implementation so float
 * formatting and string escaping stay byte-identical. The float formatter
 * reproduces C printf "%.<P>g" with round-half-to-even using exact BigInt
 * arithmetic.
 */

import * as F from "./format.ts";
import { Buf } from "./format.ts";
import * as E from "./encode.ts";

export const RC_OK = 0;
export const RC_ERROR = 1;

const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;
const HEX = "0123456789abcdef";

// ── C printf "%.<P>g" with round-half-to-even ───────────────────────
function decompose(x: number): [bigint, number] {
  const dv = new DataView(new ArrayBuffer(8));
  dv.setFloat64(0, x, false);
  const hi = dv.getUint32(0, false) >>> 0;
  const lo = dv.getUint32(4, false) >>> 0;
  const expBits = (hi >>> 20) & 0x7ff;
  const mantHi = hi & 0xfffff;
  let mant = (BigInt(mantHi) << 32n) | BigInt(lo);
  let exp: number;
  if (expBits === 0) {
    exp = -1074;
  } else {
    mant |= 1n << 52n;
    exp = expBits - 1075;
  }
  return [mant, exp];
}

// Sign of (m*2^e - 10^k), computed exactly with BigInt.
function cmpPow10(m: bigint, e: number, k: number): number {
  let lhs = m;
  let rhs = 1n;
  if (e >= 0) lhs <<= BigInt(e);
  else rhs <<= BigInt(-e);
  if (k >= 0) rhs *= 10n ** BigInt(k);
  else lhs *= 10n ** BigInt(-k);
  return lhs < rhs ? -1 : lhs > rhs ? 1 : 0;
}

function roundToDigits(m: bigint, e: number, p: number): [string, number] {
  // Exact decimal exponent: the integer x with 10^x <= value < 10^(x+1).
  let x = Math.floor((Math.log(Number(m)) + e * Math.LN2) / Math.LN10);
  if (!Number.isFinite(x)) x = 0;
  while (cmpPow10(m, e, x + 1) >= 0) x += 1;
  while (cmpPow10(m, e, x) < 0) x -= 1;

  // Round value / 10^(x+1-p) to the nearest integer, ties to even.
  const s = x + 1 - p;
  let num = m;
  let den = 1n;
  if (e >= 0) num <<= BigInt(e);
  else den <<= BigInt(-e);
  if (s >= 0) den *= 10n ** BigInt(s);
  else num *= 10n ** BigInt(-s);
  let q = num / den;
  const r = num % den;
  const twice = r * 2n;
  if (twice > den || (twice === den && (q & 1n) === 1n)) q += 1n;

  let digits = q.toString();
  if (digits.length > p) {
    // Rounding carried across a power of ten (e.g. 9.99e0 -> 1.0e1).
    x += 1;
    digits = "1" + "0".repeat(p - 1);
  }
  return [digits, x];
}

export function cFormatG(value: number, p: number): string {
  if (p <= 0) p = 1;
  if (value === 0) return Object.is(value, -0) ? "-0" : "0";
  const neg = value < 0;
  const [m, e] = decompose(Math.abs(value));
  const [digits, x] = roundToDigits(m, e, p);
  let out: string;
  if (x >= -4 && x < p) {
    if (x >= 0) {
      const intLen = x + 1;
      const frac = digits.slice(intLen);
      out = digits.slice(0, intLen) + (frac ? "." + frac : "");
    } else {
      out = "0." + "0".repeat(-x - 1) + digits;
    }
    if (out.indexOf(".") >= 0) out = out.replace(/0+$/, "").replace(/\.$/, "");
  } else {
    let mant = digits[0] + (digits.length > 1 ? "." + digits.slice(1) : "");
    if (mant.indexOf(".") >= 0) mant = mant.replace(/0+$/, "").replace(/\.$/, "");
    let ea = Math.abs(x).toString();
    if (ea.length < 2) ea = "0" + ea;
    out = mant + "e" + (x < 0 ? "-" : "+") + ea;
  }
  return neg ? "-" + out : out;
}

function fmtDouble(d: number): string {
  let s = cFormatG(d, 17);
  if (!/[.eE]/.test(s)) {
    s = (d < 0 || Object.is(d, -0) ? "-" : "") + Math.abs(d).toFixed(1);
  }
  return s;
}

function fmtFloat32(f: number): string {
  return cFormatG(f, 7);
}

// ── JSON output ─────────────────────────────────────────────────────
function pushAscii(out: Buf, s: string): void {
  for (let i = 0; i < s.length; i++) out.push(s.charCodeAt(i));
}

function escapeStr(out: Buf, s: Uint8Array): void {
  out.push(0x22);
  let start = 0;
  const n = s.length;
  for (let j = 0; j < n; j++) {
    const c = s[j];
    if (c >= 0x20 && c !== 0x22 && c !== 0x5c) continue;
    if (j > start) out.pushSlice(s, start, j);
    if (c === 0x22) {
      out.push(0x5c);
      out.push(0x22);
    } else if (c === 0x5c) {
      out.push(0x5c);
      out.push(0x5c);
    } else if (c === 0x0a) {
      out.push(0x5c);
      out.push(0x6e);
    } else if (c === 0x0d) {
      out.push(0x5c);
      out.push(0x72);
    } else if (c === 0x09) {
      out.push(0x5c);
      out.push(0x74);
    } else {
      pushAscii(out, "\\u" + c.toString(16).padStart(4, "0"));
    }
    start = j + 1;
  }
  if (n > start) out.pushSlice(s, start, n);
  out.push(0x22);
}

function newline(out: Buf, depth: number, indentW: number): void {
  out.push(0x0a);
  const sp = depth * indentW;
  for (let k = 0; k < sp; k++) out.push(0x20);
}

function toJsonAt(out: Buf, a: Uint8Array, n: number, i: number, pretty: boolean, depth: number, indentW: number): void {
  if (i >= n || depth > F.MAX_DEPTH) {
    pushAscii(out, "null");
    return;
  }
  const b = a[i];

  if (b === F.MP_NIL) {
    pushAscii(out, "null");
    return;
  }
  if (b === F.MP_FALSE) {
    pushAscii(out, "false");
    return;
  }
  if (b === F.MP_TRUE) {
    pushAscii(out, "true");
    return;
  }
  if (b <= 0x7f) {
    pushAscii(out, String(b));
    return;
  }
  if (b >= 0xe0) {
    pushAscii(out, String(b - 256));
    return;
  }

  switch (b) {
    case F.MP_UINT8:
      if (i + 2 <= n) {
        pushAscii(out, String(a[i + 1]));
        return;
      }
      break;
    case F.MP_UINT16:
      if (i + 3 <= n) {
        pushAscii(out, String(F.read16(a, i + 1)));
        return;
      }
      break;
    case F.MP_UINT32:
      if (i + 5 <= n) {
        pushAscii(out, String(F.read32(a, i + 1)));
        return;
      }
      break;
    case F.MP_UINT64:
      if (i + 9 <= n) {
        pushAscii(out, F.read64(a, i + 1).toString());
        return;
      }
      break;
    case F.MP_INT8:
      if (i + 2 <= n) {
        const v = a[i + 1];
        pushAscii(out, String(v >= 128 ? v - 256 : v));
        return;
      }
      break;
    case F.MP_INT16:
      if (i + 3 <= n) {
        const v = F.read16(a, i + 1);
        pushAscii(out, String(v >= 0x8000 ? v - 0x10000 : v));
        return;
      }
      break;
    case F.MP_INT32:
      if (i + 5 <= n) {
        const v = F.read32(a, i + 1);
        pushAscii(out, String(v >= 0x80000000 ? v - 0x100000000 : v));
        return;
      }
      break;
    case F.MP_INT64:
      if (i + 9 <= n) {
        pushAscii(out, BigInt.asIntN(64, F.read64(a, i + 1)).toString());
        return;
      }
      break;
    case F.MP_FLOAT32:
      if (i + 5 <= n) {
        const f = F.readF32(a, i + 1);
        if (!Number.isFinite(f)) {
          pushAscii(out, "null");
          return;
        }
        pushAscii(out, fmtFloat32(f));
        return;
      }
      break;
    case F.MP_FLOAT64:
      if (i + 9 <= n) {
        const d = F.readF64(a, i + 1);
        if (!Number.isFinite(d)) {
          pushAscii(out, "null");
          return;
        }
        pushAscii(out, fmtDouble(d));
        return;
      }
      break;
    default:
      break;
  }

  // str
  let soff = 0;
  let slen = 0;
  if (b >= 0xa0 && b <= 0xbf) {
    slen = b & 0x1f;
    soff = i + 1;
  } else if (b === F.MP_STR8 && i + 2 <= n) {
    slen = a[i + 1];
    soff = i + 2;
  } else if (b === F.MP_STR16 && i + 3 <= n) {
    slen = F.read16(a, i + 1);
    soff = i + 3;
  } else if (b === F.MP_STR32 && i + 5 <= n) {
    slen = F.read32(a, i + 1);
    soff = i + 5;
  }
  if (soff) {
    if (slen > n - soff) slen = n - soff;
    escapeStr(out, a.subarray(soff, soff + slen));
    return;
  }

  // bin → hex string
  let boff = 0;
  let blen = 0;
  if (b === F.MP_BIN8 && i + 2 <= n) {
    blen = a[i + 1];
    boff = i + 2;
  } else if (b === F.MP_BIN16 && i + 3 <= n) {
    blen = F.read16(a, i + 1);
    boff = i + 3;
  } else if (b === F.MP_BIN32 && i + 5 <= n) {
    blen = F.read32(a, i + 1);
    boff = i + 5;
  }
  if (boff) {
    if (blen > n - boff) blen = n - boff;
    out.push(0x22);
    for (let j = 0; j < blen; j++) {
      const by = a[boff + j];
      out.push(HEX.charCodeAt(by >> 4));
      out.push(HEX.charCodeAt(by & 0xf));
    }
    out.push(0x22);
    return;
  }

  // array
  let isArr = false;
  let count = 0;
  let dataOff = 0;
  if (b >= 0x90 && b <= 0x9f) {
    isArr = true;
    count = b & 0x0f;
    dataOff = i + 1;
  } else if (b === F.MP_ARRAY16 && i + 3 <= n) {
    isArr = true;
    count = F.read16(a, i + 1);
    dataOff = i + 3;
  } else if (b === F.MP_ARRAY32 && i + 5 <= n) {
    isArr = true;
    count = F.read32(a, i + 1);
    dataOff = i + 5;
  }
  if (isArr) {
    let cur = dataOff;
    out.push(0x5b);
    for (let j = 0; j < count; j++) {
      if (cur >= n) break;
      const nxt = F.skipOne(a, n, cur);
      if (j > 0) out.push(0x2c);
      if (pretty) newline(out, depth + 1, indentW);
      toJsonAt(out, a, n, cur, pretty, depth + 1, indentW);
      cur = nxt ? nxt : n;
    }
    if (pretty && count > 0) newline(out, depth, indentW);
    out.push(0x5d);
    return;
  }

  // map
  let isMap = false;
  count = 0;
  dataOff = 0;
  if (b >= 0x80 && b <= 0x8f) {
    isMap = true;
    count = b & 0x0f;
    dataOff = i + 1;
  } else if (b === F.MP_MAP16 && i + 3 <= n) {
    isMap = true;
    count = F.read16(a, i + 1);
    dataOff = i + 3;
  } else if (b === F.MP_MAP32 && i + 5 <= n) {
    isMap = true;
    count = F.read32(a, i + 1);
    dataOff = i + 5;
  }
  if (isMap) {
    let cur = dataOff;
    out.push(0x7b);
    for (let j = 0; j < count; j++) {
      if (cur >= n) break;
      const valOff = F.skipOne(a, n, cur);
      const pairEnd = valOff ? F.skipOne(a, n, valOff) : 0;
      if (j > 0) out.push(0x2c);
      if (pretty) newline(out, depth + 1, indentW);
      toJsonAt(out, a, n, cur, pretty, depth + 1, indentW);
      out.push(0x3a);
      if (pretty) out.push(0x20);
      toJsonAt(out, a, n, valOff ? valOff : n, pretty, depth + 1, indentW);
      cur = pairEnd ? pairEnd : n;
    }
    if (pretty && count > 0) newline(out, depth, indentW);
    out.push(0x7d);
    return;
  }

  // ext / unknown → null
  pushAscii(out, "null");
}

export function toJson(a: Uint8Array, n: number, pretty: boolean, indent: number): string {
  const out = new Buf();
  toJsonAt(out, a, n, 0, pretty, 0, indent);
  return F.utf8Decode(out.toBytes());
}

// ── JSON parser → msgpack ───────────────────────────────────────────
class P {
  z: Uint8Array;
  n: number;
  i: number;
  constructor(z: Uint8Array) {
    this.z = z;
    this.n = z.length;
    this.i = 0;
  }
}

function skipWs(p: P): void {
  while (p.i < p.n && (p.z[p.i] === 0x20 || p.z[p.i] === 0x09 || p.z[p.i] === 0x0a || p.z[p.i] === 0x0d)) p.i++;
}

function hex4(z: Uint8Array, off: number): number {
  let v = 0;
  for (let j = 0; j < 4; j++) {
    const c = z[off + j];
    let h: number;
    if (c >= 0x30 && c <= 0x39) h = c - 0x30;
    else if (c >= 0x61 && c <= 0x66) h = c - 0x61 + 10;
    else if (c >= 0x41 && c <= 0x46) h = c - 0x41 + 10;
    else return -1;
    v = (v << 4) | h;
  }
  return v;
}

function cpToUtf8(out: Buf, cp: number): void {
  if (cp < 0x80) out.push(cp);
  else if (cp < 0x800) {
    out.push(0xc0 | (cp >> 6));
    out.push(0x80 | (cp & 0x3f));
  } else if (cp < 0x10000) {
    out.push(0xe0 | (cp >> 12));
    out.push(0x80 | ((cp >> 6) & 0x3f));
    out.push(0x80 | (cp & 0x3f));
  } else {
    out.push(0xf0 | (cp >> 18));
    out.push(0x80 | ((cp >> 12) & 0x3f));
    out.push(0x80 | ((cp >> 6) & 0x3f));
    out.push(0x80 | (cp & 0x3f));
  }
}

function parseString(p: P, out: Buf): number {
  const sb = new Buf();
  p.i++;
  while (p.i < p.n) {
    const c = p.z[p.i];
    if (c === 0x22) {
      p.i++;
      break;
    }
    if (c === 0x5c) {
      p.i++;
      if (p.i >= p.n) return RC_ERROR;
      const esc = p.z[p.i++];
      if (esc === 0x22) sb.push(0x22);
      else if (esc === 0x5c) sb.push(0x5c);
      else if (esc === 0x2f) sb.push(0x2f);
      else if (esc === 0x6e) sb.push(0x0a);
      else if (esc === 0x72) sb.push(0x0d);
      else if (esc === 0x74) sb.push(0x09);
      else if (esc === 0x62) sb.push(0x08);
      else if (esc === 0x66) sb.push(0x0c);
      else if (esc === 0x75) {
        if (p.i + 4 > p.n) return RC_ERROR;
        let cp = hex4(p.z, p.i);
        p.i += 4;
        if (cp < 0) return RC_ERROR;
        if (cp >= 0xd800 && cp <= 0xdbff && p.i + 6 <= p.n && p.z[p.i] === 0x5c && p.z[p.i + 1] === 0x75) {
          const lo = hex4(p.z, p.i + 2);
          if (lo >= 0xdc00 && lo <= 0xdfff) {
            p.i += 6;
            cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
          }
        }
        cpToUtf8(sb, cp);
      } else sb.push(esc);
    } else {
      sb.push(c);
      p.i++;
    }
  }
  E.encString(out, sb.toBytes());
  return RC_OK;
}

function parseNumber(p: P, out: Buf): number {
  const start = p.i;
  let isFloat = false;
  if (p.i < p.n && p.z[p.i] === 0x2d) p.i++;
  while (p.i < p.n && p.z[p.i] >= 0x30 && p.z[p.i] <= 0x39) p.i++;
  if (p.i < p.n && p.z[p.i] === 0x2e) {
    isFloat = true;
    p.i++;
    while (p.i < p.n && p.z[p.i] >= 0x30 && p.z[p.i] <= 0x39) p.i++;
  }
  if (p.i < p.n && (p.z[p.i] === 0x65 || p.z[p.i] === 0x45)) {
    isFloat = true;
    p.i++;
    if (p.i < p.n && (p.z[p.i] === 0x2b || p.z[p.i] === 0x2d)) p.i++;
    while (p.i < p.n && p.z[p.i] >= 0x30 && p.z[p.i] <= 0x39) p.i++;
  }
  const length = p.i - start;
  if (length <= 0 || length >= 64) return RC_ERROR;
  const text = F.utf8Decode(p.z.subarray(start, p.i));

  if (isFloat) {
    E.encReal(out, Number(text));
  } else {
    let v = BigInt(text);
    if (v > I64_MAX) v = I64_MAX;
    else if (v < I64_MIN) v = I64_MIN;
    if (v >= 0n) E.encUnsigned(out, v);
    else E.encInteger(out, v);
  }
  return RC_OK;
}

function parseArray(p: P, out: Buf): number {
  const tmp = new Buf();
  let count = 0;
  p.i++;
  skipWs(p);
  while (p.i < p.n && p.z[p.i] !== 0x5d) {
    if (count > 0) {
      skipWs(p);
      if (p.i >= p.n || p.z[p.i] !== 0x2c) return RC_ERROR;
      p.i++;
    }
    skipWs(p);
    if (parseValue(p, tmp) !== RC_OK) return RC_ERROR;
    count++;
    skipWs(p);
  }
  if (p.i >= p.n) return RC_ERROR;
  p.i++;
  E.encArrayHeader(out, count);
  out.pushBytes(tmp.toBytes());
  return RC_OK;
}

function parseObject(p: P, out: Buf): number {
  const tmp = new Buf();
  let count = 0;
  p.i++;
  skipWs(p);
  while (p.i < p.n && p.z[p.i] !== 0x7d) {
    if (count > 0) {
      skipWs(p);
      if (p.i >= p.n || p.z[p.i] !== 0x2c) return RC_ERROR;
      p.i++;
    }
    skipWs(p);
    if (p.i >= p.n || p.z[p.i] !== 0x22) return RC_ERROR;
    if (parseString(p, tmp) !== RC_OK) return RC_ERROR;
    skipWs(p);
    if (p.i >= p.n || p.z[p.i] !== 0x3a) return RC_ERROR;
    p.i++;
    skipWs(p);
    if (parseValue(p, tmp) !== RC_OK) return RC_ERROR;
    count++;
    skipWs(p);
  }
  if (p.i >= p.n) return RC_ERROR;
  p.i++;
  E.encMapHeader(out, count);
  out.pushBytes(tmp.toBytes());
  return RC_OK;
}

function matches(z: Uint8Array, i: number, word: string): boolean {
  for (let k = 0; k < word.length; k++) if (z[i + k] !== word.charCodeAt(k)) return false;
  return true;
}

function parseValue(p: P, out: Buf): number {
  skipWs(p);
  if (p.i >= p.n) return RC_ERROR;
  const c = p.z[p.i];
  if (c === 0x6e && p.i + 4 <= p.n && matches(p.z, p.i, "null")) {
    p.i += 4;
    out.push(F.MP_NIL);
    return RC_OK;
  }
  if (c === 0x74 && p.i + 4 <= p.n && matches(p.z, p.i, "true")) {
    p.i += 4;
    out.push(F.MP_TRUE);
    return RC_OK;
  }
  if (c === 0x66 && p.i + 5 <= p.n && matches(p.z, p.i, "false")) {
    p.i += 5;
    out.push(F.MP_FALSE);
    return RC_OK;
  }
  if (c === 0x22) return parseString(p, out);
  if (c === 0x5b) return parseArray(p, out);
  if (c === 0x7b) return parseObject(p, out);
  if (c === 0x2d || (c >= 0x30 && c <= 0x39)) return parseNumber(p, out);
  return RC_ERROR;
}

export function fromJson(json: string | Uint8Array | null | undefined): Uint8Array {
  if (json === null || json === undefined) return new Uint8Array(0);
  const z = typeof json === "string" ? F.utf8Encode(json) : json;
  const p = new P(z);
  const out = new Buf();
  if (parseValue(p, out) !== RC_OK) return new Uint8Array(0);
  return out.toBytes();
}
