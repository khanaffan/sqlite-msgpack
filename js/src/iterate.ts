/* Internal: container iteration (mirrors msgpack_blob_iterate.cpp). */

import * as F from "./format.ts";
import { decodeElement, getType } from "./decode.ts";
import { Type, Value } from "./value.ts";

export class EachRow {
  fullkey: string = "$";
  path: string = "$";
  id: number = 0;
  type: Type = Type.Nil;
  value: Value = Value.nil();
  key: string = ""; // map key ("" for arrays / tree rows)
  index: number = 0; // array/pair index (each mode only)
}

function keyStr(a: Uint8Array, n: number, i: number): string | null {
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
  return F.utf8Decode(a.subarray(koff, koff + klen));
}

interface Container {
  isArr: boolean;
  isMap: boolean;
  count: number;
  dataOff: number;
}

function container(a: Uint8Array, n: number, i: number): Container {
  const b = a[i];
  if (b >= 0x90 && b <= 0x9f) return { isArr: true, isMap: false, count: b & 0x0f, dataOff: i + 1 };
  if (b === F.MP_ARRAY16 && i + 3 <= n) return { isArr: true, isMap: false, count: F.read16(a, i + 1), dataOff: i + 3 };
  if (b === F.MP_ARRAY32 && i + 5 <= n) return { isArr: true, isMap: false, count: F.read32(a, i + 1), dataOff: i + 5 };
  if (b >= 0x80 && b <= 0x8f) return { isArr: false, isMap: true, count: b & 0x0f, dataOff: i + 1 };
  if (b === F.MP_MAP16 && i + 3 <= n) return { isArr: false, isMap: true, count: F.read16(a, i + 1), dataOff: i + 3 };
  if (b === F.MP_MAP32 && i + 5 <= n) return { isArr: false, isMap: true, count: F.read32(a, i + 1), dataOff: i + 5 };
  return { isArr: false, isMap: false, count: 0, dataOff: 0 };
}

export function eachIter(a: Uint8Array, n: number, icont: number, zbase: string): EachRow[] {
  const rows: EachRow[] = [];
  if (icont >= n) return rows;
  const c = container(a, n, icont);
  if (!c.isArr && !c.isMap) return rows;

  const remaining = c.dataOff <= n ? n - c.dataOff : 0;
  const minBytes = c.isMap ? 2 : 1;
  if (c.count > Math.floor(remaining / minBytes) + 1) return rows;

  let cur = c.dataOff;
  for (let j = 0; j < c.count; j++) {
    if (cur >= n) break;
    if (c.isArr) {
      const cEnd = F.skipOne(a, n, cur);
      if (!cEnd) break;
      const row = new EachRow();
      row.fullkey = `${zbase}[${j}]`;
      row.path = zbase;
      row.id = cur;
      row.type = getType(a, n, cur);
      row.value = decodeElement(a, n, cur, cEnd);
      row.key = "";
      row.index = j;
      rows.push(row);
      cur = cEnd;
    } else {
      const ks = keyStr(a, n, cur);
      const vOff = F.skipOne(a, n, cur);
      if (!vOff) break;
      const pEnd = F.skipOne(a, n, vOff);
      if (!pEnd) break;
      const key = ks !== null ? ks : "?";
      const row = new EachRow();
      row.fullkey = `${zbase}.${key}`;
      row.path = zbase;
      row.id = vOff;
      row.type = getType(a, n, vOff);
      row.value = decodeElement(a, n, vOff, pEnd);
      row.key = key;
      row.index = j;
      rows.push(row);
      cur = pEnd;
    }
  }
  return rows;
}

export function treeWalk(a: Uint8Array, n: number, ioff: number, zfull: string, zparPath: string, depth: number, rows: EachRow[]): void {
  if (depth > F.MAX_DEPTH || ioff >= n) return;
  const iend = F.skipOne(a, n, ioff);
  if (!iend) return;

  const row = new EachRow();
  row.fullkey = zfull;
  row.path = zparPath;
  row.id = ioff;
  row.type = getType(a, n, ioff);
  row.value = decodeElement(a, n, ioff, iend);
  rows.push(row);

  const c = container(a, n, ioff);
  if (!c.isArr && !c.isMap) return;

  const remaining = c.dataOff <= n ? n - c.dataOff : 0;
  const minBytes = c.isMap ? 2 : 1;
  if (c.count > Math.floor(remaining / minBytes) + 1) return;

  let cur = c.dataOff;
  for (let j = 0; j < c.count; j++) {
    if (cur >= n) break;
    if (c.isArr) {
      const cEnd = F.skipOne(a, n, cur);
      if (!cEnd) break;
      treeWalk(a, n, cur, `${zfull}[${j}]`, zfull, depth + 1, rows);
      cur = cEnd;
    } else {
      const ks = keyStr(a, n, cur);
      const vOff = F.skipOne(a, n, cur);
      if (!vOff) break;
      const pEnd = F.skipOne(a, n, vOff);
      if (!pEnd) break;
      const key = ks !== null ? ks : "?";
      treeWalk(a, n, vOff, `${zfull}.${key}`, zfull, depth + 1, rows);
      cur = pEnd;
    }
  }
}
