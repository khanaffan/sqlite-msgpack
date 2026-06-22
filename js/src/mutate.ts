/* Internal: copy-on-write mutation (mirrors msgpack_blob_mutate.cpp). */

import * as F from "./format.ts";
import { Buf } from "./format.ts";
import * as E from "./encode.ts";
import { pathStep } from "./decode.ts";

export const RC_OK = 0;
export const RC_ERROR = 1;
export const RC_NOTFOUND = 2;

export const EDIT_SET = 0;
export const EDIT_INSERT = 1;
export const EDIT_REPLACE = 2;
export const EDIT_REMOVE = 3;
export const EDIT_ARRAY_INS = 4;

function mapKey(a: Uint8Array, n: number, i: number): Uint8Array | null {
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
  return a.subarray(koff, koff + klen);
}

function bytesEqual(x: Uint8Array, y: Uint8Array): boolean {
  if (x.length !== y.length) return false;
  for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return false;
  return true;
}

interface StepResult {
  rc: number;
  skip: boolean;
}

function editMap(
  out: Buf,
  a: Uint8Array,
  n: number,
  icur: number,
  zkey: Uint8Array,
  zpath: string,
  pi: number,
  newBin: Uint8Array,
  mode: number,
): number {
  if (icur >= n) return RC_ERROR;
  const b = a[icur];
  let count: number;
  let dataOff: number;
  if (b >= 0x80 && b <= 0x8f) {
    count = b & 0x0f;
    dataOff = icur + 1;
  } else if (b === F.MP_MAP16) {
    if (icur + 3 > n) return RC_ERROR;
    count = F.read16(a, icur + 1);
    dataOff = icur + 3;
  } else if (b === F.MP_MAP32) {
    if (icur + 5 > n) return RC_ERROR;
    count = F.read32(a, icur + 1);
    dataOff = icur + 5;
  } else {
    if (mode === EDIT_REPLACE || mode === EDIT_REMOVE) {
      const iend = F.skipOne(a, n, icur);
      if (iend) out.pushSlice(a, icur, iend);
      return RC_OK;
    }
    return RC_ERROR;
  }

  let newCount = count;
  const tmp = new Buf();
  let cur2 = dataOff;
  let foundKey = false;

  for (let j = 0; j < count; j++) {
    if (cur2 >= n) return RC_ERROR;
    const kstr = mapKey(a, n, cur2);
    const valOff = F.skipOne(a, n, cur2);
    if (!valOff) return RC_ERROR;
    const pairEnd = F.skipOne(a, n, valOff);
    if (!pairEnd) return RC_ERROR;

    const isMatch = kstr !== null && bytesEqual(kstr, zkey);

    if (isMatch) {
      foundKey = true;
      if (mode === EDIT_INSERT) {
        tmp.pushSlice(a, cur2, pairEnd);
      } else {
        const vbuf = new Buf();
        const res = editStep(vbuf, a, n, valOff, zpath, pi, newBin, mode);
        if (res.rc !== RC_OK) return res.rc;
        if (res.skip) {
          newCount--;
        } else {
          tmp.pushSlice(a, cur2, valOff);
          tmp.pushBytes(vbuf.toBytes());
        }
      }
    } else {
      tmp.pushSlice(a, cur2, pairEnd);
    }
    cur2 = pairEnd;
  }

  if (!foundKey) {
    if (mode === EDIT_SET || mode === EDIT_INSERT) {
      if (pathStep(zpath, pi).kind !== 0) {
        const iend = F.skipOne(a, n, icur);
        if (iend) out.pushSlice(a, icur, iend);
        return RC_OK;
      }
      E.encString(tmp, zkey);
      tmp.pushBytes(newBin);
      newCount++;
    } else {
      const iend = F.skipOne(a, n, icur);
      if (iend) out.pushSlice(a, icur, iend);
      return RC_OK;
    }
  }

  E.encMapHeader(out, newCount);
  out.pushBytes(tmp.toBytes());
  return RC_OK;
}

function editArray(
  out: Buf,
  a: Uint8Array,
  n: number,
  icur: number,
  stepIdx: number,
  zpath: string,
  pi: number,
  newBin: Uint8Array,
  mode: number,
): number {
  if (icur >= n) return RC_ERROR;
  const b = a[icur];
  let count: number;
  let dataOff: number;
  if (b >= 0x90 && b <= 0x9f) {
    count = b & 0x0f;
    dataOff = icur + 1;
  } else if (b === F.MP_ARRAY16) {
    if (icur + 3 > n) return RC_ERROR;
    count = F.read16(a, icur + 1);
    dataOff = icur + 3;
  } else if (b === F.MP_ARRAY32) {
    if (icur + 5 > n) return RC_ERROR;
    count = F.read32(a, icur + 1);
    dataOff = icur + 5;
  } else {
    if (mode === EDIT_REPLACE || mode === EDIT_REMOVE) {
      const iend = F.skipOne(a, n, icur);
      if (iend) out.pushSlice(a, icur, iend);
      return RC_OK;
    }
    return RC_ERROR;
  }

  let newCount = count;
  const tmp = new Buf();
  let cur2 = dataOff;
  let foundIt = false;

  for (let j = 0; j < count; j++) {
    const eEnd = F.skipOne(a, n, cur2);
    if (!eEnd) return RC_ERROR;

    if (j === stepIdx) {
      foundIt = true;
      if (mode === EDIT_ARRAY_INS) {
        tmp.pushBytes(newBin);
        tmp.pushSlice(a, cur2, eEnd);
        newCount++;
      } else if (mode === EDIT_INSERT) {
        tmp.pushSlice(a, cur2, eEnd);
      } else {
        const ebuf = new Buf();
        const res = editStep(ebuf, a, n, cur2, zpath, pi, newBin, mode);
        if (res.rc !== RC_OK) return res.rc;
        if (res.skip) newCount--;
        else tmp.pushBytes(ebuf.toBytes());
      }
    } else {
      tmp.pushSlice(a, cur2, eEnd);
    }
    cur2 = eEnd;
  }

  if (!foundIt) {
    if (mode === EDIT_ARRAY_INS) {
      tmp.pushBytes(newBin);
      newCount++;
    } else if ((mode === EDIT_SET || mode === EDIT_INSERT) && stepIdx === count) {
      tmp.pushBytes(newBin);
      newCount++;
    } else if (mode === EDIT_REPLACE || mode === EDIT_REMOVE) {
      const iend = F.skipOne(a, n, icur);
      if (iend) out.pushSlice(a, icur, iend);
      return RC_OK;
    } else {
      return RC_NOTFOUND;
    }
  }

  E.encArrayHeader(out, newCount);
  out.pushBytes(tmp.toBytes());
  return RC_OK;
}

function editStep(
  out: Buf,
  a: Uint8Array,
  n: number,
  icur: number,
  zpath: string,
  pi: number,
  newBin: Uint8Array,
  mode: number,
): StepResult {
  const st = pathStep(zpath, pi);

  if (st.kind === 0) {
    if (mode === EDIT_REMOVE) return { rc: RC_OK, skip: true };
    if (mode === EDIT_ARRAY_INS) return { rc: RC_ERROR, skip: false };
    if (mode === EDIT_INSERT) {
      const iend = F.skipOne(a, n, icur);
      if (iend) out.pushSlice(a, icur, iend);
      return { rc: RC_OK, skip: false };
    }
    out.pushBytes(newBin);
    return { rc: RC_OK, skip: false };
  }
  if (st.kind === -1) return { rc: RC_ERROR, skip: false };

  if (st.kind === "k") {
    const zkey = F.utf8Encode(st.key);
    return { rc: editMap(out, a, n, icur, zkey, zpath, st.pi, newBin, mode), skip: false };
  }
  return { rc: editArray(out, a, n, icur, st.idx, zpath, st.pi, newBin, mode), skip: false };
}

export function applyEdit(a: Uint8Array, n: number, zpath: string, newBin: Uint8Array, mode: number): { rc: number; out: Uint8Array } {
  if (!zpath || zpath[0] !== "$") return { rc: RC_ERROR, out: new Uint8Array(0) };
  const out = new Buf();
  const res = editStep(out, a, n, 0, zpath, 1, newBin, mode);
  return { rc: res.rc, out: out.toBytes() };
}

// ── merge_patch (RFC 7386) ──────────────────────────────────────────
export function mergePatch(a: Uint8Array, n: number, ia: number, p: Uint8Array, np: number, ip: number, depth: number): { rc: number; out: Uint8Array } {
  const out = new Buf();
  const rc = mergePatchInto(out, a, n, ia, p, np, ip, depth);
  return { rc, out: out.toBytes() };
}

function mergePatchInto(out: Buf, a: Uint8Array, n: number, ia: number, p: Uint8Array, np: number, ip: number, depth: number): number {
  if (ip >= np) return RC_ERROR;
  if (depth > F.MAX_DEPTH) return RC_ERROR;
  const pb = p[ip];

  if (pb === F.MP_NIL) {
    out.push(F.MP_NIL);
    return RC_OK;
  }

  const pIsMap = (pb >= 0x80 && pb <= 0x8f) || pb === F.MP_MAP16 || pb === F.MP_MAP32;
  if (!pIsMap) {
    const pEnd = F.skipOne(p, np, ip);
    if (pEnd) out.pushSlice(p, ip, pEnd);
    return RC_OK;
  }

  const ab = ia < n ? a[ia] : 0;
  let aIsMap = (ab >= 0x80 && ab <= 0x8f) || ab === F.MP_MAP16 || ab === F.MP_MAP32;

  let pCount: number;
  let pDataOff: number;
  if (pb >= 0x80 && pb <= 0x8f) {
    pCount = pb & 0x0f;
    pDataOff = ip + 1;
  } else if (pb === F.MP_MAP16) {
    if (ip + 3 > np) return RC_ERROR;
    pCount = F.read16(p, ip + 1);
    pDataOff = ip + 3;
  } else {
    if (ip + 5 > np) return RC_ERROR;
    pCount = F.read32(p, ip + 1);
    pDataOff = ip + 5;
  }

  let aCount = 0;
  let aDataOff = 0;
  if (aIsMap) {
    if (ab >= 0x80 && ab <= 0x8f) {
      aCount = ab & 0x0f;
      aDataOff = ia + 1;
    } else if (ab === F.MP_MAP16) {
      if (ia + 3 > n) aIsMap = false;
      else {
        aCount = F.read16(a, ia + 1);
        aDataOff = ia + 3;
      }
    } else {
      if (ia + 5 > n) aIsMap = false;
      else {
        aCount = F.read32(a, ia + 1);
        aDataOff = ia + 5;
      }
    }
  }

  // Pre-scan patch keys: [key|null, keyOff, valOff, pairEnd, matched]
  if (pCount > Math.floor((np - pDataOff) / 2) + 1) return RC_ERROR;
  const pIdx: Array<[Uint8Array | null, number, number, number, boolean]> = [];
  let pc2 = pDataOff;
  for (let k = 0; k < pCount; k++) {
    if (pc2 >= np) return RC_ERROR;
    const key = mapKey(p, np, pc2);
    const valOff = F.skipOne(p, np, pc2);
    if (!valOff) return RC_ERROR;
    const pairEnd = F.skipOne(p, np, valOff);
    if (!pairEnd) return RC_ERROR;
    pIdx.push([key, pc2, valOff, pairEnd, false]);
    pc2 = pairEnd;
  }

  const tmp = new Buf();
  let newCount = 0;

  if (aIsMap) {
    let ac = aDataOff;
    for (let j = 0; j < aCount; j++) {
      if (ac >= n) return RC_ERROR;
      const kstr = mapKey(a, n, ac);
      const aValOff = F.skipOne(a, n, ac);
      if (!aValOff) return RC_ERROR;
      const aPairEnd = F.skipOne(a, n, aValOff);
      if (!aPairEnd) return RC_ERROR;

      let foundInPatch = false;
      let patchIsNil = false;
      let pMatchVal = 0;
      for (const entry of pIdx) {
        if (entry[0] !== null && kstr !== null && bytesEqual(entry[0], kstr)) {
          foundInPatch = true;
          pMatchVal = entry[2];
          patchIsNil = entry[2] < np && p[entry[2]] === F.MP_NIL;
          entry[4] = true;
          break;
        }
      }

      if (foundInPatch && patchIsNil) {
        // drop
      } else if (foundInPatch) {
        const mb = new Buf();
        const mrc = mergePatchInto(mb, a, n, aValOff, p, np, pMatchVal, depth + 1);
        if (mrc === RC_OK) {
          tmp.pushSlice(a, ac, aValOff);
          tmp.pushBytes(mb.toBytes());
          newCount++;
        }
      } else {
        tmp.pushSlice(a, ac, aPairEnd);
        newCount++;
      }
      ac = aPairEnd;
    }
  }

  for (const entry of pIdx) {
    if (!entry[4] && entry[2] < np && p[entry[2]] !== F.MP_NIL) {
      tmp.pushSlice(p, entry[1], entry[3]);
      newCount++;
    }
  }

  E.encMapHeader(out, newCount);
  out.pushBytes(tmp.toBytes());
  return RC_OK;
}
