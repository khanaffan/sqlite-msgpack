/*
** msgpack_blob_mutate.cpp — copy-on-write mutation
**
** Part of the standalone C++ MsgPack Blob library (see msgpack_blob.hpp).
** Contains: path-targeted edits (set/insert/replace/remove/array_insert),
** RFC 7386 merge patch, and the public Blob mutation methods. Every
** operation produces a brand-new blob; the source is never modified.
*/

#include "msgpack_blob_detail.hpp"

#include <cstring>
#include <vector>

namespace msgpack {
namespace detail {

/* ── Mutation internals ───────────────────────────────────────────── */

static int edit_step(Buf& out, const uint8_t* a, uint32_t n, uint32_t iCur,
                     const char* zPath, int pi,
                     const uint8_t* newBin, uint32_t nNew,
                     int mode, int* pSkip);

static int edit_map(
    Buf& out, const uint8_t* a, uint32_t n, uint32_t iCur,
    const char* zKey, int nKey,
    const char* zPath, int pi,
    const uint8_t* newBin, uint32_t nNew, int mode
) {
    if (iCur >= n) return RC_ERROR;
    uint8_t b = a[iCur];
    uint32_t count, dataOff;

    if (b >= 0x80 && b <= 0x8f) { count = b & 0x0f; dataOff = iCur + 1; }
    else if (b == MP_MAP16) {
        if (iCur + 3 > n) return RC_ERROR;
        count = read16(a + iCur + 1); dataOff = iCur + 3;
    } else if (b == MP_MAP32) {
        if (iCur + 5 > n) return RC_ERROR;
        count = read32(a + iCur + 1); dataOff = iCur + 5;
    } else {
        if (mode == EDIT_REPLACE || mode == EDIT_REMOVE) {
            uint32_t iEnd = skip_one(a, n, iCur);
            if (iEnd) out.append(a + iCur, iEnd - iCur);
            return RC_OK;
        }
        return RC_ERROR;
    }

    uint32_t newCount = count;
    Buf tmp;
    uint32_t cur2 = dataOff;
    bool foundKey = false;
    int rc = RC_OK;

    for (uint32_t j = 0; j < count; j++) {
        if (cur2 >= n) return RC_ERROR;
        uint8_t kb = a[cur2];
        const char* kStr = nullptr; uint32_t kLen = 0;
        if (kb >= 0xa0 && kb <= 0xbf) {
            kLen = kb & 0x1f; kStr = reinterpret_cast<const char*>(a + cur2 + 1);
        } else if (kb == MP_STR8 && cur2 + 2 <= n) {
            kLen = a[cur2 + 1]; kStr = reinterpret_cast<const char*>(a + cur2 + 2);
        } else if (kb == MP_STR16 && cur2 + 3 <= n) {
            kLen = read16(a + cur2 + 1); kStr = reinterpret_cast<const char*>(a + cur2 + 3);
        } else if (kb == MP_STR32 && cur2 + 5 <= n) {
            kLen = read32(a + cur2 + 1); kStr = reinterpret_cast<const char*>(a + cur2 + 5);
        }

        uint32_t valOff = skip_one(a, n, cur2);
        if (!valOff) return RC_ERROR;
        uint32_t pairEnd = skip_one(a, n, valOff);
        if (!pairEnd) return RC_ERROR;

        bool isMatch = (kStr && static_cast<int>(kLen) == nKey &&
                        std::memcmp(kStr, zKey, static_cast<size_t>(nKey)) == 0);

        if (isMatch) {
            foundKey = true;
            if (mode == EDIT_INSERT) {
                tmp.append(a + cur2, pairEnd - cur2);
            } else {
                Buf vbuf; int skip = 0;
                rc = edit_step(vbuf, a, n, valOff, zPath, pi, newBin, nNew, mode, &skip);
                if (rc != RC_OK) return rc;
                if (skip) {
                    newCount--;
                } else {
                    tmp.append(a + cur2, valOff - cur2);
                    tmp.append(vbuf.ptr(), vbuf.size());
                }
            }
        } else {
            tmp.append(a + cur2, pairEnd - cur2);
        }
        cur2 = pairEnd;
    }

    if (!foundKey) {
        if (mode == EDIT_SET || mode == EDIT_INSERT) {
            int pi2 = pi; const char* zk2; int nk2; int64_t idx2;
            if (path_step(zPath, &pi2, &zk2, &nk2, &idx2) != 0) {
                uint32_t iEnd = skip_one(a, n, iCur);
                if (iEnd) out.append(a + iCur, iEnd - iCur);
                return RC_OK;
            }
            encode_string(tmp, zKey, static_cast<uint32_t>(nKey));
            tmp.append(newBin, nNew);
            newCount++;
        } else {
            uint32_t iEnd = skip_one(a, n, iCur);
            if (iEnd) out.append(a + iCur, iEnd - iCur);
            return RC_OK;
        }
    }

    encode_map_header(out, newCount);
    out.append(tmp.ptr(), tmp.size());
    return RC_OK;
}

static int edit_array(
    Buf& out, const uint8_t* a, uint32_t n, uint32_t iCur,
    int64_t stepIdx,
    const char* zPath, int pi,
    const uint8_t* newBin, uint32_t nNew, int mode
) {
    if (iCur >= n) return RC_ERROR;
    uint8_t b = a[iCur];
    uint32_t count, dataOff;

    if (b >= 0x90 && b <= 0x9f) { count = b & 0x0f; dataOff = iCur + 1; }
    else if (b == MP_ARRAY16) {
        if (iCur + 3 > n) return RC_ERROR;
        count = read16(a + iCur + 1); dataOff = iCur + 3;
    } else if (b == MP_ARRAY32) {
        if (iCur + 5 > n) return RC_ERROR;
        count = read32(a + iCur + 1); dataOff = iCur + 5;
    } else {
        if (mode == EDIT_REPLACE || mode == EDIT_REMOVE) {
            uint32_t iEnd = skip_one(a, n, iCur);
            if (iEnd) out.append(a + iCur, iEnd - iCur);
            return RC_OK;
        }
        return RC_ERROR;
    }

    uint32_t newCount = count;
    Buf tmp;
    uint32_t cur2 = dataOff;
    bool foundIt = false;
    int rc = RC_OK;

    for (uint32_t j = 0; j < count; j++) {
        uint32_t eEnd = skip_one(a, n, cur2);
        if (!eEnd) return RC_ERROR;

        if (static_cast<int64_t>(j) == stepIdx) {
            foundIt = true;
            if (mode == EDIT_ARRAY_INS) {
                tmp.append(newBin, nNew);
                tmp.append(a + cur2, eEnd - cur2);
                newCount++;
            } else if (mode == EDIT_INSERT) {
                tmp.append(a + cur2, eEnd - cur2);
            } else {
                Buf ebuf; int skip = 0;
                rc = edit_step(ebuf, a, n, cur2, zPath, pi, newBin, nNew, mode, &skip);
                if (rc != RC_OK) return rc;
                if (skip) {
                    newCount--;
                } else {
                    tmp.append(ebuf.ptr(), ebuf.size());
                }
            }
        } else {
            tmp.append(a + cur2, eEnd - cur2);
        }
        cur2 = eEnd;
    }

    if (!foundIt) {
        if (mode == EDIT_ARRAY_INS) {
            tmp.append(newBin, nNew);
            newCount++;
        } else if ((mode == EDIT_SET || mode == EDIT_INSERT) &&
                   static_cast<uint64_t>(stepIdx) == count) {
            tmp.append(newBin, nNew);
            newCount++;
        } else if (mode == EDIT_REPLACE || mode == EDIT_REMOVE) {
            uint32_t iEnd = skip_one(a, n, iCur);
            if (iEnd) out.append(a + iCur, iEnd - iCur);
            return RC_OK;
        } else {
            return RC_NOTFOUND;
        }
    }

    encode_array_header(out, newCount);
    out.append(tmp.ptr(), tmp.size());
    return RC_OK;
}

static int edit_step(
    Buf& out, const uint8_t* a, uint32_t n, uint32_t iCur,
    const char* zPath, int pi,
    const uint8_t* newBin, uint32_t nNew,
    int mode, int* pSkip
) {
    const char* zKey = nullptr; int nKey = 0; int64_t stepIdx = 0;
    int step = path_step(zPath, &pi, &zKey, &nKey, &stepIdx);
    if (pSkip) *pSkip = 0;

    if (step == 0) {
        if (mode == EDIT_REMOVE) {
            if (pSkip) *pSkip = 1;
            return RC_OK;
        }
        if (mode == EDIT_ARRAY_INS) return RC_ERROR;
        if (mode == EDIT_INSERT) {
            uint32_t iEnd = skip_one(a, n, iCur);
            if (iEnd) out.append(a + iCur, iEnd - iCur);
            return RC_OK;
        }
        out.append(newBin, nNew);
        return RC_OK;
    }
    if (step < 0) return RC_ERROR;

    if (step == 'k') {
        return edit_map(out, a, n, iCur, zKey, nKey, zPath, pi, newBin, nNew, mode);
    } else {
        return edit_array(out, a, n, iCur, stepIdx, zPath, pi, newBin, nNew, mode);
    }
}

static int apply_edit(
    Buf& out,
    const uint8_t* a, uint32_t n,
    const char* zPath,
    const uint8_t* newBin, uint32_t nNew,
    int mode
) {
    if (!zPath || zPath[0] != '$') return RC_ERROR;
    return edit_step(out, a, n, 0, zPath, 1, newBin, nNew, mode, nullptr);
}

/* ── merge_patch (RFC 7386) ───────────────────────────────────────── */

static int merge_patch(
    Buf& out,
    const uint8_t* a, uint32_t n, uint32_t ia,
    const uint8_t* p, uint32_t np, uint32_t ip,
    int depth
) {
    if (ip >= np) return RC_ERROR;
    if (depth > kMaxDepth) return RC_ERROR;
    uint8_t pb = p[ip];

    if (pb == MP_NIL) { out.append1(MP_NIL); return RC_OK; }

    bool pIsMap = (pb >= 0x80 && pb <= 0x8f) || pb == MP_MAP16 || pb == MP_MAP32;
    if (!pIsMap) {
        uint32_t pEnd = skip_one(p, np, ip);
        if (pEnd) out.append(p + ip, pEnd - ip);
        return RC_OK;
    }

    uint8_t ab = (ia < n) ? a[ia] : 0;
    bool aIsMap = (ab >= 0x80 && ab <= 0x8f) || ab == MP_MAP16 || ab == MP_MAP32;

    uint32_t pCount, pDataOff;
    if (pb >= 0x80 && pb <= 0x8f) { pCount = pb & 0x0f; pDataOff = ip + 1; }
    else if (pb == MP_MAP16) {
        if (ip + 3 > np) return RC_ERROR;
        pCount = read16(p + ip + 1); pDataOff = ip + 3;
    } else {
        if (ip + 5 > np) return RC_ERROR;
        pCount = read32(p + ip + 1); pDataOff = ip + 5;
    }

    uint32_t aCount = 0, aDataOff = 0;
    if (aIsMap) {
        if (ab >= 0x80 && ab <= 0x8f) { aCount = ab & 0x0f; aDataOff = ia + 1; }
        else if (ab == MP_MAP16) {
            if (ia + 3 > n) { aIsMap = false; }
            else { aCount = read16(a + ia + 1); aDataOff = ia + 3; }
        } else {
            if (ia + 5 > n) { aIsMap = false; }
            else { aCount = read32(a + ia + 1); aDataOff = ia + 5; }
        }
    }

    /* Pre-scan patch keys */
    struct PatchEntry {
        const char* zKey; uint32_t nKey;
        uint32_t keyOff, valOff, pairEnd;
        bool matched;
    };
    /* Sanity: each map pair needs at least 2 bytes; reject implausible counts */
    if (pCount > (np - pDataOff) / 2 + 1) return RC_ERROR;
    std::vector<PatchEntry> pIdx(pCount);
    {
        uint32_t pc2 = pDataOff;
        for (uint32_t k = 0; k < pCount; k++) {
            if (pc2 >= np) return RC_ERROR;
            uint8_t pkb = p[pc2];
            pIdx[k] = {nullptr, 0, pc2, 0, 0, false};
            if (pkb >= 0xa0 && pkb <= 0xbf) {
                pIdx[k].nKey = pkb & 0x1f;
                pIdx[k].zKey = reinterpret_cast<const char*>(p + pc2 + 1);
            } else if (pkb == MP_STR8 && pc2 + 2 <= np) {
                pIdx[k].nKey = p[pc2 + 1];
                pIdx[k].zKey = reinterpret_cast<const char*>(p + pc2 + 2);
            } else if (pkb == MP_STR16 && pc2 + 3 <= np) {
                pIdx[k].nKey = read16(p + pc2 + 1);
                pIdx[k].zKey = reinterpret_cast<const char*>(p + pc2 + 3);
            } else if (pkb == MP_STR32 && pc2 + 5 <= np) {
                pIdx[k].nKey = read32(p + pc2 + 1);
                pIdx[k].zKey = reinterpret_cast<const char*>(p + pc2 + 5);
            }
            pIdx[k].valOff = skip_one(p, np, pc2);
            if (!pIdx[k].valOff) return RC_ERROR;
            pIdx[k].pairEnd = skip_one(p, np, pIdx[k].valOff);
            if (!pIdx[k].pairEnd) return RC_ERROR;
            pc2 = pIdx[k].pairEnd;
        }
    }

    Buf tmp;
    uint32_t newCount = 0;

    /* Phase 1: iterate target pairs */
    if (aIsMap) {
        uint32_t ac = aDataOff;
        for (uint32_t j = 0; j < aCount; j++) {
            if (ac >= n) return RC_ERROR;
            uint8_t kb = a[ac];
            const char* kStr = nullptr; uint32_t kLen = 0;
            if (kb >= 0xa0 && kb <= 0xbf) {
                kLen = kb & 0x1f; kStr = reinterpret_cast<const char*>(a + ac + 1);
            } else if (kb == MP_STR8 && ac + 2 <= n) {
                kLen = a[ac + 1]; kStr = reinterpret_cast<const char*>(a + ac + 2);
            } else if (kb == MP_STR16 && ac + 3 <= n) {
                kLen = read16(a + ac + 1); kStr = reinterpret_cast<const char*>(a + ac + 3);
            } else if (kb == MP_STR32 && ac + 5 <= n) {
                kLen = read32(a + ac + 1); kStr = reinterpret_cast<const char*>(a + ac + 5);
            }

            uint32_t aValOff = skip_one(a, n, ac);
            if (!aValOff) return RC_ERROR;
            uint32_t aPairEnd = skip_one(a, n, aValOff);
            if (!aPairEnd) return RC_ERROR;

            bool foundInPatch = false, patchIsNil = false;
            uint32_t pMatchVal = 0;
            for (uint32_t k = 0; k < pCount; k++) {
                if (pIdx[k].zKey && kStr && pIdx[k].nKey == kLen &&
                    std::memcmp(pIdx[k].zKey, kStr, kLen) == 0) {
                    foundInPatch = true;
                    pMatchVal = pIdx[k].valOff;
                    patchIsNil = (pIdx[k].valOff < np && p[pIdx[k].valOff] == MP_NIL);
                    pIdx[k].matched = true;
                    break;
                }
            }

            if (foundInPatch && patchIsNil) {
                /* Drop this pair */
            } else if (foundInPatch) {
                Buf mb;
                int mrc = merge_patch(mb, a, n, aValOff, p, np, pMatchVal, depth + 1);
                if (mrc == RC_OK) {
                    tmp.append(a + ac, aValOff - ac);
                    tmp.append(mb.ptr(), mb.size());
                    newCount++;
                }
            } else {
                tmp.append(a + ac, aPairEnd - ac);
                newCount++;
            }
            ac = aPairEnd;
        }
    }

    /* Phase 2: add unmatched patch pairs */
    for (uint32_t k = 0; k < pCount; k++) {
        if (!pIdx[k].matched && pIdx[k].valOff < np && p[pIdx[k].valOff] != MP_NIL) {
            tmp.append(p + pIdx[k].keyOff, pIdx[k].pairEnd - pIdx[k].keyOff);
            newCount++;
        }
    }

    encode_map_header(out, newCount);
    out.append(tmp.ptr(), tmp.size());
    return RC_OK;
}

}  /* namespace detail */

using namespace detail;

/* ══════════════════════════════════════════════════════════════════════
** Public API: Blob mutation (copy-on-write)
** ══════════════════════════════════════════════════════════════════════ */

static Blob apply_mutation(const Blob& blob, const char* path, const Value& val, int mode) {
    Builder enc;
    enc.value(val);
    Buf out;
    int rc = apply_edit(out, blob.data(), static_cast<uint32_t>(blob.size()),
                        path, enc.buf_data(), static_cast<uint32_t>(enc.buf_size()), mode);
    if (rc != RC_OK) return blob;
    return Blob(std::move(out.data));
}

Blob Blob::set(const char* path, const Value& val) const {
    return apply_mutation(*this, path, val, EDIT_SET);
}

Blob Blob::set(const char* path, const Blob& sub) const {
    Buf out;
    int rc = apply_edit(out, data_.data(), static_cast<uint32_t>(data_.size()),
                        path, sub.data(), static_cast<uint32_t>(sub.size()), EDIT_SET);
    if (rc != RC_OK) return *this;
    return Blob(std::move(out.data));
}

Blob Blob::insert(const char* path, const Value& val) const {
    return apply_mutation(*this, path, val, EDIT_INSERT);
}

Blob Blob::replace(const char* path, const Value& val) const {
    return apply_mutation(*this, path, val, EDIT_REPLACE);
}

Blob Blob::remove(const char* path) const {
    Buf out;
    int rc = apply_edit(out, data_.data(), static_cast<uint32_t>(data_.size()),
                        path, nullptr, 0, EDIT_REMOVE);
    if (rc != RC_OK) return *this;
    return Blob(std::move(out.data));
}

Blob Blob::array_insert(const char* path, const Value& val) const {
    return apply_mutation(*this, path, val, EDIT_ARRAY_INS);
}

Blob Blob::patch(const Blob& mp) const {
    Buf out;
    int rc = merge_patch(out,
                         data_.data(), static_cast<uint32_t>(data_.size()), 0,
                         mp.data(), static_cast<uint32_t>(mp.size()), 0, 0);
    if (rc != RC_OK) return *this;
    return Blob(std::move(out.data));
}

}  /* namespace msgpack */
