/*
** msgpack_blob_iterate.cpp — container iteration
**
** Part of the standalone C++ MsgPack Blob library (see msgpack_blob.hpp).
** Contains: flat (each) and recursive (tree) traversal of container children
** and the public Iterator cursor.
*/

#include "msgpack_blob_detail.hpp"

#include <string>
#include <vector>

namespace msgpack {
namespace detail {

/* ── Iteration internals ──────────────────────────────────────────── */

static void each_iter(
    const uint8_t* a, uint32_t n, uint32_t iCont,
    const std::string& zBase, std::vector<EachRow>& rows
) {
    if (iCont >= n) return;
    uint8_t b = a[iCont];
    bool isArr = false, isMap = false;
    uint32_t count = 0, dataOff = 0;

    if (b >= 0x90 && b <= 0x9f) { isArr = true; count = b & 0x0f; dataOff = iCont + 1; }
    else if (b == MP_ARRAY16 && iCont + 3 <= n) { isArr = true; count = read16(a + iCont + 1); dataOff = iCont + 3; }
    else if (b == MP_ARRAY32 && iCont + 5 <= n) { isArr = true; count = read32(a + iCont + 1); dataOff = iCont + 5; }
    else if (b >= 0x80 && b <= 0x8f) { isMap = true; count = b & 0x0f; dataOff = iCont + 1; }
    else if (b == MP_MAP16 && iCont + 3 <= n) { isMap = true; count = read16(a + iCont + 1); dataOff = iCont + 3; }
    else if (b == MP_MAP32 && iCont + 5 <= n) { isMap = true; count = read32(a + iCont + 1); dataOff = iCont + 5; }

    if (!isArr && !isMap) return;

    /* Sanity: reject counts that exceed remaining data capacity */
    uint32_t remaining = (dataOff <= n) ? (n - dataOff) : 0;
    uint32_t minBytesPerElem = isMap ? 2u : 1u;
    if (count > remaining / minBytesPerElem + 1) return;

    uint32_t cur = dataOff;

    for (uint32_t j = 0; j < count; j++) {
        if (cur >= n) break;
        if (isArr) {
            uint32_t cEnd = skip_one(a, n, cur);
            if (!cEnd) break;
            EachRow row;
            row.index = static_cast<int64_t>(j);
            row.fullkey = zBase + "[" + std::to_string(j) + "]";
            row.path = zBase;
            row.id = cur;
            row.type = get_type(a, n, cur);
            row.value = decode_element(a, n, cur, cEnd);
            rows.push_back(std::move(row));
            cur = cEnd;
        } else {
            uint8_t kb = a[cur];
            const char* zKey = nullptr; uint32_t nKey = 0;
            if (kb >= 0xa0 && kb <= 0xbf) { nKey = kb & 0x1f; zKey = reinterpret_cast<const char*>(a + cur + 1); }
            else if (kb == MP_STR8 && cur + 2 <= n) { nKey = a[cur + 1]; zKey = reinterpret_cast<const char*>(a + cur + 2); }
            else if (kb == MP_STR16 && cur + 3 <= n) { nKey = read16(a + cur + 1); zKey = reinterpret_cast<const char*>(a + cur + 3); }
            else if (kb == MP_STR32 && cur + 5 <= n) { nKey = read32(a + cur + 1); zKey = reinterpret_cast<const char*>(a + cur + 5); }
            uint32_t vOff = skip_one(a, n, cur);
            if (!vOff) break;
            uint32_t pEnd = skip_one(a, n, vOff);
            if (!pEnd) break;

            EachRow row;
            row.key = zKey ? std::string(zKey, nKey) : "?";
            row.index = static_cast<int64_t>(j);
            row.fullkey = zBase + "." + row.key;
            row.path = zBase;
            row.id = vOff;
            row.type = get_type(a, n, vOff);
            row.value = decode_element(a, n, vOff, pEnd);
            rows.push_back(std::move(row));
            cur = pEnd;
        }
    }
}

static void tree_walk(
    const uint8_t* a, uint32_t n, uint32_t iOff,
    const std::string& zFull, const std::string& zParPath,
    int depth, std::vector<EachRow>& rows
) {
    if (depth > kMaxDepth || iOff >= n) return;
    uint32_t iEnd = skip_one(a, n, iOff);
    if (!iEnd) return;

    /* Yield this element */
    {
        EachRow row;
        row.fullkey = zFull;
        row.path = zParPath;
        row.id = iOff;
        row.type = get_type(a, n, iOff);
        row.value = decode_element(a, n, iOff, iEnd);
        rows.push_back(std::move(row));
    }

    uint8_t b = a[iOff];
    bool isArr = false, isMap = false;
    uint32_t count = 0, dataOff = 0;

    if (b >= 0x90 && b <= 0x9f) { isArr = true; count = b & 0x0f; dataOff = iOff + 1; }
    else if (b == MP_ARRAY16 && iOff + 3 <= n) { isArr = true; count = read16(a + iOff + 1); dataOff = iOff + 3; }
    else if (b == MP_ARRAY32 && iOff + 5 <= n) { isArr = true; count = read32(a + iOff + 1); dataOff = iOff + 5; }
    else if (b >= 0x80 && b <= 0x8f) { isMap = true; count = b & 0x0f; dataOff = iOff + 1; }
    else if (b == MP_MAP16 && iOff + 3 <= n) { isMap = true; count = read16(a + iOff + 1); dataOff = iOff + 3; }
    else if (b == MP_MAP32 && iOff + 5 <= n) { isMap = true; count = read32(a + iOff + 1); dataOff = iOff + 5; }

    if (!isArr && !isMap) return;

    /* Sanity: reject counts that exceed remaining data capacity */
    uint32_t tRemaining = (dataOff <= n) ? (n - dataOff) : 0;
    uint32_t tMinBytes = isMap ? 2u : 1u;
    if (count > tRemaining / tMinBytes + 1) return;

    uint32_t cur = dataOff;

    for (uint32_t j = 0; j < count; j++) {
        if (cur >= n) break;
        if (isArr) {
            uint32_t cEnd = skip_one(a, n, cur); if (!cEnd) break;
            std::string childFull = zFull + "[" + std::to_string(j) + "]";
            tree_walk(a, n, cur, childFull, zFull, depth + 1, rows);
            cur = cEnd;
        } else {
            uint8_t kb = a[cur];
            const char* zKey = nullptr; uint32_t nKey = 0;
            if (kb >= 0xa0 && kb <= 0xbf) { nKey = kb & 0x1f; zKey = reinterpret_cast<const char*>(a + cur + 1); }
            else if (kb == MP_STR8 && cur + 2 <= n) { nKey = a[cur + 1]; zKey = reinterpret_cast<const char*>(a + cur + 2); }
            else if (kb == MP_STR16 && cur + 3 <= n) { nKey = read16(a + cur + 1); zKey = reinterpret_cast<const char*>(a + cur + 3); }
            else if (kb == MP_STR32 && cur + 5 <= n) { nKey = read32(a + cur + 1); zKey = reinterpret_cast<const char*>(a + cur + 5); }
            uint32_t vOff = skip_one(a, n, cur); if (!vOff) break;
            uint32_t pEnd = skip_one(a, n, vOff); if (!pEnd) break;
            std::string keyStr = zKey ? std::string(zKey, nKey) : "?";
            std::string childFull = zFull + "." + keyStr;
            tree_walk(a, n, vOff, childFull, zFull, depth + 1, rows);
            cur = pEnd;
        }
    }
}

}  /* namespace detail */

using namespace detail;

/* ══════════════════════════════════════════════════════════════════════
** Public API: Iterator
** ══════════════════════════════════════════════════════════════════════ */

Iterator::Iterator(const Blob& blob, const char* path, bool recursive)
    : blob_(blob), base_path_(path ? path : "$"),
      recursive_(recursive), cursor_(-1), populated_(false) {}

void Iterator::populate() {
    if (populated_) return;
    populated_ = true;
    rows_.clear();

    const uint8_t* a = blob_.data();
    auto n = static_cast<uint32_t>(blob_.size());
    if (!a || n == 0) return;

    uint32_t iRoot = 0;
    std::string zBase = base_path_;

    if (base_path_ != "$") {
        uint32_t iStart, iEnd;
        if (lookup(a, n, 0, base_path_.c_str(), &iStart, &iEnd) == RC_OK) {
            iRoot = iStart;
        } else {
            return;
        }
    }

    if (recursive_) {
        tree_walk(a, n, iRoot, zBase, zBase, 0, rows_);
    } else {
        each_iter(a, n, iRoot, zBase, rows_);
    }
}

bool Iterator::next() {
    populate();
    cursor_++;
    return cursor_ < static_cast<int>(rows_.size());
}

const EachRow& Iterator::current() const {
    return rows_[static_cast<size_t>(cursor_)];
}

void Iterator::reset() {
    cursor_ = -1;
}

}  /* namespace msgpack */
