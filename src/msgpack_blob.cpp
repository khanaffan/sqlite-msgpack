/*
** msgpack_blob.cpp — Standalone C++ MsgPack Blob API
**
** Ports the core internal functions from the sqlite-msgpack extension
** (msgpack.c) into a self-contained C++ implementation with zero
** SQLite dependency. Produces byte-identical msgpack encoding.
*/

#include "msgpack_blob.hpp"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>

namespace msgpack {

/* ── MessagePack format constants (same as msgpack.c) ─────────────── */

static constexpr uint8_t MP_NIL       = 0xc0;
static constexpr uint8_t MP_FALSE     = 0xc2;
static constexpr uint8_t MP_TRUE      = 0xc3;
static constexpr uint8_t MP_BIN8      = 0xc4;
static constexpr uint8_t MP_BIN16     = 0xc5;
static constexpr uint8_t MP_BIN32     = 0xc6;
static constexpr uint8_t MP_EXT8      = 0xc7;
static constexpr uint8_t MP_EXT16     = 0xc8;
static constexpr uint8_t MP_EXT32     = 0xc9;
static constexpr uint8_t MP_FLOAT32   = 0xca;
static constexpr uint8_t MP_FLOAT64   = 0xcb;
static constexpr uint8_t MP_UINT8     = 0xcc;
static constexpr uint8_t MP_UINT16    = 0xcd;
static constexpr uint8_t MP_UINT32    = 0xce;
static constexpr uint8_t MP_UINT64    = 0xcf;
static constexpr uint8_t MP_INT8      = 0xd0;
static constexpr uint8_t MP_INT16     = 0xd1;
static constexpr uint8_t MP_INT32     = 0xd2;
static constexpr uint8_t MP_INT64     = 0xd3;
static constexpr uint8_t MP_FIXEXT1   = 0xd4;
static constexpr uint8_t MP_FIXEXT2   = 0xd5;
static constexpr uint8_t MP_FIXEXT4   = 0xd6;
static constexpr uint8_t MP_FIXEXT8   = 0xd7;
static constexpr uint8_t MP_FIXEXT16  = 0xd8;
static constexpr uint8_t MP_STR8      = 0xd9;
static constexpr uint8_t MP_STR16     = 0xda;
static constexpr uint8_t MP_STR32     = 0xdb;
static constexpr uint8_t MP_ARRAY16   = 0xdc;
static constexpr uint8_t MP_ARRAY32   = 0xdd;
static constexpr uint8_t MP_MAP16     = 0xde;
static constexpr uint8_t MP_MAP32     = 0xdf;

static constexpr uint8_t MP_FIXMAP_MASK   = 0x80;
static constexpr uint8_t MP_FIXARRAY_MASK = 0x90;
static constexpr uint8_t MP_FIXSTR_MASK   = 0xa0;

/* Edit modes */
static constexpr int EDIT_SET       = 0;
static constexpr int EDIT_INSERT    = 1;
static constexpr int EDIT_REPLACE   = 2;
static constexpr int EDIT_REMOVE    = 3;
static constexpr int EDIT_ARRAY_INS = 4;

/* Result codes (internal, not exposed) */
static constexpr int RC_OK       = 0;
static constexpr int RC_ERROR    = 1;
static constexpr int RC_NOTFOUND = 2;

/* ── Big-endian byte-order helpers ────────────────────────────────── */

static inline uint16_t read16(const uint8_t* p) {
    return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}
static inline uint32_t read32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) |
           (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8)  | p[3];
}
static inline uint64_t read64(const uint8_t* p) {
    return (static_cast<uint64_t>(read32(p)) << 32) | read32(p + 4);
}
static inline void write16(uint8_t* p, uint16_t v) {
    p[0] = static_cast<uint8_t>(v >> 8);
    p[1] = static_cast<uint8_t>(v);
}
static inline void write32(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v >> 24);
    p[1] = static_cast<uint8_t>(v >> 16);
    p[2] = static_cast<uint8_t>(v >> 8);
    p[3] = static_cast<uint8_t>(v);
}
static inline void write64(uint8_t* p, uint64_t v) {
    write32(p, static_cast<uint32_t>(v >> 32));
    write32(p + 4, static_cast<uint32_t>(v));
}

/* ── Buf — growable output buffer ─────────────────────────────────── */

class Buf {
public:
    std::vector<uint8_t> data;

    void append(const uint8_t* p, size_t n) {
        data.insert(data.end(), p, p + n);
    }
    void append1(uint8_t b) {
        data.push_back(b);
    }
    uint8_t* reserve(size_t n) {
        size_t old = data.size();
        data.resize(old + n);
        return data.data() + old;
    }
    void clear() { data.clear(); }
    size_t size() const { return data.size(); }
    const uint8_t* ptr() const { return data.data(); }
};

/* ── skip_one — skip one complete msgpack element ─────────────────── */

static uint32_t skip_one(const uint8_t* a, uint32_t n, uint32_t i) {
    if (i >= n) return 0;
    uint8_t b = a[i++];

    if (b <= 0x7f) return i;  /* positive fixint */
    if (b >= 0xe0) return i;  /* negative fixint */

    switch (b) {
        case MP_NIL: case MP_FALSE: case MP_TRUE:
            return i;
        case MP_FLOAT32:
            return (i + 4 <= n) ? i + 4 : 0;
        case MP_FLOAT64: case MP_INT64: case MP_UINT64:
            return (i + 8 <= n) ? i + 8 : 0;
        case MP_UINT8: case MP_INT8:
            return (i + 1 <= n) ? i + 1 : 0;
        case MP_UINT16: case MP_INT16:
            return (i + 2 <= n) ? i + 2 : 0;
        case MP_UINT32: case MP_INT32:
            return (i + 4 <= n) ? i + 4 : 0;
        case MP_BIN8: {
            if (i + 1 > n) return 0;
            uint32_t sz = a[i]; i++;
            return (sz <= n - i) ? i + sz : 0;
        }
        case MP_BIN16: {
            if (i + 2 > n) return 0;
            uint32_t sz = read16(a + i); i += 2;
            return (sz <= n - i) ? i + sz : 0;
        }
        case MP_BIN32: {
            if (i + 4 > n) return 0;
            uint32_t sz = read32(a + i); i += 4;
            return (sz <= n - i) ? i + sz : 0;
        }
        case MP_STR8: {
            if (i + 1 > n) return 0;
            uint32_t sz = a[i]; i++;
            return (sz <= n - i) ? i + sz : 0;
        }
        case MP_STR16: {
            if (i + 2 > n) return 0;
            uint32_t sz = read16(a + i); i += 2;
            return (sz <= n - i) ? i + sz : 0;
        }
        case MP_STR32: {
            if (i + 4 > n) return 0;
            uint32_t sz = read32(a + i); i += 4;
            return (sz <= n - i) ? i + sz : 0;
        }
        case MP_FIXEXT1:  return (i + 2 <= n) ? i + 2 : 0;
        case MP_FIXEXT2:  return (i + 3 <= n) ? i + 3 : 0;
        case MP_FIXEXT4:  return (i + 5 <= n) ? i + 5 : 0;
        case MP_FIXEXT8:  return (i + 9 <= n) ? i + 9 : 0;
        case MP_FIXEXT16: return (i + 17 <= n) ? i + 17 : 0;
        case MP_EXT8: {
            if (i + 2 > n) return 0;
            uint32_t sz = a[i]; i += 2;
            return (sz <= n - i) ? i + sz : 0;
        }
        case MP_EXT16: {
            if (i + 3 > n) return 0;
            uint32_t sz = read16(a + i); i += 3;
            return (sz <= n - i) ? i + sz : 0;
        }
        case MP_EXT32: {
            if (i + 5 > n) return 0;
            uint32_t sz = read32(a + i); i += 5;
            return (sz <= n - i) ? i + sz : 0;
        }
        default: break;
    }

    /* fixstr */
    if (b >= 0xa0 && b <= 0xbf) {
        uint32_t sz = b & 0x1f;
        return (sz <= n - i) ? i + sz : 0;
    }

    /* fixarray */
    if (b >= 0x90 && b <= 0x9f) {
        uint32_t count = b & 0x0f;
        for (uint32_t j = 0; j < count; j++) {
            i = skip_one(a, n, i);
            if (!i) return 0;
        }
        return i;
    }

    /* fixmap */
    if (b >= 0x80 && b <= 0x8f) {
        uint32_t count = b & 0x0f;
        for (uint32_t j = 0; j < count; j++) {
            i = skip_one(a, n, i); if (!i) return 0;
            i = skip_one(a, n, i); if (!i) return 0;
        }
        return i;
    }

    /* array16/32 */
    if (b == MP_ARRAY16 || b == MP_ARRAY32) {
        uint32_t count;
        if (b == MP_ARRAY16) {
            if (i + 2 > n) return 0;
            count = read16(a + i); i += 2;
        } else {
            if (i + 4 > n) return 0;
            count = read32(a + i); i += 4;
        }
        for (uint32_t j = 0; j < count; j++) {
            i = skip_one(a, n, i);
            if (!i) return 0;
        }
        return i;
    }

    /* map16/32 */
    if (b == MP_MAP16 || b == MP_MAP32) {
        uint32_t count;
        if (b == MP_MAP16) {
            if (i + 2 > n) return 0;
            count = read16(a + i); i += 2;
        } else {
            if (i + 4 > n) return 0;
            count = read32(a + i); i += 4;
        }
        for (uint32_t j = 0; j < count; j++) {
            i = skip_one(a, n, i); if (!i) return 0;
            i = skip_one(a, n, i); if (!i) return 0;
        }
        return i;
    }

    return 0;
}

/* ── is_valid ─────────────────────────────────────────────────────── */

static bool is_valid(const uint8_t* a, uint32_t n) {
    if (n == 0) return false;
    uint32_t end = skip_one(a, n, 0);
    return end == n;
}

/* ── error_position_of — byte offset of first error ───────────────── */

static size_t error_position_of(const uint8_t* a, uint32_t n) {
    if (n == 0) return 0;
    uint32_t end = skip_one(a, n, 0);
    if (end == n) return 0;
    /* Walk byte by byte to find where it goes wrong */
    for (uint32_t i = 0; i < n;) {
        uint32_t next = skip_one(a, n, i);
        if (!next) return i;
        i = next;
    }
    return 0;
}

/* ── encode helpers ───────────────────────────────────────────────── */

static void encode_array_header(Buf& buf, uint32_t count) {
    if (count <= 15) {
        buf.append1(static_cast<uint8_t>(MP_FIXARRAY_MASK | count));
    } else if (count <= 0xffff) {
        uint8_t h[3]; h[0] = MP_ARRAY16; write16(h + 1, static_cast<uint16_t>(count));
        buf.append(h, 3);
    } else {
        uint8_t h[5]; h[0] = MP_ARRAY32; write32(h + 1, count);
        buf.append(h, 5);
    }
}

static void encode_map_header(Buf& buf, uint32_t count) {
    if (count <= 15) {
        buf.append1(static_cast<uint8_t>(MP_FIXMAP_MASK | count));
    } else if (count <= 0xffff) {
        uint8_t h[3]; h[0] = MP_MAP16; write16(h + 1, static_cast<uint16_t>(count));
        buf.append(h, 3);
    } else {
        uint8_t h[5]; h[0] = MP_MAP32; write32(h + 1, count);
        buf.append(h, 5);
    }
}

static void encode_string(Buf& buf, const char* s, uint32_t len) {
    if (len <= 31) {
        buf.append1(static_cast<uint8_t>(MP_FIXSTR_MASK | len));
    } else if (len <= 0xff) {
        uint8_t h[2] = {MP_STR8, static_cast<uint8_t>(len)};
        buf.append(h, 2);
    } else if (len <= 0xffff) {
        uint8_t h[3]; h[0] = MP_STR16; write16(h + 1, static_cast<uint16_t>(len));
        buf.append(h, 3);
    } else {
        uint8_t h[5]; h[0] = MP_STR32; write32(h + 1, len);
        buf.append(h, 5);
    }
    buf.append(reinterpret_cast<const uint8_t*>(s), len);
}

/* ── is_timestamp_ext — check if element at offset is a timestamp ext ── */

static constexpr uint8_t MP_TIMESTAMP_TYPE = 0xFF;

static bool is_timestamp_ext(const uint8_t* a, uint32_t n, uint32_t i) {
    if (i >= n) return false;
    uint8_t b = a[i];
    if (b == MP_FIXEXT4  && i + 6 <= n  && a[i+1] == MP_TIMESTAMP_TYPE) return true;
    if (b == MP_FIXEXT8  && i + 10 <= n && a[i+1] == MP_TIMESTAMP_TYPE) return true;
    if (b == MP_EXT8 && i + 3 <= n && a[i+1] == 12 && a[i+2] == MP_TIMESTAMP_TYPE) return true;
    return false;
}

static bool decode_timestamp(const uint8_t* a, uint32_t n, uint32_t i,
                             int64_t* pSec, uint32_t* pNsec) {
    if (i >= n) return false;
    uint8_t b = a[i];
    if (b == MP_FIXEXT4 && i + 6 <= n && a[i+1] == MP_TIMESTAMP_TYPE) {
        *pSec = static_cast<int64_t>(read32(a + i + 2));
        *pNsec = 0;
        return true;
    }
    if (b == MP_FIXEXT8 && i + 10 <= n && a[i+1] == MP_TIMESTAMP_TYPE) {
        uint64_t v = read64(a + i + 2);
        *pNsec = static_cast<uint32_t>(v >> 34);
        *pSec = static_cast<int64_t>(v & 0x3FFFFFFFFULL);
        return true;
    }
    if (b == MP_EXT8 && i + 15 <= n && a[i+1] == 12 && a[i+2] == MP_TIMESTAMP_TYPE) {
        *pNsec = read32(a + i + 3);
        *pSec = static_cast<int64_t>(read64(a + i + 7));
        return true;
    }
    return false;
}

/* ── get_type — return Type for element at offset ─────────────────── */

static Type get_type(const uint8_t* a, uint32_t n, uint32_t i) {
    if (i >= n) return Type::Nil;
    uint8_t b = a[i];
    if (b == MP_NIL)   return Type::Nil;
    if (b == MP_TRUE)  return Type::True;
    if (b == MP_FALSE) return Type::False;
    if (b <= 0x7f || b >= 0xe0) return Type::Integer;
    if (b >= 0xa0 && b <= 0xbf) return Type::String;
    if (b >= 0x90 && b <= 0x9f) return Type::Array;
    if (b >= 0x80 && b <= 0x8f) return Type::Map;
    switch (b) {
        case MP_UINT8: case MP_UINT16: case MP_UINT32: case MP_UINT64:
        case MP_INT8:  case MP_INT16:  case MP_INT32:  case MP_INT64:
            return Type::Integer;
        case MP_FLOAT32:
            return Type::Float32;
        case MP_FLOAT64:
            return Type::Real;
        case MP_STR8: case MP_STR16: case MP_STR32:
            return Type::String;
        case MP_BIN8: case MP_BIN16: case MP_BIN32:
            return Type::Binary;
        case MP_ARRAY16: case MP_ARRAY32:
            return Type::Array;
        case MP_MAP16: case MP_MAP32:
            return Type::Map;
        case MP_EXT8: case MP_EXT16: case MP_EXT32:
        case MP_FIXEXT1: case MP_FIXEXT2: case MP_FIXEXT4:
        case MP_FIXEXT8: case MP_FIXEXT16:
            if (is_timestamp_ext(a, n, i)) return Type::Timestamp;
            return Type::Ext;
        default:
            return Type::Nil;
    }
}

static const char* get_type_str_at(const uint8_t* a, uint32_t n, uint32_t i) {
    switch (get_type(a, n, i)) {
        case Type::Nil:       return "null";
        case Type::True:      return "true";
        case Type::False:     return "false";
        case Type::Integer:   return "integer";
        case Type::Real:      return "real";
        case Type::Float32:   return "float32";
        case Type::String:    return "text";
        case Type::Binary:    return "binary";
        case Type::Array:     return "array";
        case Type::Map:       return "map";
        case Type::Ext:       return "ext";
        case Type::Timestamp: return "timestamp";
    }
    return "null";
}

/* ── get_container_count ──────────────────────────────────────────── */

static int64_t get_container_count(const uint8_t* a, uint32_t n, uint32_t i) {
    if (i >= n) return -1;
    uint8_t b = a[i];
    if (b >= 0x90 && b <= 0x9f) return b & 0x0f;
    if (b >= 0x80 && b <= 0x8f) return b & 0x0f;
    if (b == MP_ARRAY16 && i + 3 <= n) return read16(a + i + 1);
    if (b == MP_ARRAY32 && i + 5 <= n) return read32(a + i + 1);
    if (b == MP_MAP16   && i + 3 <= n) return read16(a + i + 1);
    if (b == MP_MAP32   && i + 5 <= n) return read32(a + i + 1);
    return -1;
}

/* ── path_step — parse one step of $.path[0].key syntax ───────────── */

static int path_step(
    const char* zPath, int* pi,
    const char** pKey, int* nKey,
    int64_t* pIdx
) {
    int i = *pi;
    if (zPath[i] == '\0') return 0;
    if (zPath[i] == '.') {
        int start;
        i++;
        start = i;
        while (zPath[i] && zPath[i] != '.' && zPath[i] != '[') i++;
        *pKey = zPath + start;
        *nKey = i - start;
        *pi = i;
        return 'k';
    }
    if (zPath[i] == '[') {
        int64_t idx = 0;
        int hasDigit = 0;
        i++;
        while (zPath[i] >= '0' && zPath[i] <= '9') {
            idx = idx * 10 + (zPath[i] - '0');
            i++;
            hasDigit = 1;
        }
        if (!hasDigit || zPath[i] != ']') return -1;
        i++;
        *pIdx = idx;
        *pi = i;
        return 'i';
    }
    return -1;
}

/* ── lookup — resolve path to byte range ──────────────────────────── */

static int lookup(
    const uint8_t* a, uint32_t n, uint32_t iRoot,
    const char* zPath,
    uint32_t* piStart, uint32_t* piEnd
) {
    int pi;
    uint32_t iCur = iRoot;
    if (!zPath || zPath[0] != '$') return RC_ERROR;
    pi = 1;

    for (;;) {
        const char* zKey = nullptr;
        int nKey = 0;
        int64_t idx = 0;
        int step = path_step(zPath, &pi, &zKey, &nKey, &idx);

        if (step == 0) {
            uint32_t iNext = skip_one(a, n, iCur);
            *piStart = iCur;
            *piEnd = iNext ? iNext : n;
            return (iNext || iCur == n) ? RC_OK : RC_ERROR;
        }
        if (step < 0) return RC_ERROR;
        if (iCur >= n) return RC_NOTFOUND;

        if (step == 'i') {
            uint8_t b = a[iCur];
            uint32_t count, elemOff;
            if (b >= 0x90 && b <= 0x9f) {
                count = b & 0x0f; elemOff = iCur + 1;
            } else if (b == MP_ARRAY16) {
                if (iCur + 3 > n) return RC_ERROR;
                count = read16(a + iCur + 1); elemOff = iCur + 3;
            } else if (b == MP_ARRAY32) {
                if (iCur + 5 > n) return RC_ERROR;
                count = read32(a + iCur + 1); elemOff = iCur + 5;
            } else {
                return RC_NOTFOUND;
            }
            if (idx < 0 || static_cast<uint64_t>(idx) >= count) return RC_NOTFOUND;
            iCur = elemOff;
            for (int64_t j = 0; j < idx; j++) {
                iCur = skip_one(a, n, iCur);
                if (!iCur) return RC_ERROR;
            }
        } else {
            uint8_t b = a[iCur];
            uint32_t count, elemOff;
            bool found = false;
            if (b >= 0x80 && b <= 0x8f) {
                count = b & 0x0f; elemOff = iCur + 1;
            } else if (b == MP_MAP16) {
                if (iCur + 3 > n) return RC_ERROR;
                count = read16(a + iCur + 1); elemOff = iCur + 3;
            } else if (b == MP_MAP32) {
                if (iCur + 5 > n) return RC_ERROR;
                count = read32(a + iCur + 1); elemOff = iCur + 5;
            } else {
                return RC_NOTFOUND;
            }
            iCur = elemOff;
            for (uint32_t j = 0; j < count && !found; j++) {
                if (iCur >= n) return RC_ERROR;
                uint8_t kb = a[iCur];
                const char* kStr = nullptr;
                uint32_t kLen = 0;
                if (kb >= 0xa0 && kb <= 0xbf) {
                    kLen = kb & 0x1f; kStr = reinterpret_cast<const char*>(a + iCur + 1);
                } else if (kb == MP_STR8 && iCur + 2 <= n) {
                    kLen = a[iCur + 1]; kStr = reinterpret_cast<const char*>(a + iCur + 2);
                } else if (kb == MP_STR16 && iCur + 3 <= n) {
                    kLen = read16(a + iCur + 1); kStr = reinterpret_cast<const char*>(a + iCur + 3);
                } else if (kb == MP_STR32 && iCur + 5 <= n) {
                    kLen = read32(a + iCur + 1); kStr = reinterpret_cast<const char*>(a + iCur + 5);
                }
                uint32_t valOff = skip_one(a, n, iCur);
                if (!valOff) return RC_ERROR;
                if (kStr && static_cast<int>(kLen) == nKey &&
                    std::memcmp(kStr, zKey, static_cast<size_t>(nKey)) == 0) {
                    iCur = valOff;
                    found = true;
                } else {
                    iCur = skip_one(a, n, valOff);
                    if (!iCur) return RC_ERROR;
                }
            }
            if (!found) return RC_NOTFOUND;
        }
    }
}

/* ── decode_element — decode element at offset into Value ─────────── */

static Value decode_element(const uint8_t* a, uint32_t n, uint32_t iStart, uint32_t iEnd) {
    if (iStart >= n || iStart >= iEnd) return Value::nil();
    uint8_t b = a[iStart];

    if (b == MP_NIL)   return Value::nil();
    if (b == MP_FALSE) return Value::boolean(false);
    if (b == MP_TRUE)  return Value::boolean(true);
    if (b <= 0x7f)     return Value::integer(static_cast<int64_t>(b));
    if (b >= 0xe0)     return Value::integer(static_cast<int64_t>(static_cast<int8_t>(b)));

    switch (b) {
        case MP_UINT8:
            if (iStart + 2 <= n) return Value::integer(static_cast<int64_t>(a[iStart + 1]));
            break;
        case MP_UINT16:
            if (iStart + 3 <= n) return Value::integer(static_cast<int64_t>(read16(a + iStart + 1)));
            break;
        case MP_UINT32:
            if (iStart + 5 <= n) return Value::integer(static_cast<int64_t>(read32(a + iStart + 1)));
            break;
        case MP_UINT64:
            if (iStart + 9 <= n) {
                uint64_t v = read64(a + iStart + 1);
                return Value::unsigned_integer(v);
            }
            break;
        case MP_INT8:
            if (iStart + 2 <= n) return Value::integer(static_cast<int64_t>(static_cast<int8_t>(a[iStart + 1])));
            break;
        case MP_INT16:
            if (iStart + 3 <= n) return Value::integer(static_cast<int64_t>(static_cast<int16_t>(read16(a + iStart + 1))));
            break;
        case MP_INT32:
            if (iStart + 5 <= n) return Value::integer(static_cast<int64_t>(static_cast<int32_t>(read32(a + iStart + 1))));
            break;
        case MP_INT64:
            if (iStart + 9 <= n) return Value::integer(static_cast<int64_t>(read64(a + iStart + 1)));
            break;
        case MP_FLOAT32:
            if (iStart + 5 <= n) {
                uint32_t bits = read32(a + iStart + 1);
                float f;
                std::memcpy(&f, &bits, 4);
                return Value::real32(f);
            }
            break;
        case MP_FLOAT64:
            if (iStart + 9 <= n) {
                uint64_t bits = read64(a + iStart + 1);
                double d;
                std::memcpy(&d, &bits, 8);
                return Value::real(d);
            }
            break;
        default: break;
    }

    /* str → String */
    uint32_t sLen = 0, sOff = 0;
    if (b >= 0xa0 && b <= 0xbf) {
        sLen = b & 0x1f; sOff = iStart + 1;
    } else if (b == MP_STR8 && iStart + 2 <= n) {
        sLen = a[iStart + 1]; sOff = iStart + 2;
    } else if (b == MP_STR16 && iStart + 3 <= n) {
        sLen = read16(a + iStart + 1); sOff = iStart + 3;
    } else if (b == MP_STR32 && iStart + 5 <= n) {
        sLen = read32(a + iStart + 1); sOff = iStart + 5;
    }
    if (sOff) {
        if (sLen > n - sOff) sLen = n - sOff;
        return Value::string(std::string_view(reinterpret_cast<const char*>(a + sOff), sLen));
    }

    /* bin → Binary (payload only, no header) */
    {
        uint32_t bLen = 0, bOff = 0;
        if (b == MP_BIN8 && iStart + 2 <= n) {
            bLen = a[iStart + 1]; bOff = iStart + 2;
        } else if (b == MP_BIN16 && iStart + 3 <= n) {
            bLen = read16(a + iStart + 1); bOff = iStart + 3;
        } else if (b == MP_BIN32 && iStart + 5 <= n) {
            bLen = read32(a + iStart + 1); bOff = iStart + 5;
        }
        if (bOff) {
            if (bLen > n - bOff) bLen = n - bOff;
            return Value::binary(a + bOff, bLen);
        }
    }

    /* timestamp ext → Timestamp value */
    {
        int64_t tsec; uint32_t tnsec;
        if (decode_timestamp(a, n, iStart, &tsec, &tnsec)) {
            return Value::timestamp(tsec, tnsec);
        }
    }

    /* ext → Ext (type code + payload, no header) */
    {
        int8_t tc = 0;
        uint32_t elen = 0, eOff = 0;
        switch (b) {
            case MP_FIXEXT1:  if (iStart+3<=n) { tc=static_cast<int8_t>(a[iStart+1]); elen=1;  eOff=iStart+2; } break;
            case MP_FIXEXT2:  if (iStart+4<=n) { tc=static_cast<int8_t>(a[iStart+1]); elen=2;  eOff=iStart+2; } break;
            case MP_FIXEXT4:  if (iStart+6<=n) { tc=static_cast<int8_t>(a[iStart+1]); elen=4;  eOff=iStart+2; } break;
            case MP_FIXEXT8:  if (iStart+10<=n){ tc=static_cast<int8_t>(a[iStart+1]); elen=8;  eOff=iStart+2; } break;
            case MP_FIXEXT16: if (iStart+18<=n){ tc=static_cast<int8_t>(a[iStart+1]); elen=16; eOff=iStart+2; } break;
            case MP_EXT8:
                if (iStart+3<=n) { elen=a[iStart+1]; tc=static_cast<int8_t>(a[iStart+2]); eOff=iStart+3; } break;
            case MP_EXT16:
                if (iStart+4<=n) { elen=read16(a+iStart+1); tc=static_cast<int8_t>(a[iStart+3]); eOff=iStart+4; } break;
            case MP_EXT32:
                if (iStart+6<=n) { elen=read32(a+iStart+1); tc=static_cast<int8_t>(a[iStart+5]); eOff=iStart+6; } break;
            default: break;
        }
        if (eOff) {
            if (elen > n - eOff) elen = n - eOff;
            return Value::ext(tc, a + eOff, elen);
        }
    }

    /* containers → raw binary blob (includes header) */
    return Value::binary(a + iStart, iEnd - iStart);
}

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

/* ── JSON output ──────────────────────────────────────────────────── */

static void json_escape_str(Buf& out, const uint8_t* s, uint32_t len) {
    out.append1('"');
    uint32_t start = 0;
    for (uint32_t j = 0; j < len; j++) {
        uint8_t c = s[j];
        if (c >= 0x20 && c != '"' && c != '\\') continue;
        if (j > start) out.append(s + start, j - start);
        if (c == '"') { uint8_t b[2] = {'\\', '"'}; out.append(b, 2); }
        else if (c == '\\') { uint8_t b[2] = {'\\', '\\'}; out.append(b, 2); }
        else if (c == '\n') { uint8_t b[2] = {'\\', 'n'}; out.append(b, 2); }
        else if (c == '\r') { uint8_t b[2] = {'\\', 'r'}; out.append(b, 2); }
        else if (c == '\t') { uint8_t b[2] = {'\\', 't'}; out.append(b, 2); }
        else {
            char esc[8]; std::snprintf(esc, 8, "\\u%04x", static_cast<int>(c));
            out.append(reinterpret_cast<const uint8_t*>(esc), 6);
        }
        start = j + 1;
    }
    if (len > start) out.append(s + start, len - start);
    out.append1('"');
}

static void json_newline(Buf& out, int depth, int indentW) {
    static const char spaces[] =
        "                                                                ";
    int nSpaces = depth * indentW;
    out.append1('\n');
    while (nSpaces > 0) {
        int chunk = nSpaces > static_cast<int>(sizeof(spaces) - 1)
                        ? static_cast<int>(sizeof(spaces) - 1) : nSpaces;
        out.append(reinterpret_cast<const uint8_t*>(spaces), static_cast<size_t>(chunk));
        nSpaces -= chunk;
    }
}

static void to_json_at(
    Buf& out, const uint8_t* a, uint32_t n, uint32_t i,
    bool pretty, int depth, int indentW
) {
    char s[64];
    if (i >= n || depth > kMaxDepth) {
        out.append(reinterpret_cast<const uint8_t*>("null"), 4); return;
    }
    uint8_t b = a[i];

    if (b == MP_NIL)  { out.append(reinterpret_cast<const uint8_t*>("null"), 4); return; }
    if (b == MP_FALSE) { out.append(reinterpret_cast<const uint8_t*>("false"), 5); return; }
    if (b == MP_TRUE) { out.append(reinterpret_cast<const uint8_t*>("true"), 4); return; }
    if (b <= 0x7f) {
        int len = std::snprintf(s, sizeof(s), "%d", static_cast<int>(b));
        out.append(reinterpret_cast<const uint8_t*>(s), static_cast<size_t>(len)); return;
    }
    if (b >= 0xe0) {
        int len = std::snprintf(s, sizeof(s), "%d", static_cast<int>(static_cast<int8_t>(b)));
        out.append(reinterpret_cast<const uint8_t*>(s), static_cast<size_t>(len)); return;
    }

    switch (b) {
        case MP_UINT8:  if (i+2>n) break; { int l=std::snprintf(s,sizeof(s),"%u",static_cast<unsigned>(a[i+1])); out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return; }
        case MP_UINT16: if (i+3>n) break; { int l=std::snprintf(s,sizeof(s),"%u",static_cast<unsigned>(read16(a+i+1))); out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return; }
        case MP_UINT32: if (i+5>n) break; { int l=std::snprintf(s,sizeof(s),"%u",static_cast<unsigned>(read32(a+i+1))); out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return; }
        case MP_UINT64: if (i+9>n) break; { int l=std::snprintf(s,sizeof(s),"%llu",static_cast<unsigned long long>(read64(a+i+1))); out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return; }
        case MP_INT8:   if (i+2>n) break; { int l=std::snprintf(s,sizeof(s),"%d",static_cast<int>(static_cast<int8_t>(a[i+1]))); out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return; }
        case MP_INT16:  if (i+3>n) break; { int l=std::snprintf(s,sizeof(s),"%d",static_cast<int>(static_cast<int16_t>(read16(a+i+1)))); out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return; }
        case MP_INT32:  if (i+5>n) break; { int l=std::snprintf(s,sizeof(s),"%d",static_cast<int>(static_cast<int32_t>(read32(a+i+1)))); out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return; }
        case MP_INT64:  if (i+9>n) break; { int l=std::snprintf(s,sizeof(s),"%lld",static_cast<long long>(read64(a+i+1))); out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return; }
        case MP_FLOAT32: {
            if (i+5>n) break;
            uint32_t bits = read32(a+i+1); float f; std::memcpy(&f, &bits, 4);
            if (!std::isfinite(static_cast<double>(f))) { out.append(reinterpret_cast<const uint8_t*>("null"),4); return; }
            int l=std::snprintf(s,sizeof(s),"%.7g",static_cast<double>(f));
            out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return;
        }
        case MP_FLOAT64: {
            if (i+9>n) break;
            uint64_t bits = read64(a+i+1); double d; std::memcpy(&d, &bits, 8);
            if (!std::isfinite(d)) { out.append(reinterpret_cast<const uint8_t*>("null"),4); return; }
            int l = std::snprintf(s,sizeof(s),"%.17g",d);
            if (!std::strchr(s,'.') && !std::strchr(s,'e') && !std::strchr(s,'E'))
                l = std::snprintf(s,sizeof(s),"%.1f",d);
            out.append(reinterpret_cast<const uint8_t*>(s),static_cast<size_t>(l)); return;
        }
        default: break;
    }

    /* str */
    {
        uint32_t sLen = 0, sOff = 0;
        if (b >= 0xa0 && b <= 0xbf) { sLen = b & 0x1f; sOff = i + 1; }
        else if (b == MP_STR8 && i + 2 <= n) { sLen = a[i+1]; sOff = i + 2; }
        else if (b == MP_STR16 && i + 3 <= n) { sLen = read16(a+i+1); sOff = i + 3; }
        else if (b == MP_STR32 && i + 5 <= n) { sLen = read32(a+i+1); sOff = i + 5; }
        if (sOff) {
            if (sLen > n - sOff) sLen = n - sOff;
            json_escape_str(out, a + sOff, sLen);
            return;
        }
    }

    /* bin → hex string */
    {
        uint32_t bLen = 0, bOff = 0;
        if (b == MP_BIN8 && i + 2 <= n) { bLen = a[i+1]; bOff = i + 2; }
        else if (b == MP_BIN16 && i + 3 <= n) { bLen = read16(a+i+1); bOff = i + 3; }
        else if (b == MP_BIN32 && i + 5 <= n) { bLen = read32(a+i+1); bOff = i + 5; }
        if (bOff) {
            static const char hex[] = "0123456789abcdef";
            if (bLen > n - bOff) bLen = n - bOff;
            out.append1('"');
            for (uint32_t j = 0; j < bLen; j++) {
                uint8_t by = a[bOff + j];
                out.append1(static_cast<uint8_t>(hex[by >> 4]));
                out.append1(static_cast<uint8_t>(hex[by & 0xf]));
            }
            out.append1('"');
            return;
        }
    }

    /* array */
    {
        bool isArr = false; uint32_t count = 0, dataOff = 0;
        if (b >= 0x90 && b <= 0x9f) { isArr = true; count = b & 0x0f; dataOff = i + 1; }
        else if (b == MP_ARRAY16 && i + 3 <= n) { isArr = true; count = read16(a+i+1); dataOff = i + 3; }
        else if (b == MP_ARRAY32 && i + 5 <= n) { isArr = true; count = read32(a+i+1); dataOff = i + 5; }
        if (isArr) {
            uint32_t cur = dataOff;
            out.append1('[');
            for (uint32_t j = 0; j < count; j++) {
                if (cur >= n) break;
                uint32_t next = skip_one(a, n, cur);
                if (j > 0) out.append1(',');
                if (pretty) json_newline(out, depth + 1, indentW);
                to_json_at(out, a, n, cur, pretty, depth + 1, indentW);
                cur = next ? next : n;
            }
            if (pretty && count > 0) json_newline(out, depth, indentW);
            out.append1(']');
            return;
        }
    }

    /* map */
    {
        bool isMap = false; uint32_t count = 0, dataOff = 0;
        if (b >= 0x80 && b <= 0x8f) { isMap = true; count = b & 0x0f; dataOff = i + 1; }
        else if (b == MP_MAP16 && i + 3 <= n) { isMap = true; count = read16(a+i+1); dataOff = i + 3; }
        else if (b == MP_MAP32 && i + 5 <= n) { isMap = true; count = read32(a+i+1); dataOff = i + 5; }
        if (isMap) {
            uint32_t cur = dataOff;
            out.append1('{');
            for (uint32_t j = 0; j < count; j++) {
                if (cur >= n) break;
                uint32_t valOff = skip_one(a, n, cur);
                uint32_t pairEnd = valOff ? skip_one(a, n, valOff) : 0;
                if (j > 0) out.append1(',');
                if (pretty) json_newline(out, depth + 1, indentW);
                to_json_at(out, a, n, cur, pretty, depth + 1, indentW);
                out.append1(':');
                if (pretty) out.append1(' ');
                to_json_at(out, a, n, valOff ? valOff : n, pretty, depth + 1, indentW);
                cur = pairEnd ? pairEnd : n;
            }
            if (pretty && count > 0) json_newline(out, depth, indentW);
            out.append1('}');
            return;
        }
    }

    /* ext / unknown → null */
    out.append(reinterpret_cast<const uint8_t*>("null"), 4);
}

/* ── JSON parser → msgpack ────────────────────────────────────────── */

struct JsonParser {
    const char* z;
    int n, i;
};

static void jp_skip_ws(JsonParser& p) {
    while (p.i < p.n && (p.z[p.i] == ' ' || p.z[p.i] == '\t' ||
                          p.z[p.i] == '\n' || p.z[p.i] == '\r')) p.i++;
}

static int jp_hex4(const char* z) {
    int v = 0;
    for (int j = 0; j < 4; j++) {
        char c = z[j]; int h;
        if (c >= '0' && c <= '9') h = c - '0';
        else if (c >= 'a' && c <= 'f') h = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') h = c - 'A' + 10;
        else return -1;
        v = (v << 4) | h;
    }
    return v;
}

static int jp_codepoint_to_utf8(uint32_t cp, uint8_t* buf) {
    if (cp < 0x80) { buf[0] = static_cast<uint8_t>(cp); return 1; }
    if (cp < 0x800) { buf[0] = static_cast<uint8_t>(0xc0 | (cp >> 6)); buf[1] = static_cast<uint8_t>(0x80 | (cp & 0x3f)); return 2; }
    if (cp < 0x10000) { buf[0] = static_cast<uint8_t>(0xe0 | (cp >> 12)); buf[1] = static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3f)); buf[2] = static_cast<uint8_t>(0x80 | (cp & 0x3f)); return 3; }
    buf[0] = static_cast<uint8_t>(0xf0 | (cp >> 18)); buf[1] = static_cast<uint8_t>(0x80 | ((cp >> 12) & 0x3f));
    buf[2] = static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3f)); buf[3] = static_cast<uint8_t>(0x80 | (cp & 0x3f)); return 4;
}

static int jp_parse_value(JsonParser& p, Buf& out);

static int jp_parse_string(JsonParser& p, Buf& out) {
    Buf sb;
    p.i++; /* skip '"' */
    while (p.i < p.n) {
        auto c = static_cast<unsigned char>(p.z[p.i]);
        if (c == '"') { p.i++; break; }
        if (c == '\\') {
            p.i++;
            if (p.i >= p.n) return RC_ERROR;
            char esc = p.z[p.i++];
            switch (esc) {
                case '"':  sb.append1('"'); break;
                case '\\': sb.append1('\\'); break;
                case '/':  sb.append1('/'); break;
                case 'n':  sb.append1('\n'); break;
                case 'r':  sb.append1('\r'); break;
                case 't':  sb.append1('\t'); break;
                case 'b':  sb.append1('\b'); break;
                case 'f':  sb.append1('\f'); break;
                case 'u': {
                    if (p.i + 4 > p.n) return RC_ERROR;
                    int cp = jp_hex4(p.z + p.i); p.i += 4;
                    if (cp < 0) return RC_ERROR;
                    if (cp >= 0xD800 && cp <= 0xDBFF && p.i + 6 <= p.n &&
                        p.z[p.i] == '\\' && p.z[p.i + 1] == 'u') {
                        int lo = jp_hex4(p.z + p.i + 2);
                        if (lo >= 0xDC00 && lo <= 0xDFFF) {
                            p.i += 6;
                            cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                        }
                    }
                    uint8_t utf[4];
                    int ulen = jp_codepoint_to_utf8(static_cast<uint32_t>(cp), utf);
                    sb.append(utf, static_cast<size_t>(ulen));
                    break;
                }
                default: sb.append1(static_cast<uint8_t>(esc)); break;
            }
        } else {
            sb.append1(c); p.i++;
        }
    }
    encode_string(out, reinterpret_cast<const char*>(sb.ptr()),
                  static_cast<uint32_t>(sb.size()));
    return RC_OK;
}

static int jp_parse_number(JsonParser& p, Buf& out) {
    int start = p.i;
    bool isFloat = false;
    if (p.i < p.n && p.z[p.i] == '-') p.i++;
    while (p.i < p.n && p.z[p.i] >= '0' && p.z[p.i] <= '9') p.i++;
    if (p.i < p.n && p.z[p.i] == '.') {
        isFloat = true; p.i++;
        while (p.i < p.n && p.z[p.i] >= '0' && p.z[p.i] <= '9') p.i++;
    }
    if (p.i < p.n && (p.z[p.i] == 'e' || p.z[p.i] == 'E')) {
        isFloat = true; p.i++;
        if (p.i < p.n && (p.z[p.i] == '+' || p.z[p.i] == '-')) p.i++;
        while (p.i < p.n && p.z[p.i] >= '0' && p.z[p.i] <= '9') p.i++;
    }
    int len = p.i - start;
    if (len <= 0 || len >= 64) return RC_ERROR;
    char buf[64];
    std::memcpy(buf, p.z + start, static_cast<size_t>(len)); buf[len] = '\0';

    if (isFloat) {
        double d = std::strtod(buf, nullptr);
        uint8_t b[9]; uint64_t bits; b[0] = MP_FLOAT64;
        std::memcpy(&bits, &d, 8); write64(b + 1, bits);
        out.append(b, 9);
    } else {
        int64_t v = static_cast<int64_t>(std::strtoll(buf, nullptr, 10));
        if (v >= 0) {
            if (v <= 0x7f) out.append1(static_cast<uint8_t>(v));
            else if (v <= 0xff) { uint8_t b[2] = {MP_UINT8, static_cast<uint8_t>(v)}; out.append(b, 2); }
            else if (v <= 0xffff) { uint8_t b[3]; b[0] = MP_UINT16; write16(b+1, static_cast<uint16_t>(v)); out.append(b, 3); }
            else if (v <= static_cast<int64_t>(0xffffffff)) { uint8_t b[5]; b[0] = MP_UINT32; write32(b+1, static_cast<uint32_t>(v)); out.append(b, 5); }
            else { uint8_t b[9]; b[0] = MP_UINT64; write64(b+1, static_cast<uint64_t>(v)); out.append(b, 9); }
        } else {
            if (v >= -32) out.append1(static_cast<uint8_t>(v));
            else if (v >= -128) { uint8_t b[2] = {MP_INT8, static_cast<uint8_t>(v)}; out.append(b, 2); }
            else if (v >= -32768) { uint8_t b[3]; b[0] = MP_INT16; write16(b+1, static_cast<uint16_t>(v)); out.append(b, 3); }
            else if (v >= static_cast<int64_t>(-2147483648LL)) { uint8_t b[5]; b[0] = MP_INT32; write32(b+1, static_cast<uint32_t>(v)); out.append(b, 5); }
            else { uint8_t b[9]; b[0] = MP_INT64; write64(b+1, static_cast<uint64_t>(v)); out.append(b, 9); }
        }
    }
    return RC_OK;
}

static int jp_parse_array(JsonParser& p, Buf& out) {
    Buf tmp; uint32_t count = 0;
    p.i++; /* skip '[' */
    jp_skip_ws(p);
    while (p.i < p.n && p.z[p.i] != ']') {
        if (count > 0) {
            jp_skip_ws(p);
            if (p.i >= p.n || p.z[p.i] != ',') return RC_ERROR;
            p.i++;
        }
        jp_skip_ws(p);
        if (jp_parse_value(p, tmp) != RC_OK) return RC_ERROR;
        count++;
        jp_skip_ws(p);
    }
    if (p.i >= p.n) return RC_ERROR;
    p.i++; /* skip ']' */
    encode_array_header(out, count);
    out.append(tmp.ptr(), tmp.size());
    return RC_OK;
}

static int jp_parse_object(JsonParser& p, Buf& out) {
    Buf tmp; uint32_t count = 0;
    p.i++; /* skip '{' */
    jp_skip_ws(p);
    while (p.i < p.n && p.z[p.i] != '}') {
        if (count > 0) {
            jp_skip_ws(p);
            if (p.i >= p.n || p.z[p.i] != ',') return RC_ERROR;
            p.i++;
        }
        jp_skip_ws(p);
        if (p.i >= p.n || p.z[p.i] != '"') return RC_ERROR;
        if (jp_parse_string(p, tmp) != RC_OK) return RC_ERROR;
        jp_skip_ws(p);
        if (p.i >= p.n || p.z[p.i] != ':') return RC_ERROR;
        p.i++;
        jp_skip_ws(p);
        if (jp_parse_value(p, tmp) != RC_OK) return RC_ERROR;
        count++;
        jp_skip_ws(p);
    }
    if (p.i >= p.n) return RC_ERROR;
    p.i++; /* skip '}' */
    encode_map_header(out, count);
    out.append(tmp.ptr(), tmp.size());
    return RC_OK;
}

static int jp_parse_value(JsonParser& p, Buf& out) {
    jp_skip_ws(p);
    if (p.i >= p.n) return RC_ERROR;
    char c = p.z[p.i];
    if (c == 'n' && p.i + 4 <= p.n && std::memcmp(p.z + p.i, "null", 4) == 0) {
        p.i += 4; out.append1(MP_NIL); return RC_OK;
    }
    if (c == 't' && p.i + 4 <= p.n && std::memcmp(p.z + p.i, "true", 4) == 0) {
        p.i += 4; out.append1(MP_TRUE); return RC_OK;
    }
    if (c == 'f' && p.i + 5 <= p.n && std::memcmp(p.z + p.i, "false", 5) == 0) {
        p.i += 5; out.append1(MP_FALSE); return RC_OK;
    }
    if (c == '"') return jp_parse_string(p, out);
    if (c == '[') return jp_parse_array(p, out);
    if (c == '{') return jp_parse_object(p, out);
    if (c == '-' || (c >= '0' && c <= '9')) return jp_parse_number(p, out);
    return RC_ERROR;
}

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
    if (depth > 64 || iOff >= n) return;
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

/* ══════════════════════════════════════════════════════════════════════
** Public API implementations
** ══════════════════════════════════════════════════════════════════════ */

/* ── type_str ─────────────────────────────────────────────────────── */

const char* type_str(Type t) noexcept {
    switch (t) {
        case Type::Nil:       return "null";
        case Type::True:      return "true";
        case Type::False:     return "false";
        case Type::Integer:   return "integer";
        case Type::Real:      return "real";
        case Type::Float32:   return "float32";
        case Type::String:    return "text";
        case Type::Binary:    return "binary";
        case Type::Array:     return "array";
        case Type::Map:       return "map";
        case Type::Ext:       return "ext";
        case Type::Timestamp: return "timestamp";
    }
    return "null";
}

/* ── Value ────────────────────────────────────────────────────────── */

Value::Value() noexcept : type_(Type::Nil), i64_(0) {}

Type Value::type() const noexcept { return type_; }
bool Value::is_nil() const noexcept { return type_ == Type::Nil; }

bool Value::as_bool() const noexcept {
    return type_ == Type::True;
}

int64_t Value::as_int64() const noexcept {
    if (type_ == Type::Integer) return i64_;
    if (type_ == Type::Real) return static_cast<int64_t>(f64_);
    if (type_ == Type::Float32) return static_cast<int64_t>(f32_);
    if (type_ == Type::Timestamp) return i64_;
    if (type_ == Type::True) return 1;
    return 0;
}

uint64_t Value::as_uint64() const noexcept {
    if (type_ == Type::Integer) return u64_;
    return 0;
}

double Value::as_double() const noexcept {
    if (type_ == Type::Real) return f64_;
    if (type_ == Type::Float32) return static_cast<double>(f32_);
    if (type_ == Type::Integer) return static_cast<double>(i64_);
    return 0.0;
}

float Value::as_float() const noexcept {
    if (type_ == Type::Float32) return f32_;
    if (type_ == Type::Real) return static_cast<float>(f64_);
    return 0.0f;
}

int8_t Value::ext_type() const noexcept { return ext_type_; }

int64_t Value::timestamp_seconds() const noexcept {
    if (type_ == Type::Timestamp) return i64_;
    return 0;
}

uint32_t Value::timestamp_nanoseconds() const noexcept {
    if (type_ == Type::Timestamp) return ts_nsec_;
    return 0;
}

IntWidth Value::int_width() const noexcept { return int_width_; }

std::string_view Value::as_string() const noexcept {
    if (type_ == Type::String) return str_;
    return {};
}

const uint8_t* Value::blob_data() const noexcept { return blob_ptr_; }
size_t Value::blob_size() const noexcept { return blob_len_; }

Value Value::nil() {
    Value v; v.type_ = Type::Nil; return v;
}

Value Value::boolean(bool b) {
    Value v; v.type_ = b ? Type::True : Type::False; return v;
}

Value Value::integer(int64_t x) {
    Value v; v.type_ = Type::Integer; v.i64_ = x; return v;
}

Value Value::unsigned_integer(uint64_t x) {
    Value v; v.type_ = Type::Integer; v.u64_ = x; return v;
}

Value Value::real(double d) {
    Value v; v.type_ = Type::Real; v.f64_ = d; return v;
}

Value Value::real32(float f) {
    Value v; v.type_ = Type::Float32; v.f32_ = f; return v;
}

Value Value::string(std::string_view s) {
    Value v; v.type_ = Type::String; v.str_ = std::string(s); return v;
}

Value Value::binary(const uint8_t* data, size_t len) {
    Value v; v.type_ = Type::Binary; v.blob_ptr_ = data; v.blob_len_ = len; return v;
}

Value Value::ext(int8_t type_code, const uint8_t* data, size_t len) {
    Value v;
    v.type_ = Type::Ext;
    v.ext_type_ = type_code;
    v.owned_blob_.assign(data, data + len);
    v.blob_ptr_ = v.owned_blob_.data();
    v.blob_len_ = len;
    return v;
}

Value Value::timestamp(int64_t seconds) {
    Value v; v.type_ = Type::Timestamp; v.i64_ = seconds; v.ts_nsec_ = 0; return v;
}

Value Value::timestamp(int64_t seconds, uint32_t nanoseconds) {
    Value v; v.type_ = Type::Timestamp; v.i64_ = seconds; v.ts_nsec_ = nanoseconds; return v;
}

Value Value::int8(int8_t x) {
    Value v; v.type_ = Type::Integer; v.i64_ = x; v.int_width_ = IntWidth::Int8; return v;
}

Value Value::int16(int16_t x) {
    Value v; v.type_ = Type::Integer; v.i64_ = x; v.int_width_ = IntWidth::Int16; return v;
}

Value Value::int32(int32_t x) {
    Value v; v.type_ = Type::Integer; v.i64_ = x; v.int_width_ = IntWidth::Int32; return v;
}

Value Value::int64(int64_t x) {
    Value v; v.type_ = Type::Integer; v.i64_ = x; v.int_width_ = IntWidth::Int64; return v;
}

Value Value::uint8(uint8_t x) {
    Value v; v.type_ = Type::Integer; v.u64_ = x; v.int_width_ = IntWidth::Uint8; return v;
}

Value Value::uint16(uint16_t x) {
    Value v; v.type_ = Type::Integer; v.u64_ = x; v.int_width_ = IntWidth::Uint16; return v;
}

Value Value::uint32(uint32_t x) {
    Value v; v.type_ = Type::Integer; v.u64_ = x; v.int_width_ = IntWidth::Uint32; return v;
}

Value Value::uint64(uint64_t x) {
    Value v; v.type_ = Type::Integer; v.u64_ = x; v.int_width_ = IntWidth::Uint64; return v;
}

/* ── Blob ─────────────────────────────────────────────────────────── */

Blob::Blob() = default;

Blob::Blob(const uint8_t* data, size_t size)
    : data_(data, data + size) {}

Blob::Blob(std::vector<uint8_t> data)
    : data_(std::move(data)) {}

const uint8_t* Blob::data() const noexcept { return data_.data(); }
size_t Blob::size() const noexcept { return data_.size(); }
bool Blob::empty() const noexcept { return data_.empty(); }

bool Blob::valid() const {
    if (data_.empty()) return false;
    return is_valid(data_.data(), static_cast<uint32_t>(data_.size()));
}

size_t Blob::error_position() const {
    if (data_.empty()) return 0;
    return error_position_of(data_.data(), static_cast<uint32_t>(data_.size()));
}

Type Blob::type() const {
    if (data_.empty()) return Type::Nil;
    return get_type(data_.data(), static_cast<uint32_t>(data_.size()), 0);
}

Type Blob::type(const char* path) const {
    uint32_t iStart, iEnd;
    int rc = lookup(data_.data(), static_cast<uint32_t>(data_.size()), 0, path, &iStart, &iEnd);
    if (rc != RC_OK) return Type::Nil;
    return get_type(data_.data(), static_cast<uint32_t>(data_.size()), iStart);
}

const char* Blob::type_str() const {
    if (data_.empty()) return "null";
    return get_type_str_at(data_.data(), static_cast<uint32_t>(data_.size()), 0);
}

const char* Blob::type_str(const char* path) const {
    uint32_t iStart, iEnd;
    int rc = lookup(data_.data(), static_cast<uint32_t>(data_.size()), 0, path, &iStart, &iEnd);
    if (rc != RC_OK) return "null";
    return get_type_str_at(data_.data(), static_cast<uint32_t>(data_.size()), iStart);
}

Value Blob::extract(const char* path) const {
    uint32_t iStart, iEnd;
    int rc = lookup(data_.data(), static_cast<uint32_t>(data_.size()), 0, path, &iStart, &iEnd);
    if (rc != RC_OK) return Value::nil();
    return decode_element(data_.data(), static_cast<uint32_t>(data_.size()), iStart, iEnd);
}

int64_t Blob::array_length() const {
    if (data_.empty()) return -1;
    return get_container_count(data_.data(), static_cast<uint32_t>(data_.size()), 0);
}

int64_t Blob::array_length(const char* path) const {
    uint32_t iStart, iEnd;
    int rc = lookup(data_.data(), static_cast<uint32_t>(data_.size()), 0, path, &iStart, &iEnd);
    if (rc != RC_OK) return -1;
    return get_container_count(data_.data(), static_cast<uint32_t>(data_.size()), iStart);
}

/* Mutation helpers */

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

std::string Blob::to_json() const {
    if (data_.empty()) return "null";
    Buf out;
    to_json_at(out, data_.data(), static_cast<uint32_t>(data_.size()), 0, false, 0, 0);
    return std::string(reinterpret_cast<const char*>(out.ptr()), out.size());
}

std::string Blob::to_json_pretty(int indent) const {
    if (data_.empty()) return "null";
    if (indent < 0) indent = 0;
    if (indent > 8) indent = 8;
    Buf out;
    to_json_at(out, data_.data(), static_cast<uint32_t>(data_.size()), 0, true, 0, indent);
    return std::string(reinterpret_cast<const char*>(out.ptr()), out.size());
}

Blob Blob::from_json(const char* json) {
    if (!json) return Blob();
    JsonParser p{json, static_cast<int>(std::strlen(json)), 0};
    Buf out;
    if (jp_parse_value(p, out) != RC_OK) return Blob();
    return Blob(std::move(out.data));
}

Blob Blob::from_json(const std::string& json) {
    return from_json(json.c_str());
}

/* ── Builder ──────────────────────────────────────────────────────── */

Builder::Builder() = default;

void Builder::append(const uint8_t* data, size_t n) {
    buf_.insert(buf_.end(), data, data + n);
}
void Builder::append1(uint8_t b) { buf_.push_back(b); }
uint8_t* Builder::reserve(size_t n) {
    size_t old = buf_.size();
    buf_.resize(old + n);
    return buf_.data() + old;
}

Builder& Builder::nil() { append1(MP_NIL); return *this; }

Builder& Builder::boolean(bool v) {
    append1(v ? MP_TRUE : MP_FALSE);
    return *this;
}

Builder& Builder::integer(int64_t x) {
    if (x >= 0) {
        if (x <= 0x7f) {
            append1(static_cast<uint8_t>(x));
        } else if (x <= 0xff) {
            uint8_t b[2] = {MP_UINT8, static_cast<uint8_t>(x)};
            append(b, 2);
        } else if (x <= 0xffff) {
            uint8_t b[3]; b[0] = MP_UINT16; write16(b + 1, static_cast<uint16_t>(x));
            append(b, 3);
        } else if (x <= static_cast<int64_t>(0xffffffff)) {
            uint8_t b[5]; b[0] = MP_UINT32; write32(b + 1, static_cast<uint32_t>(x));
            append(b, 5);
        } else {
            uint8_t b[9]; b[0] = MP_UINT64; write64(b + 1, static_cast<uint64_t>(x));
            append(b, 9);
        }
    } else {
        if (x >= -32) {
            append1(static_cast<uint8_t>(x));
        } else if (x >= -128) {
            uint8_t b[2] = {MP_INT8, static_cast<uint8_t>(x)};
            append(b, 2);
        } else if (x >= -32768) {
            uint8_t b[3]; b[0] = MP_INT16; write16(b + 1, static_cast<uint16_t>(x));
            append(b, 3);
        } else if (x >= static_cast<int64_t>(-2147483648LL)) {
            uint8_t b[5]; b[0] = MP_INT32; write32(b + 1, static_cast<uint32_t>(x));
            append(b, 5);
        } else {
            uint8_t b[9]; b[0] = MP_INT64; write64(b + 1, static_cast<uint64_t>(x));
            append(b, 9);
        }
    }
    return *this;
}

Builder& Builder::unsigned_integer(uint64_t x) {
    if (x <= 0x7f) {
        append1(static_cast<uint8_t>(x));
    } else if (x <= 0xff) {
        uint8_t b[2] = {MP_UINT8, static_cast<uint8_t>(x)};
        append(b, 2);
    } else if (x <= 0xffff) {
        uint8_t b[3]; b[0] = MP_UINT16; write16(b + 1, static_cast<uint16_t>(x));
        append(b, 3);
    } else if (x <= 0xffffffff) {
        uint8_t b[5]; b[0] = MP_UINT32; write32(b + 1, static_cast<uint32_t>(x));
        append(b, 5);
    } else {
        uint8_t b[9]; b[0] = MP_UINT64; write64(b + 1, x);
        append(b, 9);
    }
    return *this;
}

Builder& Builder::real(double d) {
    uint8_t b[9]; uint64_t bits;
    b[0] = MP_FLOAT64;
    std::memcpy(&bits, &d, 8);
    write64(b + 1, bits);
    append(b, 9);
    return *this;
}

Builder& Builder::real32(float f) {
    uint8_t b[5]; uint32_t bits;
    b[0] = MP_FLOAT32;
    std::memcpy(&bits, &f, 4);
    write32(b + 1, bits);
    append(b, 5);
    return *this;
}

Builder& Builder::string(std::string_view s) {
    auto len = static_cast<uint32_t>(s.size());
    if (len <= 31) {
        append1(static_cast<uint8_t>(MP_FIXSTR_MASK | len));
    } else if (len <= 0xff) {
        uint8_t h[2] = {MP_STR8, static_cast<uint8_t>(len)};
        append(h, 2);
    } else if (len <= 0xffff) {
        uint8_t h[3]; h[0] = MP_STR16; write16(h + 1, static_cast<uint16_t>(len));
        append(h, 3);
    } else {
        uint8_t h[5]; h[0] = MP_STR32; write32(h + 1, len);
        append(h, 5);
    }
    append(reinterpret_cast<const uint8_t*>(s.data()), len);
    return *this;
}

Builder& Builder::binary(const uint8_t* data, size_t len) {
    auto n = static_cast<uint32_t>(len);
    if (n <= 0xff) {
        uint8_t h[2] = {MP_BIN8, static_cast<uint8_t>(n)};
        append(h, 2);
    } else if (n <= 0xffff) {
        uint8_t h[3]; h[0] = MP_BIN16; write16(h + 1, static_cast<uint16_t>(n));
        append(h, 3);
    } else {
        uint8_t h[5]; h[0] = MP_BIN32; write32(h + 1, n);
        append(h, 5);
    }
    if (data) append(data, n);
    return *this;
}

Builder& Builder::ext(int8_t type_code, const uint8_t* data, size_t len) {
    auto n = static_cast<uint32_t>(len);
    switch (n) {
        case 1:  append1(MP_FIXEXT1); break;
        case 2:  append1(MP_FIXEXT2); break;
        case 4:  append1(MP_FIXEXT4); break;
        case 8:  append1(MP_FIXEXT8); break;
        case 16: append1(MP_FIXEXT16); break;
        default:
            if (n <= 0xff) {
                uint8_t h[2] = {MP_EXT8, static_cast<uint8_t>(n)};
                append(h, 2);
            } else if (n <= 0xffff) {
                uint8_t h[3]; h[0] = MP_EXT16; write16(h + 1, static_cast<uint16_t>(n));
                append(h, 3);
            } else {
                uint8_t h[5]; h[0] = MP_EXT32; write32(h + 1, n);
                append(h, 5);
            }
            break;
    }
    append1(static_cast<uint8_t>(type_code));
    if (data) append(data, n);
    return *this;
}

Builder& Builder::int8(int8_t x) {
    uint8_t b[2] = {MP_INT8, static_cast<uint8_t>(x)};
    append(b, 2); return *this;
}

Builder& Builder::int16(int16_t x) {
    uint8_t b[3]; b[0] = MP_INT16; write16(b + 1, static_cast<uint16_t>(x));
    append(b, 3); return *this;
}

Builder& Builder::int32(int32_t x) {
    uint8_t b[5]; b[0] = MP_INT32; write32(b + 1, static_cast<uint32_t>(x));
    append(b, 5); return *this;
}

Builder& Builder::int64(int64_t x) {
    uint8_t b[9]; b[0] = MP_INT64; write64(b + 1, static_cast<uint64_t>(x));
    append(b, 9); return *this;
}

Builder& Builder::uint8(uint8_t x) {
    uint8_t b[2] = {MP_UINT8, x};
    append(b, 2); return *this;
}

Builder& Builder::uint16(uint16_t x) {
    uint8_t b[3]; b[0] = MP_UINT16; write16(b + 1, x);
    append(b, 3); return *this;
}

Builder& Builder::uint32(uint32_t x) {
    uint8_t b[5]; b[0] = MP_UINT32; write32(b + 1, x);
    append(b, 5); return *this;
}

Builder& Builder::uint64(uint64_t x) {
    uint8_t b[9]; b[0] = MP_UINT64; write64(b + 1, x);
    append(b, 9); return *this;
}

Builder& Builder::array_header(uint32_t count) {
    if (count <= 15) {
        append1(static_cast<uint8_t>(MP_FIXARRAY_MASK | count));
    } else if (count <= 0xffff) {
        uint8_t h[3]; h[0] = MP_ARRAY16; write16(h + 1, static_cast<uint16_t>(count));
        append(h, 3);
    } else {
        uint8_t h[5]; h[0] = MP_ARRAY32; write32(h + 1, count);
        append(h, 5);
    }
    return *this;
}

Builder& Builder::map_header(uint32_t count) {
    if (count <= 15) {
        append1(static_cast<uint8_t>(MP_FIXMAP_MASK | count));
    } else if (count <= 0xffff) {
        uint8_t h[3]; h[0] = MP_MAP16; write16(h + 1, static_cast<uint16_t>(count));
        append(h, 3);
    } else {
        uint8_t h[5]; h[0] = MP_MAP32; write32(h + 1, count);
        append(h, 5);
    }
    return *this;
}

Builder& Builder::raw(const uint8_t* data, size_t len) {
    append(data, len);
    return *this;
}

Builder& Builder::raw(const Blob& blob) {
    append(blob.data(), blob.size());
    return *this;
}

Builder& Builder::timestamp(int64_t sec) {
    return timestamp(sec, 0);
}

Builder& Builder::timestamp(int64_t sec, uint32_t nsec) {
    if (nsec == 0 && sec >= 0 && sec <= static_cast<int64_t>(0xFFFFFFFFLL)) {
        uint8_t b[6]; b[0] = MP_FIXEXT4; b[1] = 0xFF;
        write32(b + 2, static_cast<uint32_t>(sec));
        append(b, 6);
    } else if (sec >= 0 && sec <= static_cast<int64_t>(0x3FFFFFFFFLL)) {
        uint8_t b[10]; b[0] = MP_FIXEXT8; b[1] = 0xFF;
        uint64_t v64 = (static_cast<uint64_t>(nsec) << 34) | static_cast<uint64_t>(sec);
        write64(b + 2, v64);
        append(b, 10);
    } else {
        uint8_t b[15]; b[0] = MP_EXT8; b[1] = 12; b[2] = 0xFF;
        write32(b + 3, nsec);
        write64(b + 7, static_cast<uint64_t>(sec));
        append(b, 15);
    }
    return *this;
}

const uint8_t* Builder::buf_data() const noexcept { return buf_.data(); }
size_t Builder::buf_size() const noexcept { return buf_.size(); }

Builder& Builder::value(const Value& v) {
    switch (v.type()) {
        case Type::Nil:     return nil();
        case Type::True:    return boolean(true);
        case Type::False:   return boolean(false);
        case Type::Integer: {
            IntWidth w = v.int_width();
            switch (w) {
                case IntWidth::Int8:   return int8(static_cast<int8_t>(v.as_int64()));
                case IntWidth::Int16:  return int16(static_cast<int16_t>(v.as_int64()));
                case IntWidth::Int32:  return int32(static_cast<int32_t>(v.as_int64()));
                case IntWidth::Int64:  return int64(v.as_int64());
                case IntWidth::Uint8:  return uint8(static_cast<uint8_t>(v.as_uint64()));
                case IntWidth::Uint16: return uint16(static_cast<uint16_t>(v.as_uint64()));
                case IntWidth::Uint32: return uint32(static_cast<uint32_t>(v.as_uint64()));
                case IntWidth::Uint64: return uint64(v.as_uint64());
                case IntWidth::Auto:   break;
            }
            return integer(v.as_int64());
        }
        case Type::Real:      return real(v.as_double());
        case Type::Float32:   return real32(v.as_float());
        case Type::String:    return string(v.as_string());
        case Type::Binary:    return binary(v.blob_data(), v.blob_size());
        case Type::Ext:       return ext(v.ext_type(), v.blob_data(), v.blob_size());
        case Type::Timestamp: return timestamp(v.timestamp_seconds(), v.timestamp_nanoseconds());
        default:              return nil();
    }
}

Blob Builder::build() {
    return Blob(std::move(buf_));
}

Blob Builder::quote(const Value& v) {
    Builder b;
    b.value(v);
    return b.build();
}

/* ── Iterator ─────────────────────────────────────────────────────── */

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

} /* namespace msgpack */
