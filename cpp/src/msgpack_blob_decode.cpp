/*
** msgpack_blob_decode.cpp — decoding & inspection
**
** Part of the standalone C++ MsgPack Blob library (see msgpack_blob.hpp).
** Contains: element skipping, validation, type inspection, path resolution,
** element decoding, the Value type, and the read-only Blob accessors.
*/

#include "msgpack_blob_detail.hpp"

#include <cstring>
#include <string>
#include <string_view>
#include <climits>

namespace msgpack {
namespace detail {

/* ── skip_one — skip one complete msgpack element ─────────────────── */

static uint32_t skip_one_d(const uint8_t* a, uint32_t n, uint32_t i, int depth);

uint32_t skip_one(const uint8_t* a, uint32_t n, uint32_t i) {
    return skip_one_d(a, n, i, 0);
}
static uint32_t skip_one_d(const uint8_t* a, uint32_t n, uint32_t i, int depth) {
    if (depth > kMaxDepth) return 0;
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
            i = skip_one_d(a, n, i, depth + 1);
            if (!i) return 0;
        }
        return i;
    }

    /* fixmap */
    if (b >= 0x80 && b <= 0x8f) {
        uint32_t count = b & 0x0f;
        for (uint32_t j = 0; j < count; j++) {
            i = skip_one_d(a, n, i, depth + 1); if (!i) return 0;
            i = skip_one_d(a, n, i, depth + 1); if (!i) return 0;
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
            i = skip_one_d(a, n, i, depth + 1);
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
            i = skip_one_d(a, n, i, depth + 1); if (!i) return 0;
            i = skip_one_d(a, n, i, depth + 1); if (!i) return 0;
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

/* ── is_timestamp_ext — check if element at offset is a timestamp ext ── */

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

Type get_type(const uint8_t* a, uint32_t n, uint32_t i) {
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

int64_t get_container_count(const uint8_t* a, uint32_t n, uint32_t i) {
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

int path_step(
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

int lookup(
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

Value decode_element(const uint8_t* a, uint32_t n, uint32_t iStart, uint32_t iEnd) {
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

}  /* namespace detail */

using namespace detail;

/* ══════════════════════════════════════════════════════════════════════
** Public API: type_str, Value, and the read-only Blob accessors
** ══════════════════════════════════════════════════════════════════════ */

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

const uint8_t* Value::blob_data() const noexcept {
    return !owned_blob_.empty() ? owned_blob_.data() : blob_ptr_;
}
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
    Value v; v.type_ = Type::Integer; v.u64_ = x;
    /* Values that don't fit in int64 must be encoded as unsigned to round-trip. */
    if (x > static_cast<uint64_t>(INT64_MAX)) {
        v.int_width_ = IntWidth::Uint64;
    }
    return v;
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
    Value v;
    v.type_ = Type::Binary;
    if (len > 0 && data) {
        v.owned_blob_.assign(data, data + len);
        v.blob_ptr_ = v.owned_blob_.data();
    } else {
        v.blob_ptr_ = nullptr;
    }
    v.blob_len_ = len;
    return v;
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

/* ── Blob — construction & read-only accessors ────────────────────── */

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

}  /* namespace msgpack */
