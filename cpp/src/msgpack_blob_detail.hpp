/*
** msgpack_blob_detail.hpp — internal shared declarations for the
** standalone C++ MsgPack Blob library.
**
** This header is PRIVATE to the implementation files under src/. It is not
** installed and is not part of the public API (include/msgpack_blob.hpp).
** It exposes the format constants, byte-order helpers, the growable output
** buffer, and the internal codec routines shared between the decode, encode,
** json, mutate, and iterate translation units.
*/

#ifndef MSGPACK_BLOB_DETAIL_HPP
#define MSGPACK_BLOB_DETAIL_HPP

#include "msgpack_blob.hpp"

#include <cstdint>
#include <cstddef>
#include <vector>

namespace msgpack {
namespace detail {

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

/* Ext type code reserved for the msgpack timestamp extension. */
static constexpr uint8_t MP_TIMESTAMP_TYPE = 0xFF;

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

/* ── Cross-module internal routines ───────────────────────────────────
** Each routine is defined in exactly one translation unit but may be called
** from several. Single-TU helpers stay file-local in their .cpp.
*/

/* decode module */
uint32_t skip_one(const uint8_t* a, uint32_t n, uint32_t i);
Type     get_type(const uint8_t* a, uint32_t n, uint32_t i);
int64_t  get_container_count(const uint8_t* a, uint32_t n, uint32_t i);
int      path_step(const char* zPath, int* pi,
                   const char** pKey, int* nKey, int64_t* pIdx);
int      lookup(const uint8_t* a, uint32_t n, uint32_t iRoot,
                const char* zPath, uint32_t* piStart, uint32_t* piEnd);
Value    decode_element(const uint8_t* a, uint32_t n,
                        uint32_t iStart, uint32_t iEnd);

/* encode module */
void encode_array_header(Buf& buf, uint32_t count);
void encode_map_header(Buf& buf, uint32_t count);
void encode_string(Buf& buf, const char* s, uint32_t len);

}  /* namespace detail */
}  /* namespace msgpack */

#endif /* MSGPACK_BLOB_DETAIL_HPP */
