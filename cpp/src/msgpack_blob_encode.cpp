/*
** msgpack_blob_encode.cpp — encoding
**
** Part of the standalone C++ MsgPack Blob library (see msgpack_blob.hpp).
** Contains: the low-level header encoders shared with the json/mutate
** modules, and the streaming Builder used to construct blobs.
*/

#include "msgpack_blob_detail.hpp"

#include <cstring>
#include <string_view>

namespace msgpack {
namespace detail {

/* ── encode helpers ───────────────────────────────────────────────── */

void encode_array_header(Buf& buf, uint32_t count) {
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

void encode_map_header(Buf& buf, uint32_t count) {
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

void encode_string(Buf& buf, const char* s, uint32_t len) {
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

}  /* namespace detail */

using namespace detail;

/* ══════════════════════════════════════════════════════════════════════
** Public API: Builder
** ══════════════════════════════════════════════════════════════════════ */

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

}  /* namespace msgpack */
