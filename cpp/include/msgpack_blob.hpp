/*
** msgpack_blob.hpp — Standalone C++ MsgPack Blob API
**
** A zero-dependency C++ library for creating, querying, mutating, and
** iterating MessagePack binary blobs. Produces byte-identical output to
** the sqlite-msgpack extension so blobs are fully interchangeable.
**
** Classes:
**   msgpack::Value     — decoded scalar / sub-blob value
**   msgpack::Blob      — owning byte buffer with read/write operations
**   msgpack::Builder   — streaming encoder for constructing blobs
**   msgpack::Iterator  — cursor over container children (each/tree)
**   msgpack::EachRow   — row yielded by Iterator
*/

#ifndef MSGPACK_BLOB_HPP
#define MSGPACK_BLOB_HPP

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace msgpack {

/* Maximum nesting depth (matches sqlite extension) */
static constexpr int kMaxDepth = 200;
/* Maximum output buffer size */
static constexpr size_t kMaxOutput = 64 * 1024 * 1024;

/* ── Type enumeration ─────────────────────────────────────────────────── */

enum class Type {
    Nil,
    True,
    False,
    Integer,
    Real,
    Float32,
    String,
    Binary,
    Array,
    Map,
    Ext,
    Timestamp
};

const char* type_str(Type t) noexcept;

/* ── Integer encoding width hint ──────────────────────────────────────── */

enum class IntWidth {
    Auto,    /* compact encoding (default) */
    Int8,    /* force MP_INT8   (1-byte signed)   */
    Int16,   /* force MP_INT16  (2-byte signed)   */
    Int32,   /* force MP_INT32  (4-byte signed)   */
    Int64,   /* force MP_INT64  (8-byte signed)   */
    Uint8,   /* force MP_UINT8  (1-byte unsigned) */
    Uint16,  /* force MP_UINT16 (2-byte unsigned) */
    Uint32,  /* force MP_UINT32 (4-byte unsigned) */
    Uint64   /* force MP_UINT64 (8-byte unsigned) */
};

/* ── Forward declarations ─────────────────────────────────────────────── */

class Blob;
class Builder;
class Iterator;
struct EachRow;

/* ── Value ────────────────────────────────────────────────────────────── */

class Value {
public:
    Value() noexcept;

    Type type() const noexcept;
    bool is_nil() const noexcept;
    bool as_bool() const noexcept;
    int64_t as_int64() const noexcept;
    uint64_t as_uint64() const noexcept;
    double as_double() const noexcept;
    float as_float() const noexcept;
    std::string_view as_string() const noexcept;

    /* For containers / binary / ext — raw msgpack bytes */
    const uint8_t* blob_data() const noexcept;
    size_t blob_size() const noexcept;

    /* Ext type code (-128..127); meaningful only when type()==Ext or Timestamp */
    int8_t ext_type() const noexcept;

    /* Timestamp accessors (meaningful when type()==Timestamp) */
    int64_t timestamp_seconds() const noexcept;
    uint32_t timestamp_nanoseconds() const noexcept;

    /* Integer encoding width hint (meaningful when type()==Integer) */
    IntWidth int_width() const noexcept;

    /* Static constructors */
    static Value nil();
    static Value boolean(bool v);
    static Value integer(int64_t v);
    static Value unsigned_integer(uint64_t v);
    static Value real(double v);
    static Value real32(float v);
    static Value string(std::string_view s);
    static Value binary(const uint8_t* data, size_t len);
    static Value ext(int8_t type_code, const uint8_t* data, size_t len);
    static Value timestamp(int64_t seconds);
    static Value timestamp(int64_t seconds, uint32_t nanoseconds);

    /* Fixed-width integer constructors (force specific wire encoding) */
    static Value int8(int8_t v);
    static Value int16(int16_t v);
    static Value int32(int32_t v);
    static Value int64(int64_t v);
    static Value uint8(uint8_t v);
    static Value uint16(uint16_t v);
    static Value uint32(uint32_t v);
    static Value uint64(uint64_t v);

private:
    Type type_;
    union {
        int64_t  i64_;
        uint64_t u64_;
        double   f64_;
        float    f32_;
    };
    /* String / binary payload (stored externally or copied) */
    std::string str_;
    std::vector<uint8_t> owned_blob_;
    const uint8_t* blob_ptr_ = nullptr;
    size_t blob_len_ = 0;
    int8_t ext_type_ = 0;
    uint32_t ts_nsec_ = 0;
    IntWidth int_width_ = IntWidth::Auto;
};

/* ── EachRow ──────────────────────────────────────────────────────────── */

struct EachRow {
    std::string key;       /* map key ("" for arrays) */
    int64_t     index;     /* array index or pair index */
    Value       value;
    Type        type;
    std::string fullkey;   /* e.g. "$.users[0].name" */
    std::string path;      /* parent path */
    size_t      id;        /* byte offset in blob */
};

/* ── Blob ─────────────────────────────────────────────────────────────── */

class Blob {
public:
    Blob();
    Blob(const uint8_t* data, size_t size);
    explicit Blob(std::vector<uint8_t> data);

    /* Raw access */
    const uint8_t* data() const noexcept;
    size_t size() const noexcept;
    bool empty() const noexcept;

    /* Validation */
    bool valid() const;
    size_t error_position() const;

    /* Type inspection */
    Type type() const;
    Type type(const char* path) const;
    const char* type_str() const;
    const char* type_str(const char* path) const;

    /* Extraction */
    Value extract(const char* path) const;

    /* Container info */
    int64_t array_length() const;
    int64_t array_length(const char* path) const;

    /* Mutation (copy-on-write — returns new Blob) */
    Blob set(const char* path, const Value& val) const;
    Blob set(const char* path, const Blob& sub) const;
    Blob insert(const char* path, const Value& val) const;
    Blob replace(const char* path, const Value& val) const;
    Blob remove(const char* path) const;
    Blob array_insert(const char* path, const Value& val) const;
    Blob patch(const Blob& merge_patch) const;

    /* JSON conversion */
    std::string to_json() const;
    std::string to_json_pretty(int indent = 2) const;
    static Blob from_json(const char* json);
    static Blob from_json(const std::string& json);

private:
    std::vector<uint8_t> data_;
};

/* ── Builder ──────────────────────────────────────────────────────────── */

class Builder {
public:
    Builder();

    /* Scalars */
    Builder& nil();
    Builder& boolean(bool v);
    Builder& integer(int64_t v);
    Builder& unsigned_integer(uint64_t v);
    Builder& real(double v);
    Builder& real32(float v);
    Builder& string(std::string_view s);
    Builder& binary(const uint8_t* data, size_t len);
    Builder& ext(int8_t type_code, const uint8_t* data, size_t len);

    /* Fixed-width integer encoders (force specific wire format) */
    Builder& int8(int8_t v);
    Builder& int16(int16_t v);
    Builder& int32(int32_t v);
    Builder& int64(int64_t v);
    Builder& uint8(uint8_t v);
    Builder& uint16(uint16_t v);
    Builder& uint32(uint32_t v);
    Builder& uint64(uint64_t v);

    /* Container headers */
    Builder& array_header(uint32_t count);
    Builder& map_header(uint32_t count);

    /* Embed existing msgpack */
    Builder& raw(const uint8_t* data, size_t len);
    Builder& raw(const Blob& blob);

    /* Encode a Value */
    Builder& value(const Value& v);

    /* Timestamp */
    Builder& timestamp(int64_t sec);
    Builder& timestamp(int64_t sec, uint32_t nsec);

    /* Finalize */
    Blob build();

    /* Internal buffer access (used by mutation internals) */
    const uint8_t* buf_data() const noexcept;
    size_t buf_size() const noexcept;

    /* One-shot convenience */
    static Blob quote(const Value& v);

private:
    std::vector<uint8_t> buf_;
    void append(const uint8_t* data, size_t n);
    void append1(uint8_t b);
    uint8_t* reserve(size_t n);
};

/* ── Iterator ─────────────────────────────────────────────────────────── */

class Iterator {
public:
    Iterator(const Blob& blob, const char* path = "$", bool recursive = false);

    bool next();
    const EachRow& current() const;
    void reset();

private:
    const Blob& blob_;
    std::string base_path_;
    bool recursive_;
    std::vector<EachRow> rows_;
    int cursor_;
    bool populated_;

    void populate();
};

} /* namespace msgpack */

#endif /* MSGPACK_BLOB_HPP */
