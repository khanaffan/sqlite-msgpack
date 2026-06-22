/*
** fuzz_msgpack_blob.cpp — libFuzzer harness for the standalone C++ MsgPack API.
**
** Exercises ALL public entry points of msgpack::Blob, msgpack::Value,
** msgpack::Builder, and msgpack::Iterator with arbitrary byte sequences.
**
** Build with: -fsanitize=fuzzer,address -std=c++17
** Link against: msgpack_blob_static
**
** Every function tested here must handle arbitrary (malformed) input without
** crashing, leaking memory, or reading out of bounds.
*/

#include "msgpack_blob.hpp"
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string>

using namespace msgpack;

/* ── Helpers ────────────────────────────────────────────────────────── */

/* Consume a NUL-terminated path from the tail of the fuzz input. */
static void split_blob_path(const uint8_t *data, size_t size,
                            const uint8_t *&blob, size_t &blobsz,
                            char *pathbuf, size_t pathcap, size_t &pathsz)
{
    size_t split = size / 2;
    blob   = data;
    blobsz = split > 0 ? split : size;
    pathsz = size - split;
    if (pathsz >= pathcap) pathsz = pathcap - 1;
    if (pathsz > 0) std::memcpy(pathbuf, data + split, pathsz);
    pathbuf[pathsz] = '\0';
}

/* Prevent compiler from optimising away a value. */
template <typename T>
static void do_not_optimise(T const &val) {
    asm volatile("" : : "r,m"(val) : "memory");
}

/* ── Blob: validation & type inspection ────────────────────────────── */

static void fuzz_blob_inspect(const uint8_t *data, size_t size) {
    Blob b(data, size);

    do_not_optimise(b.valid());
    do_not_optimise(b.error_position());
    do_not_optimise(b.type());
    do_not_optimise(b.type_str());
    do_not_optimise(b.array_length());
    do_not_optimise(b.empty());
    do_not_optimise(b.size());
}

/* ── Blob: path-based type inspection ─────────────────────────────── */

static void fuzz_blob_path_inspect(const uint8_t *data, size_t size,
                                    const char *path) {
    Blob b(data, size);

    do_not_optimise(b.type(path));
    do_not_optimise(b.type_str(path));
    do_not_optimise(b.array_length(path));
}

/* ── Blob: extraction ─────────────────────────────────────────────── */

static void fuzz_blob_extract(const uint8_t *data, size_t size,
                               const char *path) {
    Blob b(data, size);
    Value v = b.extract(path);

    /* Exercise every accessor regardless of type — must not crash */
    do_not_optimise(v.type());
    do_not_optimise(v.is_nil());
    do_not_optimise(v.as_bool());
    do_not_optimise(v.as_int64());
    do_not_optimise(v.as_uint64());
    do_not_optimise(v.as_double());
    do_not_optimise(v.as_float());
    do_not_optimise(v.as_string());
    do_not_optimise(v.blob_data());
    do_not_optimise(v.blob_size());
    do_not_optimise(v.ext_type());
    do_not_optimise(v.timestamp_seconds());
    do_not_optimise(v.timestamp_nanoseconds());
    do_not_optimise(v.int_width());
}

/* ── Blob: extraction with static paths ───────────────────────────── */

static void fuzz_blob_extract_static(const uint8_t *data, size_t size) {
    Blob b(data, size);

    /* Exercise common path patterns */
    b.extract("$");
    b.extract("$[0]");
    b.extract("$.a");
    b.extract("$[0].a");
    b.extract("$.a[0]");
    b.extract("$.a.b.c");
    b.extract("$[0][1][2]");
}

/* ── Blob: mutation with static paths ─────────────────────────────── */

static void fuzz_blob_mutate_static(const uint8_t *data, size_t size) {
    Blob b(data, size);
    Value intval = Value::integer(42);
    Value strval = Value::string("fuzz");
    Value nilval = Value::nil();

    /* set */
    do_not_optimise(b.set("$", intval));
    do_not_optimise(b.set("$[0]", strval));
    do_not_optimise(b.set("$.a", intval));

    /* insert */
    do_not_optimise(b.insert("$.new_key", intval));
    do_not_optimise(b.insert("$[0]", strval));

    /* replace */
    do_not_optimise(b.replace("$[0]", intval));
    do_not_optimise(b.replace("$.a", strval));

    /* remove */
    do_not_optimise(b.remove("$[0]"));
    do_not_optimise(b.remove("$.a"));

    /* array_insert */
    do_not_optimise(b.array_insert("$[0]", intval));

    /* set with sub-blob */
    Builder sub;
    sub.array_header(2).integer(1).integer(2);
    do_not_optimise(b.set("$.arr", sub.build()));
}

/* ── Blob: mutation with fuzzed path ──────────────────────────────── */

static void fuzz_blob_mutate_fuzzed(const uint8_t *data, size_t size,
                                     const char *path) {
    Blob b(data, size);
    Value intval = Value::integer(99);

    do_not_optimise(b.set(path, intval));
    do_not_optimise(b.insert(path, intval));
    do_not_optimise(b.replace(path, intval));
    do_not_optimise(b.remove(path));
    do_not_optimise(b.array_insert(path, intval));
}

/* ── Blob: patch ──────────────────────────────────────────────────── */

static void fuzz_blob_patch(const uint8_t *data, size_t size) {
    /* Use the same data as both target and patch */
    Blob b(data, size);
    do_not_optimise(b.patch(b));

    /* Also try split: first half = target, second half = patch */
    size_t split = size / 2;
    if (split > 0) {
        Blob target(data, split);
        Blob patch_blob(data + split, size - split);
        do_not_optimise(target.patch(patch_blob));
    }
}

/* ── Blob: JSON conversion ────────────────────────────────────────── */

static void fuzz_blob_json(const uint8_t *data, size_t size) {
    Blob b(data, size);

    /* msgpack → JSON */
    std::string json = b.to_json();
    do_not_optimise(json);

    if (size <= 256) {
        std::string pretty = b.to_json_pretty(2);
        do_not_optimise(pretty);
    }

    /* JSON → msgpack (treat bytes as text) */
    if (size > 0 && size <= 2048) {
        std::string text(reinterpret_cast<const char *>(data), size);
        Blob from = Blob::from_json(text);
        do_not_optimise(from);
    }

    /* Round-trip: JSON text → msgpack → JSON text */
    if (size > 0 && size <= 1024) {
        std::string text(reinterpret_cast<const char *>(data), size);
        Blob rt = Blob::from_json(text);
        if (!rt.empty()) {
            std::string back = rt.to_json();
            do_not_optimise(back);
        }
    }
}

/* ── Iterator: each (flat) ────────────────────────────────────────── */

static void fuzz_iterator_each(const uint8_t *data, size_t size) {
    Blob b(data, size);
    Iterator it(b, "$", false);
    int limit = 10000;
    while (it.next() && --limit > 0) {
        const EachRow &row = it.current();
        do_not_optimise(row.key);
        do_not_optimise(row.index);
        do_not_optimise(row.value.type());
        do_not_optimise(row.fullkey);
        do_not_optimise(row.path);
        do_not_optimise(row.id);
    }
}

/* ── Iterator: tree (recursive) ───────────────────────────────────── */

static void fuzz_iterator_tree(const uint8_t *data, size_t size) {
    Blob b(data, size);
    Iterator it(b, "$", true);
    int limit = 10000;
    while (it.next() && --limit > 0) {
        const EachRow &row = it.current();
        do_not_optimise(row.key);
        do_not_optimise(row.index);
        do_not_optimise(row.value.type());
        do_not_optimise(row.fullkey);
        do_not_optimise(row.path);
        do_not_optimise(row.id);
    }
}

/* ── Iterator: with fuzzed path ───────────────────────────────────── */

static void fuzz_iterator_path(const uint8_t *data, size_t size,
                                const char *path) {
    Blob b(data, size);

    { /* each */
        Iterator it(b, path, false);
        int limit = 1000;
        while (it.next() && --limit > 0) {
            do_not_optimise(it.current().type);
        }
    }
    { /* tree */
        Iterator it(b, path, true);
        int limit = 1000;
        while (it.next() && --limit > 0) {
            do_not_optimise(it.current().type);
        }
    }
}

/* ── Builder: construct from fuzzed data ──────────────────────────── */

static void fuzz_builder(const uint8_t *data, size_t size) {
    if (size < 1) return;

    Builder b;

    /* Interpret bytes as a sequence of type tags + small payloads */
    size_t i = 0;
    int depth = 0;
    while (i < size && depth < 50) {
        uint8_t tag = data[i++];
        switch (tag & 0x0f) {
        case 0: b.nil(); break;
        case 1: b.boolean(tag & 0x10); break;
        case 2: {
            int64_t v = 0;
            if (i + 8 <= size) { std::memcpy(&v, data + i, 8); i += 8; }
            else if (i < size) { v = static_cast<int8_t>(data[i++]); }
            b.integer(v);
            break;
        }
        case 3: {
            double v = 0.0;
            if (i + 8 <= size) { std::memcpy(&v, data + i, 8); i += 8; }
            b.real(v);
            break;
        }
        case 4: {
            float v = 0.0f;
            if (i + 4 <= size) { std::memcpy(&v, data + i, 4); i += 4; }
            b.real32(v);
            break;
        }
        case 5: {
            size_t len = (i < size) ? (data[i++] & 0x3f) : 0;
            if (i + len > size) len = size - i;
            std::string_view sv(reinterpret_cast<const char*>(data + i), len);
            b.string(sv);
            i += len;
            break;
        }
        case 6: {
            size_t len = (i < size) ? (data[i++] & 0x1f) : 0;
            if (i + len > size) len = size - i;
            b.binary(data + i, len);
            i += len;
            break;
        }
        case 7: {
            int8_t tc = (i < size) ? static_cast<int8_t>(data[i++]) : 0;
            size_t len = (i < size) ? (data[i++] & 0x1f) : 0;
            if (i + len > size) len = size - i;
            b.ext(tc, data + i, len);
            i += len;
            break;
        }
        case 8: {
            int64_t sec = 0;
            if (i + 8 <= size) { std::memcpy(&sec, data + i, 8); i += 8; }
            b.timestamp(sec);
            break;
        }
        case 9: {
            int64_t sec = 0;
            uint32_t nsec = 0;
            if (i + 8 <= size) { std::memcpy(&sec, data + i, 8); i += 8; }
            if (i + 4 <= size) { std::memcpy(&nsec, data + i, 4); i += 4; }
            nsec &= 0x3FFFFFFF; /* cap at 999999999 range */
            b.timestamp(sec, nsec);
            break;
        }
        case 10: {
            uint32_t count = (i < size) ? (data[i++] & 0x0f) : 0;
            b.array_header(count);
            depth++;
            break;
        }
        case 11: {
            uint32_t count = (i < size) ? (data[i++] & 0x07) : 0;
            b.map_header(count);
            depth++;
            break;
        }
        case 12: {
            /* fixed-width integers */
            uint8_t sub = (i < size) ? data[i++] : 0;
            int64_t v = 0;
            if (i + 8 <= size) { std::memcpy(&v, data + i, 8); i += 8; }
            switch (sub & 0x07) {
            case 0: b.int8(static_cast<int8_t>(v)); break;
            case 1: b.int16(static_cast<int16_t>(v)); break;
            case 2: b.int32(static_cast<int32_t>(v)); break;
            case 3: b.int64(v); break;
            case 4: b.uint8(static_cast<uint8_t>(v)); break;
            case 5: b.uint16(static_cast<uint16_t>(v)); break;
            case 6: b.uint32(static_cast<uint32_t>(v)); break;
            case 7: b.uint64(static_cast<uint64_t>(v)); break;
            }
            break;
        }
        default:
            b.nil();
            break;
        }
    }

    Blob result = b.build();
    do_not_optimise(result);

    /* Feed builder output back through Blob inspection */
    if (!result.empty()) {
        do_not_optimise(result.valid());
        do_not_optimise(result.type());
        do_not_optimise(result.to_json());
    }
}

/* ── Builder::value() round-trip through all Value types ──────────── */

static void fuzz_builder_value(const uint8_t *data, size_t size) {
    Blob b(data, size);
    Value v = b.extract("$");

    /* Round-trip: extract → value → build → extract */
    Blob rt = Builder::quote(v);
    do_not_optimise(rt);

    if (!rt.empty()) {
        Value v2 = rt.extract("$");
        do_not_optimise(v2.type());
    }
}

/* ── Builder: raw embed and re-parse ──────────────────────────────── */

static void fuzz_builder_raw(const uint8_t *data, size_t size) {
    Builder b;
    b.array_header(1);
    b.raw(data, size);
    Blob result = b.build();

    /* Parse what we built */
    if (!result.empty()) {
        do_not_optimise(result.valid());
        do_not_optimise(result.type());
        Value v = result.extract("$[0]");
        do_not_optimise(v.type());
    }
}

/* ── Cross-API: build, mutate, iterate ────────────────────────────── */

static void fuzz_cross_api(const uint8_t *data, size_t size) {
    if (size < 2) return;

    /* Build a small map from fuzz data */
    Builder b;
    b.map_header(2);
    b.string("x");
    b.integer(static_cast<int8_t>(data[0]));
    b.string("y");

    size_t payload = size - 1;
    if (payload > 64) payload = 64;
    b.binary(data + 1, payload);

    Blob blob = b.build();

    /* Mutate with fuzz data */
    Value v = Value::integer(static_cast<int8_t>(data[size - 1]));
    Blob m1 = blob.set("$.x", v);
    do_not_optimise(m1);

    /* Iterate */
    Iterator it(m1, "$", false);
    while (it.next()) {
        do_not_optimise(it.current().value.type());
    }

    /* Patch: use original data as a potential patch */
    Blob patch_blob(data, size);
    Blob patched = blob.patch(patch_blob);
    do_not_optimise(patched);

    /* JSON round-trip on the patched result */
    std::string json = patched.to_json();
    Blob from = Blob::from_json(json);
    do_not_optimise(from);
}

/* ── Entrypoints ──────────────────────────────────────────────────── */

extern "C" int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 4096) return 0;

    /* --- Split input: first half = blob, second half = path --- */
    char pathbuf[2048];
    const uint8_t *blob;
    size_t blobsz, pathsz;
    split_blob_path(data, size, blob, blobsz, pathbuf, sizeof(pathbuf), pathsz);

    /* ── Blob: validation & type inspection ───────────────────── */
    fuzz_blob_inspect(data, size);

    /* ── Blob: path-based inspection with fuzzed path ─────────── */
    if (pathsz > 0) {
        fuzz_blob_path_inspect(blob, blobsz, pathbuf);
    }

    /* ── Blob: extraction ─────────────────────────────────────── */
    fuzz_blob_extract(data, size, "$");
    if (pathsz > 0) {
        fuzz_blob_extract(blob, blobsz, pathbuf);
    }
    fuzz_blob_extract_static(data, size);

    /* ── Blob: mutation with static paths ─────────────────────── */
    fuzz_blob_mutate_static(data, size);

    /* ── Blob: mutation with fuzzed path ──────────────────────── */
    if (pathsz > 0) {
        fuzz_blob_mutate_fuzzed(blob, blobsz, pathbuf);
    }

    /* ── Blob: patch ──────────────────────────────────────────── */
    fuzz_blob_patch(data, size);

    /* ── Blob: JSON conversion ────────────────────────────────── */
    fuzz_blob_json(data, size);

    /* ── Iterator: each (flat) ────────────────────────────────── */
    fuzz_iterator_each(data, size);

    /* ── Iterator: tree (recursive) ───────────────────────────── */
    if (size <= 512) {
        fuzz_iterator_tree(data, size);
    }

    /* ── Iterator: fuzzed path ────────────────────────────────── */
    if (pathsz > 0) {
        fuzz_iterator_path(blob, blobsz, pathbuf);
    }

    /* ── Builder: construct from fuzzed data ──────────────────── */
    fuzz_builder(data, size);

    /* ── Builder: value round-trip ────────────────────────────── */
    fuzz_builder_value(data, size);

    /* ── Builder: raw embed ───────────────────────────────────── */
    fuzz_builder_raw(data, size);

    /* ── Cross-API interactions ───────────────────────────────── */
    fuzz_cross_api(data, size);

    return 0;
}
