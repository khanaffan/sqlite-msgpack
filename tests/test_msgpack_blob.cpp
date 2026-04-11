/*
** test_msgpack_blob.cpp — Unit tests for the standalone C++ MsgPack Blob API
**
** Tests cover: Builder, extraction, mutation, iteration, JSON round-trip,
** and cross-compatibility with the SQLite msgpack extension.
*/

#include "msgpack_blob.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do {            \
    if (cond) { g_pass++; }              \
    else { g_fail++; std::fprintf(stderr, \
        "FAIL [%s:%d] %s\n", __FILE__, __LINE__, msg); } \
} while (0)

#define CHECK_EQ_INT(a, b, msg) CHECK((a) == (b), msg)
#define CHECK_EQ_STR(a, b, msg) CHECK(std::string(a) == std::string(b), msg)

/* ── Helpers ──────────────────────────────────────────────────────── */

/* Build the canonical msgpack for {"name":"Alice","age":30} */
static msgpack::Blob make_person() {
    msgpack::Builder b;
    b.map_header(2)
     .string("name").string("Alice")
     .string("age").integer(30);
    return b.build();
}

/* Build an array [10, 20, 30] */
static msgpack::Blob make_array() {
    msgpack::Builder b;
    b.array_header(3).integer(10).integer(20).integer(30);
    return b.build();
}

/* ── Builder basic tests ──────────────────────────────────────────── */

static void test_builder_scalars() {
    using namespace msgpack;

    /* nil */
    {
        Builder b; b.nil();
        Blob blob = b.build();
        CHECK(blob.valid(), "nil blob valid");
        CHECK(blob.type() == Type::Nil, "nil type");
        CHECK_EQ_INT(blob.size(), 1u, "nil size == 1");
        CHECK_EQ_INT(blob.data()[0], 0xc0, "nil byte == 0xc0");
    }

    /* booleans */
    {
        Builder b; b.boolean(true);
        Blob blob = b.build();
        CHECK(blob.valid(), "true blob valid");
        CHECK(blob.type() == Type::True, "true type");
    }
    {
        Builder b; b.boolean(false);
        Blob blob = b.build();
        CHECK(blob.valid(), "false blob valid");
        CHECK(blob.type() == Type::False, "false type");
    }

    /* positive fixint */
    {
        Blob blob = Builder::quote(Value::integer(42));
        CHECK(blob.valid(), "int42 valid");
        CHECK(blob.type() == Type::Integer, "int42 type");
        CHECK_EQ_INT(blob.size(), 1u, "fixint size == 1");
        CHECK_EQ_INT(blob.data()[0], 42, "fixint byte == 42");
    }

    /* negative fixint */
    {
        Blob blob = Builder::quote(Value::integer(-5));
        CHECK(blob.valid(), "int-5 valid");
        CHECK_EQ_INT(blob.size(), 1u, "neg fixint size == 1");
    }

    /* uint8 */
    {
        Blob blob = Builder::quote(Value::integer(200));
        CHECK(blob.valid(), "uint8 valid");
        CHECK_EQ_INT(blob.size(), 2u, "uint8 size == 2");
    }

    /* uint16 */
    {
        Blob blob = Builder::quote(Value::integer(1000));
        CHECK(blob.valid(), "uint16 valid");
        CHECK_EQ_INT(blob.size(), 3u, "uint16 size == 3");
    }

    /* float64 */
    {
        Builder b; b.real(3.14);
        Blob blob = b.build();
        CHECK(blob.valid(), "float64 valid");
        CHECK(blob.type() == Type::Real, "float64 type");
        CHECK_EQ_INT(blob.size(), 9u, "float64 size == 9");
    }

    /* string */
    {
        Builder b; b.string("hello");
        Blob blob = b.build();
        CHECK(blob.valid(), "string valid");
        CHECK(blob.type() == Type::String, "string type");
    }
}

/* ── Builder container tests ──────────────────────────────────────── */

static void test_builder_containers() {
    using namespace msgpack;

    /* array */
    {
        Blob blob = make_array();
        CHECK(blob.valid(), "array valid");
        CHECK(blob.type() == Type::Array, "array type");
        CHECK_EQ_INT(blob.array_length(), 3, "array length == 3");
    }

    /* map */
    {
        Blob blob = make_person();
        CHECK(blob.valid(), "map valid");
        CHECK(blob.type() == Type::Map, "map type");
        CHECK_EQ_INT(blob.array_length(), 2, "map length == 2");
    }

    /* nested */
    {
        Builder b;
        b.map_header(1)
         .string("scores").array_header(3)
           .integer(100).integer(95).integer(88);
        Blob blob = b.build();
        CHECK(blob.valid(), "nested valid");
        CHECK_EQ_INT(blob.array_length("$.scores"), 3, "nested array length");
    }
}

/* ── Extraction tests ─────────────────────────────────────────────── */

static void test_extraction() {
    using namespace msgpack;
    Blob blob = make_person();

    /* root */
    {
        Value v = blob.extract("$");
        CHECK(v.type() == Type::Binary, "root extract is binary (raw container)");
    }

    /* map key */
    {
        Value v = blob.extract("$.name");
        CHECK(v.type() == Type::String, "extract name type");
        CHECK_EQ_STR(std::string(v.as_string()), "Alice", "extract name value");
    }
    {
        Value v = blob.extract("$.age");
        CHECK(v.type() == Type::Integer, "extract age type");
        CHECK_EQ_INT(v.as_int64(), 30, "extract age value");
    }

    /* missing key */
    {
        Value v = blob.extract("$.missing");
        CHECK(v.is_nil(), "missing key → nil");
    }

    /* array index */
    {
        Blob arr = make_array();
        Value v0 = arr.extract("$[0]");
        CHECK_EQ_INT(v0.as_int64(), 10, "arr[0] == 10");
        Value v2 = arr.extract("$[2]");
        CHECK_EQ_INT(v2.as_int64(), 30, "arr[2] == 30");
    }

    /* out-of-bounds index */
    {
        Blob arr = make_array();
        Value v = arr.extract("$[99]");
        CHECK(v.is_nil(), "out of bounds → nil");
    }

    /* nested path */
    {
        Builder b;
        b.map_header(1)
         .string("users").array_header(1)
           .map_header(1).string("name").string("Bob");
        Blob blob2 = b.build();
        Value v = blob2.extract("$.users[0].name");
        CHECK_EQ_STR(std::string(v.as_string()), "Bob", "nested path extract");
    }
}

/* ── Type inspection tests ────────────────────────────────────────── */

static void test_type_inspection() {
    using namespace msgpack;
    Blob blob = make_person();

    CHECK_EQ_STR(blob.type_str(), "map", "type_str root");
    CHECK_EQ_STR(blob.type_str("$.name"), "text", "type_str name");
    CHECK_EQ_STR(blob.type_str("$.age"), "integer", "type_str age");
}

/* ── Mutation tests ───────────────────────────────────────────────── */

static void test_mutation_set() {
    using namespace msgpack;
    Blob blob = make_person();

    /* set existing key */
    Blob b2 = blob.set("$.name", Value::string("Bob"));
    CHECK(b2.valid(), "set valid");
    Value v = b2.extract("$.name");
    CHECK_EQ_STR(std::string(v.as_string()), "Bob", "set name → Bob");

    /* original unchanged */
    Value v_orig = blob.extract("$.name");
    CHECK_EQ_STR(std::string(v_orig.as_string()), "Alice", "original unchanged");

    /* set new key */
    Blob b3 = blob.set("$.email", Value::string("a@b.com"));
    CHECK(b3.valid(), "set new key valid");
    Value v3 = b3.extract("$.email");
    CHECK_EQ_STR(std::string(v3.as_string()), "a@b.com", "set new key value");
}

static void test_mutation_insert() {
    using namespace msgpack;
    Blob blob = make_person();

    /* insert on existing key → no-op */
    Blob b2 = blob.insert("$.name", Value::string("XXX"));
    Value v = b2.extract("$.name");
    CHECK_EQ_STR(std::string(v.as_string()), "Alice", "insert existing → no-op");

    /* insert on new key → creates */
    Blob b3 = blob.insert("$.email", Value::string("a@b.com"));
    Value v3 = b3.extract("$.email");
    CHECK_EQ_STR(std::string(v3.as_string()), "a@b.com", "insert new key");
}

static void test_mutation_replace() {
    using namespace msgpack;
    Blob blob = make_person();

    /* replace existing → works */
    Blob b2 = blob.replace("$.name", Value::string("Bob"));
    Value v = b2.extract("$.name");
    CHECK_EQ_STR(std::string(v.as_string()), "Bob", "replace existing");

    /* replace missing → no-op */
    Blob b3 = blob.replace("$.email", Value::string("x"));
    Value v3 = b3.extract("$.email");
    CHECK(v3.is_nil(), "replace missing → no-op");
}

static void test_mutation_remove() {
    using namespace msgpack;
    Blob blob = make_person();

    Blob b2 = blob.remove("$.name");
    CHECK(b2.valid(), "remove valid");
    Value v = b2.extract("$.name");
    CHECK(v.is_nil(), "removed key → nil");

    /* age still there */
    Value v2 = b2.extract("$.age");
    CHECK_EQ_INT(v2.as_int64(), 30, "age still present after remove name");
}

static void test_mutation_array() {
    using namespace msgpack;
    Blob arr = make_array(); /* [10, 20, 30] */

    /* set array element */
    Blob a2 = arr.set("$[1]", Value::integer(99));
    CHECK_EQ_INT(a2.extract("$[1]").as_int64(), 99, "set arr[1] → 99");

    /* remove array element */
    Blob a3 = arr.remove("$[0]");
    CHECK_EQ_INT(a3.array_length(), 2, "remove arr[0] → length 2");
    CHECK_EQ_INT(a3.extract("$[0]").as_int64(), 20, "after remove, arr[0] == 20");

    /* array_insert */
    Blob a4 = arr.array_insert("$[1]", Value::integer(15));
    CHECK_EQ_INT(a4.array_length(), 4, "array_insert → length 4");
    CHECK_EQ_INT(a4.extract("$[0]").as_int64(), 10, "array_insert [0]");
    CHECK_EQ_INT(a4.extract("$[1]").as_int64(), 15, "array_insert [1] == 15");
    CHECK_EQ_INT(a4.extract("$[2]").as_int64(), 20, "array_insert [2] == 20");
}

/* ── Merge patch tests ────────────────────────────────────────────── */

static void test_patch() {
    using namespace msgpack;
    Blob blob = make_person(); /* {"name":"Alice","age":30} */

    /* patch: change name, remove age, add email */
    Builder pb;
    pb.map_header(3)
      .string("name").string("Carol")
      .string("age").nil()
      .string("email").string("c@d.com");
    Blob patch = pb.build();

    Blob result = blob.patch(patch);
    CHECK(result.valid(), "patch valid");
    CHECK_EQ_STR(std::string(result.extract("$.name").as_string()), "Carol", "patch name");
    CHECK(result.extract("$.age").is_nil(), "patch removed age");
    CHECK_EQ_STR(std::string(result.extract("$.email").as_string()), "c@d.com", "patch added email");
}

/* ── JSON round-trip tests ────────────────────────────────────────── */

static void test_json() {
    using namespace msgpack;

    /* to_json */
    {
        Blob blob = make_person();
        std::string json = blob.to_json();
        CHECK(json.find("\"name\"") != std::string::npos, "json has name key");
        CHECK(json.find("\"Alice\"") != std::string::npos, "json has Alice");
        CHECK(json.find("30") != std::string::npos, "json has 30");
    }

    /* from_json → to_json round-trip */
    {
        const char* json = R"({"x":1,"arr":[true,false,null],"pi":3.14})";
        Blob blob = Blob::from_json(json);
        CHECK(blob.valid(), "from_json valid");

        Value x = blob.extract("$.x");
        CHECK_EQ_INT(x.as_int64(), 1, "from_json x == 1");

        Value arr0 = blob.extract("$.arr[0]");
        CHECK(arr0.type() == Type::True, "from_json arr[0] == true");

        Value arr2 = blob.extract("$.arr[2]");
        CHECK(arr2.is_nil(), "from_json arr[2] == null");
    }

    /* from_json integers */
    {
        Blob blob = Blob::from_json("42");
        CHECK(blob.valid(), "from_json 42 valid");
        Value v = blob.extract("$");
        CHECK_EQ_INT(v.as_int64(), 42, "from_json 42 value");
    }

    /* pretty print */
    {
        Blob blob = make_array();
        std::string pretty = blob.to_json_pretty(2);
        CHECK(pretty.find('\n') != std::string::npos, "pretty has newlines");
    }
}

/* ── Iterator tests ───────────────────────────────────────────────── */

static void test_iterator_each() {
    using namespace msgpack;

    /* array iteration */
    {
        Blob arr = make_array();
        Iterator it(arr);
        int count = 0;
        while (it.next()) {
            count++;
        }
        CHECK_EQ_INT(count, 3, "each array → 3 rows");
    }

    /* map iteration */
    {
        Blob map = make_person();
        Iterator it(map);
        int count = 0;
        bool foundName = false, foundAge = false;
        while (it.next()) {
            auto& row = it.current();
            if (row.key == "name") foundName = true;
            if (row.key == "age") foundAge = true;
            count++;
        }
        CHECK_EQ_INT(count, 2, "each map → 2 rows");
        CHECK(foundName, "each map has name");
        CHECK(foundAge, "each map has age");
    }

    /* sub-path iteration */
    {
        Builder b;
        b.map_header(1)
         .string("scores").array_header(3)
           .integer(100).integer(95).integer(88);
        Blob blob = b.build();
        Iterator it(blob, "$.scores");
        int count = 0;
        while (it.next()) count++;
        CHECK_EQ_INT(count, 3, "each at sub-path → 3 rows");
    }
}

static void test_iterator_tree() {
    using namespace msgpack;

    Builder b;
    b.map_header(2)
     .string("name").string("Alice")
     .string("scores").array_header(2)
       .integer(100).integer(95);
    Blob blob = b.build();

    Iterator it(blob, "$", true);
    int count = 0;
    while (it.next()) count++;
    /* DFS: root map, "name" value, "scores" array, [0]=100, [1]=95 → 5 nodes */
    CHECK_EQ_INT(count, 5, "tree walk → 5 nodes");
}

/* ── Builder embed raw blob test ──────────────────────────────────── */

static void test_builder_embed() {
    using namespace msgpack;

    Blob inner = make_array();
    Builder b;
    b.map_header(1).string("data").raw(inner);
    Blob outer = b.build();

    CHECK(outer.valid(), "embed valid");
    CHECK_EQ_INT(outer.array_length("$.data"), 3, "embedded array length");
    CHECK_EQ_INT(outer.extract("$.data[0]").as_int64(), 10, "embedded data[0]");
}

/* ── Validation tests ─────────────────────────────────────────────── */

static void test_validation() {
    using namespace msgpack;

    /* empty blob is invalid */
    {
        Blob empty;
        CHECK(!empty.valid(), "empty blob invalid");
    }

    /* truncated blob */
    {
        uint8_t trunc[] = {0xcc}; /* uint8 but missing byte */
        Blob blob(trunc, 1);
        CHECK(!blob.valid(), "truncated blob invalid");
    }

    /* valid single-byte nil */
    {
        uint8_t nil_byte[] = {0xc0};
        Blob blob(nil_byte, 1);
        CHECK(blob.valid(), "nil blob valid");
    }
}

/* ── Cross-compatibility test (SQLite extension format) ───────────── */

static void test_sqlite_compat() {
    using namespace msgpack;

    /* The SQLite extension encodes msgpack_quote(42) as a single fixint byte.
    ** Verify our Builder produces the same. */
    {
        Blob blob = Builder::quote(Value::integer(42));
        CHECK_EQ_INT(blob.size(), 1u, "quote(42) size == 1");
        CHECK_EQ_INT(blob.data()[0], 42u, "quote(42) byte == 0x2a");
    }

    /* msgpack_array(1,2,3) → fixarray(3) + fixint(1) + fixint(2) + fixint(3) */
    {
        Builder b;
        b.array_header(3).integer(1).integer(2).integer(3);
        Blob blob = b.build();
        CHECK_EQ_INT(blob.size(), 4u, "array(1,2,3) size == 4");
        CHECK_EQ_INT(blob.data()[0], 0x93u, "fixarray(3) marker");
        CHECK_EQ_INT(blob.data()[1], 1u, "elem 1");
        CHECK_EQ_INT(blob.data()[2], 2u, "elem 2");
        CHECK_EQ_INT(blob.data()[3], 3u, "elem 3");
    }

    /* msgpack_object("a",1) → fixmap(1) + fixstr(1)+"a" + fixint(1) */
    {
        Builder b;
        b.map_header(1).string("a").integer(1);
        Blob blob = b.build();
        CHECK_EQ_INT(blob.size(), 4u, "object(a,1) size == 4");
        CHECK_EQ_INT(blob.data()[0], 0x81u, "fixmap(1) marker");
        CHECK_EQ_INT(blob.data()[1], 0xa1u, "fixstr(1) marker");
        CHECK_EQ_INT(blob.data()[2], 'a', "key byte");
        CHECK_EQ_INT(blob.data()[3], 1u, "val byte");
    }

    /* msgpack_nil() → 0xc0, msgpack_true() → 0xc3, msgpack_false() → 0xc2 */
    {
        CHECK_EQ_INT(Builder::quote(Value::nil()).data()[0], 0xc0u, "nil format");
        CHECK_EQ_INT(Builder::quote(Value::boolean(true)).data()[0], 0xc3u, "true format");
        CHECK_EQ_INT(Builder::quote(Value::boolean(false)).data()[0], 0xc2u, "false format");
    }

    /* Negative integer encoding matches: -1 → 0xff (negative fixint) */
    {
        Blob blob = Builder::quote(Value::integer(-1));
        CHECK_EQ_INT(blob.size(), 1u, "quote(-1) size == 1");
        CHECK_EQ_INT(blob.data()[0], 0xffu, "neg fixint -1 == 0xff");
    }

    /* String encoding: "hello" → fixstr(5) + "hello" */
    {
        Blob blob = Builder::quote(Value::string("hello"));
        CHECK_EQ_INT(blob.size(), 6u, "string(hello) size == 6");
        CHECK_EQ_INT(blob.data()[0], 0xa5u, "fixstr(5) marker");
    }
}

/* ── Float32 ──────────────────────────────────────────────────────── */

static void test_float32() {
    std::printf("== float32 ==\n");

    /* Builder::real32 encodes as MP_FLOAT32 (0xca) */
    {
        msgpack::Builder b;
        b.real32(3.14f);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 5, "real32 size 5");
        CHECK(blob.data()[0] == 0xca, "real32 tag 0xca");
        CHECK(blob.type() == msgpack::Type::Float32, "real32 type Float32");
        CHECK_EQ_STR(blob.type_str(), "float32", "real32 type_str");
    }

    /* Extract float32 value */
    {
        msgpack::Builder b;
        b.real32(1.5f);
        msgpack::Blob blob = b.build();
        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Float32, "extract f32 type");
        CHECK(v.as_float() == 1.5f, "extract f32 as_float");
        CHECK(v.as_double() == 1.5, "extract f32 as_double");
    }

    /* Float32 vs Float64 distinguishable */
    {
        msgpack::Builder b;
        b.map_header(2)
          .string("f32").real32(1.0f)
          .string("f64").real(1.0);
        msgpack::Blob blob = b.build();
        CHECK(blob.type("$.f32") == msgpack::Type::Float32, "f32 field type");
        CHECK(blob.type("$.f64") == msgpack::Type::Real, "f64 field type");
    }

    /* Mutate with float32 value */
    {
        msgpack::Builder b;
        b.map_header(1).string("x").integer(42);
        msgpack::Blob blob = b.build();
        msgpack::Blob blob2 = blob.set("$.x", msgpack::Value::real32(2.5f));
        msgpack::Value v = blob2.extract("$.x");
        CHECK(v.type() == msgpack::Type::Float32, "mutate f32 type");
        CHECK(v.as_float() == 2.5f, "mutate f32 val");
        CHECK(blob2.data()[blob2.size() - 5] == 0xca, "mutate f32 tag");
    }

    /* Insert float32 into array */
    {
        msgpack::Builder b;
        b.array_header(1).integer(1);
        msgpack::Blob blob = b.build();
        msgpack::Blob blob2 = blob.array_insert("$[1]", msgpack::Value::real32(9.0f));
        msgpack::Value v = blob2.extract("$[1]");
        CHECK(v.type() == msgpack::Type::Float32, "array_insert f32 type");
        CHECK(v.as_float() == 9.0f, "array_insert f32 val");
    }

    /* Float32 in JSON renders as number */
    {
        msgpack::Builder b;
        b.real32(1.5f);
        msgpack::Blob blob = b.build();
        std::string json = blob.to_json();
        CHECK(json.find("1.5") != std::string::npos, "f32 json");
    }
}

/* ── Ext type ─────────────────────────────────────────────────────── */

static void test_ext() {
    std::printf("== ext ==\n");

    /* Builder::ext encodes ext type with data */
    {
        uint8_t data[] = {0x01, 0x02, 0x03, 0x04};
        msgpack::Builder b;
        b.ext(42, data, 4);
        msgpack::Blob blob = b.build();
        CHECK(blob.type() == msgpack::Type::Ext, "ext type");
        CHECK_EQ_STR(blob.type_str(), "ext", "ext type_str");
        /* fixext4: 0xd6, type, 4 bytes data */
        CHECK(blob.size() == 6, "fixext4 size");
        CHECK(blob.data()[0] == 0xd6, "fixext4 tag");
        CHECK(blob.data()[1] == 42, "fixext4 type code");
    }

    /* Builder::ext with fixext1/2/4/8/16 sizes */
    {
        uint8_t d1[1] = {0xAA};
        msgpack::Builder b1; b1.ext(1, d1, 1);
        msgpack::Blob bl1 = b1.build();
        CHECK(bl1.data()[0] == 0xd4 && bl1.size() == 3, "fixext1");

        uint8_t d2[2] = {0xAA, 0xBB};
        msgpack::Builder b2; b2.ext(2, d2, 2);
        msgpack::Blob bl2 = b2.build();
        CHECK(bl2.data()[0] == 0xd5 && bl2.size() == 4, "fixext2");

        uint8_t d8[8] = {};
        msgpack::Builder b8; b8.ext(3, d8, 8);
        msgpack::Blob bl8 = b8.build();
        CHECK(bl8.data()[0] == 0xd7 && bl8.size() == 10, "fixext8");

        uint8_t d16[16] = {};
        msgpack::Builder b16; b16.ext(4, d16, 16);
        msgpack::Blob bl16 = b16.build();
        CHECK(bl16.data()[0] == 0xd8 && bl16.size() == 18, "fixext16");
    }

    /* Builder::ext with non-fixed size (e.g., 3 bytes -> ext8) */
    {
        uint8_t d3[3] = {0x01, 0x02, 0x03};
        msgpack::Builder b; b.ext(5, d3, 3);
        msgpack::Blob bl = b.build();
        CHECK(bl.data()[0] == 0xc7, "ext8 tag");
        CHECK(bl.data()[1] == 3, "ext8 len");
        CHECK(bl.data()[2] == 5, "ext8 type code");
        CHECK(bl.size() == 6, "ext8 size");
    }

    /* Mutate with ext value */
    {
        msgpack::Builder b;
        b.map_header(1).string("data").integer(0);
        msgpack::Blob blob = b.build();
        uint8_t extdata[] = {0xFF};
        msgpack::Blob blob2 = blob.set("$.data", msgpack::Value::ext(7, extdata, 1));
        CHECK(blob2.type("$.data") == msgpack::Type::Ext, "mutate ext type");
    }
}

/* ── Timestamp ────────────────────────────────────────────────────── */

static void test_timestamp() {
    std::printf("== timestamp ==\n");

    /* ts32 format: sec in [0, UINT32_MAX], nsec=0 -> fixext4 (6 bytes) */
    {
        msgpack::Builder b;
        b.value(msgpack::Value::timestamp(1000));
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 6, "ts32 size");
        CHECK(blob.data()[0] == 0xd6, "ts32 fixext4");
        CHECK(static_cast<uint8_t>(blob.data()[1]) == 0xFF, "ts32 type");
        CHECK(blob.type() == msgpack::Type::Timestamp, "ts32 Type");
        CHECK_EQ_STR(blob.type_str(), "timestamp", "ts32 type_str");

        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Timestamp, "ts32 extract type");
        CHECK(v.timestamp_seconds() == 1000, "ts32 seconds");
        CHECK(v.timestamp_nanoseconds() == 0, "ts32 nsec");
    }

    /* ts64 format: sec in [0, 2^34-1] with nsec > 0 -> fixext8 (10 bytes) */
    {
        msgpack::Builder b;
        b.value(msgpack::Value::timestamp(100, 500000000));
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 10, "ts64 size");
        CHECK(blob.data()[0] == 0xd7, "ts64 fixext8");
        CHECK(static_cast<uint8_t>(blob.data()[1]) == 0xFF, "ts64 type");
        CHECK(blob.type() == msgpack::Type::Timestamp, "ts64 Type");

        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Timestamp, "ts64 extract type");
        CHECK(v.timestamp_seconds() == 100, "ts64 seconds");
        CHECK(v.timestamp_nanoseconds() == 500000000, "ts64 nsec");
    }

    /* ts96 format: negative seconds -> ext8/12 (15 bytes) */
    {
        msgpack::Builder b;
        b.value(msgpack::Value::timestamp(-1000, 0));
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 15, "ts96 size");
        CHECK(blob.data()[0] == 0xc7, "ts96 ext8");
        CHECK(blob.data()[1] == 12, "ts96 payload 12");
        CHECK(static_cast<uint8_t>(blob.data()[2]) == 0xFF, "ts96 type");
        CHECK(blob.type() == msgpack::Type::Timestamp, "ts96 Type");

        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Timestamp, "ts96 extract type");
        CHECK(v.timestamp_seconds() == -1000, "ts96 seconds");
        CHECK(v.timestamp_nanoseconds() == 0, "ts96 nsec");
    }

    /* ts96 with nanoseconds and negative sec */
    {
        msgpack::Builder b;
        b.value(msgpack::Value::timestamp(-500, 123456789));
        msgpack::Blob blob = b.build();
        msgpack::Value v = blob.extract("$");
        CHECK(v.timestamp_seconds() == -500, "ts96neg sec");
        CHECK(v.timestamp_nanoseconds() == 123456789, "ts96neg nsec");
    }

    /* ts32 with sec=0 */
    {
        msgpack::Builder b;
        b.value(msgpack::Value::timestamp(0));
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 6, "ts32 zero size");
        msgpack::Value v = blob.extract("$");
        CHECK(v.timestamp_seconds() == 0, "ts32 zero sec");
        CHECK(v.timestamp_nanoseconds() == 0, "ts32 zero nsec");
    }

    /* Mutate: set a field to timestamp */
    {
        msgpack::Builder b;
        b.map_header(1).string("ts").integer(0);
        msgpack::Blob blob = b.build();
        msgpack::Blob blob2 = blob.set("$.ts", msgpack::Value::timestamp(1700000000));
        CHECK(blob2.type("$.ts") == msgpack::Type::Timestamp, "mutate ts type");
        msgpack::Value v = blob2.extract("$.ts");
        CHECK(v.timestamp_seconds() == 1700000000, "mutate ts sec");
    }

    /* Insert timestamp into array */
    {
        msgpack::Builder b;
        b.array_header(1).integer(1);
        msgpack::Blob blob = b.build();
        msgpack::Blob blob2 = blob.array_insert("$[1]", msgpack::Value::timestamp(42, 999));
        msgpack::Value v = blob2.extract("$[1]");
        CHECK(v.type() == msgpack::Type::Timestamp, "array_insert ts type");
        CHECK(v.timestamp_seconds() == 42, "array_insert ts sec");
        CHECK(v.timestamp_nanoseconds() == 999, "array_insert ts nsec");
    }

    /* Large positive sec that fits in ts64 (> UINT32_MAX) */
    {
        int64_t sec = static_cast<int64_t>(0x200000000LL); /* 2^33 */
        msgpack::Builder b;
        b.value(msgpack::Value::timestamp(sec, 100));
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 10, "ts64 large sec size");
        msgpack::Value v = blob.extract("$");
        CHECK(v.timestamp_seconds() == sec, "ts64 large sec val");
        CHECK(v.timestamp_nanoseconds() == 100, "ts64 large sec nsec");
    }

    /* Timestamp type inspection via path */
    {
        msgpack::Builder b;
        b.map_header(2)
          .string("created").value(msgpack::Value::timestamp(1000))
          .string("name").string("test");
        msgpack::Blob blob = b.build();
        CHECK(blob.type("$.created") == msgpack::Type::Timestamp, "path ts type");
        CHECK(blob.type("$.name") == msgpack::Type::String, "path str type");
        CHECK_EQ_STR(blob.type_str("$.created"), "timestamp", "path ts type_str");
    }
}

/* ── Value::real32 / as_float accessors ────────────────────────────── */

static void test_value_typed_accessors() {
    std::printf("== value typed accessors ==\n");

    /* Value::real32 */
    {
        msgpack::Value v = msgpack::Value::real32(3.14f);
        CHECK(v.type() == msgpack::Type::Float32, "val real32 type");
        CHECK(v.as_float() == 3.14f, "val real32 as_float");
        CHECK(std::abs(v.as_double() - 3.14) < 0.01, "val real32 as_double");
    }

    /* Value::real -> as_float conversion */
    {
        msgpack::Value v = msgpack::Value::real(2.5);
        CHECK(v.as_float() == 2.5f, "val real as_float");
    }

    /* Value::ext */
    {
        uint8_t d[] = {0x01, 0x02};
        msgpack::Value v = msgpack::Value::ext(42, d, 2);
        CHECK(v.type() == msgpack::Type::Ext, "val ext type");
        CHECK(v.ext_type() == 42, "val ext type_code");
        CHECK(v.blob_size() == 2, "val ext blob_size");
        CHECK(v.blob_data()[0] == 0x01, "val ext byte0");
        CHECK(v.blob_data()[1] == 0x02, "val ext byte1");
    }

    /* Value::timestamp */
    {
        msgpack::Value v = msgpack::Value::timestamp(999, 500);
        CHECK(v.type() == msgpack::Type::Timestamp, "val ts type");
        CHECK(v.timestamp_seconds() == 999, "val ts sec");
        CHECK(v.timestamp_nanoseconds() == 500, "val ts nsec");
        CHECK(v.as_int64() == 999, "val ts as_int64");
    }
}

/* ── Fixed-width integers ─────────────────────────────────────────── */

static void test_fixed_width_integers() {
    std::printf("== fixed-width integers ==\n");

    /* ---- Builder fixed-width signed ---- */

    /* int8: MP_INT8 (0xd0), 2 bytes total */
    {
        msgpack::Builder b;
        b.int8(42);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 2, "int8 size");
        CHECK(blob.data()[0] == 0xd0, "int8 tag");
        CHECK(blob.data()[1] == 42, "int8 val");
        CHECK(blob.type() == msgpack::Type::Integer, "int8 type");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == 42, "int8 extract");
    }

    /* int8 negative */
    {
        msgpack::Builder b;
        b.int8(-100);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 2, "int8 neg size");
        CHECK(blob.data()[0] == 0xd0, "int8 neg tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == -100, "int8 neg val");
    }

    /* int16: MP_INT16 (0xd1), 3 bytes total */
    {
        msgpack::Builder b;
        b.int16(1000);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 3, "int16 size");
        CHECK(blob.data()[0] == 0xd1, "int16 tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == 1000, "int16 extract");
    }

    /* int16 negative */
    {
        msgpack::Builder b;
        b.int16(-30000);
        msgpack::Blob blob = b.build();
        CHECK(blob.data()[0] == 0xd1, "int16 neg tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == -30000, "int16 neg val");
    }

    /* int32: MP_INT32 (0xd2), 5 bytes total */
    {
        msgpack::Builder b;
        b.int32(100000);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 5, "int32 size");
        CHECK(blob.data()[0] == 0xd2, "int32 tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == 100000, "int32 extract");
    }

    /* int32 negative */
    {
        msgpack::Builder b;
        b.int32(-2000000000);
        msgpack::Blob blob = b.build();
        CHECK(blob.data()[0] == 0xd2, "int32 neg tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == -2000000000, "int32 neg val");
    }

    /* int64: MP_INT64 (0xd3), 9 bytes total */
    {
        msgpack::Builder b;
        b.int64(1LL << 40);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 9, "int64 size");
        CHECK(blob.data()[0] == 0xd3, "int64 tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == (1LL << 40), "int64 extract");
    }

    /* int64 small value — still forced to 9 bytes */
    {
        msgpack::Builder b;
        b.int64(5);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 9, "int64 small size");
        CHECK(blob.data()[0] == 0xd3, "int64 small tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == 5, "int64 small val");
    }

    /* ---- Builder fixed-width unsigned ---- */

    /* uint8: MP_UINT8 (0xcc), 2 bytes */
    {
        msgpack::Builder b;
        b.uint8(200);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 2, "uint8 size");
        CHECK(blob.data()[0] == 0xcc, "uint8 tag");
        CHECK(blob.data()[1] == 200, "uint8 val");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == 200, "uint8 extract");
    }

    /* uint8 small — still forced to 2 bytes (not fixint) */
    {
        msgpack::Builder b;
        b.uint8(5);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 2, "uint8 small size");
        CHECK(blob.data()[0] == 0xcc, "uint8 small tag");
    }

    /* uint16: MP_UINT16 (0xcd), 3 bytes */
    {
        msgpack::Builder b;
        b.uint16(50000);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 3, "uint16 size");
        CHECK(blob.data()[0] == 0xcd, "uint16 tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == 50000, "uint16 extract");
    }

    /* uint32: MP_UINT32 (0xce), 5 bytes */
    {
        msgpack::Builder b;
        b.uint32(3000000000u);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 5, "uint32 size");
        CHECK(blob.data()[0] == 0xce, "uint32 tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_int64() == 3000000000LL, "uint32 extract");
    }

    /* uint64: MP_UINT64 (0xcf), 9 bytes */
    {
        msgpack::Builder b;
        uint64_t big = 0xFFFFFFFFFFFFFFFFULL;
        b.uint64(big);
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 9, "uint64 size");
        CHECK(blob.data()[0] == 0xcf, "uint64 tag");
        msgpack::Value v = blob.extract("$");
        CHECK(v.as_uint64() == big, "uint64 extract");
    }

    /* ---- Value fixed-width in mutation ---- */

    /* set with Value::int16 */
    {
        msgpack::Builder b;
        b.map_header(1).string("x").integer(0);
        msgpack::Blob blob = b.build();
        msgpack::Blob blob2 = blob.set("$.x", msgpack::Value::int16(500));
        msgpack::Value v = blob2.extract("$.x");
        CHECK(v.type() == msgpack::Type::Integer, "set int16 type");
        CHECK(v.as_int64() == 500, "set int16 val");
        /* Verify forced encoding: last 3 bytes are MP_INT16 + 2-byte value */
        size_t off = blob2.size() - 3;
        CHECK(blob2.data()[off] == 0xd1, "set int16 tag in blob");
    }

    /* set with Value::uint32 */
    {
        msgpack::Builder b;
        b.map_header(1).string("n").integer(0);
        msgpack::Blob blob = b.build();
        msgpack::Blob blob2 = blob.set("$.n", msgpack::Value::uint32(42));
        size_t off = blob2.size() - 5;
        CHECK(blob2.data()[off] == 0xce, "set uint32 tag");
        msgpack::Value v = blob2.extract("$.n");
        CHECK(v.as_int64() == 42, "set uint32 val");
    }

    /* array_insert with Value::int8 */
    {
        msgpack::Builder b;
        b.array_header(1).integer(1);
        msgpack::Blob blob = b.build();
        msgpack::Blob blob2 = blob.array_insert("$[1]", msgpack::Value::int8(-5));
        msgpack::Value v = blob2.extract("$[1]");
        CHECK(v.as_int64() == -5, "array_insert int8 val");
    }

    /* insert with Value::uint64 */
    {
        msgpack::Builder b;
        b.map_header(1).string("a").integer(1);
        msgpack::Blob blob = b.build();
        msgpack::Blob blob2 = blob.insert("$.big", msgpack::Value::uint64(999));
        msgpack::Value v = blob2.extract("$.big");
        CHECK(v.as_int64() == 999, "insert uint64 val");
        CHECK(v.int_width() == msgpack::IntWidth::Auto, "extracted int_width is Auto");
    }

    /* ---- Value int_width accessor ---- */
    {
        msgpack::Value v1 = msgpack::Value::integer(10);
        CHECK(v1.int_width() == msgpack::IntWidth::Auto, "integer auto width");

        msgpack::Value v2 = msgpack::Value::int8(10);
        CHECK(v2.int_width() == msgpack::IntWidth::Int8, "int8 width");
        CHECK(v2.as_int64() == 10, "int8 as_int64");

        msgpack::Value v3 = msgpack::Value::uint16(1000);
        CHECK(v3.int_width() == msgpack::IntWidth::Uint16, "uint16 width");
        CHECK(v3.as_int64() == 1000, "uint16 as_int64");

        msgpack::Value v4 = msgpack::Value::int64(-42);
        CHECK(v4.int_width() == msgpack::IntWidth::Int64, "int64 width");
        CHECK(v4.as_int64() == -42, "int64 as_int64");

        msgpack::Value v5 = msgpack::Value::uint64(12345);
        CHECK(v5.int_width() == msgpack::IntWidth::Uint64, "uint64 width");
        CHECK(v5.as_uint64() == 12345, "uint64 as_uint64");
    }

    /* ---- Builder in containers ---- */
    {
        msgpack::Builder b;
        b.map_header(4)
          .string("i8").int8(-1)
          .string("u16").uint16(60000)
          .string("i32").int32(-100000)
          .string("u64").uint64(0xDEADBEEFULL);
        msgpack::Blob blob = b.build();
        CHECK(blob.extract("$.i8").as_int64() == -1, "container i8");
        CHECK(blob.extract("$.u16").as_int64() == 60000, "container u16");
        CHECK(blob.extract("$.i32").as_int64() == -100000, "container i32");
        CHECK(blob.extract("$.u64").as_uint64() == 0xDEADBEEFULL, "container u64");
    }

    /* ---- Builder::value() with fixed-width Value ---- */
    {
        msgpack::Builder b;
        b.value(msgpack::Value::int16(300));
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 3, "value(int16) size");
        CHECK(blob.data()[0] == 0xd1, "value(int16) tag");
        CHECK(blob.extract("$").as_int64() == 300, "value(int16) extract");
    }
    {
        msgpack::Builder b;
        b.value(msgpack::Value::uint8(200));
        msgpack::Blob blob = b.build();
        CHECK(blob.size() == 2, "value(uint8) size");
        CHECK(blob.data()[0] == 0xcc, "value(uint8) tag");
    }

    /* ---- JSON round-trip with forced-width integers ---- */
    {
        msgpack::Builder b;
        b.map_header(1).string("x").int32(42);
        msgpack::Blob blob = b.build();
        std::string json = blob.to_json();
        CHECK(json == "{\"x\":42}", "int32 json");
    }
}

/* ── Binary and Ext extraction ────────────────────────────────────── */

static void test_binary_extraction() {
    std::printf("== binary extraction ==\n");

    /* Binary payload extracted without header bytes */
    {
        uint8_t payload[] = {0xDE, 0xAD, 0xBE, 0xEF};
        msgpack::Builder b;
        b.binary(payload, 4);
        msgpack::Blob blob = b.build();
        CHECK(blob.type() == msgpack::Type::Binary, "bin type");

        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Binary, "bin extract type");
        CHECK(v.blob_size() == 4, "bin payload size");
        CHECK(v.blob_data()[0] == 0xDE, "bin byte0");
        CHECK(v.blob_data()[1] == 0xAD, "bin byte1");
        CHECK(v.blob_data()[2] == 0xBE, "bin byte2");
        CHECK(v.blob_data()[3] == 0xEF, "bin byte3");
    }

    /* Binary inside a map */
    {
        uint8_t payload[] = {0x01, 0x02, 0x03};
        msgpack::Builder b;
        b.map_header(1).string("data").binary(payload, 3);
        msgpack::Blob blob = b.build();
        msgpack::Value v = blob.extract("$.data");
        CHECK(v.type() == msgpack::Type::Binary, "bin in map type");
        CHECK(v.blob_size() == 3, "bin in map size");
        CHECK(v.blob_data()[0] == 0x01, "bin in map byte0");
    }

    /* Empty binary */
    {
        msgpack::Builder b;
        b.binary(nullptr, 0);
        msgpack::Blob blob = b.build();
        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Binary, "empty bin type");
        CHECK(v.blob_size() == 0, "empty bin size");
    }
}

static void test_ext_extraction() {
    std::printf("== ext extraction ==\n");

    /* Ext payload extracted with type code, without header bytes */
    {
        uint8_t payload[] = {0xAA, 0xBB, 0xCC, 0xDD};
        msgpack::Builder b;
        b.ext(42, payload, 4);
        msgpack::Blob blob = b.build();
        CHECK(blob.type() == msgpack::Type::Ext, "ext type");

        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Ext, "ext extract type");
        CHECK(v.ext_type() == 42, "ext type code");
        CHECK(v.blob_size() == 4, "ext payload size");
        CHECK(v.blob_data()[0] == 0xAA, "ext byte0");
        CHECK(v.blob_data()[3] == 0xDD, "ext byte3");
    }

    /* Ext fixext1 */
    {
        uint8_t p1[] = {0xFF};
        msgpack::Builder b;
        b.ext(1, p1, 1);
        msgpack::Blob blob = b.build();
        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Ext, "fixext1 type");
        CHECK(v.ext_type() == 1, "fixext1 type code");
        CHECK(v.blob_size() == 1, "fixext1 size");
        CHECK(v.blob_data()[0] == 0xFF, "fixext1 data");
    }

    /* Ext fixext16 */
    {
        uint8_t p16[16];
        for (int j = 0; j < 16; j++) p16[j] = static_cast<uint8_t>(j);
        msgpack::Builder b;
        b.ext(99, p16, 16);
        msgpack::Blob blob = b.build();
        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Ext, "fixext16 type");
        CHECK(v.ext_type() == 99, "fixext16 type code");
        CHECK(v.blob_size() == 16, "fixext16 size");
        CHECK(v.blob_data()[15] == 15, "fixext16 last byte");
    }

    /* Ext with variable size (ext8, 3 bytes) */
    {
        uint8_t p3[] = {0x10, 0x20, 0x30};
        msgpack::Builder b;
        b.ext(-5, p3, 3);
        msgpack::Blob blob = b.build();
        msgpack::Value v = blob.extract("$");
        CHECK(v.type() == msgpack::Type::Ext, "ext8 type");
        CHECK(v.ext_type() == -5, "ext8 negative type code");
        CHECK(v.blob_size() == 3, "ext8 size");
    }

    /* Ext inside a map */
    {
        uint8_t p2[] = {0xAA, 0xBB};
        msgpack::Builder b;
        b.map_header(2)
          .string("meta").ext(10, p2, 2)
          .string("name").string("test");
        msgpack::Blob blob = b.build();
        msgpack::Value v = blob.extract("$.meta");
        CHECK(v.type() == msgpack::Type::Ext, "ext in map type");
        CHECK(v.ext_type() == 10, "ext in map type code");
        CHECK(v.blob_size() == 2, "ext in map payload size");
    }

    /* Ext round-trip: build → extract → rebuild */
    {
        uint8_t p4[] = {0x01, 0x02, 0x03, 0x04};
        msgpack::Builder b;
        b.ext(42, p4, 4);
        msgpack::Blob blob1 = b.build();

        msgpack::Value v = blob1.extract("$");
        msgpack::Builder b2;
        b2.ext(v.ext_type(), v.blob_data(), v.blob_size());
        msgpack::Blob blob2 = b2.build();

        CHECK(blob1.size() == blob2.size(), "ext round-trip size");
        CHECK(std::memcmp(blob1.data(), blob2.data(), blob1.size()) == 0, "ext round-trip bytes");
    }
}

/* ── main ─────────────────────────────────────────────────────────── */

int main() {
    test_builder_scalars();
    test_builder_containers();
    test_extraction();
    test_type_inspection();
    test_mutation_set();
    test_mutation_insert();
    test_mutation_replace();
    test_mutation_remove();
    test_mutation_array();
    test_patch();
    test_json();
    test_iterator_each();
    test_iterator_tree();
    test_builder_embed();
    test_validation();
    test_sqlite_compat();
    test_float32();
    test_ext();
    test_timestamp();
    test_value_typed_accessors();
    test_fixed_width_integers();
    test_binary_extraction();
    test_ext_extraction();

    std::printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
