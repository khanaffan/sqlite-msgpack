/*
** test_interop.cpp — Integration tests: SQLite ↔ C++ msgpack API interop.
**
** Creates blobs via SQLite SQL, parses them with the C++ API.
** Builds blobs via C++ Builder, binds them to SQLite, reads back.
** Verifies mutation round-trips in both directions.
**
** Compile with -DSQLITE_CORE and link: sqlite3_amalg + msgpack_static + msgpack_blob_static
*/
#include "msgpack_blob.hpp"
#include "sqlite3.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <functional>

/* ── Forward-declare extension entry point ──────────────────────── */
#ifdef SQLITE_CORE
extern "C" int sqlite3_msgpack_init(sqlite3 *db, char **pzErr,
                                     const sqlite3_api_routines *pApi);
#endif

/* ── Minimal test harness ──────────────────────────────────────── */
static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do {                              \
    if (cond) { g_pass++; }                                \
    else { g_fail++; std::fprintf(stderr,                  \
        "FAIL [%s:%d] %s\n", __FILE__, __LINE__, msg); }  \
} while (0)

#define SECTION(name) std::printf("== %s ==\n", name)

/* ── SQLite helpers ────────────────────────────────────────────── */

/* Execute SQL, return single text result (heap-allocated via sqlite3_mprintf). */
static char *exec_text(sqlite3 *db, const char *sql) {
    sqlite3_stmt *s = nullptr;
    char *r = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) return nullptr;
    if (sqlite3_step(s) == SQLITE_ROW) {
        auto *z = reinterpret_cast<const char *>(sqlite3_column_text(s, 0));
        if (z) r = sqlite3_mprintf("%s", z);
    }
    sqlite3_finalize(s);
    return r;
}

/* Execute SQL, return single int64 result. */
static int64_t exec_int(sqlite3 *db, const char *sql) {
    sqlite3_stmt *s = nullptr;
    int64_t v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) return -1;
    if (sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    return v;
}

/* Execute SQL, return blob result as a std::vector<uint8_t>. */
static std::vector<uint8_t> exec_blob(sqlite3 *db, const char *sql) {
    sqlite3_stmt *s = nullptr;
    std::vector<uint8_t> result;
    if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) return result;
    if (sqlite3_step(s) == SQLITE_ROW) {
        auto *p = static_cast<const uint8_t *>(sqlite3_column_blob(s, 0));
        int n = sqlite3_column_bytes(s, 0);
        if (p && n > 0) result.assign(p, p + n);
    }
    sqlite3_finalize(s);
    return result;
}

/* Execute SQL with a blob bound as ?1, return text. */
static char *exec_text_blob(sqlite3 *db, const char *sql,
                             const uint8_t *blob, size_t len) {
    sqlite3_stmt *s = nullptr;
    char *r = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) return nullptr;
    sqlite3_bind_blob(s, 1, blob, static_cast<int>(len), SQLITE_STATIC);
    if (sqlite3_step(s) == SQLITE_ROW) {
        auto *z = reinterpret_cast<const char *>(sqlite3_column_text(s, 0));
        if (z) r = sqlite3_mprintf("%s", z);
    }
    sqlite3_finalize(s);
    return r;
}

/* Execute SQL with a blob bound as ?1, return int64. */
static int64_t exec_int_blob(sqlite3 *db, const char *sql,
                              const uint8_t *blob, size_t len) {
    sqlite3_stmt *s = nullptr;
    int64_t v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) return -1;
    sqlite3_bind_blob(s, 1, blob, static_cast<int>(len), SQLITE_STATIC);
    if (sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    return v;
}

/* Execute SQL with a blob bound as ?1, return double. */
static double exec_double_blob(sqlite3 *db, const char *sql,
                                const uint8_t *blob, size_t len) {
    sqlite3_stmt *s = nullptr;
    double v = 0.0;
    if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) return 0.0;
    sqlite3_bind_blob(s, 1, blob, static_cast<int>(len), SQLITE_STATIC);
    if (sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_double(s, 0);
    sqlite3_finalize(s);
    return v;
}

/* Execute SQL with a blob bound as ?1, return blob result. */
static std::vector<uint8_t> exec_blob_blob(sqlite3 *db, const char *sql,
                                            const uint8_t *blob, size_t len) {
    sqlite3_stmt *s = nullptr;
    std::vector<uint8_t> result;
    if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) return result;
    sqlite3_bind_blob(s, 1, blob, static_cast<int>(len), SQLITE_STATIC);
    if (sqlite3_step(s) == SQLITE_ROW) {
        auto *p = static_cast<const uint8_t *>(sqlite3_column_blob(s, 0));
        int n = sqlite3_column_bytes(s, 0);
        if (p && n > 0) result.assign(p, p + n);
    }
    sqlite3_finalize(s);
    return result;
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 1: SQLite creates blobs → C++ API reads them               */
/* ────────────────────────────────────────────────────────────────── */

static void test_sqlite_to_cpp_scalars(sqlite3 *db) {
    SECTION("sqlite→cpp scalars");
    using namespace msgpack;

    /* nil */
    {
        auto bytes = exec_blob(db, "SELECT msgpack_nil()");
        CHECK(!bytes.empty(), "nil: got blob");
        Blob b(bytes.data(), bytes.size());
        CHECK(b.valid(), "nil: valid");
        CHECK(b.type() == Type::Nil, "nil: type");
        Value v = b.extract("$");
        CHECK(v.is_nil(), "nil: value");
    }

    /* booleans */
    {
        auto bt = exec_blob(db, "SELECT msgpack_true()");
        Blob tb(bt.data(), bt.size());
        CHECK(tb.type() == Type::True, "true: type");
        CHECK(tb.extract("$").as_bool() == true, "true: value");

        auto bf = exec_blob(db, "SELECT msgpack_false()");
        Blob fb(bf.data(), bf.size());
        CHECK(fb.type() == Type::False, "false: type");
        CHECK(fb.extract("$").as_bool() == false, "false: value");
    }

    /* integers */
    {
        auto b42 = exec_blob(db, "SELECT msgpack_quote(42)");
        Blob blob(b42.data(), b42.size());
        CHECK(blob.extract("$").as_int64() == 42, "int 42");

        auto bneg = exec_blob(db, "SELECT msgpack_quote(-100)");
        Blob bn(bneg.data(), bneg.size());
        CHECK(bn.extract("$").as_int64() == -100, "int -100");

        auto big = exec_blob(db, "SELECT msgpack_quote(1000000000)");
        Blob bb(big.data(), big.size());
        CHECK(bb.extract("$").as_int64() == 1000000000, "int 1e9");
    }

    /* fixed-width integers via SQL */
    {
        auto i8 = exec_blob(db, "SELECT msgpack_int8(42)");
        Blob bi8(i8.data(), i8.size());
        CHECK(bi8.extract("$").as_int64() == 42, "int8(42)");
        CHECK(i8.size() == 2, "int8: 2 bytes");

        auto i16 = exec_blob(db, "SELECT msgpack_int16(1000)");
        Blob bi16(i16.data(), i16.size());
        CHECK(bi16.extract("$").as_int64() == 1000, "int16(1000)");
        CHECK(i16.size() == 3, "int16: 3 bytes");

        auto i32 = exec_blob(db, "SELECT msgpack_int32(100000)");
        Blob bi32(i32.data(), i32.size());
        CHECK(bi32.extract("$").as_int64() == 100000, "int32(100000)");
        CHECK(i32.size() == 5, "int32: 5 bytes");

        auto u8 = exec_blob(db, "SELECT msgpack_uint8(200)");
        Blob bu8(u8.data(), u8.size());
        CHECK(bu8.extract("$").as_int64() == 200, "uint8(200)");
        CHECK(u8.size() == 2, "uint8: 2 bytes");

        auto u16 = exec_blob(db, "SELECT msgpack_uint16(50000)");
        Blob bu16(u16.data(), u16.size());
        CHECK(bu16.extract("$").as_int64() == 50000, "uint16(50000)");
        CHECK(u16.size() == 3, "uint16: 3 bytes");

        auto u32 = exec_blob(db, "SELECT msgpack_uint32(3000000000)");
        Blob bu32(u32.data(), u32.size());
        CHECK(bu32.extract("$").as_uint64() == 3000000000ULL, "uint32(3e9)");
        CHECK(u32.size() == 5, "uint32: 5 bytes");

        auto u64 = exec_blob(db, "SELECT msgpack_uint64(9000000000000000000)");
        Blob bu64(u64.data(), u64.size());
        CHECK(bu64.extract("$").as_uint64() == 9000000000000000000ULL, "uint64(9e18)");
        CHECK(u64.size() == 9, "uint64: 9 bytes");
    }

    /* float64 (double) */
    {
        auto f = exec_blob(db, "SELECT msgpack_quote(3.14)");
        Blob fb(f.data(), f.size());
        CHECK(fb.type() == Type::Real, "float64: type");
        CHECK(std::fabs(fb.extract("$").as_double() - 3.14) < 1e-12, "float64: value");
    }

    /* float32 */
    {
        auto f32 = exec_blob(db, "SELECT msgpack_float32(2.5)");
        Blob fb32(f32.data(), f32.size());
        CHECK(fb32.type() == Type::Float32, "float32: type");
        CHECK(std::fabs(fb32.extract("$").as_float() - 2.5f) < 1e-6f, "float32: value");
    }

    /* string */
    {
        auto s = exec_blob(db, "SELECT msgpack_quote('hello world')");
        Blob sb(s.data(), s.size());
        CHECK(sb.type() == Type::String, "string: type");
        CHECK(sb.extract("$").as_string() == "hello world", "string: value");
    }

    /* binary */
    {
        auto bin = exec_blob(db, "SELECT msgpack_bin(X'DEADBEEF')");
        Blob bb(bin.data(), bin.size());
        CHECK(bb.type() == Type::Binary, "binary: type");
        Value v = bb.extract("$");
        CHECK(v.blob_size() == 4, "binary: size");
        CHECK(v.blob_data()[0] == 0xDE, "binary: byte 0");
        CHECK(v.blob_data()[3] == 0xEF, "binary: byte 3");
    }

    /* ext */
    {
        auto ext = exec_blob(db, "SELECT msgpack_ext(7, X'0102030405')");
        Blob eb(ext.data(), ext.size());
        CHECK(eb.type() == Type::Ext, "ext: type");
        Value v = eb.extract("$");
        CHECK(v.ext_type() == 7, "ext: type code");
        CHECK(v.blob_size() == 5, "ext: payload size");
        CHECK(v.blob_data()[0] == 0x01, "ext: payload[0]");
    }

    /* timestamp */
    {
        auto ts = exec_blob(db, "SELECT msgpack_timestamp(1000000)");
        Blob tb(ts.data(), ts.size());
        CHECK(tb.type() == Type::Timestamp, "timestamp: type");
        Value v = tb.extract("$");
        CHECK(v.timestamp_seconds() == 1000000, "timestamp: seconds");
        CHECK(v.timestamp_nanoseconds() == 0, "timestamp: nsec=0");
    }
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 2: SQLite creates arrays & maps → C++ reads them           */
/* ────────────────────────────────────────────────────────────────── */

static void test_sqlite_to_cpp_containers(sqlite3 *db) {
    SECTION("sqlite→cpp containers");
    using namespace msgpack;

    /* simple array */
    {
        auto bytes = exec_blob(db, "SELECT msgpack_array(1, 2, 3, 'four', NULL)");
        Blob b(bytes.data(), bytes.size());
        CHECK(b.valid(), "array: valid");
        CHECK(b.type() == Type::Array, "array: type");
        CHECK(b.array_length() == 5, "array: length");
        CHECK(b.extract("$[0]").as_int64() == 1, "array[0]=1");
        CHECK(b.extract("$[1]").as_int64() == 2, "array[1]=2");
        CHECK(b.extract("$[2]").as_int64() == 3, "array[2]=3");
        CHECK(b.extract("$[3]").as_string() == "four", "array[3]='four'");
        CHECK(b.extract("$[4]").is_nil(), "array[4]=nil");
    }

    /* simple map via msgpack_object */
    {
        auto bytes = exec_blob(db,
            "SELECT msgpack_object('name','Alice','age',30,'active',1)");
        Blob b(bytes.data(), bytes.size());
        CHECK(b.valid(), "map: valid");
        CHECK(b.type() == Type::Map, "map: type");
        CHECK(b.extract("$.name").as_string() == "Alice", "map.name");
        CHECK(b.extract("$.age").as_int64() == 30, "map.age");
        CHECK(b.extract("$.active").as_int64() == 1, "map.active");
    }

    /* nested: map with array value */
    {
        auto bytes = exec_blob(db,
            "SELECT msgpack_set("
            "  msgpack_object('name','Bob'),"
            "  '$.scores',"
            "  msgpack_array(95, 87, 92)"
            ")");
        Blob b(bytes.data(), bytes.size());
        CHECK(b.valid(), "nested: valid");
        CHECK(b.extract("$.name").as_string() == "Bob", "nested.name");
        CHECK(b.type("$.scores") == Type::Array, "nested.scores: array");
        CHECK(b.array_length("$.scores") == 3, "nested.scores: len=3");
        CHECK(b.extract("$.scores[0]").as_int64() == 95, "scores[0]=95");
        CHECK(b.extract("$.scores[1]").as_int64() == 87, "scores[1]=87");
        CHECK(b.extract("$.scores[2]").as_int64() == 92, "scores[2]=92");
    }

    /* nested: array of maps */
    {
        auto bytes = exec_blob(db,
            "SELECT msgpack_array("
            "  msgpack_object('id',1,'val','a'),"
            "  msgpack_object('id',2,'val','b'),"
            "  msgpack_object('id',3,'val','c')"
            ")");
        Blob b(bytes.data(), bytes.size());
        CHECK(b.type() == Type::Array, "arr-of-maps: type");
        CHECK(b.array_length() == 3, "arr-of-maps: len");
        CHECK(b.extract("$[0].id").as_int64() == 1, "[0].id=1");
        CHECK(b.extract("$[1].val").as_string() == "b", "[1].val='b'");
        CHECK(b.extract("$[2].id").as_int64() == 3, "[2].id=3");
    }

    /* deeply nested */
    {
        auto bytes = exec_blob(db,
            "SELECT msgpack_object("
            "  'a', msgpack_object("
            "    'b', msgpack_object("
            "      'c', msgpack_array(10, 20, 30)"
            "    )"
            "  )"
            ")");
        Blob b(bytes.data(), bytes.size());
        CHECK(b.extract("$.a.b.c[0]").as_int64() == 10, "deep: a.b.c[0]=10");
        CHECK(b.extract("$.a.b.c[2]").as_int64() == 30, "deep: a.b.c[2]=30");
    }

    /* iterate array via C++ Iterator */
    {
        auto bytes = exec_blob(db, "SELECT msgpack_array(10, 20, 30)");
        Blob b(bytes.data(), bytes.size());
        Iterator it(b, "$", false);
        int sum = 0;
        int count = 0;
        while (it.next()) {
            sum += static_cast<int>(it.current().value.as_int64());
            count++;
        }
        CHECK(count == 3, "iter: count=3");
        CHECK(sum == 60, "iter: sum=60");
    }

    /* iterate map via C++ Iterator */
    {
        auto bytes = exec_blob(db,
            "SELECT msgpack_object('x',1,'y',2,'z',3)");
        Blob b(bytes.data(), bytes.size());
        Iterator it(b, "$", false);
        int count = 0;
        while (it.next()) {
            count++;
        }
        CHECK(count == 3, "iter map: count=3");
    }

    /* recursive tree walk */
    {
        auto bytes = exec_blob(db,
            "SELECT msgpack_object('a', msgpack_array(1, 2))");
        Blob b(bytes.data(), bytes.size());
        Iterator it(b, "$", true);
        int count = 0;
        while (it.next()) count++;
        /* Should yield: "a" key → [1,2] (array), [0]→1, [1]→2 = at least 3 */
        CHECK(count >= 3, "tree: count>=3");
    }
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 3: SQLite creates mixed-type blob → C++ reads all types    */
/* ────────────────────────────────────────────────────────────────── */

static void test_sqlite_to_cpp_all_types(sqlite3 *db) {
    SECTION("sqlite→cpp all types in one blob");
    using namespace msgpack;

    /* Build a map with every type via SQL */
    auto bytes = exec_blob(db,
        "SELECT msgpack_set("
        "  msgpack_set("
        "  msgpack_set("
        "  msgpack_set("
        "  msgpack_set("
        "  msgpack_set("
        "  msgpack_set("
        "  msgpack_set("
        "  msgpack_set("
        "    msgpack_object('nil_val', NULL, 'bool_t', 1, 'bool_f', 0),"
        "    '$.int_val', 42),"
        "    '$.neg_int', -999),"
        "    '$.float64', 3.14159),"
        "    '$.str', 'hello'),"
        "    '$.arr', msgpack_array(1, 'two', NULL)),"
        "    '$.nested', msgpack_object('x', 10)),"
        "    '$.float32', msgpack_float32(1.5)),"
        "    '$.ts', msgpack_timestamp(1700000000)),"
        "    '$.bin', msgpack_bin(X'CAFE'))");

    Blob b(bytes.data(), bytes.size());
    CHECK(b.valid(), "alltype: valid");
    CHECK(b.type() == Type::Map, "alltype: root is map");

    CHECK(b.extract("$.nil_val").is_nil(), "alltype: nil");
    /* SQLite has no boolean type — 1/0 are integers, not msgpack bools */
    CHECK(b.extract("$.bool_t").as_int64() == 1, "alltype: true (int)");
    CHECK(b.extract("$.bool_f").as_int64() == 0, "alltype: false (int)");
    CHECK(b.extract("$.int_val").as_int64() == 42, "alltype: int");
    CHECK(b.extract("$.neg_int").as_int64() == -999, "alltype: neg int");
    CHECK(std::fabs(b.extract("$.float64").as_double() - 3.14159) < 1e-10,
          "alltype: float64");
    CHECK(b.extract("$.str").as_string() == "hello", "alltype: string");
    CHECK(b.type("$.arr") == Type::Array, "alltype: array type");
    CHECK(b.extract("$.arr[0]").as_int64() == 1, "alltype: arr[0]");
    CHECK(b.extract("$.arr[1]").as_string() == "two", "alltype: arr[1]");
    CHECK(b.extract("$.arr[2]").is_nil(), "alltype: arr[2] nil");
    CHECK(b.extract("$.nested.x").as_int64() == 10, "alltype: nested.x");
    CHECK(b.type("$.float32") == Type::Float32, "alltype: float32 type");
    CHECK(std::fabs(b.extract("$.float32").as_float() - 1.5f) < 1e-6f,
          "alltype: float32 val");
    CHECK(b.type("$.ts") == Type::Timestamp, "alltype: ts type");
    CHECK(b.extract("$.ts").timestamp_seconds() == 1700000000,
          "alltype: ts seconds");
    CHECK(b.type("$.bin") == Type::Binary, "alltype: bin type");
    CHECK(b.extract("$.bin").blob_size() == 2, "alltype: bin size");
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 4: C++ builds blobs → SQLite reads them back               */
/* ────────────────────────────────────────────────────────────────── */

static void test_cpp_to_sqlite_scalars(sqlite3 *db) {
    SECTION("cpp→sqlite scalars");
    using namespace msgpack;
    char *r;

    /* nil */
    {
        Blob b = Builder::quote(Value::nil());
        r = exec_text_blob(db, "SELECT msgpack_type(?1)", b.data(), b.size());
        CHECK(r && std::strcmp(r, "null") == 0, "cpp nil → sqlite type");
        sqlite3_free(r);
    }

    /* boolean true */
    {
        Blob b = Builder::quote(Value::boolean(true));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$')",
                            b.data(), b.size()) == 1, "cpp true → sqlite");
    }

    /* boolean false */
    {
        Blob b = Builder::quote(Value::boolean(false));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$')",
                            b.data(), b.size()) == 0, "cpp false → sqlite");
    }

    /* integer */
    {
        Blob b = Builder::quote(Value::integer(12345));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$')",
                            b.data(), b.size()) == 12345, "cpp int → sqlite");
    }

    /* negative integer */
    {
        Blob b = Builder::quote(Value::integer(-500));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$')",
                            b.data(), b.size()) == -500, "cpp neg int → sqlite");
    }

    /* fixed-width integers */
    {
        Blob bi8 = Builder::quote(Value::int8(42));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1,'$')",
                            bi8.data(), bi8.size()) == 42, "cpp int8 → sqlite");
        CHECK(exec_int_blob(db, "SELECT msgpack_valid(?1)",
                            bi8.data(), bi8.size()) == 1, "cpp int8: valid");

        Blob bu16 = Builder::quote(Value::uint16(60000));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1,'$')",
                            bu16.data(), bu16.size()) == 60000, "cpp uint16 → sqlite");

        Blob bi32 = Builder::quote(Value::int32(-100000));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1,'$')",
                            bi32.data(), bi32.size()) == -100000, "cpp int32 → sqlite");

        Blob bu64 = Builder::quote(Value::uint64(9000000000000000000ULL));
        r = exec_text_blob(db, "SELECT msgpack_extract(?1,'$')",
                           bu64.data(), bu64.size());
        CHECK(r && std::strcmp(r, "9000000000000000000") == 0, "cpp uint64 → sqlite");
        sqlite3_free(r);
    }

    /* float64 */
    {
        Blob b = Builder::quote(Value::real(2.718281828));
        double v = exec_double_blob(db, "SELECT msgpack_extract(?1, '$')",
                                    b.data(), b.size());
        CHECK(std::fabs(v - 2.718281828) < 1e-9, "cpp float64 → sqlite");
    }

    /* float32 */
    {
        Blob b = Builder::quote(Value::real32(1.25f));
        r = exec_text_blob(db, "SELECT msgpack_type(?1)", b.data(), b.size());
        CHECK(r && std::strcmp(r, "real") == 0, "cpp float32: type");
        sqlite3_free(r);
        double v = exec_double_blob(db, "SELECT msgpack_extract(?1, '$')",
                                    b.data(), b.size());
        CHECK(std::fabs(v - 1.25) < 1e-6, "cpp float32 → sqlite value");
    }

    /* string */
    {
        Blob b = Builder::quote(Value::string("hello from cpp"));
        r = exec_text_blob(db, "SELECT msgpack_extract(?1, '$')",
                           b.data(), b.size());
        CHECK(r && std::strcmp(r, "hello from cpp") == 0, "cpp string → sqlite");
        sqlite3_free(r);
    }

    /* binary */
    {
        uint8_t payload[] = {0xDE, 0xAD, 0xBE, 0xEF};
        Blob b = Builder::quote(Value::binary(payload, 4));
        CHECK(exec_int_blob(db, "SELECT msgpack_valid(?1)",
                            b.data(), b.size()) == 1, "cpp binary: valid");
        r = exec_text_blob(db, "SELECT msgpack_type(?1)", b.data(), b.size());
        CHECK(r && std::strcmp(r, "blob") == 0, "cpp binary: type=blob");
        sqlite3_free(r);
    }

    /* ext */
    {
        uint8_t payload[] = {0x01, 0x02, 0x03};
        Blob b = Builder::quote(Value::ext(5, payload, 3));
        CHECK(exec_int_blob(db, "SELECT msgpack_valid(?1)",
                            b.data(), b.size()) == 1, "cpp ext: valid");
    }

    /* timestamp */
    {
        Builder tb;
        tb.timestamp(1700000000);
        Blob b = tb.build();
        CHECK(exec_int_blob(db, "SELECT msgpack_valid(?1)",
                            b.data(), b.size()) == 1, "cpp timestamp: valid");
        r = exec_text_blob(db, "SELECT msgpack_type(?1)", b.data(), b.size());
        CHECK(r && std::strcmp(r, "timestamp") == 0, "cpp timestamp: type");
        sqlite3_free(r);
        CHECK(exec_int_blob(db, "SELECT msgpack_timestamp_s(?1)",
                            b.data(), b.size()) == 1700000000,
              "cpp timestamp: seconds via sql");
    }

    /* timestamp with nanoseconds */
    {
        Builder tb;
        tb.timestamp(1000, 500000000);
        Blob b = tb.build();
        CHECK(exec_int_blob(db, "SELECT msgpack_timestamp_s(?1)",
                            b.data(), b.size()) == 1000,
              "cpp ts+nsec: seconds");
        CHECK(exec_int_blob(db, "SELECT msgpack_timestamp_ns(?1)",
                            b.data(), b.size()) == 500000000,
              "cpp ts+nsec: nanoseconds");
    }
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 5: C++ builds containers → SQLite reads them               */
/* ────────────────────────────────────────────────────────────────── */

static void test_cpp_to_sqlite_containers(sqlite3 *db) {
    SECTION("cpp→sqlite containers");
    using namespace msgpack;
    char *r;

    /* simple array */
    {
        Builder b;
        b.array_header(4).integer(10).string("twenty").boolean(true).nil();
        Blob blob = b.build();

        CHECK(exec_int_blob(db, "SELECT msgpack_valid(?1)",
                            blob.data(), blob.size()) == 1, "cpp arr: valid");
        CHECK(exec_int_blob(db, "SELECT msgpack_array_length(?1)",
                            blob.data(), blob.size()) == 4, "cpp arr: len=4");
        r = exec_text_blob(db, "SELECT msgpack_type(?1)", blob.data(), blob.size());
        CHECK(r && std::strcmp(r, "array") == 0, "cpp arr: type");
        sqlite3_free(r);

        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$[0]')",
                            blob.data(), blob.size()) == 10, "cpp arr[0]=10");
        r = exec_text_blob(db, "SELECT msgpack_extract(?1, '$[1]')",
                           blob.data(), blob.size());
        CHECK(r && std::strcmp(r, "twenty") == 0, "cpp arr[1]='twenty'");
        sqlite3_free(r);
    }

    /* simple map */
    {
        Builder b;
        b.map_header(3)
         .string("name").string("Alice")
         .string("age").integer(30)
         .string("active").boolean(true);
        Blob blob = b.build();

        CHECK(exec_int_blob(db, "SELECT msgpack_valid(?1)",
                            blob.data(), blob.size()) == 1, "cpp map: valid");
        r = exec_text_blob(db, "SELECT msgpack_extract(?1, '$.name')",
                           blob.data(), blob.size());
        CHECK(r && std::strcmp(r, "Alice") == 0, "cpp map.name");
        sqlite3_free(r);
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.age')",
                            blob.data(), blob.size()) == 30, "cpp map.age=30");
    }

    /* nested: map with array */
    {
        Builder inner;
        inner.array_header(3).integer(95).integer(87).integer(92);
        Blob inner_blob = inner.build();

        Builder b;
        b.map_header(2)
         .string("name").string("Bob")
         .string("scores").raw(inner_blob);
        Blob blob = b.build();

        CHECK(exec_int_blob(db, "SELECT msgpack_valid(?1)",
                            blob.data(), blob.size()) == 1, "cpp nested: valid");
        r = exec_text_blob(db, "SELECT msgpack_extract(?1, '$.name')",
                           blob.data(), blob.size());
        CHECK(r && std::strcmp(r, "Bob") == 0, "cpp nested.name");
        sqlite3_free(r);
        CHECK(exec_int_blob(db, "SELECT msgpack_array_length(?1, '$.scores')",
                            blob.data(), blob.size()) == 3, "cpp nested scores len");
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.scores[1]')",
                            blob.data(), blob.size()) == 87, "cpp scores[1]=87");
    }

    /* array of maps */
    {
        Builder b;
        b.array_header(2);
        b.map_header(2).string("id").integer(1).string("v").string("a");
        b.map_header(2).string("id").integer(2).string("v").string("b");
        Blob blob = b.build();

        CHECK(exec_int_blob(db, "SELECT msgpack_array_length(?1)",
                            blob.data(), blob.size()) == 2, "cpp arr-of-maps: len");
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$[0].id')",
                            blob.data(), blob.size()) == 1, "cpp [0].id=1");
        r = exec_text_blob(db, "SELECT msgpack_extract(?1, '$[1].v')",
                           blob.data(), blob.size());
        CHECK(r && std::strcmp(r, "b") == 0, "cpp [1].v='b'");
        sqlite3_free(r);
    }

    /* deep nesting */
    {
        Builder b;
        b.map_header(1).string("a");
        b.map_header(1).string("b");
        b.map_header(1).string("c");
        b.array_header(3).integer(10).integer(20).integer(30);
        Blob blob = b.build();

        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.a.b.c[0]')",
                            blob.data(), blob.size()) == 10, "cpp deep a.b.c[0]=10");
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.a.b.c[2]')",
                            blob.data(), blob.size()) == 30, "cpp deep a.b.c[2]=30");
    }
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 6: Byte-identical round-trip (SQL builds, C++ matches hex) */
/* ────────────────────────────────────────────────────────────────── */

static void test_byte_identical(sqlite3 *db) {
    SECTION("byte-identical encoding");
    using namespace msgpack;

    /* Compare hex output of SQL-built vs C++-built blobs */
    auto compare = [&](const char *sql, Blob cpp_blob, const char *label) {
        char *sql_hex = exec_text(db, sql);
        /* Get hex of C++ blob */
        char *cpp_hex = exec_text_blob(db, "SELECT hex(?1)",
                                       cpp_blob.data(), cpp_blob.size());
        bool match = (sql_hex && cpp_hex && std::strcmp(sql_hex, cpp_hex) == 0);
        CHECK(match, label);
        if (!match && sql_hex && cpp_hex) {
            std::fprintf(stderr, "  SQL: %s\n  CPP: %s\n", sql_hex, cpp_hex);
        }
        sqlite3_free(sql_hex);
        sqlite3_free(cpp_hex);
    };

    /* nil */
    compare("SELECT hex(msgpack_nil())", Builder::quote(Value::nil()), "ident: nil");

    /* true / false */
    compare("SELECT hex(msgpack_true())",
            Builder::quote(Value::boolean(true)), "ident: true");
    compare("SELECT hex(msgpack_false())",
            Builder::quote(Value::boolean(false)), "ident: false");

    /* small int (fixint) */
    compare("SELECT hex(msgpack_quote(42))",
            Builder::quote(Value::integer(42)), "ident: fixint 42");

    /* negative fixint */
    compare("SELECT hex(msgpack_quote(-5))",
            Builder::quote(Value::integer(-5)), "ident: neg fixint -5");

    /* int8 forced */
    compare("SELECT hex(msgpack_int8(42))",
            Builder::quote(Value::int8(42)), "ident: int8(42)");

    /* uint16 forced */
    compare("SELECT hex(msgpack_uint16(1000))",
            Builder::quote(Value::uint16(1000)), "ident: uint16(1000)");

    /* int32 forced */
    compare("SELECT hex(msgpack_int32(-100000))",
            Builder::quote(Value::int32(-100000)), "ident: int32(-100000)");

    /* float32 */
    compare("SELECT hex(msgpack_float32(2.5))",
            Builder::quote(Value::real32(2.5f)), "ident: float32(2.5)");

    /* float64 */
    compare("SELECT hex(msgpack_quote(3.14))",
            Builder::quote(Value::real(3.14)), "ident: float64(3.14)");

    /* string */
    compare("SELECT hex(msgpack_quote('hello'))",
            Builder::quote(Value::string("hello")), "ident: str 'hello'");

    /* timestamp */
    {
        Builder tb; tb.timestamp(1000000);
        compare("SELECT hex(msgpack_timestamp(1000000))",
                tb.build(), "ident: timestamp(1e6)");
    }

    /* simple array */
    {
        Builder b;
        b.array_header(3).integer(1).integer(2).integer(3);
        compare("SELECT hex(msgpack_array(1,2,3))", b.build(), "ident: array [1,2,3]");
    }

    /* simple map */
    {
        Builder b;
        b.map_header(2)
         .string("a").integer(1)
         .string("b").integer(2);
        compare("SELECT hex(msgpack_object('a',1,'b',2))",
                b.build(), "ident: map {a:1,b:2}");
    }
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 7: C++ mutates blob → SQLite confirms; SQL mutates → C++   */
/* ────────────────────────────────────────────────────────────────── */

static void test_mutation_roundtrip(sqlite3 *db) {
    SECTION("mutation round-trip");
    using namespace msgpack;

    /* Build a starting blob in C++ */
    Builder b;
    b.map_header(3)
     .string("name").string("Alice")
     .string("age").integer(30)
     .string("scores").array_header(3).integer(90).integer(85).integer(95);
    Blob original = b.build();

    /* C++ set: change age to 31 */
    {
        Blob m = original.set("$.age", Value::integer(31));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.age')",
                            m.data(), m.size()) == 31, "cpp set age=31 → sql");
        /* name untouched */
        char *r = exec_text_blob(db, "SELECT msgpack_extract(?1, '$.name')",
                                 m.data(), m.size());
        CHECK(r && std::strcmp(r, "Alice") == 0, "cpp set: name untouched");
        sqlite3_free(r);
    }

    /* C++ insert: add email */
    {
        Blob m = original.insert("$.email", Value::string("alice@test.com"));
        char *r = exec_text_blob(db, "SELECT msgpack_extract(?1, '$.email')",
                                 m.data(), m.size());
        CHECK(r && std::strcmp(r, "alice@test.com") == 0, "cpp insert email → sql");
        sqlite3_free(r);
    }

    /* C++ remove: remove scores */
    {
        Blob m = original.remove("$.scores");
        char *r = exec_text_blob(db, "SELECT msgpack_type(?1, '$.scores')",
                                 m.data(), m.size());
        /* After removal, type should be null (key doesn't exist) */
        CHECK(r == nullptr || std::strcmp(r, "null") == 0, "cpp remove scores → sql");
        sqlite3_free(r);
    }

    /* C++ replace: change scores[1] */
    {
        Blob m = original.replace("$.scores[1]", Value::integer(99));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.scores[1]')",
                            m.data(), m.size()) == 99, "cpp replace scores[1]=99");
    }

    /* C++ array_insert: insert at scores[0] */
    {
        Blob m = original.array_insert("$.scores[0]", Value::integer(100));
        CHECK(exec_int_blob(db, "SELECT msgpack_array_length(?1, '$.scores')",
                            m.data(), m.size()) == 4, "cpp array_insert: len=4");
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.scores[0]')",
                            m.data(), m.size()) == 100, "cpp array_insert: [0]=100");
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.scores[1]')",
                            m.data(), m.size()) == 90, "cpp array_insert: [1]=90");
    }

    /* SQL mutates → C++ reads back */
    {
        auto mutated = exec_blob_blob(db,
            "SELECT msgpack_set(?1, '$.age', 35)",
            original.data(), original.size());
        CHECK(!mutated.empty(), "sql set → got blob");
        Blob m(mutated.data(), mutated.size());
        CHECK(m.extract("$.age").as_int64() == 35, "sql set age=35 → cpp");
        CHECK(m.extract("$.name").as_string() == "Alice", "sql set: name ok");
    }

    {
        auto mutated = exec_blob_blob(db,
            "SELECT msgpack_remove(?1, '$.scores')",
            original.data(), original.size());
        Blob m(mutated.data(), mutated.size());
        CHECK(m.extract("$.scores").is_nil(), "sql remove scores → cpp nil");
    }

    /* C++ patch */
    {
        Builder patch_b;
        patch_b.map_header(2)
               .string("age").integer(32)
               .string("city").string("NYC");
        Blob patch_blob = patch_b.build();
        Blob patched = original.patch(patch_blob);

        CHECK(patched.extract("$.age").as_int64() == 32, "cpp patch: age=32");
        CHECK(patched.extract("$.city").as_string() == "NYC", "cpp patch: city=NYC");
        CHECK(patched.extract("$.name").as_string() == "Alice", "cpp patch: name kept");

        /* Verify via SQL too */
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.age')",
                            patched.data(), patched.size()) == 32,
              "cpp patch → sql: age=32");
    }

    /* C++ set with typed values */
    {
        Blob m = original.set("$.age", Value::real32(30.5f));
        char *r = exec_text_blob(db, "SELECT msgpack_type(?1, '$.age')",
                                 m.data(), m.size());
        CHECK(r && std::strcmp(r, "real") == 0, "cpp set float32: type=real");
        sqlite3_free(r);
    }

    {
        Blob m = original.set("$.age", Value::int16(31));
        CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.age')",
                            m.data(), m.size()) == 31, "cpp set int16(31) → sql");
    }
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 8: JSON round-trip: SQL → C++ JSON → SQL → C++ verify      */
/* ────────────────────────────────────────────────────────────────── */

static void test_json_roundtrip(sqlite3 *db) {
    SECTION("JSON round-trip");
    using namespace msgpack;

    /* Build in SQL, convert to JSON in C++, parse JSON back in C++ */
    {
        auto bytes = exec_blob(db,
            "SELECT msgpack_object('name','Alice','scores',msgpack_array(1,2,3))");
        Blob original(bytes.data(), bytes.size());
        std::string json = original.to_json();
        Blob from_json = Blob::from_json(json);

        CHECK(from_json.valid(), "json rt: valid");
        CHECK(from_json.extract("$.name").as_string() == "Alice", "json rt: name");
        CHECK(from_json.extract("$.scores[0]").as_int64() == 1, "json rt: scores[0]");
        CHECK(from_json.extract("$.scores[2]").as_int64() == 3, "json rt: scores[2]");
    }

    /* Build in C++, JSON via SQL, re-parse in C++ */
    {
        Builder b;
        b.map_header(2).string("x").integer(10).string("y").string("hello");
        Blob blob = b.build();

        char *sql_json = exec_text_blob(db, "SELECT msgpack_to_json(?1)",
                                        blob.data(), blob.size());
        CHECK(sql_json != nullptr, "json rt2: got json from sql");

        if (sql_json) {
            Blob rt = Blob::from_json(sql_json);
            CHECK(rt.extract("$.x").as_int64() == 10, "json rt2: x=10");
            CHECK(rt.extract("$.y").as_string() == "hello", "json rt2: y=hello");
            sqlite3_free(sql_json);
        }
    }

    /* C++ JSON → SQL msgpack_from_json → C++ read */
    {
        const char *json = R"({"a":[1,2,3],"b":"test","c":null})";
        Blob cpp_blob = Blob::from_json(json);
        CHECK(cpp_blob.valid(), "json rt3: cpp from_json valid");

        auto sql_blob = exec_blob_blob(db,
            "SELECT msgpack_from_json(?1)",
            reinterpret_cast<const uint8_t *>(json), std::strlen(json));
        /* Bind json as text, not blob */
        sqlite3_stmt *s = nullptr;
        sqlite3_prepare_v2(db, "SELECT msgpack_from_json(?1)", -1, &s, nullptr);
        sqlite3_bind_text(s, 1, json, -1, SQLITE_STATIC);
        std::vector<uint8_t> sql_result;
        if (sqlite3_step(s) == SQLITE_ROW) {
            auto *p = static_cast<const uint8_t *>(sqlite3_column_blob(s, 0));
            int n = sqlite3_column_bytes(s, 0);
            if (p && n > 0) sql_result.assign(p, p + n);
        }
        sqlite3_finalize(s);

        if (!sql_result.empty()) {
            Blob sql_blob2(sql_result.data(), sql_result.size());
            CHECK(sql_blob2.extract("$.a[0]").as_int64() == 1, "json rt3: sql a[0]=1");
            CHECK(sql_blob2.extract("$.b").as_string() == "test", "json rt3: sql b=test");
            CHECK(sql_blob2.extract("$.c").is_nil(), "json rt3: sql c=nil");
        }

        CHECK(cpp_blob.extract("$.a[0]").as_int64() == 1, "json rt3: cpp a[0]=1");
        CHECK(cpp_blob.extract("$.b").as_string() == "test", "json rt3: cpp b=test");
        CHECK(cpp_blob.extract("$.c").is_nil(), "json rt3: cpp c=nil");
    }
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 9: Chained mutations: build → mutate × N → verify          */
/* ────────────────────────────────────────────────────────────────── */

static void test_chained_mutations(sqlite3 *db) {
    SECTION("chained mutations");
    using namespace msgpack;

    /* Start with empty map */
    Builder b;
    b.map_header(0);
    Blob blob = b.build();

    /* Chain mutations in C++ */
    blob = blob.set("$.name", Value::string("Alice"));
    blob = blob.set("$.age", Value::integer(30));
    blob = blob.set("$.scores", Value::nil()); /* placeholder */

    /* Replace scores with a real array built by Builder */
    Builder arr;
    arr.array_header(3).integer(90).integer(85).integer(95);
    blob = blob.set("$.scores", arr.build());

    /* Add nested object */
    Builder addr;
    addr.map_header(2)
        .string("city").string("NYC")
        .string("zip").integer(10001);
    blob = blob.set("$.address", addr.build());

    /* Set typed values */
    blob = blob.set("$.rating", Value::real32(4.5f));
    blob = blob.set("$.id", Value::uint32(1000000));

    /* Verify in C++ */
    CHECK(blob.valid(), "chain: valid");
    CHECK(blob.extract("$.name").as_string() == "Alice", "chain: name");
    CHECK(blob.extract("$.age").as_int64() == 30, "chain: age");
    CHECK(blob.extract("$.scores[0]").as_int64() == 90, "chain: scores[0]");
    CHECK(blob.extract("$.scores[2]").as_int64() == 95, "chain: scores[2]");
    CHECK(blob.extract("$.address.city").as_string() == "NYC", "chain: addr.city");
    CHECK(blob.extract("$.address.zip").as_int64() == 10001, "chain: addr.zip");
    CHECK(std::fabs(blob.extract("$.rating").as_float() - 4.5f) < 1e-6f,
          "chain: rating");
    CHECK(blob.extract("$.id").as_uint64() == 1000000, "chain: id");

    /* Verify via SQLite */
    char *r = exec_text_blob(db, "SELECT msgpack_to_json(?1)",
                             blob.data(), blob.size());
    CHECK(r != nullptr, "chain: sql to_json not null");
    sqlite3_free(r);

    CHECK(exec_int_blob(db, "SELECT msgpack_valid(?1)",
                        blob.data(), blob.size()) == 1, "chain: sql valid");
    r = exec_text_blob(db, "SELECT msgpack_extract(?1, '$.name')",
                       blob.data(), blob.size());
    CHECK(r && std::strcmp(r, "Alice") == 0, "chain → sql: name");
    sqlite3_free(r);
    CHECK(exec_int_blob(db, "SELECT msgpack_extract(?1, '$.address.zip')",
                        blob.data(), blob.size()) == 10001, "chain → sql: zip");

    /* Now mutate in SQL, read back in C++ */
    auto mutated = exec_blob_blob(db,
        "SELECT msgpack_set(msgpack_set(?1, '$.age', 31), '$.active', 1)",
        blob.data(), blob.size());
    Blob m(mutated.data(), mutated.size());
    CHECK(m.extract("$.age").as_int64() == 31, "chain sql mut: age=31");
    CHECK(m.extract("$.active").as_int64() == 1, "chain sql mut: active=1");
    CHECK(m.extract("$.name").as_string() == "Alice", "chain sql mut: name kept");
}

/* ────────────────────────────────────────────────────────────────── */
/*  TEST 10: Virtual table ↔ Iterator cross-check                   */
/* ────────────────────────────────────────────────────────────────── */

static void test_vtab_vs_iterator(sqlite3 *db) {
    SECTION("vtab vs iterator");
    using namespace msgpack;

    /* Build a blob in C++ */
    Builder b;
    b.map_header(3)
     .string("a").integer(1)
     .string("b").string("two")
     .string("c").boolean(true);
    Blob blob = b.build();

    /* Count rows via SQL msgpack_each */
    sqlite3_stmt *s = nullptr;
    sqlite3_prepare_v2(db, "SELECT count(*) FROM msgpack_each(?1)", -1, &s, nullptr);
    sqlite3_bind_blob(s, 1, blob.data(), static_cast<int>(blob.size()), SQLITE_STATIC);
    int sql_count = 0;
    if (sqlite3_step(s) == SQLITE_ROW) sql_count = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);

    /* Count via C++ Iterator */
    Iterator it(blob, "$", false);
    int cpp_count = 0;
    while (it.next()) cpp_count++;

    CHECK(sql_count == cpp_count, "vtab vs iter: same count");
    CHECK(cpp_count == 3, "vtab vs iter: count=3");

    /* Compare recursive tree walk counts */
    sqlite3_prepare_v2(db, "SELECT count(*) FROM msgpack_tree(?1)", -1, &s, nullptr);
    sqlite3_bind_blob(s, 1, blob.data(), static_cast<int>(blob.size()), SQLITE_STATIC);
    int sql_tree = 0;
    if (sqlite3_step(s) == SQLITE_ROW) sql_tree = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);

    Iterator tree_it(blob, "$", true);
    int cpp_tree = 0;
    while (tree_it.next()) cpp_tree++;

    CHECK(sql_tree == cpp_tree, "vtab vs tree iter: same count");
}

/* ────────────────────────────────────────────────────────────────── */
/*  main                                                            */
/* ────────────────────────────────────────────────────────────────── */

int main() {
    sqlite3 *db = nullptr;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        std::fprintf(stderr, "Failed to open database\n");
        return 1;
    }

#ifdef SQLITE_CORE
    {
        char *zErr = nullptr;
        if (sqlite3_msgpack_init(db, &zErr, nullptr) != SQLITE_OK) {
            std::fprintf(stderr, "Failed to init msgpack: %s\n",
                         zErr ? zErr : "?");
            sqlite3_free(zErr);
            sqlite3_close(db);
            return 1;
        }
    }
#else
    std::fprintf(stderr, "Recompile with -DSQLITE_CORE\n");
    sqlite3_close(db);
    return 1;
#endif

    test_sqlite_to_cpp_scalars(db);
    test_sqlite_to_cpp_containers(db);
    test_sqlite_to_cpp_all_types(db);
    test_cpp_to_sqlite_scalars(db);
    test_cpp_to_sqlite_containers(db);
    test_byte_identical(db);
    test_mutation_roundtrip(db);
    test_json_roundtrip(db);
    test_chained_mutations(db);
    test_vtab_vs_iterator(db);

    sqlite3_close(db);

    std::printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
