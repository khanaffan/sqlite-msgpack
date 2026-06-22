/*
** gen_blob_vectors.cpp — generate cross-language test vectors
**
** Uses the standalone C++ MsgPack Blob library (the reference implementation)
** to emit a language-neutral JSON file of test vectors. The Python and JS/TS
** ports replay these vectors to prove they produce byte-identical output.
**
** Build:  linked against msgpack_blob_static (see CMakeLists target
**         `blob_vectors_gen`).
** Run:    ./blob_vectors_gen > tests/vectors/blob_vectors.json
**
** A "ValueSpec" is a small JSON object the ports interpret to build a Value:
**   {"k":"nil"} {"k":"bool","v":true}
**   {"k":"int","v":"42"}  {"k":"uint","v":"...."}        (compact)
**   {"k":"int8","v":"-5"} ... {"k":"uint64","v":"...."}  (fixed width)
**   {"k":"real","v":1.5}  {"k":"real32","v":1.5}
**   {"k":"str","v":"hi"}  {"k":"binary","hex":"deadbeef"}
**   {"k":"ext","type":42,"hex":"0102"}  {"k":"timestamp","sec":"..","nsec":0}
**   {"k":"blob","json":"[1,2,3]"}        (sub-blob via from_json)
** Integers are decimal strings so 64-bit values survive JSON without loss.
*/

#include "msgpack_blob.hpp"

#include <cstdint>
#include <cstdio>
#include <sstream>
#include <string>
#include <vector>

using namespace msgpack;

/* ── tiny JSON writer ─────────────────────────────────────────────── */

static std::string jesc(const std::string& s) {
    std::string o;
    o.reserve(s.size() + 2);
    for (unsigned char c : s) {
        switch (c) {
            case '"':  o += "\\\""; break;
            case '\\': o += "\\\\"; break;
            case '\n': o += "\\n";  break;
            case '\r': o += "\\r";  break;
            case '\t': o += "\\t";  break;
            case '\b': o += "\\b";  break;
            case '\f': o += "\\f";  break;
            default:
                if (c < 0x20) { char b[8]; std::snprintf(b, 8, "\\u%04x", c); o += b; }
                else o += static_cast<char>(c);
        }
    }
    return o;
}
static std::string q(const std::string& s) { return "\"" + jesc(s) + "\""; }

static std::string to_hex(const Blob& b) {
    static const char* hx = "0123456789abcdef";
    std::string o;
    const uint8_t* d = b.data();
    for (size_t i = 0; i < b.size(); i++) { o += hx[d[i] >> 4]; o += hx[d[i] & 0xf]; }
    return o;
}
static std::vector<uint8_t> from_hex(const std::string& h) {
    std::vector<uint8_t> o;
    auto nib = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return 0;
    };
    for (size_t i = 0; i + 1 < h.size(); i += 2)
        o.push_back(static_cast<uint8_t>((nib(h[i]) << 4) | nib(h[i + 1])));
    return o;
}

/* ── object accumulation ──────────────────────────────────────────── */

struct Obj {
    std::vector<std::string> kv;
    Obj& s(const std::string& k, const std::string& v) { kv.push_back(q(k) + ":" + q(v)); return *this; }
    Obj& raw(const std::string& k, const std::string& v) { kv.push_back(q(k) + ":" + v); return *this; }
    Obj& i(const std::string& k, long long v) { kv.push_back(q(k) + ":" + std::to_string(v)); return *this; }
    Obj& b(const std::string& k, bool v) { kv.push_back(q(k) + std::string(":") + (v ? "true" : "false")); return *this; }
    std::string str() const {
        std::string o = "{";
        for (size_t j = 0; j < kv.size(); j++) { if (j) o += ","; o += kv[j]; }
        return o + "}";
    }
};

struct Section {
    std::string name;
    std::vector<std::string> items;
    explicit Section(std::string n) : name(std::move(n)) {}
    void add(const std::string& o) { items.push_back(o); }
    std::string str() const {
        std::string o = q(name) + ":[";
        for (size_t j = 0; j < items.size(); j++) { if (j) o += ","; o += "\n    " + items[j]; }
        o += "]";
        return o;
    }
};

/* ── ValueSpec helpers: build a Value and its spec JSON together ───── */

struct Spec { Value v; std::string json; };

static Spec sp_nil()            { return {Value::nil(), "{\"k\":\"nil\"}"}; }
static Spec sp_bool(bool x)     { return {Value::boolean(x), std::string("{\"k\":\"bool\",\"v\":") + (x?"true":"false") + "}"}; }
static Spec sp_int(int64_t x)   { return {Value::integer(x), "{\"k\":\"int\",\"v\":\"" + std::to_string(x) + "\"}"}; }
static Spec sp_uint(uint64_t x) { return {Value::unsigned_integer(x), "{\"k\":\"uint\",\"v\":\"" + std::to_string(x) + "\"}"}; }
static Spec sp_i8(int8_t x)     { return {Value::int8(x),  "{\"k\":\"int8\",\"v\":\""  + std::to_string((int)x) + "\"}"}; }
static Spec sp_i16(int16_t x)   { return {Value::int16(x), "{\"k\":\"int16\",\"v\":\"" + std::to_string((int)x) + "\"}"}; }
static Spec sp_i32(int32_t x)   { return {Value::int32(x), "{\"k\":\"int32\",\"v\":\"" + std::to_string((long)x) + "\"}"}; }
static Spec sp_i64(int64_t x)   { return {Value::int64(x), "{\"k\":\"int64\",\"v\":\"" + std::to_string(x) + "\"}"}; }
static Spec sp_u8(uint8_t x)    { return {Value::uint8(x),  "{\"k\":\"uint8\",\"v\":\""  + std::to_string((unsigned)x) + "\"}"}; }
static Spec sp_u16(uint16_t x)  { return {Value::uint16(x), "{\"k\":\"uint16\",\"v\":\"" + std::to_string((unsigned)x) + "\"}"}; }
static Spec sp_u32(uint32_t x)  { return {Value::uint32(x), "{\"k\":\"uint32\",\"v\":\"" + std::to_string((unsigned long)x) + "\"}"}; }
static Spec sp_u64(uint64_t x)  { return {Value::uint64(x), "{\"k\":\"uint64\",\"v\":\"" + std::to_string(x) + "\"}"}; }
static Spec sp_real(double x, const std::string& lit) { return {Value::real(x), "{\"k\":\"real\",\"v\":" + lit + "}"}; }
static Spec sp_real32(float x, const std::string& lit){ return {Value::real32(x), "{\"k\":\"real32\",\"v\":" + lit + "}"}; }
static Spec sp_str(const std::string& s) { return {Value::string(s), "{\"k\":\"str\",\"v\":" + q(s) + "}"}; }
static Spec sp_bin(const std::string& hex) {
    auto d = from_hex(hex);
    return {Value::binary(d.data(), d.size()), "{\"k\":\"binary\",\"hex\":" + q(hex) + "}"};
}
static Spec sp_ext(int8_t t, const std::string& hex) {
    auto d = from_hex(hex);
    return {Value::ext(t, d.data(), d.size()),
            "{\"k\":\"ext\",\"type\":" + std::to_string((int)t) + ",\"hex\":" + q(hex) + "}"};
}
static Spec sp_ts(int64_t sec, uint32_t nsec) {
    return {Value::timestamp(sec, nsec),
            "{\"k\":\"timestamp\",\"sec\":\"" + std::to_string(sec) + "\",\"nsec\":" + std::to_string((long long)nsec) + "}"};
}

int main() {
    Section from_json("from_json"), to_json("to_json"), to_json_pretty("to_json_pretty");
    Section typed("typed"), mutate("mutate"), extract("extract"),
            array_length("array_length"), iterate("iterate");

    /* ── from_json: JSON input → expected msgpack hex ─────────────── */
    const char* json_inputs[] = {
        "null", "true", "false",
        "0", "1", "127", "128", "255", "256", "65535", "65536",
        "4294967295", "4294967296", "9007199254740991",
        "-1", "-32", "-33", "-128", "-129", "-32768", "-32769",
        "-2147483648", "-2147483649",
        "1.5", "0.1", "-0.0", "3.0", "1e10", "1.25e-3", "95.5",
        "1e-7", "1e-4", "1e-5", "1e20", "1e21", "1e22", "9.999999e-8",
        "0.30000000000000004", "123456789012345680000",
        "\"\"", "\"hello\"", "\"a string longer than thirty-one chars!!\"",
        "\"unicode: \\u00e9\\u4e2d\\ud83d\\ude00\"",
        "[]", "{}", "[1,2,3]", "{\"a\":1,\"b\":2}",
        "{\"name\":\"Alice\",\"age\":30,\"scores\":[95,87,91]}",
        "[[1,2],[3,4],{\"x\":[true,null,false]}]",
        "{\"nested\":{\"deep\":{\"value\":42}}}",
    };
    for (auto* j : json_inputs) {
        Blob b = Blob::from_json(j);
        from_json.add(Obj().s("json", j).s("hex", to_hex(b)).str());
        /* round-trip → canonical JSON */
        to_json.add(Obj().s("hex", to_hex(b)).s("json", b.to_json()).str());
    }

    /* ── to_json: special hex inputs (bin, ext, float32, mixed) ───── */
    {
        const char* hexes[] = {
            "ca3f800000",          /* float32 1.0 */
            "ca40490fdb",          /* float32 pi-ish */
            "cb3ff0000000000000",  /* float64 1.0 → 1.0 */
            "cb3fb999999999999a",  /* float64 0.1 */
            "c403abcdef",          /* bin8 → hex string */
            "d6ff0102",            /* fixext... actually timestamp? d6 ff = fixext4 type -1 */
            "d40102",              /* fixext1 type 1 → null */
            "92c2c3",              /* [false,true] */
            "81a16382",            /* truncated-ish; map {"c": fixmap2...} */
        };
        for (auto* h : hexes) {
            auto d = from_hex(h);
            Blob b(d.data(), d.size());
            to_json.add(Obj().s("hex", h).s("json", b.to_json()).str());
        }
    }

    /* ── to_json_pretty ───────────────────────────────────────────── */
    {
        struct PJ { const char* json; int indent; };
        PJ cases[] = {
            {"{\"a\":1,\"b\":[2,3]}", 2},
            {"[1,[2,[3]]]", 4},
            {"{}", 2}, {"[]", 2},
            {"{\"x\":{\"y\":1}}", 0},
        };
        for (auto& c : cases) {
            Blob b = Blob::from_json(c.json);
            to_json_pretty.add(Obj().s("hex", to_hex(b)).i("indent", c.indent)
                               .s("json", b.to_json_pretty(c.indent)).str());
        }
    }

    /* ── typed: Value → Builder::quote → hex ──────────────────────── */
    std::vector<Spec> specs;
    specs.push_back(sp_nil());
    specs.push_back(sp_bool(true)); specs.push_back(sp_bool(false));
    specs.push_back(sp_int(0)); specs.push_back(sp_int(127)); specs.push_back(sp_int(128));
    specs.push_back(sp_int(255)); specs.push_back(sp_int(256)); specs.push_back(sp_int(65536));
    specs.push_back(sp_int(4294967296LL));
    specs.push_back(sp_int(-1)); specs.push_back(sp_int(-32)); specs.push_back(sp_int(-33));
    specs.push_back(sp_int(-128)); specs.push_back(sp_int(-129)); specs.push_back(sp_int(-32769));
    specs.push_back(sp_uint(0)); specs.push_back(sp_uint(255));
    specs.push_back(sp_uint(18446744073709551615ULL));
    specs.push_back(sp_i8(-5)); specs.push_back(sp_i16(500)); specs.push_back(sp_i32(-70000));
    specs.push_back(sp_i64(-5)); specs.push_back(sp_u8(200)); specs.push_back(sp_u16(60000));
    specs.push_back(sp_u32(4000000000u)); specs.push_back(sp_u64(42));
    specs.push_back(sp_real(1.5, "1.5")); specs.push_back(sp_real(0.1, "0.1"));
    specs.push_back(sp_real(3.0, "3.0"));
    specs.push_back(sp_real32(1.5f, "1.5")); specs.push_back(sp_real32(0.5f, "0.5"));
    specs.push_back(sp_str("")); specs.push_back(sp_str("hello"));
    specs.push_back(sp_str(std::string(40, 'x')));
    specs.push_back(sp_bin("deadbeef")); specs.push_back(sp_bin(""));
    specs.push_back(sp_ext(42, "0102")); specs.push_back(sp_ext(1, "00"));
    specs.push_back(sp_ext(7, "0102030405"));  /* len5 → ext8 */
    specs.push_back(sp_ts(0, 0)); specs.push_back(sp_ts(1700000000, 0));
    specs.push_back(sp_ts(1700000000, 500000000));
    specs.push_back(sp_ts(17000000000LL, 0));     /* needs fixext8 */
    specs.push_back(sp_ts(-1, 0));                 /* needs ext8 */
    for (auto& s : specs) {
        Blob b = Builder::quote(s.v);
        typed.add(Obj().raw("spec", s.json).s("hex", to_hex(b)).str());
    }

    /* ── mutate: base + op + path (+ spec/patch) → hex ────────────── */
    auto add_mut = [&](const std::string& base, const std::string& op,
                       const std::string& path, const Spec& s) {
        Blob b = Blob::from_json(base.c_str());
        Blob r;
        if (op == "set")          r = b.set(path.c_str(), s.v);
        else if (op == "insert")  r = b.insert(path.c_str(), s.v);
        else if (op == "replace") r = b.replace(path.c_str(), s.v);
        else if (op == "array_insert") r = b.array_insert(path.c_str(), s.v);
        mutate.add(Obj().s("base", base).s("op", op).s("path", path)
                   .raw("spec", s.json).s("hex", to_hex(r)).str());
    };
    add_mut("{\"a\":1}", "set", "$.b", sp_int(2));
    add_mut("{\"a\":1}", "set", "$.a", sp_int(99));
    add_mut("{\"a\":1}", "set", "$.a", sp_i16(1000));
    add_mut("{\"x\":0}", "set", "$.created", sp_ts(1700000000, 0));
    add_mut("{\"a\":1}", "insert", "$.b", sp_str("new"));
    add_mut("{\"a\":1}", "insert", "$.a", sp_int(5));     /* exists → no-op */
    add_mut("{\"a\":1,\"b\":2}", "replace", "$.a", sp_real(2.5, "2.5"));
    add_mut("{\"a\":1}", "replace", "$.zzz", sp_int(9));  /* missing → no-op */
    add_mut("[1,2,3]", "set", "$[1]", sp_int(20));
    add_mut("[1,2,3]", "set", "$[3]", sp_int(4));         /* append */
    add_mut("[1,2,3]", "array_insert", "$[1]", sp_int(99));
    add_mut("[1,2,3]", "array_insert", "$[0]", sp_str("head"));
    /* remove */
    {
        struct RM { const char* base; const char* path; };
        RM rms[] = {
            {"{\"a\":1,\"b\":2}", "$.a"},
            {"{\"a\":1,\"b\":2}", "$.b"},
            {"[1,2,3]", "$[1]"},
            {"{\"a\":{\"b\":1,\"c\":2}}", "$.a.b"},
        };
        for (auto& rm : rms) {
            Blob b = Blob::from_json(rm.base);
            Blob r = b.remove(rm.path);
            mutate.add(Obj().s("base", rm.base).s("op", "remove").s("path", rm.path)
                       .s("hex", to_hex(r)).str());
        }
    }
    /* set_blob (sub-blob) */
    {
        Blob base = Blob::from_json("{\"a\":1}");
        Blob sub = Blob::from_json("[1,2,3]");
        Blob r = base.set("$.b", sub);
        mutate.add(Obj().s("base", "{\"a\":1}").s("op", "set_blob").s("path", "$.b")
                   .raw("spec", "{\"k\":\"blob\",\"json\":\"[1,2,3]\"}").s("hex", to_hex(r)).str());
    }
    /* patch (RFC 7386) */
    {
        struct PT { const char* base; const char* patch; };
        PT pts[] = {
            {"{\"a\":1,\"b\":2}", "{\"a\":9}"},
            {"{\"a\":1,\"b\":2}", "{\"b\":null}"},
            {"{\"a\":1}", "{\"c\":3}"},
            {"{\"a\":{\"x\":1,\"y\":2}}", "{\"a\":{\"y\":null,\"z\":3}}"},
            {"{\"a\":1}", "{\"a\":{\"nested\":true}}"},
        };
        for (auto& pt : pts) {
            Blob base = Blob::from_json(pt.base);
            Blob patch = Blob::from_json(pt.patch);
            Blob r = base.patch(patch);
            mutate.add(Obj().s("base", pt.base).s("op", "patch")
                       .raw("patch", "\"" + jesc(pt.patch) + "\"")
                       .s("hex", to_hex(r)).str());
        }
    }

    /* ── extract: base + path → type + value-as-json ──────────────── */
    auto add_ext = [&](const std::string& base, const std::string& path) {
        Blob b = Blob::from_json(base.c_str());
        const char* ty = b.type_str(path.c_str());
        /* represent the extracted scalar through a one-element round trip */
        Value v = b.extract(path.c_str());
        std::string vj = Builder::quote(v).empty() ? "null" : Builder::quote(v).to_json();
        extract.add(Obj().s("base", base).s("path", path).s("type", ty).s("vjson", vj).str());
    };
    add_ext("{\"name\":\"Alice\",\"age\":30}", "$.name");
    add_ext("{\"name\":\"Alice\",\"age\":30}", "$.age");
    add_ext("{\"a\":[10,20,30]}", "$.a[1]");
    add_ext("{\"a\":[10,20,30]}", "$.a");
    add_ext("{\"a\":1}", "$.missing");
    add_ext("{\"f\":1.5}", "$.f");
    add_ext("{\"b\":true,\"n\":null}", "$.b");
    add_ext("{\"b\":true,\"n\":null}", "$.n");
    add_ext("[{\"x\":1}]", "$[0].x");

    /* ── array_length ─────────────────────────────────────────────── */
    auto add_len = [&](const std::string& base, const std::string& path) {
        Blob b = Blob::from_json(base.c_str());
        int64_t n = (path == "$") ? b.array_length() : b.array_length(path.c_str());
        array_length.add(Obj().s("base", base).s("path", path).i("len", n).str());
    };
    add_len("[1,2,3]", "$");
    add_len("{\"a\":[1,2,3,4]}", "$.a");
    add_len("{\"a\":1}", "$");          /* not a container → -1 */
    add_len("{\"m\":{\"x\":1,\"y\":2}}", "$.m");  /* map count */
    add_len("[]", "$");

    /* ── iterate: each + tree → rows ──────────────────────────────── */
    auto add_iter = [&](const std::string& base, const std::string& path, bool recursive) {
        Blob b = Blob::from_json(base.c_str());
        Iterator it(b, path.c_str(), recursive);
        std::string rows = "[";
        bool first = true;
        while (it.next()) {
            const EachRow& r = it.current();
            if (!first) rows += ",";
            first = false;
            /* key/index are only meaningful for flat (each) iteration; tree
            ** rows identify themselves by fullkey/path/id instead. */
            Obj o;
            if (!recursive) o.s("key", r.key).i("index", r.index);
            o.s("fullkey", r.fullkey).s("path", r.path)
             .i("id", (long long)r.id).s("type", type_str(r.type));
            rows += o.str();
        }
        rows += "]";
        iterate.add(Obj().s("base", base).s("path", path).b("recursive", recursive)
                    .raw("rows", rows).str());
    };
    add_iter("{\"a\":1,\"b\":2,\"c\":3}", "$", false);
    add_iter("[10,20,30]", "$", false);
    add_iter("{\"x\":{\"y\":[1,2,3]}}", "$", true);
    add_iter("{\"users\":[{\"name\":\"A\"},{\"name\":\"B\"}]}", "$", true);
    add_iter("{\"users\":[{\"name\":\"A\"},{\"name\":\"B\"}]}", "$.users", false);
    add_iter("[1,[2,3],4]", "$", true);

    /* ── emit document ────────────────────────────────────────────── */
    std::ostringstream out;
    out << "{\n  "
        << from_json.str() << ",\n  "
        << to_json.str() << ",\n  "
        << to_json_pretty.str() << ",\n  "
        << typed.str() << ",\n  "
        << mutate.str() << ",\n  "
        << extract.str() << ",\n  "
        << array_length.str() << ",\n  "
        << iterate.str() << "\n}\n";
    std::fputs(out.str().c_str(), stdout);
    return 0;
}
