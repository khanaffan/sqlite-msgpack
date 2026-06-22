/*
** msgpack_blob_json.cpp — JSON conversion
**
** Part of the standalone C++ MsgPack Blob library (see msgpack_blob.hpp).
** Contains: msgpack → JSON serialisation (compact and pretty) and a
** JSON → msgpack parser, plus the Blob JSON entry points.
*/

#include "msgpack_blob_detail.hpp"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

namespace msgpack {
namespace detail {

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

}  /* namespace detail */

using namespace detail;

/* ══════════════════════════════════════════════════════════════════════
** Public API: Blob JSON conversion
** ══════════════════════════════════════════════════════════════════════ */

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

}  /* namespace msgpack */
