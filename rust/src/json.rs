//! Internal: JSON conversion (mirrors `msgpack_blob_json.cpp`).
//!
//! `to_json` builds a byte buffer exactly like the C++ implementation so float
//! formatting and string escaping stay byte-identical. The float formatter
//! reproduces C `printf("%.<P>g")` by delegating the round-half-to-even digit
//! generation to Rust's `{:.*e}` formatter (verified to match C digit-for-digit)
//! and then re-applying C's `%g` layout rules.

use crate::encode as e;
use crate::format as f;

pub const RC_OK: i32 = 0;
pub const RC_ERROR: i32 = 1;

const HEX: &[u8; 16] = b"0123456789abcdef";

// ── C printf "%.<P>g" ───────────────────────────────────────────────
pub fn c_format_g(value: f64, p_in: i32) -> String {
    let p = if p_in <= 0 { 1 } else { p_in } as usize;
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        };
    }
    let neg = value < 0.0;
    let a = value.abs();

    // `{:.*e}` with (p-1) fractional digits yields p significant digits, rounded
    // half-to-even — identical digits/exponent to C's `%.*e`.
    let sci = format!("{:.*e}", p - 1, a);
    let (mant, exp_str) = sci.split_once('e').expect("scientific format");
    let x: i32 = exp_str.parse().expect("exponent");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();

    let mut out = if x >= -4 && x < p as i32 {
        // fixed-point notation
        let mut s = if x >= 0 {
            let int_len = (x + 1) as usize;
            let int_part = &digits[..int_len.min(digits.len())];
            let frac_part = &digits[int_len.min(digits.len())..];
            let mut t = String::from(int_part);
            if !frac_part.is_empty() {
                t.push('.');
                t.push_str(frac_part);
            }
            t
        } else {
            let mut t = String::from("0.");
            for _ in 0..(-x - 1) {
                t.push('0');
            }
            t.push_str(&digits);
            t
        };
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        s
    } else {
        // scientific notation
        let mut m = String::new();
        m.push(digits.as_bytes()[0] as char);
        if digits.len() > 1 {
            m.push('.');
            m.push_str(&digits[1..]);
        }
        if m.contains('.') {
            while m.ends_with('0') {
                m.pop();
            }
            if m.ends_with('.') {
                m.pop();
            }
        }
        let mut ea = x.unsigned_abs().to_string();
        if ea.len() < 2 {
            ea.insert(0, '0');
        }
        format!("{}e{}{}", m, if x < 0 { "-" } else { "+" }, ea)
    };

    if neg {
        out.insert(0, '-');
    }
    out
}

fn fmt_double(d: f64) -> String {
    let s = c_format_g(d, 17);
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        // C re-formats integer-valued doubles with "%.1f".
        format!("{:.1}", d)
    } else {
        s
    }
}

fn fmt_float32(val: f32) -> String {
    c_format_g(val as f64, 7)
}

// ── JSON output ─────────────────────────────────────────────────────
fn escape_str(out: &mut Vec<u8>, s: &[u8]) {
    out.push(b'"');
    let mut start = 0;
    let n = s.len();
    for j in 0..n {
        let c = s[j];
        if c >= 0x20 && c != b'"' && c != b'\\' {
            continue;
        }
        if j > start {
            out.extend_from_slice(&s[start..j]);
        }
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            _ => out.extend_from_slice(format!("\\u{:04x}", c).as_bytes()),
        }
        start = j + 1;
    }
    if n > start {
        out.extend_from_slice(&s[start..n]);
    }
    out.push(b'"');
}

fn newline(out: &mut Vec<u8>, depth: i32, indent_w: i32) {
    out.push(b'\n');
    for _ in 0..(depth * indent_w) {
        out.push(b' ');
    }
}

#[allow(clippy::too_many_arguments)]
fn to_json_at(
    out: &mut Vec<u8>,
    a: &[u8],
    n: usize,
    i: usize,
    pretty: bool,
    depth: i32,
    indent_w: i32,
) {
    if i >= n || depth > f::MAX_DEPTH {
        out.extend_from_slice(b"null");
        return;
    }
    let b = a[i];

    if b == f::MP_NIL {
        out.extend_from_slice(b"null");
        return;
    }
    if b == f::MP_FALSE {
        out.extend_from_slice(b"false");
        return;
    }
    if b == f::MP_TRUE {
        out.extend_from_slice(b"true");
        return;
    }
    if b <= 0x7f {
        out.extend_from_slice(b.to_string().as_bytes());
        return;
    }
    if b >= 0xe0 {
        out.extend_from_slice((b as i8).to_string().as_bytes());
        return;
    }

    match b {
        f::MP_UINT8 => {
            if i + 2 <= n {
                out.extend_from_slice(a[i + 1].to_string().as_bytes());
                return;
            }
        }
        f::MP_UINT16 => {
            if i + 3 <= n {
                out.extend_from_slice(f::read16(a, i + 1).to_string().as_bytes());
                return;
            }
        }
        f::MP_UINT32 => {
            if i + 5 <= n {
                out.extend_from_slice(f::read32(a, i + 1).to_string().as_bytes());
                return;
            }
        }
        f::MP_UINT64 => {
            if i + 9 <= n {
                out.extend_from_slice(f::read64(a, i + 1).to_string().as_bytes());
                return;
            }
        }
        f::MP_INT8 => {
            if i + 2 <= n {
                out.extend_from_slice((a[i + 1] as i8).to_string().as_bytes());
                return;
            }
        }
        f::MP_INT16 => {
            if i + 3 <= n {
                out.extend_from_slice((f::read16(a, i + 1) as u16 as i16).to_string().as_bytes());
                return;
            }
        }
        f::MP_INT32 => {
            if i + 5 <= n {
                out.extend_from_slice((f::read32(a, i + 1) as i32).to_string().as_bytes());
                return;
            }
        }
        f::MP_INT64 => {
            if i + 9 <= n {
                out.extend_from_slice((f::read64(a, i + 1) as i64).to_string().as_bytes());
                return;
            }
        }
        f::MP_FLOAT32 => {
            if i + 5 <= n {
                let val = f32::from_bits(f::read32(a, i + 1));
                if !val.is_finite() {
                    out.extend_from_slice(b"null");
                    return;
                }
                out.extend_from_slice(fmt_float32(val).as_bytes());
                return;
            }
        }
        f::MP_FLOAT64 => {
            if i + 9 <= n {
                let d = f64::from_bits(f::read64(a, i + 1));
                if !d.is_finite() {
                    out.extend_from_slice(b"null");
                    return;
                }
                out.extend_from_slice(fmt_double(d).as_bytes());
                return;
            }
        }
        _ => {}
    }

    // str
    let (mut slen, soff) = if (0xa0..=0xbf).contains(&b) {
        ((b & 0x1f) as usize, i + 1)
    } else if b == f::MP_STR8 && i + 2 <= n {
        (a[i + 1] as usize, i + 2)
    } else if b == f::MP_STR16 && i + 3 <= n {
        (f::read16(a, i + 1) as usize, i + 3)
    } else if b == f::MP_STR32 && i + 5 <= n {
        (f::read32(a, i + 1) as usize, i + 5)
    } else {
        (0, 0)
    };
    if soff != 0 {
        if slen > n - soff {
            slen = n - soff;
        }
        escape_str(out, &a[soff..soff + slen]);
        return;
    }

    // bin → hex string
    let (mut blen, boff) = if b == f::MP_BIN8 && i + 2 <= n {
        (a[i + 1] as usize, i + 2)
    } else if b == f::MP_BIN16 && i + 3 <= n {
        (f::read16(a, i + 1) as usize, i + 3)
    } else if b == f::MP_BIN32 && i + 5 <= n {
        (f::read32(a, i + 1) as usize, i + 5)
    } else {
        (0, 0)
    };
    if boff != 0 {
        if blen > n - boff {
            blen = n - boff;
        }
        out.push(b'"');
        for j in 0..blen {
            let by = a[boff + j];
            out.push(HEX[(by >> 4) as usize]);
            out.push(HEX[(by & 0xf) as usize]);
        }
        out.push(b'"');
        return;
    }

    // array
    let (is_arr, count, data_off) = if (0x90..=0x9f).contains(&b) {
        (true, (b & 0x0f) as usize, i + 1)
    } else if b == f::MP_ARRAY16 && i + 3 <= n {
        (true, f::read16(a, i + 1) as usize, i + 3)
    } else if b == f::MP_ARRAY32 && i + 5 <= n {
        (true, f::read32(a, i + 1) as usize, i + 5)
    } else {
        (false, 0, 0)
    };
    if is_arr {
        let mut cur = data_off;
        out.push(b'[');
        for j in 0..count {
            if cur >= n {
                break;
            }
            let nxt = f::skip_one(a, n, cur);
            if j > 0 {
                out.push(b',');
            }
            if pretty {
                newline(out, depth + 1, indent_w);
            }
            to_json_at(out, a, n, cur, pretty, depth + 1, indent_w);
            cur = if nxt != 0 { nxt } else { n };
        }
        if pretty && count > 0 {
            newline(out, depth, indent_w);
        }
        out.push(b']');
        return;
    }

    // map
    let (is_map, count, data_off) = if (0x80..=0x8f).contains(&b) {
        (true, (b & 0x0f) as usize, i + 1)
    } else if b == f::MP_MAP16 && i + 3 <= n {
        (true, f::read16(a, i + 1) as usize, i + 3)
    } else if b == f::MP_MAP32 && i + 5 <= n {
        (true, f::read32(a, i + 1) as usize, i + 5)
    } else {
        (false, 0, 0)
    };
    if is_map {
        let mut cur = data_off;
        out.push(b'{');
        for j in 0..count {
            if cur >= n {
                break;
            }
            let val_off = f::skip_one(a, n, cur);
            let pair_end = if val_off != 0 {
                f::skip_one(a, n, val_off)
            } else {
                0
            };
            if j > 0 {
                out.push(b',');
            }
            if pretty {
                newline(out, depth + 1, indent_w);
            }
            to_json_at(out, a, n, cur, pretty, depth + 1, indent_w);
            out.push(b':');
            if pretty {
                out.push(b' ');
            }
            to_json_at(
                out,
                a,
                n,
                if val_off != 0 { val_off } else { n },
                pretty,
                depth + 1,
                indent_w,
            );
            cur = if pair_end != 0 { pair_end } else { n };
        }
        if pretty && count > 0 {
            newline(out, depth, indent_w);
        }
        out.push(b'}');
        return;
    }

    // ext / unknown → null
    out.extend_from_slice(b"null");
}

pub fn to_json_bytes(a: &[u8], n: usize, pretty: bool, indent: i32) -> Vec<u8> {
    let mut out = Vec::new();
    to_json_at(&mut out, a, n, 0, pretty, 0, indent);
    out
}

// ── JSON parser → msgpack ───────────────────────────────────────────
struct P<'a> {
    z: &'a [u8],
    n: usize,
    i: usize,
}

fn skip_ws(p: &mut P) {
    while p.i < p.n && matches!(p.z[p.i], 0x20 | 0x09 | 0x0a | 0x0d) {
        p.i += 1;
    }
}

fn hex4(z: &[u8], off: usize) -> i32 {
    let mut v: i32 = 0;
    for j in 0..4 {
        let c = z[off + j];
        let h = match c {
            b'0'..=b'9' => (c - b'0') as i32,
            b'a'..=b'f' => (c - b'a' + 10) as i32,
            b'A'..=b'F' => (c - b'A' + 10) as i32,
            _ => return -1,
        };
        v = (v << 4) | h;
    }
    v
}

fn cp_to_utf8(out: &mut Vec<u8>, cp: u32) {
    if cp < 0x80 {
        out.push(cp as u8);
    } else if cp < 0x800 {
        out.push(0xc0 | (cp >> 6) as u8);
        out.push(0x80 | (cp & 0x3f) as u8);
    } else if cp < 0x10000 {
        out.push(0xe0 | (cp >> 12) as u8);
        out.push(0x80 | ((cp >> 6) & 0x3f) as u8);
        out.push(0x80 | (cp & 0x3f) as u8);
    } else {
        out.push(0xf0 | (cp >> 18) as u8);
        out.push(0x80 | ((cp >> 12) & 0x3f) as u8);
        out.push(0x80 | ((cp >> 6) & 0x3f) as u8);
        out.push(0x80 | (cp & 0x3f) as u8);
    }
}

fn parse_string(p: &mut P, out: &mut Vec<u8>) -> i32 {
    let mut sb = Vec::new();
    p.i += 1; // skip "
    while p.i < p.n {
        let c = p.z[p.i];
        if c == b'"' {
            p.i += 1;
            break;
        }
        if c == b'\\' {
            p.i += 1;
            if p.i >= p.n {
                return RC_ERROR;
            }
            let esc = p.z[p.i];
            p.i += 1;
            match esc {
                b'"' => sb.push(b'"'),
                b'\\' => sb.push(b'\\'),
                b'/' => sb.push(b'/'),
                b'n' => sb.push(b'\n'),
                b'r' => sb.push(b'\r'),
                b't' => sb.push(b'\t'),
                b'b' => sb.push(0x08),
                b'f' => sb.push(0x0c),
                b'u' => {
                    if p.i + 4 > p.n {
                        return RC_ERROR;
                    }
                    let mut cp = hex4(p.z, p.i);
                    p.i += 4;
                    if cp < 0 {
                        return RC_ERROR;
                    }
                    if (0xd800..=0xdbff).contains(&cp)
                        && p.i + 6 <= p.n
                        && p.z[p.i] == b'\\'
                        && p.z[p.i + 1] == b'u'
                    {
                        let lo = hex4(p.z, p.i + 2);
                        if (0xdc00..=0xdfff).contains(&lo) {
                            p.i += 6;
                            cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
                        }
                    }
                    cp_to_utf8(&mut sb, cp as u32);
                }
                _ => sb.push(esc),
            }
        } else {
            sb.push(c);
            p.i += 1;
        }
    }
    e::enc_string(out, &sb);
    RC_OK
}

fn parse_number(p: &mut P, out: &mut Vec<u8>) -> i32 {
    let start = p.i;
    let mut is_float = false;
    if p.i < p.n && p.z[p.i] == b'-' {
        p.i += 1;
    }
    while p.i < p.n && p.z[p.i].is_ascii_digit() {
        p.i += 1;
    }
    if p.i < p.n && p.z[p.i] == b'.' {
        is_float = true;
        p.i += 1;
        while p.i < p.n && p.z[p.i].is_ascii_digit() {
            p.i += 1;
        }
    }
    if p.i < p.n && (p.z[p.i] == b'e' || p.z[p.i] == b'E') {
        is_float = true;
        p.i += 1;
        if p.i < p.n && (p.z[p.i] == b'+' || p.z[p.i] == b'-') {
            p.i += 1;
        }
        while p.i < p.n && p.z[p.i].is_ascii_digit() {
            p.i += 1;
        }
    }
    let len = p.i - start;
    if len == 0 || len >= 64 {
        return RC_ERROR;
    }
    let text = std::str::from_utf8(&p.z[start..p.i]).unwrap_or("");

    if is_float {
        let d: f64 = text.parse().unwrap_or(0.0);
        e::enc_real(out, d);
    } else {
        // strtoll-style saturation to the i64 range
        let v: i64 = text.parse().unwrap_or_else(|_| {
            if text.starts_with('-') {
                i64::MIN
            } else {
                i64::MAX
            }
        });
        if v >= 0 {
            e::enc_unsigned(out, v as u64);
        } else {
            e::enc_integer(out, v);
        }
    }
    RC_OK
}

fn parse_array(p: &mut P, out: &mut Vec<u8>) -> i32 {
    let mut tmp = Vec::new();
    let mut count: u32 = 0;
    p.i += 1; // skip [
    skip_ws(p);
    while p.i < p.n && p.z[p.i] != b']' {
        if count > 0 {
            skip_ws(p);
            if p.i >= p.n || p.z[p.i] != b',' {
                return RC_ERROR;
            }
            p.i += 1;
        }
        skip_ws(p);
        if parse_value(p, &mut tmp) != RC_OK {
            return RC_ERROR;
        }
        count += 1;
        skip_ws(p);
    }
    if p.i >= p.n {
        return RC_ERROR;
    }
    p.i += 1; // skip ]
    e::enc_array_header(out, count);
    out.extend_from_slice(&tmp);
    RC_OK
}

fn parse_object(p: &mut P, out: &mut Vec<u8>) -> i32 {
    let mut tmp = Vec::new();
    let mut count: u32 = 0;
    p.i += 1; // skip {
    skip_ws(p);
    while p.i < p.n && p.z[p.i] != b'}' {
        if count > 0 {
            skip_ws(p);
            if p.i >= p.n || p.z[p.i] != b',' {
                return RC_ERROR;
            }
            p.i += 1;
        }
        skip_ws(p);
        if p.i >= p.n || p.z[p.i] != b'"' {
            return RC_ERROR;
        }
        if parse_string(p, &mut tmp) != RC_OK {
            return RC_ERROR;
        }
        skip_ws(p);
        if p.i >= p.n || p.z[p.i] != b':' {
            return RC_ERROR;
        }
        p.i += 1;
        skip_ws(p);
        if parse_value(p, &mut tmp) != RC_OK {
            return RC_ERROR;
        }
        count += 1;
        skip_ws(p);
    }
    if p.i >= p.n {
        return RC_ERROR;
    }
    p.i += 1; // skip }
    e::enc_map_header(out, count);
    out.extend_from_slice(&tmp);
    RC_OK
}

fn parse_value(p: &mut P, out: &mut Vec<u8>) -> i32 {
    skip_ws(p);
    if p.i >= p.n {
        return RC_ERROR;
    }
    let c = p.z[p.i];
    if c == b'n' && p.i + 4 <= p.n && &p.z[p.i..p.i + 4] == b"null" {
        p.i += 4;
        out.push(f::MP_NIL);
        return RC_OK;
    }
    if c == b't' && p.i + 4 <= p.n && &p.z[p.i..p.i + 4] == b"true" {
        p.i += 4;
        out.push(f::MP_TRUE);
        return RC_OK;
    }
    if c == b'f' && p.i + 5 <= p.n && &p.z[p.i..p.i + 5] == b"false" {
        p.i += 5;
        out.push(f::MP_FALSE);
        return RC_OK;
    }
    if c == b'"' {
        return parse_string(p, out);
    }
    if c == b'[' {
        return parse_array(p, out);
    }
    if c == b'{' {
        return parse_object(p, out);
    }
    if c == b'-' || c.is_ascii_digit() {
        return parse_number(p, out);
    }
    RC_ERROR
}

pub fn from_json(json: &[u8]) -> Vec<u8> {
    let mut p = P {
        z: json,
        n: json.len(),
        i: 0,
    };
    let mut out = Vec::new();
    if parse_value(&mut p, &mut out) != RC_OK {
        return Vec::new();
    }
    out
}
