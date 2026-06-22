//! Internal: decoding & inspection (mirrors `msgpack_blob_decode.cpp`).

use crate::format as f;
use crate::value::{Type, Value};

pub const RC_OK: i32 = 0;
pub const RC_ERROR: i32 = 1;
pub const RC_NOTFOUND: i32 = 2;

pub fn is_valid(a: &[u8], n: usize) -> bool {
    if n == 0 {
        return false;
    }
    f::skip_one(a, n, 0) == n
}

pub fn error_position(a: &[u8], n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    if f::skip_one(a, n, 0) == n {
        return 0;
    }
    let mut i = 0;
    while i < n {
        let nxt = f::skip_one(a, n, i);
        if nxt == 0 {
            return i;
        }
        i = nxt;
    }
    0
}

fn is_timestamp_ext(a: &[u8], n: usize, i: usize) -> bool {
    if i >= n {
        return false;
    }
    let b = a[i];
    if b == f::MP_FIXEXT4 && i + 6 <= n && a[i + 1] == f::MP_TIMESTAMP_TYPE {
        return true;
    }
    if b == f::MP_FIXEXT8 && i + 10 <= n && a[i + 1] == f::MP_TIMESTAMP_TYPE {
        return true;
    }
    if b == f::MP_EXT8 && i + 3 <= n && a[i + 1] == 12 && a[i + 2] == f::MP_TIMESTAMP_TYPE {
        return true;
    }
    false
}

fn decode_timestamp(a: &[u8], n: usize, i: usize) -> Option<(i64, u32)> {
    if i >= n {
        return None;
    }
    let b = a[i];
    if b == f::MP_FIXEXT4 && i + 6 <= n && a[i + 1] == f::MP_TIMESTAMP_TYPE {
        return Some((f::read32(a, i + 2) as i64, 0));
    }
    if b == f::MP_FIXEXT8 && i + 10 <= n && a[i + 1] == f::MP_TIMESTAMP_TYPE {
        let v = f::read64(a, i + 2);
        return Some(((v & 0x3_FFFF_FFFF) as i64, (v >> 34) as u32));
    }
    if b == f::MP_EXT8 && i + 15 <= n && a[i + 1] == 12 && a[i + 2] == f::MP_TIMESTAMP_TYPE {
        let nsec = f::read32(a, i + 3);
        let sec = f::read64(a, i + 7) as i64;
        return Some((sec, nsec));
    }
    None
}

pub fn get_type(a: &[u8], n: usize, i: usize) -> Type {
    if i >= n {
        return Type::Nil;
    }
    let b = a[i];
    if b == f::MP_NIL {
        return Type::Nil;
    }
    if b == f::MP_TRUE {
        return Type::True;
    }
    if b == f::MP_FALSE {
        return Type::False;
    }
    if b <= 0x7f || b >= 0xe0 {
        return Type::Integer;
    }
    if (0xa0..=0xbf).contains(&b) {
        return Type::String;
    }
    if (0x90..=0x9f).contains(&b) {
        return Type::Array;
    }
    if (0x80..=0x8f).contains(&b) {
        return Type::Map;
    }
    match b {
        f::MP_UINT8
        | f::MP_UINT16
        | f::MP_UINT32
        | f::MP_UINT64
        | f::MP_INT8
        | f::MP_INT16
        | f::MP_INT32
        | f::MP_INT64 => Type::Integer,
        f::MP_FLOAT32 => Type::Float32,
        f::MP_FLOAT64 => Type::Real,
        f::MP_STR8 | f::MP_STR16 | f::MP_STR32 => Type::String,
        f::MP_BIN8 | f::MP_BIN16 | f::MP_BIN32 => Type::Binary,
        f::MP_ARRAY16 | f::MP_ARRAY32 => Type::Array,
        f::MP_MAP16 | f::MP_MAP32 => Type::Map,
        f::MP_EXT8
        | f::MP_EXT16
        | f::MP_EXT32
        | f::MP_FIXEXT1
        | f::MP_FIXEXT2
        | f::MP_FIXEXT4
        | f::MP_FIXEXT8
        | f::MP_FIXEXT16 => {
            if is_timestamp_ext(a, n, i) {
                Type::Timestamp
            } else {
                Type::Ext
            }
        }
        _ => Type::Nil,
    }
}

pub fn get_container_count(a: &[u8], n: usize, i: usize) -> i64 {
    if i >= n {
        return -1;
    }
    let b = a[i];
    if (0x90..=0x9f).contains(&b) {
        return (b & 0x0f) as i64;
    }
    if (0x80..=0x8f).contains(&b) {
        return (b & 0x0f) as i64;
    }
    if b == f::MP_ARRAY16 && i + 3 <= n {
        return f::read16(a, i + 1) as i64;
    }
    if b == f::MP_ARRAY32 && i + 5 <= n {
        return f::read32(a, i + 1) as i64;
    }
    if b == f::MP_MAP16 && i + 3 <= n {
        return f::read16(a, i + 1) as i64;
    }
    if b == f::MP_MAP32 && i + 5 <= n {
        return f::read32(a, i + 1) as i64;
    }
    -1
}

/// One parsed step of `$.key[idx]` syntax.
pub enum Step<'a> {
    End,
    Error,
    Key(&'a str),
    Index(i64),
}

/// Parse one step starting at byte index `pi` in `zpath`; returns the step and
/// the next index.
pub fn path_step(zpath: &[u8], pi: usize) -> (Step<'_>, usize) {
    let mut i = pi;
    if i >= zpath.len() {
        return (Step::End, i);
    }
    let c = zpath[i];
    if c == b'.' {
        i += 1;
        let start = i;
        while i < zpath.len() && zpath[i] != b'.' && zpath[i] != b'[' {
            i += 1;
        }
        // Path key bytes are ASCII/UTF-8 from the caller's &str.
        let key = std::str::from_utf8(&zpath[start..i]).unwrap_or("");
        return (Step::Key(key), i);
    }
    if c == b'[' {
        let mut idx: i64 = 0;
        let mut has_digit = false;
        i += 1;
        while i < zpath.len() && zpath[i].is_ascii_digit() {
            idx = idx * 10 + (zpath[i] - b'0') as i64;
            i += 1;
            has_digit = true;
        }
        if !has_digit || i >= zpath.len() || zpath[i] != b']' {
            return (Step::Error, i);
        }
        i += 1;
        return (Step::Index(idx), i);
    }
    (Step::Error, i)
}

/// Key bytes of a map key at `i`, or `None` if not a string key.
fn key_at(a: &[u8], n: usize, i: usize) -> Option<&[u8]> {
    let kb = a[i];
    let (klen, koff) = if (0xa0..=0xbf).contains(&kb) {
        ((kb & 0x1f) as usize, i + 1)
    } else if kb == f::MP_STR8 && i + 2 <= n {
        (a[i + 1] as usize, i + 2)
    } else if kb == f::MP_STR16 && i + 3 <= n {
        (f::read16(a, i + 1) as usize, i + 3)
    } else if kb == f::MP_STR32 && i + 5 <= n {
        (f::read32(a, i + 1) as usize, i + 5)
    } else {
        return None;
    };
    if klen > n - koff {
        return None;
    }
    Some(&a[koff..koff + klen])
}

/// Resolve `zpath` to a byte range. Returns `(rc, i_start, i_end)`.
pub fn lookup(a: &[u8], n: usize, iroot: usize, zpath: &str) -> (i32, usize, usize) {
    let zb = zpath.as_bytes();
    if zb.is_empty() || zb[0] != b'$' {
        return (RC_ERROR, 0, 0);
    }
    let mut icur = iroot;
    let mut pi = 1;

    loop {
        let (step, npi) = path_step(zb, pi);
        pi = npi;
        match step {
            Step::End => {
                let inext = f::skip_one(a, n, icur);
                let iend = if inext != 0 { inext } else { n };
                let rc = if inext != 0 || icur == n {
                    RC_OK
                } else {
                    RC_ERROR
                };
                return (rc, icur, iend);
            }
            Step::Error => return (RC_ERROR, 0, 0),
            Step::Index(idx) => {
                if icur >= n {
                    return (RC_NOTFOUND, 0, 0);
                }
                let b = a[icur];
                let (count, elem_off) = if (0x90..=0x9f).contains(&b) {
                    ((b & 0x0f) as i64, icur + 1)
                } else if b == f::MP_ARRAY16 {
                    if icur + 3 > n {
                        return (RC_ERROR, 0, 0);
                    }
                    (f::read16(a, icur + 1) as i64, icur + 3)
                } else if b == f::MP_ARRAY32 {
                    if icur + 5 > n {
                        return (RC_ERROR, 0, 0);
                    }
                    (f::read32(a, icur + 1) as i64, icur + 5)
                } else {
                    return (RC_NOTFOUND, 0, 0);
                };
                if idx < 0 || idx >= count {
                    return (RC_NOTFOUND, 0, 0);
                }
                icur = elem_off;
                for _ in 0..idx {
                    icur = f::skip_one(a, n, icur);
                    if icur == 0 {
                        return (RC_ERROR, 0, 0);
                    }
                }
            }
            Step::Key(key) => {
                if icur >= n {
                    return (RC_NOTFOUND, 0, 0);
                }
                let b = a[icur];
                let (count, elem_off) = if (0x80..=0x8f).contains(&b) {
                    ((b & 0x0f) as usize, icur + 1)
                } else if b == f::MP_MAP16 {
                    if icur + 3 > n {
                        return (RC_ERROR, 0, 0);
                    }
                    (f::read16(a, icur + 1) as usize, icur + 3)
                } else if b == f::MP_MAP32 {
                    if icur + 5 > n {
                        return (RC_ERROR, 0, 0);
                    }
                    (f::read32(a, icur + 1) as usize, icur + 5)
                } else {
                    return (RC_NOTFOUND, 0, 0);
                };
                let key_bytes = key.as_bytes();
                icur = elem_off;
                let mut found = false;
                let mut j = 0;
                while j < count && !found {
                    if icur >= n {
                        return (RC_ERROR, 0, 0);
                    }
                    let kstr = key_at(a, n, icur);
                    let val_off = f::skip_one(a, n, icur);
                    if val_off == 0 {
                        return (RC_ERROR, 0, 0);
                    }
                    if kstr == Some(key_bytes) {
                        icur = val_off;
                        found = true;
                    } else {
                        icur = f::skip_one(a, n, val_off);
                        if icur == 0 {
                            return (RC_ERROR, 0, 0);
                        }
                    }
                    j += 1;
                }
                if !found {
                    return (RC_NOTFOUND, 0, 0);
                }
            }
        }
    }
}

pub fn decode_element(a: &[u8], n: usize, istart: usize, iend: usize) -> Value {
    if istart >= n || istart >= iend {
        return Value::nil();
    }
    let b = a[istart];

    if b == f::MP_NIL {
        return Value::nil();
    }
    if b == f::MP_FALSE {
        return Value::boolean(false);
    }
    if b == f::MP_TRUE {
        return Value::boolean(true);
    }
    if b <= 0x7f {
        return Value::integer(b as i64);
    }
    if b >= 0xe0 {
        return Value::integer(b as i8 as i64);
    }

    match b {
        f::MP_UINT8 => {
            if istart + 2 <= n {
                return Value::integer(a[istart + 1] as i64);
            }
        }
        f::MP_UINT16 => {
            if istart + 3 <= n {
                return Value::integer(f::read16(a, istart + 1) as i64);
            }
        }
        f::MP_UINT32 => {
            if istart + 5 <= n {
                return Value::integer(f::read32(a, istart + 1) as i64);
            }
        }
        f::MP_UINT64 => {
            if istart + 9 <= n {
                return Value::unsigned_integer(f::read64(a, istart + 1));
            }
        }
        f::MP_INT8 => {
            if istart + 2 <= n {
                return Value::integer(a[istart + 1] as i8 as i64);
            }
        }
        f::MP_INT16 => {
            if istart + 3 <= n {
                return Value::integer(f::read16(a, istart + 1) as u16 as i16 as i64);
            }
        }
        f::MP_INT32 => {
            if istart + 5 <= n {
                return Value::integer(f::read32(a, istart + 1) as i32 as i64);
            }
        }
        f::MP_INT64 => {
            if istart + 9 <= n {
                return Value::integer(f::read64(a, istart + 1) as i64);
            }
        }
        f::MP_FLOAT32 => {
            if istart + 5 <= n {
                let bits = f::read32(a, istart + 1);
                return Value::real32(f32::from_bits(bits));
            }
        }
        f::MP_FLOAT64 => {
            if istart + 9 <= n {
                let bits = f::read64(a, istart + 1);
                return Value::real(f64::from_bits(bits));
            }
        }
        _ => {}
    }

    // str
    let (mut slen, soff) = if (0xa0..=0xbf).contains(&b) {
        ((b & 0x1f) as usize, istart + 1)
    } else if b == f::MP_STR8 && istart + 2 <= n {
        (a[istart + 1] as usize, istart + 2)
    } else if b == f::MP_STR16 && istart + 3 <= n {
        (f::read16(a, istart + 1) as usize, istart + 3)
    } else if b == f::MP_STR32 && istart + 5 <= n {
        (f::read32(a, istart + 1) as usize, istart + 5)
    } else {
        (0, 0)
    };
    if soff != 0 {
        if slen > n - soff {
            slen = n - soff;
        }
        return Value::string_bytes(&a[soff..soff + slen]);
    }

    // bin
    let (mut blen, boff) = if b == f::MP_BIN8 && istart + 2 <= n {
        (a[istart + 1] as usize, istart + 2)
    } else if b == f::MP_BIN16 && istart + 3 <= n {
        (f::read16(a, istart + 1) as usize, istart + 3)
    } else if b == f::MP_BIN32 && istart + 5 <= n {
        (f::read32(a, istart + 1) as usize, istart + 5)
    } else {
        (0, 0)
    };
    if boff != 0 {
        if blen > n - boff {
            blen = n - boff;
        }
        return Value::binary(&a[boff..boff + blen]);
    }

    // timestamp
    if let Some((sec, nsec)) = decode_timestamp(a, n, istart) {
        return Value::timestamp_ns(sec, nsec);
    }

    // ext
    let (tc, mut elen, eoff): (i8, usize, usize) = match b {
        f::MP_FIXEXT1 if istart + 3 <= n => (a[istart + 1] as i8, 1, istart + 2),
        f::MP_FIXEXT2 if istart + 4 <= n => (a[istart + 1] as i8, 2, istart + 2),
        f::MP_FIXEXT4 if istart + 6 <= n => (a[istart + 1] as i8, 4, istart + 2),
        f::MP_FIXEXT8 if istart + 10 <= n => (a[istart + 1] as i8, 8, istart + 2),
        f::MP_FIXEXT16 if istart + 18 <= n => (a[istart + 1] as i8, 16, istart + 2),
        f::MP_EXT8 if istart + 3 <= n => (a[istart + 2] as i8, a[istart + 1] as usize, istart + 3),
        f::MP_EXT16 if istart + 4 <= n => (
            a[istart + 3] as i8,
            f::read16(a, istart + 1) as usize,
            istart + 4,
        ),
        f::MP_EXT32 if istart + 6 <= n => (
            a[istart + 5] as i8,
            f::read32(a, istart + 1) as usize,
            istart + 6,
        ),
        _ => (0, 0, 0),
    };
    if eoff != 0 {
        if elen > n - eoff {
            elen = n - eoff;
        }
        return Value::ext(tc, &a[eoff..eoff + elen]);
    }

    // containers → raw binary blob (includes header)
    Value::binary(&a[istart..iend])
}
