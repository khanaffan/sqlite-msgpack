//! Internal: MessagePack format constants, byte-order helpers and `skip_one`.
//!
//! Private to the crate; mirrors `cpp/src/msgpack_blob_detail.hpp` and the skip
//! routine from the C++ decode module.

pub const MAX_DEPTH: i32 = 200;
pub const MAX_OUTPUT: usize = 64 * 1024 * 1024;

pub const MP_NIL: u8 = 0xc0;
pub const MP_FALSE: u8 = 0xc2;
pub const MP_TRUE: u8 = 0xc3;
pub const MP_BIN8: u8 = 0xc4;
pub const MP_BIN16: u8 = 0xc5;
pub const MP_BIN32: u8 = 0xc6;
pub const MP_EXT8: u8 = 0xc7;
pub const MP_EXT16: u8 = 0xc8;
pub const MP_EXT32: u8 = 0xc9;
pub const MP_FLOAT32: u8 = 0xca;
pub const MP_FLOAT64: u8 = 0xcb;
pub const MP_UINT8: u8 = 0xcc;
pub const MP_UINT16: u8 = 0xcd;
pub const MP_UINT32: u8 = 0xce;
pub const MP_UINT64: u8 = 0xcf;
pub const MP_INT8: u8 = 0xd0;
pub const MP_INT16: u8 = 0xd1;
pub const MP_INT32: u8 = 0xd2;
pub const MP_INT64: u8 = 0xd3;
pub const MP_FIXEXT1: u8 = 0xd4;
pub const MP_FIXEXT2: u8 = 0xd5;
pub const MP_FIXEXT4: u8 = 0xd6;
pub const MP_FIXEXT8: u8 = 0xd7;
pub const MP_FIXEXT16: u8 = 0xd8;
pub const MP_STR8: u8 = 0xd9;
pub const MP_STR16: u8 = 0xda;
pub const MP_STR32: u8 = 0xdb;
pub const MP_ARRAY16: u8 = 0xdc;
pub const MP_ARRAY32: u8 = 0xdd;
pub const MP_MAP16: u8 = 0xde;
pub const MP_MAP32: u8 = 0xdf;

pub const MP_FIXMAP_MASK: u8 = 0x80;
pub const MP_FIXARRAY_MASK: u8 = 0x90;
pub const MP_FIXSTR_MASK: u8 = 0xa0;

pub const MP_TIMESTAMP_TYPE: u8 = 0xff;

// ── big-endian read helpers ─────────────────────────────────────────
#[inline]
pub fn read16(a: &[u8], i: usize) -> u32 {
    ((a[i] as u32) << 8) | a[i + 1] as u32
}
#[inline]
pub fn read32(a: &[u8], i: usize) -> u32 {
    ((a[i] as u32) << 24) | ((a[i + 1] as u32) << 16) | ((a[i + 2] as u32) << 8) | a[i + 3] as u32
}
#[inline]
pub fn read64(a: &[u8], i: usize) -> u64 {
    ((read32(a, i) as u64) << 32) | read32(a, i + 4) as u64
}

// ── big-endian write helpers ────────────────────────────────────────
#[inline]
pub fn push16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}
#[inline]
pub fn push32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}
#[inline]
pub fn push64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// Return the offset just past one complete element starting at `i`,
/// or 0 on malformed / truncated input.
pub fn skip_one(a: &[u8], n: usize, i: usize) -> usize {
    skip_one_d(a, n, i, 0)
}

fn skip_one_d(a: &[u8], n: usize, mut i: usize, depth: i32) -> usize {
    if depth > MAX_DEPTH {
        return 0;
    }
    if i >= n {
        return 0;
    }
    let b = a[i];
    i += 1;

    if b <= 0x7f {
        return i;
    }
    if b >= 0xe0 {
        return i;
    }

    match b {
        MP_NIL | MP_FALSE | MP_TRUE => return i,
        MP_FLOAT32 => return if i + 4 <= n { i + 4 } else { 0 },
        MP_FLOAT64 | MP_INT64 | MP_UINT64 => return if i + 8 <= n { i + 8 } else { 0 },
        MP_UINT8 | MP_INT8 => return if i + 1 <= n { i + 1 } else { 0 },
        MP_UINT16 | MP_INT16 => return if i + 2 <= n { i + 2 } else { 0 },
        MP_UINT32 | MP_INT32 => return if i + 4 <= n { i + 4 } else { 0 },
        MP_BIN8 | MP_STR8 => {
            if i + 1 > n {
                return 0;
            }
            let sz = a[i] as usize;
            i += 1;
            return if sz <= n - i { i + sz } else { 0 };
        }
        MP_BIN16 | MP_STR16 => {
            if i + 2 > n {
                return 0;
            }
            let sz = read16(a, i) as usize;
            i += 2;
            return if sz <= n - i { i + sz } else { 0 };
        }
        MP_BIN32 | MP_STR32 => {
            if i + 4 > n {
                return 0;
            }
            let sz = read32(a, i) as usize;
            i += 4;
            return if sz <= n - i { i + sz } else { 0 };
        }
        MP_FIXEXT1 => return if i + 2 <= n { i + 2 } else { 0 },
        MP_FIXEXT2 => return if i + 3 <= n { i + 3 } else { 0 },
        MP_FIXEXT4 => return if i + 5 <= n { i + 5 } else { 0 },
        MP_FIXEXT8 => return if i + 9 <= n { i + 9 } else { 0 },
        MP_FIXEXT16 => return if i + 17 <= n { i + 17 } else { 0 },
        MP_EXT8 => {
            if i + 2 > n {
                return 0;
            }
            let sz = a[i] as usize;
            i += 2;
            return if sz <= n - i { i + sz } else { 0 };
        }
        MP_EXT16 => {
            if i + 3 > n {
                return 0;
            }
            let sz = read16(a, i) as usize;
            i += 3;
            return if sz <= n - i { i + sz } else { 0 };
        }
        MP_EXT32 => {
            if i + 5 > n {
                return 0;
            }
            let sz = read32(a, i) as usize;
            i += 5;
            return if sz <= n - i { i + sz } else { 0 };
        }
        _ => {}
    }

    // fixstr
    if (0xa0..=0xbf).contains(&b) {
        let sz = (b & 0x1f) as usize;
        return if sz <= n - i { i + sz } else { 0 };
    }

    // fixarray
    if (0x90..=0x9f).contains(&b) {
        let count = (b & 0x0f) as usize;
        for _ in 0..count {
            i = skip_one_d(a, n, i, depth + 1);
            if i == 0 {
                return 0;
            }
        }
        return i;
    }

    // fixmap
    if (0x80..=0x8f).contains(&b) {
        let count = (b & 0x0f) as usize;
        for _ in 0..count {
            i = skip_one_d(a, n, i, depth + 1);
            if i == 0 {
                return 0;
            }
            i = skip_one_d(a, n, i, depth + 1);
            if i == 0 {
                return 0;
            }
        }
        return i;
    }

    // array16/32
    if b == MP_ARRAY16 || b == MP_ARRAY32 {
        let count = if b == MP_ARRAY16 {
            if i + 2 > n {
                return 0;
            }
            let c = read16(a, i) as usize;
            i += 2;
            c
        } else {
            if i + 4 > n {
                return 0;
            }
            let c = read32(a, i) as usize;
            i += 4;
            c
        };
        for _ in 0..count {
            i = skip_one_d(a, n, i, depth + 1);
            if i == 0 {
                return 0;
            }
        }
        return i;
    }

    // map16/32
    if b == MP_MAP16 || b == MP_MAP32 {
        let count = if b == MP_MAP16 {
            if i + 2 > n {
                return 0;
            }
            let c = read16(a, i) as usize;
            i += 2;
            c
        } else {
            if i + 4 > n {
                return 0;
            }
            let c = read32(a, i) as usize;
            i += 4;
            c
        };
        for _ in 0..count {
            i = skip_one_d(a, n, i, depth + 1);
            if i == 0 {
                return 0;
            }
            i = skip_one_d(a, n, i, depth + 1);
            if i == 0 {
                return 0;
            }
        }
        return i;
    }

    0
}
