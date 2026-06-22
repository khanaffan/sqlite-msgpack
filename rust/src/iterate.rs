//! Internal: container iteration (mirrors `msgpack_blob_iterate.cpp`).

use crate::decode::{decode_element, get_type};
use crate::format as f;
use crate::value::{Type, Value};

/// A single row yielded by [`crate::Iterator`].
#[derive(Clone, Debug)]
pub struct EachRow {
    /// Map key (`""` for arrays / tree rows).
    pub key: String,
    /// Array index or pair index (meaningful for flat iteration only).
    pub index: i64,
    /// Full path, e.g. `"$.users[0].name"`.
    pub fullkey: String,
    /// Parent path.
    pub path: String,
    /// Byte offset of the element in the blob.
    pub id: usize,
    /// Element type.
    pub ty: Type,
    /// Element value.
    pub value: Value,
}

fn key_str(a: &[u8], n: usize, i: usize) -> Option<String> {
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
    // Bounds guard (matches decode::key_at): reject a key length that runs past
    // the end of the blob. each_iter then defers to skip_one, which fails and
    // stops iteration — matching the C++ reference (no out-of-bounds read).
    if klen > n - koff {
        return None;
    }
    Some(String::from_utf8_lossy(&a[koff..koff + klen]).into_owned())
}

struct Container {
    is_arr: bool,
    is_map: bool,
    count: usize,
    data_off: usize,
}

fn container(a: &[u8], n: usize, i: usize) -> Container {
    let b = a[i];
    if (0x90..=0x9f).contains(&b) {
        return Container {
            is_arr: true,
            is_map: false,
            count: (b & 0x0f) as usize,
            data_off: i + 1,
        };
    }
    if b == f::MP_ARRAY16 && i + 3 <= n {
        return Container {
            is_arr: true,
            is_map: false,
            count: f::read16(a, i + 1) as usize,
            data_off: i + 3,
        };
    }
    if b == f::MP_ARRAY32 && i + 5 <= n {
        return Container {
            is_arr: true,
            is_map: false,
            count: f::read32(a, i + 1) as usize,
            data_off: i + 5,
        };
    }
    if (0x80..=0x8f).contains(&b) {
        return Container {
            is_arr: false,
            is_map: true,
            count: (b & 0x0f) as usize,
            data_off: i + 1,
        };
    }
    if b == f::MP_MAP16 && i + 3 <= n {
        return Container {
            is_arr: false,
            is_map: true,
            count: f::read16(a, i + 1) as usize,
            data_off: i + 3,
        };
    }
    if b == f::MP_MAP32 && i + 5 <= n {
        return Container {
            is_arr: false,
            is_map: true,
            count: f::read32(a, i + 1) as usize,
            data_off: i + 5,
        };
    }
    Container {
        is_arr: false,
        is_map: false,
        count: 0,
        data_off: 0,
    }
}

pub fn each_iter(a: &[u8], n: usize, icont: usize, zbase: &str) -> Vec<EachRow> {
    let mut rows = Vec::new();
    if icont >= n {
        return rows;
    }
    let c = container(a, n, icont);
    if !c.is_arr && !c.is_map {
        return rows;
    }

    let remaining = if c.data_off <= n { n - c.data_off } else { 0 };
    let min_bytes = if c.is_map { 2 } else { 1 };
    if c.count > remaining / min_bytes + 1 {
        return rows;
    }

    let mut cur = c.data_off;
    for j in 0..c.count {
        if cur >= n {
            break;
        }
        if c.is_arr {
            let c_end = f::skip_one(a, n, cur);
            if c_end == 0 {
                break;
            }
            rows.push(EachRow {
                key: String::new(),
                index: j as i64,
                fullkey: format!("{}[{}]", zbase, j),
                path: zbase.to_string(),
                id: cur,
                ty: get_type(a, n, cur),
                value: decode_element(a, n, cur, c_end),
            });
            cur = c_end;
        } else {
            let ks = key_str(a, n, cur);
            let v_off = f::skip_one(a, n, cur);
            if v_off == 0 {
                break;
            }
            let p_end = f::skip_one(a, n, v_off);
            if p_end == 0 {
                break;
            }
            let key = ks.unwrap_or_else(|| "?".to_string());
            rows.push(EachRow {
                fullkey: format!("{}.{}", zbase, key),
                key,
                index: j as i64,
                path: zbase.to_string(),
                id: v_off,
                ty: get_type(a, n, v_off),
                value: decode_element(a, n, v_off, p_end),
            });
            cur = p_end;
        }
    }
    rows
}

pub fn tree_walk(
    a: &[u8],
    n: usize,
    ioff: usize,
    zfull: &str,
    zpar_path: &str,
    depth: i32,
    rows: &mut Vec<EachRow>,
) {
    if depth > f::MAX_DEPTH || ioff >= n {
        return;
    }
    let iend = f::skip_one(a, n, ioff);
    if iend == 0 {
        return;
    }

    rows.push(EachRow {
        key: String::new(),
        index: 0,
        fullkey: zfull.to_string(),
        path: zpar_path.to_string(),
        id: ioff,
        ty: get_type(a, n, ioff),
        value: decode_element(a, n, ioff, iend),
    });

    let c = container(a, n, ioff);
    if !c.is_arr && !c.is_map {
        return;
    }

    let remaining = if c.data_off <= n { n - c.data_off } else { 0 };
    let min_bytes = if c.is_map { 2 } else { 1 };
    if c.count > remaining / min_bytes + 1 {
        return;
    }

    let mut cur = c.data_off;
    for j in 0..c.count {
        if cur >= n {
            break;
        }
        if c.is_arr {
            let c_end = f::skip_one(a, n, cur);
            if c_end == 0 {
                break;
            }
            tree_walk(
                a,
                n,
                cur,
                &format!("{}[{}]", zfull, j),
                zfull,
                depth + 1,
                rows,
            );
            cur = c_end;
        } else {
            let ks = key_str(a, n, cur);
            let v_off = f::skip_one(a, n, cur);
            if v_off == 0 {
                break;
            }
            let p_end = f::skip_one(a, n, v_off);
            if p_end == 0 {
                break;
            }
            let key = ks.unwrap_or_else(|| "?".to_string());
            tree_walk(
                a,
                n,
                v_off,
                &format!("{}.{}", zfull, key),
                zfull,
                depth + 1,
                rows,
            );
            cur = p_end;
        }
    }
}
