//! Internal: copy-on-write mutation (mirrors `msgpack_blob_mutate.cpp`).

use crate::decode::{path_step, Step};
use crate::encode as e;
use crate::format as f;

pub const RC_OK: i32 = 0;
pub const RC_ERROR: i32 = 1;
pub const RC_NOTFOUND: i32 = 2;

pub const EDIT_SET: i32 = 0;
pub const EDIT_INSERT: i32 = 1;
pub const EDIT_REPLACE: i32 = 2;
pub const EDIT_REMOVE: i32 = 3;
pub const EDIT_ARRAY_INS: i32 = 4;

fn map_key(a: &[u8], n: usize, i: usize) -> Option<&[u8]> {
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
    // Bounds guard (matches decode::key_at): a truncated key length must not
    // slice past the end of the blob — the C++ reference defers to skip_one,
    // which fails the pair and aborts the edit gracefully.
    if klen > n - koff {
        return None;
    }
    Some(&a[koff..koff + klen])
}

struct StepResult {
    rc: i32,
    skip: bool,
}

fn edit_map(
    out: &mut Vec<u8>,
    a: &[u8],
    n: usize,
    icur: usize,
    zkey: &[u8],
    zpath: &[u8],
    pi: usize,
    new_bin: &[u8],
    mode: i32,
) -> i32 {
    if icur >= n {
        return RC_ERROR;
    }
    let b = a[icur];
    let (count, data_off) = if (0x80..=0x8f).contains(&b) {
        ((b & 0x0f) as usize, icur + 1)
    } else if b == f::MP_MAP16 {
        if icur + 3 > n {
            return RC_ERROR;
        }
        (f::read16(a, icur + 1) as usize, icur + 3)
    } else if b == f::MP_MAP32 {
        if icur + 5 > n {
            return RC_ERROR;
        }
        (f::read32(a, icur + 1) as usize, icur + 5)
    } else {
        if mode == EDIT_REPLACE || mode == EDIT_REMOVE {
            let iend = f::skip_one(a, n, icur);
            if iend != 0 {
                out.extend_from_slice(&a[icur..iend]);
            }
            return RC_OK;
        }
        return RC_ERROR;
    };

    let mut new_count = count as u32;
    let mut tmp = Vec::new();
    let mut cur2 = data_off;
    let mut found_key = false;

    for _ in 0..count {
        if cur2 >= n {
            return RC_ERROR;
        }
        let kstr = map_key(a, n, cur2);
        let val_off = f::skip_one(a, n, cur2);
        if val_off == 0 {
            return RC_ERROR;
        }
        let pair_end = f::skip_one(a, n, val_off);
        if pair_end == 0 {
            return RC_ERROR;
        }

        let is_match = kstr == Some(zkey);

        if is_match {
            found_key = true;
            if mode == EDIT_INSERT {
                tmp.extend_from_slice(&a[cur2..pair_end]);
            } else {
                let mut vbuf = Vec::new();
                let res = edit_step(&mut vbuf, a, n, val_off, zpath, pi, new_bin, mode);
                if res.rc != RC_OK {
                    return res.rc;
                }
                if res.skip {
                    new_count -= 1;
                } else {
                    tmp.extend_from_slice(&a[cur2..val_off]);
                    tmp.extend_from_slice(&vbuf);
                }
            }
        } else {
            tmp.extend_from_slice(&a[cur2..pair_end]);
        }
        cur2 = pair_end;
    }

    if !found_key {
        if mode == EDIT_SET || mode == EDIT_INSERT {
            if !matches!(path_step(zpath, pi).0, Step::End) {
                let iend = f::skip_one(a, n, icur);
                if iend != 0 {
                    out.extend_from_slice(&a[icur..iend]);
                }
                return RC_OK;
            }
            e::enc_string(&mut tmp, zkey);
            tmp.extend_from_slice(new_bin);
            new_count += 1;
        } else {
            let iend = f::skip_one(a, n, icur);
            if iend != 0 {
                out.extend_from_slice(&a[icur..iend]);
            }
            return RC_OK;
        }
    }

    e::enc_map_header(out, new_count);
    out.extend_from_slice(&tmp);
    RC_OK
}

fn edit_array(
    out: &mut Vec<u8>,
    a: &[u8],
    n: usize,
    icur: usize,
    step_idx: i64,
    zpath: &[u8],
    pi: usize,
    new_bin: &[u8],
    mode: i32,
) -> i32 {
    if icur >= n {
        return RC_ERROR;
    }
    let b = a[icur];
    let (count, data_off) = if (0x90..=0x9f).contains(&b) {
        ((b & 0x0f) as usize, icur + 1)
    } else if b == f::MP_ARRAY16 {
        if icur + 3 > n {
            return RC_ERROR;
        }
        (f::read16(a, icur + 1) as usize, icur + 3)
    } else if b == f::MP_ARRAY32 {
        if icur + 5 > n {
            return RC_ERROR;
        }
        (f::read32(a, icur + 1) as usize, icur + 5)
    } else {
        if mode == EDIT_REPLACE || mode == EDIT_REMOVE {
            let iend = f::skip_one(a, n, icur);
            if iend != 0 {
                out.extend_from_slice(&a[icur..iend]);
            }
            return RC_OK;
        }
        return RC_ERROR;
    };

    let mut new_count = count as u32;
    let mut tmp = Vec::new();
    let mut cur2 = data_off;
    let mut found_it = false;

    for j in 0..count {
        let e_end = f::skip_one(a, n, cur2);
        if e_end == 0 {
            return RC_ERROR;
        }

        if j as i64 == step_idx {
            found_it = true;
            if mode == EDIT_ARRAY_INS {
                tmp.extend_from_slice(new_bin);
                tmp.extend_from_slice(&a[cur2..e_end]);
                new_count += 1;
            } else if mode == EDIT_INSERT {
                tmp.extend_from_slice(&a[cur2..e_end]);
            } else {
                let mut ebuf = Vec::new();
                let res = edit_step(&mut ebuf, a, n, cur2, zpath, pi, new_bin, mode);
                if res.rc != RC_OK {
                    return res.rc;
                }
                if res.skip {
                    new_count -= 1;
                } else {
                    tmp.extend_from_slice(&ebuf);
                }
            }
        } else {
            tmp.extend_from_slice(&a[cur2..e_end]);
        }
        cur2 = e_end;
    }

    if !found_it {
        if mode == EDIT_ARRAY_INS {
            tmp.extend_from_slice(new_bin);
            new_count += 1;
        } else if (mode == EDIT_SET || mode == EDIT_INSERT) && step_idx == count as i64 {
            tmp.extend_from_slice(new_bin);
            new_count += 1;
        } else if mode == EDIT_REPLACE || mode == EDIT_REMOVE {
            let iend = f::skip_one(a, n, icur);
            if iend != 0 {
                out.extend_from_slice(&a[icur..iend]);
            }
            return RC_OK;
        } else {
            return RC_NOTFOUND;
        }
    }

    e::enc_array_header(out, new_count);
    out.extend_from_slice(&tmp);
    RC_OK
}

fn edit_step(
    out: &mut Vec<u8>,
    a: &[u8],
    n: usize,
    icur: usize,
    zpath: &[u8],
    pi: usize,
    new_bin: &[u8],
    mode: i32,
) -> StepResult {
    let (step, npi) = path_step(zpath, pi);

    match step {
        Step::End => {
            if mode == EDIT_REMOVE {
                return StepResult {
                    rc: RC_OK,
                    skip: true,
                };
            }
            if mode == EDIT_ARRAY_INS {
                return StepResult {
                    rc: RC_ERROR,
                    skip: false,
                };
            }
            if mode == EDIT_INSERT {
                let iend = f::skip_one(a, n, icur);
                if iend != 0 {
                    out.extend_from_slice(&a[icur..iend]);
                }
                return StepResult {
                    rc: RC_OK,
                    skip: false,
                };
            }
            out.extend_from_slice(new_bin);
            StepResult {
                rc: RC_OK,
                skip: false,
            }
        }
        Step::Error => StepResult {
            rc: RC_ERROR,
            skip: false,
        },
        Step::Key(key) => {
            let rc = edit_map(out, a, n, icur, key.as_bytes(), zpath, npi, new_bin, mode);
            StepResult { rc, skip: false }
        }
        Step::Index(idx) => {
            let rc = edit_array(out, a, n, icur, idx, zpath, npi, new_bin, mode);
            StepResult { rc, skip: false }
        }
    }
}

/// Apply a path-targeted edit; returns `(rc, out_bytes)`.
pub fn apply_edit(a: &[u8], n: usize, zpath: &str, new_bin: &[u8], mode: i32) -> (i32, Vec<u8>) {
    let zb = zpath.as_bytes();
    if zb.is_empty() || zb[0] != b'$' {
        return (RC_ERROR, Vec::new());
    }
    let mut out = Vec::new();
    let res = edit_step(&mut out, a, n, 0, zb, 1, new_bin, mode);
    (res.rc, out)
}

// ── merge_patch (RFC 7386) ──────────────────────────────────────────
/// Apply an RFC 7386 merge patch; returns `(rc, out_bytes)`.
pub fn merge_patch(
    a: &[u8],
    n: usize,
    ia: usize,
    p: &[u8],
    np: usize,
    ip: usize,
) -> (i32, Vec<u8>) {
    let mut out = Vec::new();
    let rc = merge_patch_into(&mut out, a, n, ia, p, np, ip, 0);
    (rc, out)
}

#[allow(clippy::too_many_arguments)]
fn merge_patch_into(
    out: &mut Vec<u8>,
    a: &[u8],
    n: usize,
    ia: usize,
    p: &[u8],
    np: usize,
    ip: usize,
    depth: i32,
) -> i32 {
    if ip >= np {
        return RC_ERROR;
    }
    if depth > f::MAX_DEPTH {
        return RC_ERROR;
    }
    let pb = p[ip];

    if pb == f::MP_NIL {
        out.push(f::MP_NIL);
        return RC_OK;
    }

    let p_is_map = (0x80..=0x8f).contains(&pb) || pb == f::MP_MAP16 || pb == f::MP_MAP32;
    if !p_is_map {
        let p_end = f::skip_one(p, np, ip);
        if p_end != 0 {
            out.extend_from_slice(&p[ip..p_end]);
        }
        return RC_OK;
    }

    let ab = if ia < n { a[ia] } else { 0 };
    let mut a_is_map = (0x80..=0x8f).contains(&ab) || ab == f::MP_MAP16 || ab == f::MP_MAP32;

    let (p_count, p_data_off) = if (0x80..=0x8f).contains(&pb) {
        ((pb & 0x0f) as usize, ip + 1)
    } else if pb == f::MP_MAP16 {
        if ip + 3 > np {
            return RC_ERROR;
        }
        (f::read16(p, ip + 1) as usize, ip + 3)
    } else {
        if ip + 5 > np {
            return RC_ERROR;
        }
        (f::read32(p, ip + 1) as usize, ip + 5)
    };

    let mut a_count = 0usize;
    let mut a_data_off = 0usize;
    if a_is_map {
        if (0x80..=0x8f).contains(&ab) {
            a_count = (ab & 0x0f) as usize;
            a_data_off = ia + 1;
        } else if ab == f::MP_MAP16 {
            if ia + 3 > n {
                a_is_map = false;
            } else {
                a_count = f::read16(a, ia + 1) as usize;
                a_data_off = ia + 3;
            }
        } else if ia + 5 > n {
            a_is_map = false;
        } else {
            a_count = f::read32(a, ia + 1) as usize;
            a_data_off = ia + 5;
        }
    }

    // Pre-scan patch keys.
    if p_count > (np - p_data_off) / 2 + 1 {
        return RC_ERROR;
    }
    // (key_off, val_off, pair_end, matched); key bytes resolved on demand.
    let mut p_idx: Vec<(usize, usize, usize, bool)> = Vec::with_capacity(p_count);
    let mut pc2 = p_data_off;
    for _ in 0..p_count {
        if pc2 >= np {
            return RC_ERROR;
        }
        let val_off = f::skip_one(p, np, pc2);
        if val_off == 0 {
            return RC_ERROR;
        }
        let pair_end = f::skip_one(p, np, val_off);
        if pair_end == 0 {
            return RC_ERROR;
        }
        p_idx.push((pc2, val_off, pair_end, false));
        pc2 = pair_end;
    }

    let mut tmp = Vec::new();
    let mut new_count: u32 = 0;

    if a_is_map {
        let mut ac = a_data_off;
        for _ in 0..a_count {
            if ac >= n {
                return RC_ERROR;
            }
            let kstr = map_key(a, n, ac);
            let a_val_off = f::skip_one(a, n, ac);
            if a_val_off == 0 {
                return RC_ERROR;
            }
            let a_pair_end = f::skip_one(a, n, a_val_off);
            if a_pair_end == 0 {
                return RC_ERROR;
            }

            let mut found_in_patch = false;
            let mut patch_is_nil = false;
            let mut p_match_val = 0usize;
            for entry in p_idx.iter_mut() {
                let pkey = map_key(p, np, entry.0);
                if pkey.is_some() && kstr.is_some() && pkey == kstr {
                    found_in_patch = true;
                    p_match_val = entry.1;
                    patch_is_nil = entry.1 < np && p[entry.1] == f::MP_NIL;
                    entry.3 = true;
                    break;
                }
            }

            if found_in_patch && patch_is_nil {
                // drop
            } else if found_in_patch {
                let mut mb = Vec::new();
                let mrc = merge_patch_into(&mut mb, a, n, a_val_off, p, np, p_match_val, depth + 1);
                if mrc == RC_OK {
                    tmp.extend_from_slice(&a[ac..a_val_off]);
                    tmp.extend_from_slice(&mb);
                    new_count += 1;
                }
            } else {
                tmp.extend_from_slice(&a[ac..a_pair_end]);
                new_count += 1;
            }
            ac = a_pair_end;
        }
    }

    for entry in p_idx.iter() {
        if !entry.3 && entry.1 < np && p[entry.1] != f::MP_NIL {
            tmp.extend_from_slice(&p[entry.0..entry.2]);
            new_count += 1;
        }
    }

    e::enc_map_header(out, new_count);
    out.extend_from_slice(&tmp);
    RC_OK
}
