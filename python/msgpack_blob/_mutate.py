"""Internal: copy-on-write mutation (mirrors msgpack_blob_mutate.cpp)."""

from __future__ import annotations

from . import _format as F
from . import _encode as E
from ._decode import path_step

__all__ = [
    "EDIT_SET",
    "EDIT_INSERT",
    "EDIT_REPLACE",
    "EDIT_REMOVE",
    "EDIT_ARRAY_INS",
    "apply_edit",
    "merge_patch",
]

RC_OK = 0
RC_ERROR = 1
RC_NOTFOUND = 2

EDIT_SET = 0
EDIT_INSERT = 1
EDIT_REPLACE = 2
EDIT_REMOVE = 3
EDIT_ARRAY_INS = 4


def _map_key(a, n: int, i: int):
    kb = a[i]
    if 0xA0 <= kb <= 0xBF:
        klen = kb & 0x1F
        koff = i + 1
    elif kb == F.MP_STR8 and i + 2 <= n:
        klen = a[i + 1]
        koff = i + 2
    elif kb == F.MP_STR16 and i + 3 <= n:
        klen = F.read16(a, i + 1)
        koff = i + 3
    elif kb == F.MP_STR32 and i + 5 <= n:
        klen = F.read32(a, i + 1)
        koff = i + 5
    else:
        return None
    return bytes(a[koff:koff + klen])


def _edit_map(out, a, n, icur, zkey, zpath, pi, new_bin, mode):
    b = a[icur] if icur < n else 0
    if icur >= n:
        return RC_ERROR
    if 0x80 <= b <= 0x8F:
        count = b & 0x0F
        data_off = icur + 1
    elif b == F.MP_MAP16:
        if icur + 3 > n:
            return RC_ERROR
        count = F.read16(a, icur + 1)
        data_off = icur + 3
    elif b == F.MP_MAP32:
        if icur + 5 > n:
            return RC_ERROR
        count = F.read32(a, icur + 1)
        data_off = icur + 5
    else:
        if mode in (EDIT_REPLACE, EDIT_REMOVE):
            iend = F.skip_one(a, n, icur)
            if iend:
                out += a[icur:iend]
            return RC_OK
        return RC_ERROR

    new_count = count
    tmp = bytearray()
    cur2 = data_off
    found_key = False

    for _ in range(count):
        if cur2 >= n:
            return RC_ERROR
        kstr = _map_key(a, n, cur2)
        val_off = F.skip_one(a, n, cur2)
        if not val_off:
            return RC_ERROR
        pair_end = F.skip_one(a, n, val_off)
        if not pair_end:
            return RC_ERROR

        is_match = kstr is not None and kstr == zkey

        if is_match:
            found_key = True
            if mode == EDIT_INSERT:
                tmp += a[cur2:pair_end]
            else:
                vbuf = bytearray()
                rc, skip = _edit_step(vbuf, a, n, val_off, zpath, pi, new_bin, mode)
                if rc != RC_OK:
                    return rc
                if skip:
                    new_count -= 1
                else:
                    tmp += a[cur2:val_off]
                    tmp += vbuf
        else:
            tmp += a[cur2:pair_end]
        cur2 = pair_end

    if not found_key:
        if mode in (EDIT_SET, EDIT_INSERT):
            kind = path_step(zpath, pi)[0]
            if kind != 0:
                iend = F.skip_one(a, n, icur)
                if iend:
                    out += a[icur:iend]
                return RC_OK
            E.enc_string(tmp, zkey)
            tmp += new_bin
            new_count += 1
        else:
            iend = F.skip_one(a, n, icur)
            if iend:
                out += a[icur:iend]
            return RC_OK

    E.enc_map_header(out, new_count)
    out += tmp
    return RC_OK


def _edit_array(out, a, n, icur, step_idx, zpath, pi, new_bin, mode):
    if icur >= n:
        return RC_ERROR
    b = a[icur]
    if 0x90 <= b <= 0x9F:
        count = b & 0x0F
        data_off = icur + 1
    elif b == F.MP_ARRAY16:
        if icur + 3 > n:
            return RC_ERROR
        count = F.read16(a, icur + 1)
        data_off = icur + 3
    elif b == F.MP_ARRAY32:
        if icur + 5 > n:
            return RC_ERROR
        count = F.read32(a, icur + 1)
        data_off = icur + 5
    else:
        if mode in (EDIT_REPLACE, EDIT_REMOVE):
            iend = F.skip_one(a, n, icur)
            if iend:
                out += a[icur:iend]
            return RC_OK
        return RC_ERROR

    new_count = count
    tmp = bytearray()
    cur2 = data_off
    found_it = False

    for j in range(count):
        e_end = F.skip_one(a, n, cur2)
        if not e_end:
            return RC_ERROR

        if j == step_idx:
            found_it = True
            if mode == EDIT_ARRAY_INS:
                tmp += new_bin
                tmp += a[cur2:e_end]
                new_count += 1
            elif mode == EDIT_INSERT:
                tmp += a[cur2:e_end]
            else:
                ebuf = bytearray()
                rc, skip = _edit_step(ebuf, a, n, cur2, zpath, pi, new_bin, mode)
                if rc != RC_OK:
                    return rc
                if skip:
                    new_count -= 1
                else:
                    tmp += ebuf
        else:
            tmp += a[cur2:e_end]
        cur2 = e_end

    if not found_it:
        if mode == EDIT_ARRAY_INS:
            tmp += new_bin
            new_count += 1
        elif mode in (EDIT_SET, EDIT_INSERT) and step_idx == count:
            tmp += new_bin
            new_count += 1
        elif mode in (EDIT_REPLACE, EDIT_REMOVE):
            iend = F.skip_one(a, n, icur)
            if iend:
                out += a[icur:iend]
            return RC_OK
        else:
            return RC_NOTFOUND

    E.enc_array_header(out, new_count)
    out += tmp
    return RC_OK


# path_step needs the whole path string; thread it through the recursion to
# keep the helpers reentrant.


def _edit_step(out, a, n, icur, zpath, pi, new_bin, mode):
    """Returns (rc, skip)."""
    kind, pi2, key, step_idx = path_step(zpath, pi)

    if kind == 0:
        if mode == EDIT_REMOVE:
            return (RC_OK, True)
        if mode == EDIT_ARRAY_INS:
            return (RC_ERROR, False)
        if mode == EDIT_INSERT:
            iend = F.skip_one(a, n, icur)
            if iend:
                out += a[icur:iend]
            return (RC_OK, False)
        out += new_bin
        return (RC_OK, False)
    if kind == -1:
        return (RC_ERROR, False)

    if kind == "k":
        zkey = key.encode("utf-8", "surrogateescape")
        rc = _edit_map(out, a, n, icur, zkey, zpath, pi2, new_bin, mode)
        return (rc, False)
    else:
        rc = _edit_array(out, a, n, icur, step_idx, zpath, pi2, new_bin, mode)
        return (rc, False)


def apply_edit(a, n, zpath, new_bin, mode):
    """Returns (rc, out_bytes)."""
    if not zpath or zpath[0] != "$":
        return (RC_ERROR, b"")
    out = bytearray()
    rc, _skip = _edit_step(out, a, n, 0, zpath, 1, new_bin, mode)
    return (rc, bytes(out))


# ── merge_patch (RFC 7386) ────────────────────────────────────────────
def merge_patch(a, n, ia, p, np, ip, depth):
    """Returns (rc, out_bytes)."""
    out = bytearray()
    rc = _merge_patch(out, a, n, ia, p, np, ip, depth)
    return (rc, bytes(out))


def _merge_patch(out, a, n, ia, p, np, ip, depth):
    if ip >= np:
        return RC_ERROR
    if depth > F.MAX_DEPTH:
        return RC_ERROR
    pb = p[ip]

    if pb == F.MP_NIL:
        out.append(F.MP_NIL)
        return RC_OK

    p_is_map = (0x80 <= pb <= 0x8F) or pb in (F.MP_MAP16, F.MP_MAP32)
    if not p_is_map:
        p_end = F.skip_one(p, np, ip)
        if p_end:
            out += p[ip:p_end]
        return RC_OK

    ab = a[ia] if ia < n else 0
    a_is_map = (0x80 <= ab <= 0x8F) or ab in (F.MP_MAP16, F.MP_MAP32)

    if 0x80 <= pb <= 0x8F:
        p_count = pb & 0x0F
        p_data_off = ip + 1
    elif pb == F.MP_MAP16:
        if ip + 3 > np:
            return RC_ERROR
        p_count = F.read16(p, ip + 1)
        p_data_off = ip + 3
    else:
        if ip + 5 > np:
            return RC_ERROR
        p_count = F.read32(p, ip + 1)
        p_data_off = ip + 5

    a_count = 0
    a_data_off = 0
    if a_is_map:
        if 0x80 <= ab <= 0x8F:
            a_count = ab & 0x0F
            a_data_off = ia + 1
        elif ab == F.MP_MAP16:
            if ia + 3 > n:
                a_is_map = False
            else:
                a_count = F.read16(a, ia + 1)
                a_data_off = ia + 3
        else:
            if ia + 5 > n:
                a_is_map = False
            else:
                a_count = F.read32(a, ia + 1)
                a_data_off = ia + 5

    # Pre-scan patch keys
    if p_count > (np - p_data_off) // 2 + 1:
        return RC_ERROR
    p_idx = []  # each: [key_bytes_or_None, key_off, val_off, pair_end, matched]
    pc2 = p_data_off
    for _ in range(p_count):
        if pc2 >= np:
            return RC_ERROR
        key = _map_key(p, np, pc2)
        val_off = F.skip_one(p, np, pc2)
        if not val_off:
            return RC_ERROR
        pair_end = F.skip_one(p, np, val_off)
        if not pair_end:
            return RC_ERROR
        p_idx.append([key, pc2, val_off, pair_end, False])
        pc2 = pair_end

    tmp = bytearray()
    new_count = 0

    # Phase 1: iterate target pairs
    if a_is_map:
        ac = a_data_off
        for _ in range(a_count):
            if ac >= n:
                return RC_ERROR
            kstr = _map_key(a, n, ac)
            a_val_off = F.skip_one(a, n, ac)
            if not a_val_off:
                return RC_ERROR
            a_pair_end = F.skip_one(a, n, a_val_off)
            if not a_pair_end:
                return RC_ERROR

            found_in_patch = False
            patch_is_nil = False
            p_match_val = 0
            for entry in p_idx:
                if entry[0] is not None and kstr is not None and entry[0] == kstr:
                    found_in_patch = True
                    p_match_val = entry[2]
                    patch_is_nil = entry[2] < np and p[entry[2]] == F.MP_NIL
                    entry[4] = True
                    break

            if found_in_patch and patch_is_nil:
                pass  # drop
            elif found_in_patch:
                mb = bytearray()
                mrc = _merge_patch(mb, a, n, a_val_off, p, np, p_match_val, depth + 1)
                if mrc == RC_OK:
                    tmp += a[ac:a_val_off]
                    tmp += mb
                    new_count += 1
            else:
                tmp += a[ac:a_pair_end]
                new_count += 1
            ac = a_pair_end

    # Phase 2: add unmatched patch pairs
    for entry in p_idx:
        if not entry[4] and entry[2] < np and p[entry[2]] != F.MP_NIL:
            tmp += p[entry[1]:entry[3]]
            new_count += 1

    E.enc_map_header(out, new_count)
    out += tmp
    return RC_OK
