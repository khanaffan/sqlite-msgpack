"""Internal: decoding & inspection (mirrors msgpack_blob_decode.cpp)."""

from __future__ import annotations

import struct
from typing import Optional, Tuple

from . import _format as F
from .value import Type, Value

__all__ = [
    "RC_OK",
    "RC_ERROR",
    "RC_NOTFOUND",
    "is_valid",
    "error_position",
    "get_type",
    "get_type_str",
    "get_container_count",
    "path_step",
    "lookup",
    "decode_element",
]

RC_OK = 0
RC_ERROR = 1
RC_NOTFOUND = 2

_I64_MAX = (1 << 63) - 1


def is_valid(a, n: int) -> bool:
    if n == 0:
        return False
    return F.skip_one(a, n, 0) == n


def error_position(a, n: int) -> int:
    if n == 0:
        return 0
    if F.skip_one(a, n, 0) == n:
        return 0
    i = 0
    while i < n:
        nxt = F.skip_one(a, n, i)
        if not nxt:
            return i
        i = nxt
    return 0


def _is_timestamp_ext(a, n: int, i: int) -> bool:
    if i >= n:
        return False
    b = a[i]
    if b == F.MP_FIXEXT4 and i + 6 <= n and a[i + 1] == F.MP_TIMESTAMP_TYPE:
        return True
    if b == F.MP_FIXEXT8 and i + 10 <= n and a[i + 1] == F.MP_TIMESTAMP_TYPE:
        return True
    if b == F.MP_EXT8 and i + 3 <= n and a[i + 1] == 12 and a[i + 2] == F.MP_TIMESTAMP_TYPE:
        return True
    return False


def _decode_timestamp(a, n: int, i: int) -> Optional[Tuple[int, int]]:
    if i >= n:
        return None
    b = a[i]
    if b == F.MP_FIXEXT4 and i + 6 <= n and a[i + 1] == F.MP_TIMESTAMP_TYPE:
        return (F.read32(a, i + 2), 0)
    if b == F.MP_FIXEXT8 and i + 10 <= n and a[i + 1] == F.MP_TIMESTAMP_TYPE:
        v = F.read64(a, i + 2)
        return (v & 0x3FFFFFFFF, v >> 34)
    if b == F.MP_EXT8 and i + 15 <= n and a[i + 1] == 12 and a[i + 2] == F.MP_TIMESTAMP_TYPE:
        nsec = F.read32(a, i + 3)
        sec = F.read64(a, i + 7)
        if sec >= (1 << 63):
            sec -= 1 << 64
        return (sec, nsec)
    return None


def get_type(a, n: int, i: int) -> Type:
    if i >= n:
        return Type.NIL
    b = a[i]
    if b == F.MP_NIL:
        return Type.NIL
    if b == F.MP_TRUE:
        return Type.TRUE
    if b == F.MP_FALSE:
        return Type.FALSE
    if b <= 0x7F or b >= 0xE0:
        return Type.INTEGER
    if 0xA0 <= b <= 0xBF:
        return Type.STRING
    if 0x90 <= b <= 0x9F:
        return Type.ARRAY
    if 0x80 <= b <= 0x8F:
        return Type.MAP
    if b in (F.MP_UINT8, F.MP_UINT16, F.MP_UINT32, F.MP_UINT64,
             F.MP_INT8, F.MP_INT16, F.MP_INT32, F.MP_INT64):
        return Type.INTEGER
    if b == F.MP_FLOAT32:
        return Type.FLOAT32
    if b == F.MP_FLOAT64:
        return Type.REAL
    if b in (F.MP_STR8, F.MP_STR16, F.MP_STR32):
        return Type.STRING
    if b in (F.MP_BIN8, F.MP_BIN16, F.MP_BIN32):
        return Type.BINARY
    if b in (F.MP_ARRAY16, F.MP_ARRAY32):
        return Type.ARRAY
    if b in (F.MP_MAP16, F.MP_MAP32):
        return Type.MAP
    if b in (F.MP_EXT8, F.MP_EXT16, F.MP_EXT32, F.MP_FIXEXT1, F.MP_FIXEXT2,
             F.MP_FIXEXT4, F.MP_FIXEXT8, F.MP_FIXEXT16):
        return Type.TIMESTAMP if _is_timestamp_ext(a, n, i) else Type.EXT
    return Type.NIL


def get_type_str(a, n: int, i: int) -> str:
    return get_type(a, n, i).value


def get_container_count(a, n: int, i: int) -> int:
    if i >= n:
        return -1
    b = a[i]
    if 0x90 <= b <= 0x9F:
        return b & 0x0F
    if 0x80 <= b <= 0x8F:
        return b & 0x0F
    if b == F.MP_ARRAY16 and i + 3 <= n:
        return F.read16(a, i + 1)
    if b == F.MP_ARRAY32 and i + 5 <= n:
        return F.read32(a, i + 1)
    if b == F.MP_MAP16 and i + 3 <= n:
        return F.read16(a, i + 1)
    if b == F.MP_MAP32 and i + 5 <= n:
        return F.read32(a, i + 1)
    return -1


def path_step(zpath: str, pi: int):
    """Parse one step of $.key[idx] syntax.

    Returns ``(kind, new_pi, key, idx)`` where *kind* is ``0`` (end),
    ``-1`` (error), ``'k'`` (key) or ``'i'`` (index).
    """
    i = pi
    if i >= len(zpath):
        return (0, i, None, 0)
    c = zpath[i]
    if c == ".":
        i += 1
        start = i
        while i < len(zpath) and zpath[i] not in ".[":
            i += 1
        return ("k", i, zpath[start:i], 0)
    if c == "[":
        idx = 0
        has_digit = False
        i += 1
        while i < len(zpath) and "0" <= zpath[i] <= "9":
            idx = idx * 10 + (ord(zpath[i]) - 48)
            i += 1
            has_digit = True
        if not has_digit or i >= len(zpath) or zpath[i] != "]":
            return (-1, i, None, 0)
        i += 1
        return ("i", i, None, idx)
    return (-1, i, None, 0)


def _key_at(a, n: int, i: int):
    """Return (key_bytes, value_offset) for a map key at *i*, or (None, off)."""
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
    if klen > n - koff:
        return None
    return bytes(a[koff:koff + klen])


def lookup(a, n: int, iroot: int, zpath: str):
    """Resolve *zpath* to a byte range. Returns ``(rc, iStart, iEnd)``."""
    if not zpath or zpath[0] != "$":
        return (RC_ERROR, 0, 0)
    icur = iroot
    pi = 1

    while True:
        kind, pi, key, idx = path_step(zpath, pi)

        if kind == 0:
            inext = F.skip_one(a, n, icur)
            istart = icur
            iend = inext if inext else n
            return ((RC_OK if (inext or icur == n) else RC_ERROR), istart, iend)
        if kind == -1:
            return (RC_ERROR, 0, 0)
        if icur >= n:
            return (RC_NOTFOUND, 0, 0)

        if kind == "i":
            b = a[icur]
            if 0x90 <= b <= 0x9F:
                count = b & 0x0F
                elem_off = icur + 1
            elif b == F.MP_ARRAY16:
                if icur + 3 > n:
                    return (RC_ERROR, 0, 0)
                count = F.read16(a, icur + 1)
                elem_off = icur + 3
            elif b == F.MP_ARRAY32:
                if icur + 5 > n:
                    return (RC_ERROR, 0, 0)
                count = F.read32(a, icur + 1)
                elem_off = icur + 5
            else:
                return (RC_NOTFOUND, 0, 0)
            if idx < 0 or idx >= count:
                return (RC_NOTFOUND, 0, 0)
            icur = elem_off
            for _ in range(idx):
                icur = F.skip_one(a, n, icur)
                if not icur:
                    return (RC_ERROR, 0, 0)
        else:
            b = a[icur]
            if 0x80 <= b <= 0x8F:
                count = b & 0x0F
                elem_off = icur + 1
            elif b == F.MP_MAP16:
                if icur + 3 > n:
                    return (RC_ERROR, 0, 0)
                count = F.read16(a, icur + 1)
                elem_off = icur + 3
            elif b == F.MP_MAP32:
                if icur + 5 > n:
                    return (RC_ERROR, 0, 0)
                count = F.read32(a, icur + 1)
                elem_off = icur + 5
            else:
                return (RC_NOTFOUND, 0, 0)
            key_bytes = key.encode("utf-8", "surrogateescape")
            icur = elem_off
            found = False
            j = 0
            while j < count and not found:
                if icur >= n:
                    return (RC_ERROR, 0, 0)
                kstr = _key_at(a, n, icur)
                val_off = F.skip_one(a, n, icur)
                if not val_off:
                    return (RC_ERROR, 0, 0)
                if kstr is not None and kstr == key_bytes:
                    icur = val_off
                    found = True
                else:
                    icur = F.skip_one(a, n, val_off)
                    if not icur:
                        return (RC_ERROR, 0, 0)
                j += 1
            if not found:
                return (RC_NOTFOUND, 0, 0)


def decode_element(a, n: int, istart: int, iend: int) -> Value:
    if istart >= n or istart >= iend:
        return Value.nil()
    b = a[istart]

    if b == F.MP_NIL:
        return Value.nil()
    if b == F.MP_FALSE:
        return Value.boolean(False)
    if b == F.MP_TRUE:
        return Value.boolean(True)
    if b <= 0x7F:
        return Value.integer(b)
    if b >= 0xE0:
        return Value.integer(b - 256)

    if b == F.MP_UINT8:
        if istart + 2 <= n:
            return Value.integer(a[istart + 1])
    elif b == F.MP_UINT16:
        if istart + 3 <= n:
            return Value.integer(F.read16(a, istart + 1))
    elif b == F.MP_UINT32:
        if istart + 5 <= n:
            return Value.integer(F.read32(a, istart + 1))
    elif b == F.MP_UINT64:
        if istart + 9 <= n:
            return Value.unsigned_integer(F.read64(a, istart + 1))
    elif b == F.MP_INT8:
        if istart + 2 <= n:
            v = a[istart + 1]
            return Value.integer(v - 256 if v >= 128 else v)
    elif b == F.MP_INT16:
        if istart + 3 <= n:
            v = F.read16(a, istart + 1)
            return Value.integer(v - (1 << 16) if v >= (1 << 15) else v)
    elif b == F.MP_INT32:
        if istart + 5 <= n:
            v = F.read32(a, istart + 1)
            return Value.integer(v - (1 << 32) if v >= (1 << 31) else v)
    elif b == F.MP_INT64:
        if istart + 9 <= n:
            v = F.read64(a, istart + 1)
            return Value.integer(v - (1 << 64) if v >= (1 << 63) else v)
    elif b == F.MP_FLOAT32:
        if istart + 5 <= n:
            f = struct.unpack(">f", bytes(a[istart + 1:istart + 5]))[0]
            return Value.real32(f)
    elif b == F.MP_FLOAT64:
        if istart + 9 <= n:
            d = struct.unpack(">d", bytes(a[istart + 1:istart + 9]))[0]
            return Value.real(d)

    # str → String
    soff = 0
    slen = 0
    if 0xA0 <= b <= 0xBF:
        slen = b & 0x1F
        soff = istart + 1
    elif b == F.MP_STR8 and istart + 2 <= n:
        slen = a[istart + 1]
        soff = istart + 2
    elif b == F.MP_STR16 and istart + 3 <= n:
        slen = F.read16(a, istart + 1)
        soff = istart + 3
    elif b == F.MP_STR32 and istart + 5 <= n:
        slen = F.read32(a, istart + 1)
        soff = istart + 5
    if soff:
        if slen > n - soff:
            slen = n - soff
        return Value.string(bytes(a[soff:soff + slen]))

    # bin → Binary (payload only)
    boff = 0
    blen = 0
    if b == F.MP_BIN8 and istart + 2 <= n:
        blen = a[istart + 1]
        boff = istart + 2
    elif b == F.MP_BIN16 and istart + 3 <= n:
        blen = F.read16(a, istart + 1)
        boff = istart + 3
    elif b == F.MP_BIN32 and istart + 5 <= n:
        blen = F.read32(a, istart + 1)
        boff = istart + 5
    if boff:
        if blen > n - boff:
            blen = n - boff
        return Value.binary(bytes(a[boff:boff + blen]))

    # timestamp ext
    ts = _decode_timestamp(a, n, istart)
    if ts is not None:
        return Value.timestamp(ts[0], ts[1])

    # ext → Ext (type code + payload)
    tc = 0
    elen = 0
    eoff = 0
    if b == F.MP_FIXEXT1 and istart + 3 <= n:
        tc = a[istart + 1]; elen = 1; eoff = istart + 2
    elif b == F.MP_FIXEXT2 and istart + 4 <= n:
        tc = a[istart + 1]; elen = 2; eoff = istart + 2
    elif b == F.MP_FIXEXT4 and istart + 6 <= n:
        tc = a[istart + 1]; elen = 4; eoff = istart + 2
    elif b == F.MP_FIXEXT8 and istart + 10 <= n:
        tc = a[istart + 1]; elen = 8; eoff = istart + 2
    elif b == F.MP_FIXEXT16 and istart + 18 <= n:
        tc = a[istart + 1]; elen = 16; eoff = istart + 2
    elif b == F.MP_EXT8 and istart + 3 <= n:
        elen = a[istart + 1]; tc = a[istart + 2]; eoff = istart + 3
    elif b == F.MP_EXT16 and istart + 4 <= n:
        elen = F.read16(a, istart + 1); tc = a[istart + 3]; eoff = istart + 4
    elif b == F.MP_EXT32 and istart + 6 <= n:
        elen = F.read32(a, istart + 1); tc = a[istart + 5]; eoff = istart + 6
    if eoff:
        if elen > n - eoff:
            elen = n - eoff
        tc_signed = tc - 256 if tc >= 128 else tc
        return Value.ext(tc_signed, bytes(a[eoff:eoff + elen]))

    # containers → raw binary blob (includes header)
    return Value.binary(bytes(a[istart:iend]))
