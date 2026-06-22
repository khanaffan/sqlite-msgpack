"""Internal: MessagePack format constants, byte-order helpers and skip_one.

Private to the implementation; not part of the public API. Mirrors the shared
internals from ``cpp/src/msgpack_blob_detail.hpp`` and the skip routine from the
decode module.
"""

from __future__ import annotations

__all__ = [
    "MAX_DEPTH",
    "MAX_OUTPUT",
    "skip_one",
    "read16",
    "read32",
    "read64",
    "w16",
    "w32",
    "w64",
]

# Limits (match the SQLite extension and C++ library)
MAX_DEPTH = 200
MAX_OUTPUT = 64 * 1024 * 1024

# MessagePack format bytes
MP_NIL = 0xC0
MP_FALSE = 0xC2
MP_TRUE = 0xC3
MP_BIN8 = 0xC4
MP_BIN16 = 0xC5
MP_BIN32 = 0xC6
MP_EXT8 = 0xC7
MP_EXT16 = 0xC8
MP_EXT32 = 0xC9
MP_FLOAT32 = 0xCA
MP_FLOAT64 = 0xCB
MP_UINT8 = 0xCC
MP_UINT16 = 0xCD
MP_UINT32 = 0xCE
MP_UINT64 = 0xCF
MP_INT8 = 0xD0
MP_INT16 = 0xD1
MP_INT32 = 0xD2
MP_INT64 = 0xD3
MP_FIXEXT1 = 0xD4
MP_FIXEXT2 = 0xD5
MP_FIXEXT4 = 0xD6
MP_FIXEXT8 = 0xD7
MP_FIXEXT16 = 0xD8
MP_STR8 = 0xD9
MP_STR16 = 0xDA
MP_STR32 = 0xDB
MP_ARRAY16 = 0xDC
MP_ARRAY32 = 0xDD
MP_MAP16 = 0xDE
MP_MAP32 = 0xDF

MP_FIXMAP_MASK = 0x80
MP_FIXARRAY_MASK = 0x90
MP_FIXSTR_MASK = 0xA0

MP_TIMESTAMP_TYPE = 0xFF


# ── big-endian read helpers ───────────────────────────────────────────
def read16(a, i: int) -> int:
    return (a[i] << 8) | a[i + 1]


def read32(a, i: int) -> int:
    return (a[i] << 24) | (a[i + 1] << 16) | (a[i + 2] << 8) | a[i + 3]


def read64(a, i: int) -> int:
    return (read32(a, i) << 32) | read32(a, i + 4)


# ── big-endian write helpers (return bytes) ───────────────────────────
def w16(v: int) -> bytes:
    return (v & 0xFFFF).to_bytes(2, "big")


def w32(v: int) -> bytes:
    return (v & 0xFFFFFFFF).to_bytes(4, "big")


def w64(v: int) -> bytes:
    return (v & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "big")


# ── skip_one — return the offset just past one complete element ───────
def skip_one(a, n: int, i: int) -> int:
    """Return offset just past the element at *i*, or 0 on malformed input."""
    return _skip_one_d(a, n, i, 0)


def _skip_one_d(a, n: int, i: int, depth: int) -> int:
    if depth > MAX_DEPTH:
        return 0
    if i >= n:
        return 0
    b = a[i]
    i += 1

    if b <= 0x7F:  # positive fixint
        return i
    if b >= 0xE0:  # negative fixint
        return i

    if b in (MP_NIL, MP_FALSE, MP_TRUE):
        return i
    if b == MP_FLOAT32:
        return i + 4 if i + 4 <= n else 0
    if b in (MP_FLOAT64, MP_INT64, MP_UINT64):
        return i + 8 if i + 8 <= n else 0
    if b in (MP_UINT8, MP_INT8):
        return i + 1 if i + 1 <= n else 0
    if b in (MP_UINT16, MP_INT16):
        return i + 2 if i + 2 <= n else 0
    if b in (MP_UINT32, MP_INT32):
        return i + 4 if i + 4 <= n else 0

    if b == MP_BIN8 or b == MP_STR8:
        if i + 1 > n:
            return 0
        sz = a[i]
        i += 1
        return i + sz if sz <= n - i else 0
    if b == MP_BIN16 or b == MP_STR16:
        if i + 2 > n:
            return 0
        sz = read16(a, i)
        i += 2
        return i + sz if sz <= n - i else 0
    if b == MP_BIN32 or b == MP_STR32:
        if i + 4 > n:
            return 0
        sz = read32(a, i)
        i += 4
        return i + sz if sz <= n - i else 0

    if b == MP_FIXEXT1:
        return i + 2 if i + 2 <= n else 0
    if b == MP_FIXEXT2:
        return i + 3 if i + 3 <= n else 0
    if b == MP_FIXEXT4:
        return i + 5 if i + 5 <= n else 0
    if b == MP_FIXEXT8:
        return i + 9 if i + 9 <= n else 0
    if b == MP_FIXEXT16:
        return i + 17 if i + 17 <= n else 0
    if b == MP_EXT8:
        if i + 2 > n:
            return 0
        sz = a[i]
        i += 2
        return i + sz if sz <= n - i else 0
    if b == MP_EXT16:
        if i + 3 > n:
            return 0
        sz = read16(a, i)
        i += 3
        return i + sz if sz <= n - i else 0
    if b == MP_EXT32:
        if i + 5 > n:
            return 0
        sz = read32(a, i)
        i += 5
        return i + sz if sz <= n - i else 0

    # fixstr
    if 0xA0 <= b <= 0xBF:
        sz = b & 0x1F
        return i + sz if sz <= n - i else 0

    # fixarray
    if 0x90 <= b <= 0x9F:
        count = b & 0x0F
        for _ in range(count):
            i = _skip_one_d(a, n, i, depth + 1)
            if not i:
                return 0
        return i

    # fixmap
    if 0x80 <= b <= 0x8F:
        count = b & 0x0F
        for _ in range(count):
            i = _skip_one_d(a, n, i, depth + 1)
            if not i:
                return 0
            i = _skip_one_d(a, n, i, depth + 1)
            if not i:
                return 0
        return i

    # array16/32
    if b in (MP_ARRAY16, MP_ARRAY32):
        if b == MP_ARRAY16:
            if i + 2 > n:
                return 0
            count = read16(a, i)
            i += 2
        else:
            if i + 4 > n:
                return 0
            count = read32(a, i)
            i += 4
        for _ in range(count):
            i = _skip_one_d(a, n, i, depth + 1)
            if not i:
                return 0
        return i

    # map16/32
    if b in (MP_MAP16, MP_MAP32):
        if b == MP_MAP16:
            if i + 2 > n:
                return 0
            count = read16(a, i)
            i += 2
        else:
            if i + 4 > n:
                return 0
            count = read32(a, i)
            i += 4
        for _ in range(count):
            i = _skip_one_d(a, n, i, depth + 1)
            if not i:
                return 0
            i = _skip_one_d(a, n, i, depth + 1)
            if not i:
                return 0
        return i

    return 0
