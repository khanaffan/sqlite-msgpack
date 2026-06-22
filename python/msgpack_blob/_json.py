"""Internal: JSON conversion (mirrors msgpack_blob_json.cpp).

``to_json`` builds a ``bytearray`` exactly like the C++ implementation so float
formatting and string escaping stay byte-identical; ``from_json`` parses UTF-8
bytes into msgpack.
"""

from __future__ import annotations

import math
import struct

from . import _format as F
from . import _encode as E

__all__ = ["to_json", "from_json"]

RC_OK = 0
RC_ERROR = 1

_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
_HEX = b"0123456789abcdef"


# ── float formatting (mimics C printf %.<P>g) ─────────────────────────
def _fmt_g(d: float, precision: int) -> str:
    return "%.*g" % (precision, d)


def _fmt_double(d: float) -> str:
    s = _fmt_g(d, 17)
    if "." not in s and "e" not in s and "E" not in s:
        s = "%.1f" % d
    return s


def _fmt_float32(f: float) -> str:
    return _fmt_g(f, 7)


# ── JSON output ───────────────────────────────────────────────────────
def _escape_str(out: bytearray, s: bytes) -> None:
    out.append(0x22)  # "
    start = 0
    n = len(s)
    for j in range(n):
        c = s[j]
        if c >= 0x20 and c != 0x22 and c != 0x5C:
            continue
        if j > start:
            out += s[start:j]
        if c == 0x22:
            out += b'\\"'
        elif c == 0x5C:
            out += b"\\\\"
        elif c == 0x0A:
            out += b"\\n"
        elif c == 0x0D:
            out += b"\\r"
        elif c == 0x09:
            out += b"\\t"
        else:
            out += ("\\u%04x" % c).encode("ascii")
        start = j + 1
    if n > start:
        out += s[start:n]
    out.append(0x22)


def _newline(out: bytearray, depth: int, indent_w: int) -> None:
    out.append(0x0A)
    out += b" " * (depth * indent_w)


def _to_json_at(out: bytearray, a, n: int, i: int, pretty: bool, depth: int, indent_w: int) -> None:
    if i >= n or depth > F.MAX_DEPTH:
        out += b"null"
        return
    b = a[i]

    if b == F.MP_NIL:
        out += b"null"; return
    if b == F.MP_FALSE:
        out += b"false"; return
    if b == F.MP_TRUE:
        out += b"true"; return
    if b <= 0x7F:
        out += str(b).encode("ascii"); return
    if b >= 0xE0:
        out += str(b - 256).encode("ascii"); return

    if b == F.MP_UINT8:
        if i + 2 <= n:
            out += str(a[i + 1]).encode("ascii"); return
    elif b == F.MP_UINT16:
        if i + 3 <= n:
            out += str(F.read16(a, i + 1)).encode("ascii"); return
    elif b == F.MP_UINT32:
        if i + 5 <= n:
            out += str(F.read32(a, i + 1)).encode("ascii"); return
    elif b == F.MP_UINT64:
        if i + 9 <= n:
            out += str(F.read64(a, i + 1)).encode("ascii"); return
    elif b == F.MP_INT8:
        if i + 2 <= n:
            v = a[i + 1]
            out += str(v - 256 if v >= 128 else v).encode("ascii"); return
    elif b == F.MP_INT16:
        if i + 3 <= n:
            v = F.read16(a, i + 1)
            out += str(v - (1 << 16) if v >= (1 << 15) else v).encode("ascii"); return
    elif b == F.MP_INT32:
        if i + 5 <= n:
            v = F.read32(a, i + 1)
            out += str(v - (1 << 32) if v >= (1 << 31) else v).encode("ascii"); return
    elif b == F.MP_INT64:
        if i + 9 <= n:
            v = F.read64(a, i + 1)
            out += str(v - (1 << 64) if v >= (1 << 63) else v).encode("ascii"); return
    elif b == F.MP_FLOAT32:
        if i + 5 <= n:
            f = struct.unpack(">f", bytes(a[i + 1:i + 5]))[0]
            if not math.isfinite(f):
                out += b"null"; return
            out += _fmt_float32(f).encode("ascii"); return
    elif b == F.MP_FLOAT64:
        if i + 9 <= n:
            d = struct.unpack(">d", bytes(a[i + 1:i + 9]))[0]
            if not math.isfinite(d):
                out += b"null"; return
            out += _fmt_double(d).encode("ascii"); return

    # str
    soff = 0
    slen = 0
    if 0xA0 <= b <= 0xBF:
        slen = b & 0x1F; soff = i + 1
    elif b == F.MP_STR8 and i + 2 <= n:
        slen = a[i + 1]; soff = i + 2
    elif b == F.MP_STR16 and i + 3 <= n:
        slen = F.read16(a, i + 1); soff = i + 3
    elif b == F.MP_STR32 and i + 5 <= n:
        slen = F.read32(a, i + 1); soff = i + 5
    if soff:
        if slen > n - soff:
            slen = n - soff
        _escape_str(out, bytes(a[soff:soff + slen]))
        return

    # bin → hex string
    boff = 0
    blen = 0
    if b == F.MP_BIN8 and i + 2 <= n:
        blen = a[i + 1]; boff = i + 2
    elif b == F.MP_BIN16 and i + 3 <= n:
        blen = F.read16(a, i + 1); boff = i + 3
    elif b == F.MP_BIN32 and i + 5 <= n:
        blen = F.read32(a, i + 1); boff = i + 5
    if boff:
        if blen > n - boff:
            blen = n - boff
        out.append(0x22)
        for j in range(blen):
            by = a[boff + j]
            out.append(_HEX[by >> 4])
            out.append(_HEX[by & 0xF])
        out.append(0x22)
        return

    # array
    is_arr = False
    count = 0
    data_off = 0
    if 0x90 <= b <= 0x9F:
        is_arr = True; count = b & 0x0F; data_off = i + 1
    elif b == F.MP_ARRAY16 and i + 3 <= n:
        is_arr = True; count = F.read16(a, i + 1); data_off = i + 3
    elif b == F.MP_ARRAY32 and i + 5 <= n:
        is_arr = True; count = F.read32(a, i + 1); data_off = i + 5
    if is_arr:
        cur = data_off
        out.append(0x5B)  # [
        for j in range(count):
            if cur >= n:
                break
            nxt = F.skip_one(a, n, cur)
            if j > 0:
                out.append(0x2C)
            if pretty:
                _newline(out, depth + 1, indent_w)
            _to_json_at(out, a, n, cur, pretty, depth + 1, indent_w)
            cur = nxt if nxt else n
        if pretty and count > 0:
            _newline(out, depth, indent_w)
        out.append(0x5D)  # ]
        return

    # map
    is_map = False
    count = 0
    data_off = 0
    if 0x80 <= b <= 0x8F:
        is_map = True; count = b & 0x0F; data_off = i + 1
    elif b == F.MP_MAP16 and i + 3 <= n:
        is_map = True; count = F.read16(a, i + 1); data_off = i + 3
    elif b == F.MP_MAP32 and i + 5 <= n:
        is_map = True; count = F.read32(a, i + 1); data_off = i + 5
    if is_map:
        cur = data_off
        out.append(0x7B)  # {
        for j in range(count):
            if cur >= n:
                break
            val_off = F.skip_one(a, n, cur)
            pair_end = F.skip_one(a, n, val_off) if val_off else 0
            if j > 0:
                out.append(0x2C)
            if pretty:
                _newline(out, depth + 1, indent_w)
            _to_json_at(out, a, n, cur, pretty, depth + 1, indent_w)
            out.append(0x3A)  # :
            if pretty:
                out.append(0x20)
            _to_json_at(out, a, n, val_off if val_off else n, pretty, depth + 1, indent_w)
            cur = pair_end if pair_end else n
        if pretty and count > 0:
            _newline(out, depth, indent_w)
        out.append(0x7D)  # }
        return

    # ext / unknown → null
    out += b"null"


def to_json(a, n: int, pretty: bool = False, indent: int = 0) -> str:
    out = bytearray()
    _to_json_at(out, a, n, 0, pretty, 0, indent)
    return bytes(out).decode("utf-8", "surrogateescape")


# ── JSON parser → msgpack ─────────────────────────────────────────────
class _P:
    __slots__ = ("z", "n", "i")

    def __init__(self, z: bytes) -> None:
        self.z = z
        self.n = len(z)
        self.i = 0


def _skip_ws(p: "_P") -> None:
    while p.i < p.n and p.z[p.i] in (0x20, 0x09, 0x0A, 0x0D):
        p.i += 1


def _hex4(z: bytes, off: int) -> int:
    v = 0
    for j in range(4):
        c = z[off + j]
        if 0x30 <= c <= 0x39:
            h = c - 0x30
        elif 0x61 <= c <= 0x66:
            h = c - 0x61 + 10
        elif 0x41 <= c <= 0x46:
            h = c - 0x41 + 10
        else:
            return -1
        v = (v << 4) | h
    return v


def _cp_to_utf8(cp: int) -> bytes:
    if cp < 0x80:
        return bytes((cp,))
    if cp < 0x800:
        return bytes((0xC0 | (cp >> 6), 0x80 | (cp & 0x3F)))
    if cp < 0x10000:
        return bytes((0xE0 | (cp >> 12), 0x80 | ((cp >> 6) & 0x3F), 0x80 | (cp & 0x3F)))
    return bytes((
        0xF0 | (cp >> 18),
        0x80 | ((cp >> 12) & 0x3F),
        0x80 | ((cp >> 6) & 0x3F),
        0x80 | (cp & 0x3F),
    ))


def _parse_string(p: "_P", out: bytearray) -> int:
    sb = bytearray()
    p.i += 1  # skip "
    while p.i < p.n:
        c = p.z[p.i]
        if c == 0x22:
            p.i += 1
            break
        if c == 0x5C:
            p.i += 1
            if p.i >= p.n:
                return RC_ERROR
            esc = p.z[p.i]
            p.i += 1
            if esc == 0x22:
                sb.append(0x22)
            elif esc == 0x5C:
                sb.append(0x5C)
            elif esc == 0x2F:
                sb.append(0x2F)
            elif esc == 0x6E:
                sb.append(0x0A)
            elif esc == 0x72:
                sb.append(0x0D)
            elif esc == 0x74:
                sb.append(0x09)
            elif esc == 0x62:
                sb.append(0x08)
            elif esc == 0x66:
                sb.append(0x0C)
            elif esc == 0x75:
                if p.i + 4 > p.n:
                    return RC_ERROR
                cp = _hex4(p.z, p.i)
                p.i += 4
                if cp < 0:
                    return RC_ERROR
                if 0xD800 <= cp <= 0xDBFF and p.i + 6 <= p.n and \
                        p.z[p.i] == 0x5C and p.z[p.i + 1] == 0x75:
                    lo = _hex4(p.z, p.i + 2)
                    if 0xDC00 <= lo <= 0xDFFF:
                        p.i += 6
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00)
                sb += _cp_to_utf8(cp)
            else:
                sb.append(esc)
        else:
            sb.append(c)
            p.i += 1
    E.enc_string(out, bytes(sb))
    return RC_OK


def _parse_number(p: "_P", out: bytearray) -> int:
    start = p.i
    is_float = False
    if p.i < p.n and p.z[p.i] == 0x2D:
        p.i += 1
    while p.i < p.n and 0x30 <= p.z[p.i] <= 0x39:
        p.i += 1
    if p.i < p.n and p.z[p.i] == 0x2E:
        is_float = True
        p.i += 1
        while p.i < p.n and 0x30 <= p.z[p.i] <= 0x39:
            p.i += 1
    if p.i < p.n and p.z[p.i] in (0x65, 0x45):
        is_float = True
        p.i += 1
        if p.i < p.n and p.z[p.i] in (0x2B, 0x2D):
            p.i += 1
        while p.i < p.n and 0x30 <= p.z[p.i] <= 0x39:
            p.i += 1
    length = p.i - start
    if length <= 0 or length >= 64:
        return RC_ERROR
    text = bytes(p.z[start:p.i]).decode("ascii")

    if is_float:
        E.enc_real(out, float(text))
    else:
        v = int(text)
        if v > _I64_MAX:
            v = _I64_MAX
        elif v < _I64_MIN:
            v = _I64_MIN
        enc_signed_compact(out, v)
    return RC_OK


def enc_signed_compact(out: bytearray, v: int) -> None:
    """Replicates the from_json integer path (uses uint encodings for >=0)."""
    if v >= 0:
        E.enc_unsigned(out, v)
    else:
        E.enc_integer(out, v)


def _parse_array(p: "_P", out: bytearray) -> int:
    tmp = bytearray()
    count = 0
    p.i += 1  # skip [
    _skip_ws(p)
    while p.i < p.n and p.z[p.i] != 0x5D:
        if count > 0:
            _skip_ws(p)
            if p.i >= p.n or p.z[p.i] != 0x2C:
                return RC_ERROR
            p.i += 1
        _skip_ws(p)
        if _parse_value(p, tmp) != RC_OK:
            return RC_ERROR
        count += 1
        _skip_ws(p)
    if p.i >= p.n:
        return RC_ERROR
    p.i += 1  # skip ]
    E.enc_array_header(out, count)
    out += tmp
    return RC_OK


def _parse_object(p: "_P", out: bytearray) -> int:
    tmp = bytearray()
    count = 0
    p.i += 1  # skip {
    _skip_ws(p)
    while p.i < p.n and p.z[p.i] != 0x7D:
        if count > 0:
            _skip_ws(p)
            if p.i >= p.n or p.z[p.i] != 0x2C:
                return RC_ERROR
            p.i += 1
        _skip_ws(p)
        if p.i >= p.n or p.z[p.i] != 0x22:
            return RC_ERROR
        if _parse_string(p, tmp) != RC_OK:
            return RC_ERROR
        _skip_ws(p)
        if p.i >= p.n or p.z[p.i] != 0x3A:
            return RC_ERROR
        p.i += 1
        _skip_ws(p)
        if _parse_value(p, tmp) != RC_OK:
            return RC_ERROR
        count += 1
        _skip_ws(p)
    if p.i >= p.n:
        return RC_ERROR
    p.i += 1  # skip }
    E.enc_map_header(out, count)
    out += tmp
    return RC_OK


def _parse_value(p: "_P", out: bytearray) -> int:
    _skip_ws(p)
    if p.i >= p.n:
        return RC_ERROR
    c = p.z[p.i]
    if c == 0x6E and p.i + 4 <= p.n and p.z[p.i:p.i + 4] == b"null":
        p.i += 4
        out.append(F.MP_NIL)
        return RC_OK
    if c == 0x74 and p.i + 4 <= p.n and p.z[p.i:p.i + 4] == b"true":
        p.i += 4
        out.append(F.MP_TRUE)
        return RC_OK
    if c == 0x66 and p.i + 5 <= p.n and p.z[p.i:p.i + 5] == b"false":
        p.i += 5
        out.append(F.MP_FALSE)
        return RC_OK
    if c == 0x22:
        return _parse_string(p, out)
    if c == 0x5B:
        return _parse_array(p, out)
    if c == 0x7B:
        return _parse_object(p, out)
    if c == 0x2D or 0x30 <= c <= 0x39:
        return _parse_number(p, out)
    return RC_ERROR


def from_json(json) -> bytes:
    """Parse JSON (str or bytes) into msgpack; returns b'' on failure."""
    if json is None:
        return b""
    if isinstance(json, str):
        z = json.encode("utf-8", "surrogateescape")
    else:
        z = bytes(json)
    p = _P(z)
    out = bytearray()
    if _parse_value(p, out) != RC_OK:
        return b""
    return bytes(out)
