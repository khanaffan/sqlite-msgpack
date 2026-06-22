"""Internal: encoding primitives (mirrors msgpack_blob_encode.cpp).

All functions append to a ``bytearray`` and are shared by the Builder, the JSON
parser and the mutation engine.
"""

from __future__ import annotations

import struct

from . import _format as F
from .value import IntWidth, Type, Value

__all__ = [
    "enc_nil",
    "enc_bool",
    "enc_integer",
    "enc_unsigned",
    "enc_real",
    "enc_real32",
    "enc_string",
    "enc_binary",
    "enc_ext",
    "enc_int8", "enc_int16", "enc_int32", "enc_int64",
    "enc_uint8", "enc_uint16", "enc_uint32", "enc_uint64",
    "enc_array_header",
    "enc_map_header",
    "enc_timestamp",
    "encode_value",
]

_U64 = 0xFFFFFFFFFFFFFFFF


def enc_nil(out: bytearray) -> None:
    out.append(F.MP_NIL)


def enc_bool(out: bytearray, v: bool) -> None:
    out.append(F.MP_TRUE if v else F.MP_FALSE)


def enc_integer(out: bytearray, x: int) -> None:
    """Compact signed-integer encoding (fixint → int64/uint64)."""
    if x >= 0:
        if x <= 0x7F:
            out.append(x)
        elif x <= 0xFF:
            out += bytes((F.MP_UINT8, x))
        elif x <= 0xFFFF:
            out += bytes((F.MP_UINT16,)) + F.w16(x)
        elif x <= 0xFFFFFFFF:
            out += bytes((F.MP_UINT32,)) + F.w32(x)
        else:
            out += bytes((F.MP_UINT64,)) + F.w64(x)
    else:
        if x >= -32:
            out.append(x & 0xFF)
        elif x >= -128:
            out += bytes((F.MP_INT8, x & 0xFF))
        elif x >= -32768:
            out += bytes((F.MP_INT16,)) + F.w16(x)
        elif x >= -2147483648:
            out += bytes((F.MP_INT32,)) + F.w32(x)
        else:
            out += bytes((F.MP_INT64,)) + F.w64(x)


def enc_unsigned(out: bytearray, x: int) -> None:
    """Compact unsigned-integer encoding."""
    x &= _U64
    if x <= 0x7F:
        out.append(x)
    elif x <= 0xFF:
        out += bytes((F.MP_UINT8, x))
    elif x <= 0xFFFF:
        out += bytes((F.MP_UINT16,)) + F.w16(x)
    elif x <= 0xFFFFFFFF:
        out += bytes((F.MP_UINT32,)) + F.w32(x)
    else:
        out += bytes((F.MP_UINT64,)) + F.w64(x)


def enc_real(out: bytearray, d: float) -> None:
    out += bytes((F.MP_FLOAT64,)) + struct.pack(">d", d)


def enc_real32(out: bytearray, f: float) -> None:
    out += bytes((F.MP_FLOAT32,)) + struct.pack(">f", f)


def enc_string(out: bytearray, s: bytes) -> None:
    n = len(s)
    if n <= 31:
        out.append(F.MP_FIXSTR_MASK | n)
    elif n <= 0xFF:
        out += bytes((F.MP_STR8, n))
    elif n <= 0xFFFF:
        out += bytes((F.MP_STR16,)) + F.w16(n)
    else:
        out += bytes((F.MP_STR32,)) + F.w32(n)
    out += s


def enc_binary(out: bytearray, data: bytes) -> None:
    n = len(data)
    if n <= 0xFF:
        out += bytes((F.MP_BIN8, n))
    elif n <= 0xFFFF:
        out += bytes((F.MP_BIN16,)) + F.w16(n)
    else:
        out += bytes((F.MP_BIN32,)) + F.w32(n)
    out += data


def enc_ext(out: bytearray, type_code: int, data: bytes) -> None:
    n = len(data)
    if n == 1:
        out.append(F.MP_FIXEXT1)
    elif n == 2:
        out.append(F.MP_FIXEXT2)
    elif n == 4:
        out.append(F.MP_FIXEXT4)
    elif n == 8:
        out.append(F.MP_FIXEXT8)
    elif n == 16:
        out.append(F.MP_FIXEXT16)
    elif n <= 0xFF:
        out += bytes((F.MP_EXT8, n))
    elif n <= 0xFFFF:
        out += bytes((F.MP_EXT16,)) + F.w16(n)
    else:
        out += bytes((F.MP_EXT32,)) + F.w32(n)
    out.append(type_code & 0xFF)
    out += data


def enc_int8(out: bytearray, x: int) -> None:
    out += bytes((F.MP_INT8, x & 0xFF))


def enc_int16(out: bytearray, x: int) -> None:
    out += bytes((F.MP_INT16,)) + F.w16(x)


def enc_int32(out: bytearray, x: int) -> None:
    out += bytes((F.MP_INT32,)) + F.w32(x)


def enc_int64(out: bytearray, x: int) -> None:
    out += bytes((F.MP_INT64,)) + F.w64(x)


def enc_uint8(out: bytearray, x: int) -> None:
    out += bytes((F.MP_UINT8, x & 0xFF))


def enc_uint16(out: bytearray, x: int) -> None:
    out += bytes((F.MP_UINT16,)) + F.w16(x)


def enc_uint32(out: bytearray, x: int) -> None:
    out += bytes((F.MP_UINT32,)) + F.w32(x)


def enc_uint64(out: bytearray, x: int) -> None:
    out += bytes((F.MP_UINT64,)) + F.w64(x)


def enc_array_header(out: bytearray, count: int) -> None:
    if count <= 15:
        out.append(F.MP_FIXARRAY_MASK | count)
    elif count <= 0xFFFF:
        out += bytes((F.MP_ARRAY16,)) + F.w16(count)
    else:
        out += bytes((F.MP_ARRAY32,)) + F.w32(count)


def enc_map_header(out: bytearray, count: int) -> None:
    if count <= 15:
        out.append(F.MP_FIXMAP_MASK | count)
    elif count <= 0xFFFF:
        out += bytes((F.MP_MAP16,)) + F.w16(count)
    else:
        out += bytes((F.MP_MAP32,)) + F.w32(count)


def enc_timestamp(out: bytearray, sec: int, nsec: int = 0) -> None:
    if nsec == 0 and 0 <= sec <= 0xFFFFFFFF:
        out += bytes((F.MP_FIXEXT4, 0xFF)) + F.w32(sec)
    elif 0 <= sec <= 0x3FFFFFFFF:
        out += bytes((F.MP_FIXEXT8, 0xFF)) + F.w64((nsec << 34) | sec)
    else:
        out += bytes((F.MP_EXT8, 12, 0xFF)) + F.w32(nsec) + F.w64(sec)


def encode_value(out: bytearray, v: Value) -> None:
    """Encode a Value, honouring its integer-width hint."""
    t = v.type()
    if t is Type.NIL:
        enc_nil(out)
    elif t is Type.TRUE:
        enc_bool(out, True)
    elif t is Type.FALSE:
        enc_bool(out, False)
    elif t is Type.INTEGER:
        w = v.int_width()
        if w is IntWidth.INT8:
            enc_int8(out, v.as_int64())
        elif w is IntWidth.INT16:
            enc_int16(out, v.as_int64())
        elif w is IntWidth.INT32:
            enc_int32(out, v.as_int64())
        elif w is IntWidth.INT64:
            enc_int64(out, v.as_int64())
        elif w is IntWidth.UINT8:
            enc_uint8(out, v.as_uint64())
        elif w is IntWidth.UINT16:
            enc_uint16(out, v.as_uint64())
        elif w is IntWidth.UINT32:
            enc_uint32(out, v.as_uint64())
        elif w is IntWidth.UINT64:
            enc_uint64(out, v.as_uint64())
        else:
            enc_integer(out, v.as_int64())
    elif t is Type.REAL:
        enc_real(out, v.as_double())
    elif t is Type.FLOAT32:
        enc_real32(out, v.as_float())
    elif t is Type.STRING:
        enc_string(out, v.as_bytes())
    elif t is Type.BINARY:
        enc_binary(out, v.blob_data())
    elif t is Type.EXT:
        enc_ext(out, v.ext_type(), v.blob_data())
    elif t is Type.TIMESTAMP:
        enc_timestamp(out, v.timestamp_seconds(), v.timestamp_nanoseconds())
    else:
        enc_nil(out)
