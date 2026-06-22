"""Value — a decoded scalar or sub-blob MessagePack value.

Mirrors ``msgpack::Value`` from the C++ Blob library. Values are cheap to copy
and can be used to both read from and write to blobs.
"""

from __future__ import annotations

import struct
from enum import Enum
from typing import Optional, Union

__all__ = ["Type", "IntWidth", "Value", "type_str"]

_U64 = 0xFFFFFFFFFFFFFFFF
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1


class Type(Enum):
    """Semantic type of a MessagePack element."""

    NIL = "null"
    TRUE = "true"
    FALSE = "false"
    INTEGER = "integer"
    REAL = "real"
    FLOAT32 = "float32"
    STRING = "text"
    BINARY = "binary"
    ARRAY = "array"
    MAP = "map"
    EXT = "ext"
    TIMESTAMP = "timestamp"


class IntWidth(Enum):
    """Integer encoding-width hint (forces a specific wire format)."""

    AUTO = 0
    INT8 = 1
    INT16 = 2
    INT32 = 3
    INT64 = 4
    UINT8 = 5
    UINT16 = 6
    UINT32 = 7
    UINT64 = 8


def type_str(t: Type) -> str:
    """Return the human-readable label for *t* (``"text"``, ``"integer"`` ...)."""
    return t.value


def _f32(x: float) -> float:
    """Round a Python float to float32 precision (returned as a float)."""
    return struct.unpack(">f", struct.pack(">f", x))[0]


class Value:
    """A decoded scalar or sub-blob value."""

    __slots__ = (
        "_type",
        "_int",
        "_float",
        "_str",
        "_blob",
        "_ext_type",
        "_ts_nsec",
        "_int_width",
    )

    def __init__(self) -> None:
        self._type: Type = Type.NIL
        self._int: int = 0
        self._float: float = 0.0
        self._str: bytes = b""
        self._blob: bytes = b""
        self._ext_type: int = 0
        self._ts_nsec: int = 0
        self._int_width: IntWidth = IntWidth.AUTO

    # ── accessors ─────────────────────────────────────────────────────
    def type(self) -> Type:
        return self._type

    def is_nil(self) -> bool:
        return self._type is Type.NIL

    def as_bool(self) -> bool:
        return self._type is Type.TRUE

    def as_int64(self) -> int:
        """Signed 64-bit view (Integer, Real, Float32, Timestamp, True)."""
        if self._type is Type.INTEGER:
            v = self._int & _U64
            return v - (1 << 64) if v > _I64_MAX else v
        if self._type in (Type.REAL, Type.FLOAT32):
            return int(self._float)  # truncate toward zero
        if self._type is Type.TIMESTAMP:
            return self._int
        if self._type is Type.TRUE:
            return 1
        return 0

    def as_uint64(self) -> int:
        """Raw unsigned 64-bit bits (Integer only)."""
        if self._type is Type.INTEGER:
            return self._int & _U64
        return 0

    def as_double(self) -> float:
        if self._type is Type.REAL:
            return self._float
        if self._type is Type.FLOAT32:
            return self._float
        if self._type is Type.INTEGER:
            return float(self.as_int64())
        return 0.0

    def as_float(self) -> float:
        if self._type is Type.FLOAT32:
            return self._float
        if self._type is Type.REAL:
            return _f32(self._float)
        return 0.0

    def as_string(self) -> str:
        """String payload decoded as UTF-8 (lossless round-trip via surrogateescape)."""
        if self._type is Type.STRING:
            return self._str.decode("utf-8", "surrogateescape")
        return ""

    def as_bytes(self) -> bytes:
        """Raw String payload bytes (UTF-8)."""
        if self._type is Type.STRING:
            return self._str
        return b""

    def blob_data(self) -> bytes:
        """Binary/Ext payload (no header), or raw bytes for container values."""
        return self._blob

    def blob_size(self) -> int:
        return len(self._blob)

    def ext_type(self) -> int:
        return self._ext_type

    def timestamp_seconds(self) -> int:
        return self._int if self._type is Type.TIMESTAMP else 0

    def timestamp_nanoseconds(self) -> int:
        return self._ts_nsec if self._type is Type.TIMESTAMP else 0

    def int_width(self) -> IntWidth:
        return self._int_width

    # ── static constructors ───────────────────────────────────────────
    @staticmethod
    def nil() -> "Value":
        return Value()

    @staticmethod
    def boolean(b: bool) -> "Value":
        v = Value()
        v._type = Type.TRUE if b else Type.FALSE
        return v

    @staticmethod
    def integer(x: int) -> "Value":
        v = Value()
        v._type = Type.INTEGER
        v._int = int(x)
        return v

    @staticmethod
    def unsigned_integer(x: int) -> "Value":
        v = Value()
        v._type = Type.INTEGER
        v._int = int(x) & _U64
        if v._int > _I64_MAX:
            v._int_width = IntWidth.UINT64
        return v

    @staticmethod
    def real(d: float) -> "Value":
        v = Value()
        v._type = Type.REAL
        v._float = float(d)
        return v

    @staticmethod
    def real32(f: float) -> "Value":
        v = Value()
        v._type = Type.FLOAT32
        v._float = _f32(float(f))
        return v

    @staticmethod
    def string(s: Union[str, bytes]) -> "Value":
        v = Value()
        v._type = Type.STRING
        v._str = s.encode("utf-8", "surrogateescape") if isinstance(s, str) else bytes(s)
        return v

    @staticmethod
    def binary(data: Union[bytes, bytearray, memoryview]) -> "Value":
        v = Value()
        v._type = Type.BINARY
        v._blob = bytes(data)
        return v

    @staticmethod
    def ext(type_code: int, data: Union[bytes, bytearray, memoryview]) -> "Value":
        v = Value()
        v._type = Type.EXT
        v._ext_type = int(type_code)
        v._blob = bytes(data)
        return v

    @staticmethod
    def timestamp(seconds: int, nanoseconds: int = 0) -> "Value":
        v = Value()
        v._type = Type.TIMESTAMP
        v._int = int(seconds)
        v._ts_nsec = int(nanoseconds)
        return v

    @staticmethod
    def _fixed(width: IntWidth, x: int) -> "Value":
        v = Value()
        v._type = Type.INTEGER
        v._int = int(x) & _U64
        v._int_width = width
        return v

    @staticmethod
    def int8(x: int) -> "Value":
        return Value._fixed(IntWidth.INT8, x)

    @staticmethod
    def int16(x: int) -> "Value":
        return Value._fixed(IntWidth.INT16, x)

    @staticmethod
    def int32(x: int) -> "Value":
        return Value._fixed(IntWidth.INT32, x)

    @staticmethod
    def int64(x: int) -> "Value":
        return Value._fixed(IntWidth.INT64, x)

    @staticmethod
    def uint8(x: int) -> "Value":
        return Value._fixed(IntWidth.UINT8, x)

    @staticmethod
    def uint16(x: int) -> "Value":
        return Value._fixed(IntWidth.UINT16, x)

    @staticmethod
    def uint32(x: int) -> "Value":
        return Value._fixed(IntWidth.UINT32, x)

    @staticmethod
    def uint64(x: int) -> "Value":
        return Value._fixed(IntWidth.UINT64, x)

    # ── debugging ─────────────────────────────────────────────────────
    def __repr__(self) -> str:  # pragma: no cover - debug aid
        t = self._type
        if t is Type.INTEGER:
            return f"Value(integer={self.as_int64()}, width={self._int_width.name})"
        if t in (Type.REAL, Type.FLOAT32):
            return f"Value({t.value}={self._float})"
        if t is Type.STRING:
            return f"Value(text={self.as_string()!r})"
        if t in (Type.BINARY, Type.EXT):
            return f"Value({t.value}, {self._blob.hex()})"
        if t is Type.TIMESTAMP:
            return f"Value(timestamp={self._int}.{self._ts_nsec:09d})"
        return f"Value({t.value})"
