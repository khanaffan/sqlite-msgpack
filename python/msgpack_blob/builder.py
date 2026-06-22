"""Builder — a streaming encoder that produces a :class:`Blob`."""

from __future__ import annotations

from typing import Union

from . import _encode as E
from .blob import Blob
from .value import Value

__all__ = ["Builder"]


class Builder:
    """Append msgpack elements in order, then finalise with :meth:`build`."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    # ── scalars ───────────────────────────────────────────────────────
    def nil(self) -> "Builder":
        E.enc_nil(self._buf)
        return self

    def boolean(self, v: bool) -> "Builder":
        E.enc_bool(self._buf, v)
        return self

    def integer(self, x: int) -> "Builder":
        E.enc_integer(self._buf, int(x))
        return self

    def unsigned_integer(self, x: int) -> "Builder":
        E.enc_unsigned(self._buf, int(x))
        return self

    def real(self, d: float) -> "Builder":
        E.enc_real(self._buf, float(d))
        return self

    def real32(self, f: float) -> "Builder":
        E.enc_real32(self._buf, float(f))
        return self

    def string(self, s: Union[str, bytes]) -> "Builder":
        E.enc_string(self._buf, s.encode("utf-8", "surrogateescape") if isinstance(s, str) else bytes(s))
        return self

    def binary(self, data: Union[bytes, bytearray, memoryview]) -> "Builder":
        E.enc_binary(self._buf, bytes(data))
        return self

    def ext(self, type_code: int, data: Union[bytes, bytearray, memoryview]) -> "Builder":
        E.enc_ext(self._buf, int(type_code), bytes(data))
        return self

    # ── fixed-width integers ──────────────────────────────────────────
    def int8(self, x: int) -> "Builder":
        E.enc_int8(self._buf, int(x))
        return self

    def int16(self, x: int) -> "Builder":
        E.enc_int16(self._buf, int(x))
        return self

    def int32(self, x: int) -> "Builder":
        E.enc_int32(self._buf, int(x))
        return self

    def int64(self, x: int) -> "Builder":
        E.enc_int64(self._buf, int(x))
        return self

    def uint8(self, x: int) -> "Builder":
        E.enc_uint8(self._buf, int(x))
        return self

    def uint16(self, x: int) -> "Builder":
        E.enc_uint16(self._buf, int(x))
        return self

    def uint32(self, x: int) -> "Builder":
        E.enc_uint32(self._buf, int(x))
        return self

    def uint64(self, x: int) -> "Builder":
        E.enc_uint64(self._buf, int(x))
        return self

    # ── containers ────────────────────────────────────────────────────
    def array_header(self, count: int) -> "Builder":
        E.enc_array_header(self._buf, int(count))
        return self

    def map_header(self, count: int) -> "Builder":
        E.enc_map_header(self._buf, int(count))
        return self

    # ── embedding & timestamp ─────────────────────────────────────────
    def raw(self, data: Union[bytes, bytearray, memoryview, Blob]) -> "Builder":
        if isinstance(data, Blob):
            self._buf += data.data()
        else:
            self._buf += bytes(data)
        return self

    def value(self, v: Value) -> "Builder":
        E.encode_value(self._buf, v)
        return self

    def timestamp(self, sec: int, nsec: int = 0) -> "Builder":
        E.enc_timestamp(self._buf, int(sec), int(nsec))
        return self

    # ── finalize ──────────────────────────────────────────────────────
    def build(self) -> Blob:
        return Blob(bytes(self._buf))

    def __len__(self) -> int:
        return len(self._buf)

    @staticmethod
    def quote(v: Value) -> Blob:
        b = Builder()
        b.value(v)
        return b.build()
