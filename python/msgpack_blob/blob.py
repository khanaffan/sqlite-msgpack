"""Blob — an owning byte buffer wrapping a msgpack-encoded value."""

from __future__ import annotations

from typing import Optional, Union

from . import _decode as D
from . import _encode as E
from . import _json as J
from . import _mutate as M
from .value import Type, Value

__all__ = ["Blob"]


class Blob:
    """A msgpack BLOB supporting read, mutation (copy-on-write) and JSON."""

    __slots__ = ("_data",)

    def __init__(self, data: Union[bytes, bytearray, memoryview, None] = b"") -> None:
        self._data = bytes(data) if data is not None else b""

    # ── raw access ────────────────────────────────────────────────────
    def data(self) -> bytes:
        return self._data

    def size(self) -> int:
        return len(self._data)

    def empty(self) -> bool:
        return len(self._data) == 0

    def hex(self) -> str:
        return self._data.hex()

    def __len__(self) -> int:
        return len(self._data)

    def __bytes__(self) -> bytes:
        return self._data

    def __eq__(self, other) -> bool:
        if isinstance(other, Blob):
            return self._data == other._data
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._data)

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"Blob({self._data.hex()})"

    # ── validation ────────────────────────────────────────────────────
    def valid(self) -> bool:
        return D.is_valid(self._data, len(self._data))

    def error_position(self) -> int:
        return D.error_position(self._data, len(self._data))

    # ── type inspection ───────────────────────────────────────────────
    def type(self, path: Optional[str] = None) -> Type:
        n = len(self._data)
        if path is None:
            return Type.NIL if n == 0 else D.get_type(self._data, n, 0)
        rc, istart, _ = D.lookup(self._data, n, 0, path)
        if rc != D.RC_OK:
            return Type.NIL
        return D.get_type(self._data, n, istart)

    def type_str(self, path: Optional[str] = None) -> str:
        return self.type(path).value

    # ── extraction ────────────────────────────────────────────────────
    def extract(self, path: str) -> Value:
        n = len(self._data)
        rc, istart, iend = D.lookup(self._data, n, 0, path)
        if rc != D.RC_OK:
            return Value.nil()
        return D.decode_element(self._data, n, istart, iend)

    def array_length(self, path: Optional[str] = None) -> int:
        n = len(self._data)
        if path is None:
            return -1 if n == 0 else D.get_container_count(self._data, n, 0)
        rc, istart, _ = D.lookup(self._data, n, 0, path)
        if rc != D.RC_OK:
            return -1
        return D.get_container_count(self._data, n, istart)

    # ── mutation (copy-on-write) ──────────────────────────────────────
    def _apply(self, path: str, value: Value, mode: int) -> "Blob":
        new_bin = bytearray()
        E.encode_value(new_bin, value)
        rc, out = M.apply_edit(self._data, len(self._data), path, bytes(new_bin), mode)
        return Blob(out) if rc == M.RC_OK else self

    def set(self, path: str, value: Union[Value, "Blob"]) -> "Blob":
        if isinstance(value, Blob):
            rc, out = M.apply_edit(self._data, len(self._data), path,
                                   value._data, M.EDIT_SET)
            return Blob(out) if rc == M.RC_OK else self
        return self._apply(path, value, M.EDIT_SET)

    def insert(self, path: str, value: Value) -> "Blob":
        return self._apply(path, value, M.EDIT_INSERT)

    def replace(self, path: str, value: Value) -> "Blob":
        return self._apply(path, value, M.EDIT_REPLACE)

    def array_insert(self, path: str, value: Value) -> "Blob":
        return self._apply(path, value, M.EDIT_ARRAY_INS)

    def remove(self, path: str) -> "Blob":
        rc, out = M.apply_edit(self._data, len(self._data), path, b"", M.EDIT_REMOVE)
        return Blob(out) if rc == M.RC_OK else self

    def patch(self, merge_patch: "Blob") -> "Blob":
        rc, out = M.merge_patch(self._data, len(self._data), 0,
                                merge_patch._data, len(merge_patch._data), 0, 0)
        return Blob(out) if rc == M.RC_OK else self

    # ── JSON conversion ───────────────────────────────────────────────
    def to_json(self) -> str:
        if not self._data:
            return "null"
        return J.to_json(self._data, len(self._data), False, 0)

    def to_json_pretty(self, indent: int = 2) -> str:
        if not self._data:
            return "null"
        indent = max(0, min(8, indent))
        return J.to_json(self._data, len(self._data), True, indent)

    @staticmethod
    def from_json(json: Union[str, bytes, None]) -> "Blob":
        return Blob(J.from_json(json))
