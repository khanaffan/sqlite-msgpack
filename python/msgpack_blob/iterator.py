"""Iterator — a cursor over container children (flat ``each`` / recursive ``tree``)."""

from __future__ import annotations

from typing import Iterator as _PyIterator
from typing import List, Optional

from . import _decode as D
from . import _iterate as I
from ._iterate import EachRow
from .blob import Blob

__all__ = ["Iterator", "EachRow"]


class Iterator:
    """Iterate over a container's children.

    Supports flat (``each``) and recursive (``tree``) modes, mirroring the
    SQLite extension's ``msgpack_each`` / ``msgpack_tree`` table-valued
    functions. Use it as a Python iterator or via the C++-style
    :meth:`next` / :meth:`current` cursor protocol.
    """

    __slots__ = ("_blob", "_base", "_recursive", "_rows", "_cursor", "_populated")

    def __init__(self, blob: Blob, path: str = "$", recursive: bool = False) -> None:
        self._blob = blob
        self._base = path if path else "$"
        self._recursive = recursive
        self._rows: List[EachRow] = []
        self._cursor = -1
        self._populated = False

    def _populate(self) -> None:
        if self._populated:
            return
        self._populated = True
        self._rows = []
        a = self._blob.data()
        n = len(a)
        if n == 0:
            return

        iroot = 0
        if self._base != "$":
            rc, istart, _ = D.lookup(a, n, 0, self._base)
            if rc != D.RC_OK:
                return
            iroot = istart

        if self._recursive:
            I.tree_walk(a, n, iroot, self._base, self._base, 0, self._rows)
        else:
            self._rows = I.each_iter(a, n, iroot, self._base)

    # ── C++-style cursor protocol ─────────────────────────────────────
    def next(self) -> bool:
        self._populate()
        self._cursor += 1
        return self._cursor < len(self._rows)

    def current(self) -> EachRow:
        return self._rows[self._cursor]

    def reset(self) -> None:
        self._cursor = -1

    # ── Pythonic protocols ────────────────────────────────────────────
    def rows(self) -> List[EachRow]:
        self._populate()
        return list(self._rows)

    def __iter__(self) -> _PyIterator[EachRow]:
        self._populate()
        return iter(list(self._rows))
