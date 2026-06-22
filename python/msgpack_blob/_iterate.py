"""Internal: container iteration (mirrors msgpack_blob_iterate.cpp)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from . import _format as F
from ._decode import decode_element, get_type
from .value import Type, Value

__all__ = ["EachRow", "each_iter", "tree_walk"]


@dataclass
class EachRow:
    """A single row yielded by :class:`~msgpack_blob.iterator.Iterator`."""

    fullkey: str = "$"
    path: str = "$"
    id: int = 0
    type: Type = Type.NIL
    value: Value = field(default_factory=Value)
    key: str = ""          # map key ("" for arrays / tree rows)
    index: int = 0         # array index or pair index (each mode only)


def _key_str(a, n: int, i: int):
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
    return bytes(a[koff:koff + klen]).decode("utf-8", "surrogateescape")


def _container(a, n: int, i: int):
    """Return (is_arr, is_map, count, data_off) for the container at *i*."""
    b = a[i]
    if 0x90 <= b <= 0x9F:
        return (True, False, b & 0x0F, i + 1)
    if b == F.MP_ARRAY16 and i + 3 <= n:
        return (True, False, F.read16(a, i + 1), i + 3)
    if b == F.MP_ARRAY32 and i + 5 <= n:
        return (True, False, F.read32(a, i + 1), i + 5)
    if 0x80 <= b <= 0x8F:
        return (False, True, b & 0x0F, i + 1)
    if b == F.MP_MAP16 and i + 3 <= n:
        return (False, True, F.read16(a, i + 1), i + 3)
    if b == F.MP_MAP32 and i + 5 <= n:
        return (False, True, F.read32(a, i + 1), i + 5)
    return (False, False, 0, 0)


def each_iter(a, n: int, icont: int, zbase: str) -> List[EachRow]:
    rows: List[EachRow] = []
    if icont >= n:
        return rows
    is_arr, is_map, count, data_off = _container(a, n, icont)
    if not is_arr and not is_map:
        return rows

    remaining = (n - data_off) if data_off <= n else 0
    min_bytes = 2 if is_map else 1
    if count > remaining // min_bytes + 1:
        return rows

    cur = data_off
    for j in range(count):
        if cur >= n:
            break
        if is_arr:
            c_end = F.skip_one(a, n, cur)
            if not c_end:
                break
            rows.append(EachRow(
                fullkey=f"{zbase}[{j}]",
                path=zbase,
                id=cur,
                type=get_type(a, n, cur),
                value=decode_element(a, n, cur, c_end),
                key="",
                index=j,
            ))
            cur = c_end
        else:
            ks = _key_str(a, n, cur)
            v_off = F.skip_one(a, n, cur)
            if not v_off:
                break
            p_end = F.skip_one(a, n, v_off)
            if not p_end:
                break
            key = ks if ks is not None else "?"
            rows.append(EachRow(
                fullkey=f"{zbase}.{key}",
                path=zbase,
                id=v_off,
                type=get_type(a, n, v_off),
                value=decode_element(a, n, v_off, p_end),
                key=key,
                index=j,
            ))
            cur = p_end
    return rows


def tree_walk(a, n: int, ioff: int, zfull: str, zpar_path: str,
              depth: int, rows: List[EachRow]) -> None:
    if depth > F.MAX_DEPTH or ioff >= n:
        return
    iend = F.skip_one(a, n, ioff)
    if not iend:
        return

    rows.append(EachRow(
        fullkey=zfull,
        path=zpar_path,
        id=ioff,
        type=get_type(a, n, ioff),
        value=decode_element(a, n, ioff, iend),
    ))

    is_arr, is_map, count, data_off = _container(a, n, ioff)
    if not is_arr and not is_map:
        return

    remaining = (n - data_off) if data_off <= n else 0
    min_bytes = 2 if is_map else 1
    if count > remaining // min_bytes + 1:
        return

    cur = data_off
    for j in range(count):
        if cur >= n:
            break
        if is_arr:
            c_end = F.skip_one(a, n, cur)
            if not c_end:
                break
            tree_walk(a, n, cur, f"{zfull}[{j}]", zfull, depth + 1, rows)
            cur = c_end
        else:
            ks = _key_str(a, n, cur)
            v_off = F.skip_one(a, n, cur)
            if not v_off:
                break
            p_end = F.skip_one(a, n, v_off)
            if not p_end:
                break
            key = ks if ks is not None else "?"
            tree_walk(a, n, v_off, f"{zfull}.{key}", zfull, depth + 1, rows)
            cur = p_end
