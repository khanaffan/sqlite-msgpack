"""msgpack_blob — a pure-Python MessagePack Blob library.

A zero-dependency port of the standalone C++ ``msgpack`` Blob API. It creates,
queries, mutates and iterates MessagePack binary blobs and produces
byte-identical output to the C++ library and the ``sqlite-msgpack`` extension,
so blobs are fully interchangeable across all three.

Quick start::

    from msgpack_blob import Blob, Builder, Value, Iterator

    blob = Blob.from_json('{"name":"Alice","scores":[95,87,91]}')
    blob.extract("$.name").as_string()      # 'Alice'
    blob.to_json()                           # '{"name":"Alice","scores":[95,87,91]}'

    b2 = blob.set("$.age", Value.integer(30))
    for row in Iterator(blob, "$", recursive=True):
        print(row.fullkey, row.type.value)
"""

from __future__ import annotations

from ._format import MAX_DEPTH, MAX_OUTPUT
from .blob import Blob
from .builder import Builder
from .iterator import EachRow, Iterator
from .value import IntWidth, Type, Value, type_str

__version__ = "1.7.0"

__all__ = [
    "Blob",
    "Builder",
    "Value",
    "Iterator",
    "EachRow",
    "Type",
    "IntWidth",
    "type_str",
    "MAX_DEPTH",
    "MAX_OUTPUT",
    "__version__",
]
