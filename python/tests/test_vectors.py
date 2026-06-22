"""Replay the shared cross-language vectors (tests/vectors/blob_vectors.json).

These vectors are generated from the C++ reference implementation, so passing
them proves the Python port is byte-identical.
"""

import json
import os
import unittest

from msgpack_blob import Blob, Builder, Iterator, Value

_VECTORS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "tests", "vectors", "blob_vectors.json"
)


def load_vectors():
    with open(_VECTORS_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def build_value(spec):
    """Construct a Value from a ValueSpec dict (see cpp/tests/gen_blob_vectors.cpp)."""
    k = spec["k"]
    if k == "nil":
        return Value.nil()
    if k == "bool":
        return Value.boolean(bool(spec["v"]))
    if k == "int":
        return Value.integer(int(spec["v"]))
    if k == "uint":
        return Value.unsigned_integer(int(spec["v"]))
    if k == "int8":
        return Value.int8(int(spec["v"]))
    if k == "int16":
        return Value.int16(int(spec["v"]))
    if k == "int32":
        return Value.int32(int(spec["v"]))
    if k == "int64":
        return Value.int64(int(spec["v"]))
    if k == "uint8":
        return Value.uint8(int(spec["v"]))
    if k == "uint16":
        return Value.uint16(int(spec["v"]))
    if k == "uint32":
        return Value.uint32(int(spec["v"]))
    if k == "uint64":
        return Value.uint64(int(spec["v"]))
    if k == "real":
        return Value.real(float(spec["v"]))
    if k == "real32":
        return Value.real32(float(spec["v"]))
    if k == "str":
        return Value.string(spec["v"])
    if k == "binary":
        return Value.binary(bytes.fromhex(spec["hex"]))
    if k == "ext":
        return Value.ext(int(spec["type"]), bytes.fromhex(spec["hex"]))
    if k == "timestamp":
        return Value.timestamp(int(spec["sec"]), int(spec["nsec"]))
    raise ValueError(f"unknown spec kind: {k}")


class TestFromJson(unittest.TestCase):
    def test_from_json(self):
        for v in load_vectors()["from_json"]:
            with self.subTest(json=v["json"]):
                self.assertEqual(Blob.from_json(v["json"]).hex(), v["hex"])


class TestToJson(unittest.TestCase):
    def test_to_json(self):
        for v in load_vectors()["to_json"]:
            with self.subTest(hex=v["hex"]):
                blob = Blob(bytes.fromhex(v["hex"]))
                self.assertEqual(blob.to_json(), v["json"])

    def test_to_json_pretty(self):
        for v in load_vectors()["to_json_pretty"]:
            with self.subTest(hex=v["hex"], indent=v["indent"]):
                blob = Blob(bytes.fromhex(v["hex"]))
                self.assertEqual(blob.to_json_pretty(v["indent"]), v["json"])


class TestTyped(unittest.TestCase):
    def test_typed(self):
        for v in load_vectors()["typed"]:
            with self.subTest(spec=v["spec"]):
                blob = Builder.quote(build_value(v["spec"]))
                self.assertEqual(blob.hex(), v["hex"])


class TestMutate(unittest.TestCase):
    def test_mutate(self):
        for v in load_vectors()["mutate"]:
            with self.subTest(base=v["base"], op=v["op"], path=v.get("path")):
                base = Blob.from_json(v["base"])
                op = v["op"]
                if op == "set":
                    r = base.set(v["path"], build_value(v["spec"]))
                elif op == "insert":
                    r = base.insert(v["path"], build_value(v["spec"]))
                elif op == "replace":
                    r = base.replace(v["path"], build_value(v["spec"]))
                elif op == "array_insert":
                    r = base.array_insert(v["path"], build_value(v["spec"]))
                elif op == "remove":
                    r = base.remove(v["path"])
                elif op == "set_blob":
                    r = base.set(v["path"], Blob.from_json(v["spec"]["json"]))
                elif op == "patch":
                    r = base.patch(Blob.from_json(v["patch"]))
                else:
                    self.fail(f"unknown op {op}")
                self.assertEqual(r.hex(), v["hex"])


class TestExtract(unittest.TestCase):
    def test_extract(self):
        for v in load_vectors()["extract"]:
            with self.subTest(base=v["base"], path=v["path"]):
                blob = Blob.from_json(v["base"])
                self.assertEqual(blob.type_str(v["path"]), v["type"])
                value = blob.extract(v["path"])
                self.assertEqual(Builder.quote(value).to_json(), v["vjson"])


class TestArrayLength(unittest.TestCase):
    def test_array_length(self):
        for v in load_vectors()["array_length"]:
            with self.subTest(base=v["base"], path=v["path"]):
                blob = Blob.from_json(v["base"])
                got = blob.array_length() if v["path"] == "$" else blob.array_length(v["path"])
                self.assertEqual(got, v["len"])


class TestIterate(unittest.TestCase):
    def test_iterate(self):
        for v in load_vectors()["iterate"]:
            with self.subTest(base=v["base"], path=v["path"], recursive=v["recursive"]):
                blob = Blob.from_json(v["base"])
                rows = Iterator(blob, v["path"], v["recursive"]).rows()
                self.assertEqual(len(rows), len(v["rows"]))
                for got, exp in zip(rows, v["rows"]):
                    self.assertEqual(got.fullkey, exp["fullkey"])
                    self.assertEqual(got.path, exp["path"])
                    self.assertEqual(got.id, exp["id"])
                    self.assertEqual(got.type.value, exp["type"])
                    if "key" in exp:
                        self.assertEqual(got.key, exp["key"])
                        self.assertEqual(got.index, exp["index"])


if __name__ == "__main__":
    unittest.main()
