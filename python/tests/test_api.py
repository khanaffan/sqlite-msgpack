"""API behaviour and round-trip tests for the Python port (no C++ needed)."""

import unittest

from msgpack_blob import Blob, Builder, EachRow, Iterator, Type, Value, type_str


class TestBuilder(unittest.TestCase):
    def test_builder_matches_from_json(self):
        built = (
            Builder()
            .map_header(3)
            .string("name").string("Alice")
            .string("age").integer(30)
            .string("scores").array_header(3)
            .real(95.5).real(87.5).real(91.0)
            .build()
        )
        ref = Blob.from_json('{"name":"Alice","age":30,"scores":[95.5,87.5,91.0]}')
        self.assertEqual(built.hex(), ref.hex())

    def test_quote_roundtrip(self):
        for v in [
            Value.nil(), Value.boolean(True), Value.integer(-12345),
            Value.real(3.25), Value.real32(1.5), Value.string("hello"),
            Value.binary(b"\xde\xad"), Value.ext(7, b"\x01\x02"),
            Value.timestamp(1700000000, 123456789),
        ]:
            blob = Builder.quote(v)
            self.assertTrue(blob.valid())
            self.assertEqual(blob.extract("$").type(), v.type())


class TestRoundTrip(unittest.TestCase):
    CASES = [
        "null", "true", "false", "0", "-1", "127", "128", "65536",
        "1.5", "0.1", "1e10", '"hi"', "[]", "{}", "[1,2,3]",
        '{"a":1,"b":[2,3],"c":{"d":true}}',
        '{"u":"caf\\u00e9","emoji":"\\ud83d\\ude00"}',
    ]

    def test_json_bytes_stable(self):
        for case in self.CASES:
            once = Blob.from_json(case)
            twice = Blob.from_json(once.to_json())
            self.assertEqual(once.hex(), twice.hex(), case)

    def test_to_json_idempotent(self):
        for case in self.CASES:
            blob = Blob.from_json(case)
            self.assertEqual(blob.to_json(), Blob.from_json(blob.to_json()).to_json())


class TestExtraction(unittest.TestCase):
    def setUp(self):
        self.blob = Blob.from_json(
            '{"name":"Alice","age":30,"tall":true,"pets":["cat","dog"],'
            '"addr":{"city":"NYC"}}'
        )

    def test_scalar_paths(self):
        self.assertEqual(self.blob.extract("$.name").as_string(), "Alice")
        self.assertEqual(self.blob.extract("$.age").as_int64(), 30)
        self.assertTrue(self.blob.extract("$.tall").as_bool())
        self.assertEqual(self.blob.extract("$.pets[1]").as_string(), "dog")
        self.assertEqual(self.blob.extract("$.addr.city").as_string(), "NYC")

    def test_missing(self):
        self.assertTrue(self.blob.extract("$.nope").is_nil())
        self.assertEqual(self.blob.type_str("$.nope"), "null")

    def test_types(self):
        self.assertEqual(self.blob.type_str("$.name"), "text")
        self.assertEqual(self.blob.type_str("$.age"), "integer")
        self.assertEqual(self.blob.type_str("$.pets"), "array")
        self.assertEqual(self.blob.type_str("$.addr"), "map")

    def test_array_length(self):
        self.assertEqual(self.blob.array_length("$.pets"), 2)
        self.assertEqual(self.blob.array_length("$.name"), -1)


class TestBinaryExt(unittest.TestCase):
    def test_binary(self):
        blob = Builder().binary(b"\x01\x02\x03\x04").build()
        v = blob.extract("$")
        self.assertEqual(v.type(), Type.BINARY)
        self.assertEqual(v.blob_data(), b"\x01\x02\x03\x04")
        self.assertEqual(blob.to_json(), '"01020304"')

    def test_ext(self):
        blob = Builder().ext(42, b"\xaa\xbb").build()
        v = blob.extract("$")
        self.assertEqual(v.type(), Type.EXT)
        self.assertEqual(v.ext_type(), 42)
        self.assertEqual(v.blob_data(), b"\xaa\xbb")

    def test_timestamp(self):
        blob = Builder().timestamp(1700000000, 500000000).build()
        v = blob.extract("$")
        self.assertEqual(v.type(), Type.TIMESTAMP)
        self.assertEqual(v.timestamp_seconds(), 1700000000)
        self.assertEqual(v.timestamp_nanoseconds(), 500000000)


class TestMutation(unittest.TestCase):
    def test_copy_on_write(self):
        orig = Blob.from_json('{"a":1}')
        new = orig.set("$.b", Value.integer(2))
        self.assertEqual(orig.to_json(), '{"a":1}')
        self.assertEqual(new.to_json(), '{"a":1,"b":2}')

    def test_remove_and_patch(self):
        b = Blob.from_json('{"a":1,"b":2,"c":3}')
        self.assertEqual(b.remove("$.b").to_json(), '{"a":1,"c":3}')
        patched = b.patch(Blob.from_json('{"b":null,"d":4}'))
        self.assertEqual(patched.to_json(), '{"a":1,"c":3,"d":4}')

    def test_array_ops(self):
        b = Blob.from_json("[1,2,3]")
        self.assertEqual(b.array_insert("$[1]", Value.integer(9)).to_json(), "[1,9,2,3]")
        self.assertEqual(b.set("$[3]", Value.integer(4)).to_json(), "[1,2,3,4]")


class TestIterator(unittest.TestCase):
    def test_each_map(self):
        b = Blob.from_json('{"a":1,"b":2,"c":3}')
        rows = Iterator(b).rows()
        self.assertEqual([r.key for r in rows], ["a", "b", "c"])
        self.assertEqual([r.value.as_int64() for r in rows], [1, 2, 3])
        self.assertEqual([r.index for r in rows], [0, 1, 2])

    def test_each_array(self):
        b = Blob.from_json("[10,20,30]")
        rows = list(Iterator(b))
        self.assertEqual([r.fullkey for r in rows], ["$[0]", "$[1]", "$[2]"])

    def test_tree(self):
        b = Blob.from_json('{"x":{"y":[1,2]}}')
        keys = [r.fullkey for r in Iterator(b, "$", recursive=True)]
        self.assertEqual(keys, ["$", "$.x", "$.x.y", "$.x.y[0]", "$.x.y[1]"])

    def test_cursor_protocol(self):
        b = Blob.from_json("[1,2]")
        it = Iterator(b)
        seen = []
        while it.next():
            seen.append(it.current().value.as_int64())
        self.assertEqual(seen, [1, 2])
        it.reset()
        self.assertTrue(it.next())


class TestValidity(unittest.TestCase):
    def test_valid(self):
        self.assertTrue(Blob.from_json("[1,2,3]").valid())
        self.assertFalse(Blob(b"").valid())
        self.assertFalse(Blob(b"\x91").valid())  # array claims 1 elem, none present

    def test_error_position(self):
        self.assertEqual(Blob(b"\x01").error_position(), 0)  # valid
        self.assertNotEqual(Blob(b"\x91").error_position(), -1)


class TestTypeStr(unittest.TestCase):
    def test_labels(self):
        self.assertEqual(type_str(Type.NIL), "null")
        self.assertEqual(type_str(Type.STRING), "text")
        self.assertEqual(type_str(Type.FLOAT32), "float32")
        self.assertEqual(type_str(Type.TIMESTAMP), "timestamp")


class TestNonUtf8(unittest.TestCase):
    def test_non_utf8_preserved(self):
        # {"k": <0xff 0x80 0xfe 0xc0>} — non-UTF-8 str payload from a foreign encoder.
        blob = Blob(bytes([0x81, 0xA1, 0x6B, 0xA4, 0xFF, 0x80, 0xFE, 0xC0]))
        self.assertEqual(
            blob.to_json().encode("utf-8", "surrogateescape").hex(),
            "7b226b223a22ff80fec0227d",
        )
        v = blob.extract("$.k")
        self.assertEqual(v.as_bytes(), bytes([0xFF, 0x80, 0xFE, 0xC0]))
        rebuilt = Builder().string(v.as_string()).build()
        self.assertEqual(rebuilt.hex(), "a4ff80fec0")


if __name__ == "__main__":
    unittest.main()
