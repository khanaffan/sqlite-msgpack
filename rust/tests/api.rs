//! API behaviour and round-trip tests for the Rust port.

use msgpack_blob::{type_str, Blob, Builder, Iterator, Type, Value};

#[test]
fn builder_matches_from_json() {
    let mut b = Builder::new();
    b.map_header(3)
        .string("name")
        .string("Alice")
        .string("age")
        .integer(30)
        .string("scores")
        .array_header(3)
        .real(95.5)
        .real(87.5)
        .real(91.0);
    let built = b.build();
    let reference = Blob::from_json(r#"{"name":"Alice","age":30,"scores":[95.5,87.5,91.0]}"#);
    assert_eq!(built.hex(), reference.hex());
}

#[test]
fn quote_roundtrips_type() {
    let values = [
        Value::nil(),
        Value::boolean(true),
        Value::integer(-12345),
        Value::real(3.25),
        Value::real32(1.5),
        Value::string("hello"),
        Value::binary(&[0xde, 0xad]),
        Value::ext(7, &[1, 2]),
        Value::timestamp_ns(1_700_000_000, 123_456_789),
    ];
    for v in &values {
        let blob = Builder::quote(v);
        assert!(blob.valid());
        assert_eq!(blob.extract("$").get_type(), v.get_type());
    }
}

const ROUND_TRIP: &[&str] = &[
    "null",
    "true",
    "false",
    "0",
    "-1",
    "127",
    "128",
    "65536",
    "1.5",
    "0.1",
    "1e10",
    "\"hi\"",
    "[]",
    "{}",
    "[1,2,3]",
    r#"{"a":1,"b":[2,3],"c":{"d":true}}"#,
    r#"{"u":"caf\u00e9","emoji":"\ud83d\ude00"}"#,
];

#[test]
fn json_bytes_stable() {
    for case in ROUND_TRIP {
        let once = Blob::from_json(case);
        let twice = Blob::from_json(&once.to_json());
        assert_eq!(once.hex(), twice.hex(), "{}", case);
    }
}

#[test]
fn integers_64bit_roundtrip() {
    let blob = Builder::quote(&Value::uint64(u64::MAX));
    assert_eq!(blob.hex(), "cfffffffffffffffff");
    assert_eq!(blob.extract("$").as_u64(), u64::MAX);
    assert_eq!(blob.to_json(), "18446744073709551615");

    let neg = Builder::quote(&Value::int64(i64::MIN));
    assert_eq!(neg.extract("$").as_i64(), i64::MIN);
}

#[test]
fn extraction() {
    let blob = Blob::from_json(
        r#"{"name":"Alice","age":30,"tall":true,"pets":["cat","dog"],"addr":{"city":"NYC"}}"#,
    );
    assert_eq!(blob.extract("$.name").as_string(), "Alice");
    assert_eq!(blob.extract("$.age").as_i64(), 30);
    assert!(blob.extract("$.tall").as_bool());
    assert_eq!(blob.extract("$.pets[1]").as_string(), "dog");
    assert_eq!(blob.extract("$.addr.city").as_string(), "NYC");
    assert!(blob.extract("$.nope").is_nil());
    assert_eq!(blob.type_str_at("$.pets"), "array");
    assert_eq!(blob.array_length_at("$.pets"), 2);
    assert_eq!(blob.array_length_at("$.name"), -1);
}

#[test]
fn binary_ext_timestamp() {
    let bin = Builder::new().binary(&[1, 2, 3, 4]).build();
    assert_eq!(bin.extract("$").get_type(), Type::Binary);
    assert_eq!(bin.extract("$").blob_data(), &[1, 2, 3, 4]);
    assert_eq!(bin.to_json(), "\"01020304\"");

    let ext = Builder::new().ext(42, &[0xaa, 0xbb]).build();
    assert_eq!(ext.extract("$").ext_type(), 42);

    let ts = Builder::new()
        .timestamp_ns(1_700_000_000, 500_000_000)
        .build();
    assert_eq!(ts.extract("$").timestamp_seconds(), 1_700_000_000);
    assert_eq!(ts.extract("$").timestamp_nanoseconds(), 500_000_000);
}

#[test]
fn copy_on_write_mutation() {
    let orig = Blob::from_json(r#"{"a":1}"#);
    let updated = orig.set("$.b", &Value::integer(2));
    assert_eq!(orig.to_json(), r#"{"a":1}"#);
    assert_eq!(updated.to_json(), r#"{"a":1,"b":2}"#);

    let b = Blob::from_json(r#"{"a":1,"b":2,"c":3}"#);
    assert_eq!(b.remove("$.b").to_json(), r#"{"a":1,"c":3}"#);
    assert_eq!(
        b.patch(&Blob::from_json(r#"{"b":null,"d":4}"#)).to_json(),
        r#"{"a":1,"c":3,"d":4}"#
    );

    let arr = Blob::from_json("[1,2,3]");
    assert_eq!(
        arr.array_insert("$[1]", &Value::integer(9)).to_json(),
        "[1,9,2,3]"
    );
    assert_eq!(arr.set("$[3]", &Value::integer(4)).to_json(), "[1,2,3,4]");
}

#[test]
fn iterator_each_and_tree() {
    let map = Blob::from_json(r#"{"a":1,"b":2,"c":3}"#);
    let keys: Vec<String> = Iterator::new(&map, "$", false)
        .rows()
        .iter()
        .map(|r| r.key.clone())
        .collect();
    assert_eq!(keys, vec!["a", "b", "c"]);

    let nested = Blob::from_json(r#"{"x":{"y":[1,2]}}"#);
    let fullkeys: Vec<String> = Iterator::new(&nested, "$", true)
        .into_iter()
        .map(|r| r.fullkey)
        .collect();
    assert_eq!(fullkeys, vec!["$", "$.x", "$.x.y", "$.x.y[0]", "$.x.y[1]"]);

    let arr = Blob::from_json("[1,2]");
    let mut it = Iterator::new(&arr, "$", false);
    let mut seen = Vec::new();
    while it.next() {
        seen.push(it.current().value.as_i64());
    }
    assert_eq!(seen, vec![1, 2]);
}

#[test]
fn validity() {
    assert!(Blob::from_json("[1,2,3]").valid());
    assert!(!Blob::new(&[]).valid());
    assert!(!Blob::new(&[0x91]).valid());
}

#[test]
fn type_str_labels() {
    assert_eq!(type_str(Type::Nil), "null");
    assert_eq!(type_str(Type::String), "text");
    assert_eq!(type_str(Type::Float32), "float32");
    assert_eq!(type_str(Type::Timestamp), "timestamp");
}

#[test]
fn truncated_map_key_does_not_panic() {
    // One-entry map whose str8 key declares length 10 but supplies only 2 bytes.
    // The C++ reference bails via skip_one; the port must not panic.
    let blob = Blob::new(&[0x81, 0xd9, 0x0a, 0x61, 0x62]);
    // Mutation returns the original blob unchanged.
    assert_eq!(blob.set("$.x", &Value::integer(1)).data(), blob.data());
    assert_eq!(blob.remove("$.x").data(), blob.data());
    assert_eq!(blob.patch(&Blob::from_json(r#"{"a":1}"#)).data(), blob.data());
    // Flat and recursive iteration yield no usable rows (no panic).
    assert_eq!(Iterator::new(&blob, "$", false).rows().len(), 0);
    let _ = Iterator::new(&blob, "$", true).rows();
    // Truncated fixstr key as well.
    let fix = Blob::new(&[0x81, 0xa5, 0x68, 0x69]);
    assert_eq!(fix.set("$.x", &Value::integer(1)).data(), fix.data());
    assert_eq!(Iterator::new(&fix, "$", false).rows().len(), 0);
}

#[test]
fn non_utf8_string_bytes_preserved() {
    // {"k": <0xff 0x80 0xfe 0xc0>} — a str with non-UTF-8 payload, as a foreign
    // encoder (C++/SQLite) may produce. C++ passes raw bytes through verbatim.
    let blob = Blob::new(&[0x81, 0xa1, 0x6b, 0xa4, 0xff, 0x80, 0xfe, 0xc0]);
    assert_eq!(
        blob.to_json_bytes(),
        vec![0x7b, 0x22, 0x6b, 0x22, 0x3a, 0x22, 0xff, 0x80, 0xfe, 0xc0, 0x22, 0x7d]
    );
    let v = blob.extract("$.k");
    assert_eq!(v.as_bytes(), &[0xff, 0x80, 0xfe, 0xc0]);
    // Value::string_bytes round-trips arbitrary bytes back to identical output.
    let rebuilt = Builder::new().string_bytes(v.as_bytes()).build();
    assert_eq!(rebuilt.hex(), "a4ff80fec0");
}
