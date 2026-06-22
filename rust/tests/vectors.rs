//! Replay the shared cross-language vectors (tests/vectors/blob_vectors.json).
//! Generated from the C++ reference implementation, so passing them proves the
//! Rust port is byte-identical.

mod common;

use common::{build_value, bytes_to_hex, hex_to_bytes, load_vectors};
use msgpack_blob::{Blob, Builder, Iterator};

#[test]
fn from_json_byte_identical() {
    let v = load_vectors();
    for case in v.get("from_json").as_arr() {
        let blob = Blob::from_json(case.get("json").as_str());
        assert_eq!(
            blob.hex(),
            case.get("hex").as_str(),
            "json={}",
            case.get("json").as_str()
        );
    }
}

#[test]
fn to_json_matches() {
    let v = load_vectors();
    for case in v.get("to_json").as_arr() {
        let blob = Blob::new(&hex_to_bytes(case.get("hex").as_str()));
        assert_eq!(
            blob.to_json(),
            case.get("json").as_str(),
            "hex={}",
            case.get("hex").as_str()
        );
    }
}

#[test]
fn to_json_pretty_matches() {
    let v = load_vectors();
    for case in v.get("to_json_pretty").as_arr() {
        let blob = Blob::new(&hex_to_bytes(case.get("hex").as_str()));
        let indent = case.get("indent").as_i64() as i32;
        assert_eq!(blob.to_json_pretty(indent), case.get("json").as_str());
    }
}

#[test]
fn typed_quote_matches() {
    let v = load_vectors();
    for case in v.get("typed").as_arr() {
        let value = build_value(case.get("spec"));
        let blob = Builder::quote(&value);
        assert_eq!(
            blob.hex(),
            case.get("hex").as_str(),
            "spec={:?}",
            case.get("spec")
        );
    }
}

#[test]
fn mutate_matches() {
    let v = load_vectors();
    for case in v.get("mutate").as_arr() {
        let base = Blob::from_json(case.get("base").as_str());
        let op = case.get("op").as_str();
        let result = match op {
            "set" => base.set(case.get("path").as_str(), &build_value(case.get("spec"))),
            "insert" => base.insert(case.get("path").as_str(), &build_value(case.get("spec"))),
            "replace" => base.replace(case.get("path").as_str(), &build_value(case.get("spec"))),
            "array_insert" => {
                base.array_insert(case.get("path").as_str(), &build_value(case.get("spec")))
            }
            "remove" => base.remove(case.get("path").as_str()),
            "set_blob" => base.set_blob(
                case.get("path").as_str(),
                &Blob::from_json(case.get("spec").get("json").as_str()),
            ),
            "patch" => base.patch(&Blob::from_json(case.get("patch").as_str())),
            other => panic!("unknown op {}", other),
        };
        assert_eq!(
            result.hex(),
            case.get("hex").as_str(),
            "op={} base={}",
            op,
            case.get("base").as_str()
        );
    }
}

#[test]
fn extract_matches() {
    let v = load_vectors();
    for case in v.get("extract").as_arr() {
        let blob = Blob::from_json(case.get("base").as_str());
        let path = case.get("path").as_str();
        assert_eq!(
            blob.type_str_at(path),
            case.get("type").as_str(),
            "type {} {}",
            case.get("base").as_str(),
            path
        );
        let value = blob.extract(path);
        assert_eq!(Builder::quote(&value).to_json(), case.get("vjson").as_str());
    }
}

#[test]
fn array_length_matches() {
    let v = load_vectors();
    for case in v.get("array_length").as_arr() {
        let blob = Blob::from_json(case.get("base").as_str());
        let path = case.get("path").as_str();
        let got = if path == "$" {
            blob.array_length()
        } else {
            blob.array_length_at(path)
        };
        assert_eq!(
            got,
            case.get("len").as_i64(),
            "{} {}",
            case.get("base").as_str(),
            path
        );
    }
}

#[test]
fn iterate_matches() {
    let v = load_vectors();
    for case in v.get("iterate").as_arr() {
        let blob = Blob::from_json(case.get("base").as_str());
        let rows = Iterator::new(
            &blob,
            case.get("path").as_str(),
            case.get("recursive").as_bool(),
        )
        .rows();
        let expected = case.get("rows").as_arr();
        assert_eq!(
            rows.len(),
            expected.len(),
            "{} {}",
            case.get("base").as_str(),
            case.get("path").as_str()
        );
        for (got, exp) in rows.iter().zip(expected.iter()) {
            assert_eq!(got.fullkey, exp.get("fullkey").as_str());
            assert_eq!(got.path, exp.get("path").as_str());
            assert_eq!(got.id as i64, exp.get("id").as_i64());
            assert_eq!(msgpack_blob::type_str(got.ty), exp.get("type").as_str());
            if exp.has("key") {
                assert_eq!(got.key, exp.get("key").as_str());
                assert_eq!(got.index, exp.get("index").as_i64());
            }
        }
    }
}

#[test]
fn vector_helpers_roundtrip() {
    assert_eq!(bytes_to_hex(&hex_to_bytes("deadbeef")), "deadbeef");
}
