//! `Blob` — an owning byte buffer wrapping a msgpack-encoded value.

use crate::decode as d;
use crate::encode as e;
use crate::json as j;
use crate::mutate as m;
use crate::value::{Type, Value};

/// A MessagePack BLOB supporting read, mutation (copy-on-write) and JSON.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Blob {
    data: Vec<u8>,
}

impl Blob {
    /// Construct from raw bytes (copies).
    pub fn new(data: &[u8]) -> Blob {
        Blob {
            data: data.to_vec(),
        }
    }
    /// Construct by taking ownership of a byte vector.
    pub fn from_vec(data: Vec<u8>) -> Blob {
        Blob { data }
    }

    // ── raw access ────────────────────────────────────────────────────
    pub fn data(&self) -> &[u8] {
        &self.data
    }
    pub fn size(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn hex(&self) -> String {
        let mut s = String::with_capacity(self.data.len() * 2);
        for &b in &self.data {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }

    // ── validation ────────────────────────────────────────────────────
    pub fn valid(&self) -> bool {
        d::is_valid(&self.data, self.data.len())
    }
    pub fn error_position(&self) -> usize {
        d::error_position(&self.data, self.data.len())
    }

    // ── type inspection ───────────────────────────────────────────────
    /// Type of the root element.
    pub fn root_type(&self) -> Type {
        if self.data.is_empty() {
            Type::Nil
        } else {
            d::get_type(&self.data, self.data.len(), 0)
        }
    }
    /// Type of the element at `path`.
    pub fn type_at(&self, path: &str) -> Type {
        let n = self.data.len();
        let (rc, istart, _) = d::lookup(&self.data, n, 0, path);
        if rc != d::RC_OK {
            Type::Nil
        } else {
            d::get_type(&self.data, n, istart)
        }
    }
    pub fn type_str(&self) -> &'static str {
        crate::value::type_str(self.root_type())
    }
    pub fn type_str_at(&self, path: &str) -> &'static str {
        crate::value::type_str(self.type_at(path))
    }

    // ── extraction ────────────────────────────────────────────────────
    pub fn extract(&self, path: &str) -> Value {
        let n = self.data.len();
        let (rc, istart, iend) = d::lookup(&self.data, n, 0, path);
        if rc != d::RC_OK {
            Value::nil()
        } else {
            d::decode_element(&self.data, n, istart, iend)
        }
    }

    /// Element count of the root container, or `-1`.
    pub fn array_length(&self) -> i64 {
        if self.data.is_empty() {
            -1
        } else {
            d::get_container_count(&self.data, self.data.len(), 0)
        }
    }
    /// Element count of the container at `path`, or `-1`.
    pub fn array_length_at(&self, path: &str) -> i64 {
        let n = self.data.len();
        let (rc, istart, _) = d::lookup(&self.data, n, 0, path);
        if rc != d::RC_OK {
            -1
        } else {
            d::get_container_count(&self.data, n, istart)
        }
    }

    // ── mutation (copy-on-write) ──────────────────────────────────────
    fn apply(&self, path: &str, value: &Value, mode: i32) -> Blob {
        let mut nb = Vec::new();
        e::encode_value(&mut nb, value);
        let (rc, out) = m::apply_edit(&self.data, self.data.len(), path, &nb, mode);
        if rc == m::RC_OK {
            Blob::from_vec(out)
        } else {
            self.clone()
        }
    }

    pub fn set(&self, path: &str, value: &Value) -> Blob {
        self.apply(path, value, m::EDIT_SET)
    }
    /// Set `path` to an existing sub-blob (embedded verbatim).
    pub fn set_blob(&self, path: &str, sub: &Blob) -> Blob {
        let (rc, out) = m::apply_edit(&self.data, self.data.len(), path, &sub.data, m::EDIT_SET);
        if rc == m::RC_OK {
            Blob::from_vec(out)
        } else {
            self.clone()
        }
    }
    pub fn insert(&self, path: &str, value: &Value) -> Blob {
        self.apply(path, value, m::EDIT_INSERT)
    }
    pub fn replace(&self, path: &str, value: &Value) -> Blob {
        self.apply(path, value, m::EDIT_REPLACE)
    }
    pub fn array_insert(&self, path: &str, value: &Value) -> Blob {
        self.apply(path, value, m::EDIT_ARRAY_INS)
    }
    pub fn remove(&self, path: &str) -> Blob {
        let (rc, out) = m::apply_edit(&self.data, self.data.len(), path, &[], m::EDIT_REMOVE);
        if rc == m::RC_OK {
            Blob::from_vec(out)
        } else {
            self.clone()
        }
    }
    pub fn patch(&self, merge_patch: &Blob) -> Blob {
        let (rc, out) = m::merge_patch(
            &self.data,
            self.data.len(),
            0,
            &merge_patch.data,
            merge_patch.data.len(),
            0,
        );
        if rc == m::RC_OK {
            Blob::from_vec(out)
        } else {
            self.clone()
        }
    }

    // ── JSON conversion ───────────────────────────────────────────────
    /// Byte-exact JSON serialisation (may contain non-UTF-8 string bytes,
    /// identical to the C++ library output).
    pub fn to_json_bytes(&self) -> Vec<u8> {
        if self.data.is_empty() {
            return b"null".to_vec();
        }
        j::to_json_bytes(&self.data, self.data.len(), false, 0)
    }
    /// JSON serialisation as a `String` (exact for valid UTF-8 input).
    pub fn to_json(&self) -> String {
        bytes_to_string(self.to_json_bytes())
    }
    pub fn to_json_pretty_bytes(&self, indent: i32) -> Vec<u8> {
        if self.data.is_empty() {
            return b"null".to_vec();
        }
        let indent = indent.clamp(0, 8);
        j::to_json_bytes(&self.data, self.data.len(), true, indent)
    }
    pub fn to_json_pretty(&self, indent: i32) -> String {
        bytes_to_string(self.to_json_pretty_bytes(indent))
    }

    /// Parse JSON bytes into a msgpack blob.
    pub fn from_json_bytes(json: &[u8]) -> Blob {
        Blob::from_vec(j::from_json(json))
    }
    /// Parse a JSON string into a msgpack blob.
    pub fn from_json(json: &str) -> Blob {
        Blob::from_vec(j::from_json(json.as_bytes()))
    }
}

fn bytes_to_string(b: Vec<u8>) -> String {
    match String::from_utf8(b) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}
