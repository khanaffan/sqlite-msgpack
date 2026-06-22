//! `Value` — a decoded scalar or sub-blob MessagePack value.
//!
//! Mirrors `msgpack::Value` from the C++ Blob library. Integer values are kept
//! as raw 64-bit bits so the full signed/unsigned range round-trips exactly.

/// Semantic type of a MessagePack element.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Type {
    Nil,
    True,
    False,
    Integer,
    Real,
    Float32,
    String,
    Binary,
    Array,
    Map,
    Ext,
    Timestamp,
}

/// Integer encoding-width hint (forces a specific wire format).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IntWidth {
    Auto,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
}

/// Human-readable label for a [`Type`] (`"text"`, `"integer"`, …).
pub fn type_str(t: Type) -> &'static str {
    match t {
        Type::Nil => "null",
        Type::True => "true",
        Type::False => "false",
        Type::Integer => "integer",
        Type::Real => "real",
        Type::Float32 => "float32",
        Type::String => "text",
        Type::Binary => "binary",
        Type::Array => "array",
        Type::Map => "map",
        Type::Ext => "ext",
        Type::Timestamp => "timestamp",
    }
}

/// A decoded scalar or sub-blob value.
#[derive(Clone, Debug)]
pub struct Value {
    pub(crate) ty: Type,
    pub(crate) bits: u64, // integer bits (signed or unsigned) / timestamp seconds
    pub(crate) float: f64, // real / float32 payload
    pub(crate) bytes: Vec<u8>, // string / binary / ext payload (no header)
    pub(crate) ext_type: i8,
    pub(crate) ts_nsec: u32,
    pub(crate) int_width: IntWidth,
}

impl Default for Value {
    fn default() -> Self {
        Value {
            ty: Type::Nil,
            bits: 0,
            float: 0.0,
            bytes: Vec::new(),
            ext_type: 0,
            ts_nsec: 0,
            int_width: IntWidth::Auto,
        }
    }
}

impl Value {
    // ── accessors ─────────────────────────────────────────────────────
    pub fn get_type(&self) -> Type {
        self.ty
    }
    pub fn is_nil(&self) -> bool {
        self.ty == Type::Nil
    }
    pub fn as_bool(&self) -> bool {
        self.ty == Type::True
    }
    pub fn as_i64(&self) -> i64 {
        match self.ty {
            Type::Integer => self.bits as i64,
            Type::Real | Type::Float32 => self.float as i64,
            Type::Timestamp => self.bits as i64,
            Type::True => 1,
            _ => 0,
        }
    }
    pub fn as_u64(&self) -> u64 {
        if self.ty == Type::Integer {
            self.bits
        } else {
            0
        }
    }
    pub fn as_f64(&self) -> f64 {
        match self.ty {
            Type::Real | Type::Float32 => self.float,
            Type::Integer => self.as_i64() as f64,
            _ => 0.0,
        }
    }
    pub fn as_f32(&self) -> f32 {
        match self.ty {
            Type::Float32 => self.float as f32,
            Type::Real => self.float as f32,
            _ => 0.0,
        }
    }
    /// String payload bytes (verbatim; may not be valid UTF-8).
    pub fn as_bytes(&self) -> &[u8] {
        if self.ty == Type::String {
            &self.bytes
        } else {
            &[]
        }
    }
    /// String payload decoded lossily as UTF-8 (exact for valid UTF-8).
    pub fn as_string(&self) -> String {
        if self.ty == Type::String {
            String::from_utf8_lossy(&self.bytes).into_owned()
        } else {
            String::new()
        }
    }
    /// Binary / Ext payload (no header), or raw bytes for container values.
    pub fn blob_data(&self) -> &[u8] {
        &self.bytes
    }
    pub fn blob_size(&self) -> usize {
        self.bytes.len()
    }
    pub fn ext_type(&self) -> i8 {
        self.ext_type
    }
    pub fn timestamp_seconds(&self) -> i64 {
        if self.ty == Type::Timestamp {
            self.bits as i64
        } else {
            0
        }
    }
    pub fn timestamp_nanoseconds(&self) -> u32 {
        if self.ty == Type::Timestamp {
            self.ts_nsec
        } else {
            0
        }
    }
    pub fn int_width(&self) -> IntWidth {
        self.int_width
    }

    // ── static constructors ───────────────────────────────────────────
    pub fn nil() -> Value {
        Value::default()
    }
    pub fn boolean(b: bool) -> Value {
        Value {
            ty: if b { Type::True } else { Type::False },
            ..Default::default()
        }
    }
    pub fn integer(x: i64) -> Value {
        Value {
            ty: Type::Integer,
            bits: x as u64,
            ..Default::default()
        }
    }
    pub fn unsigned_integer(x: u64) -> Value {
        let mut v = Value {
            ty: Type::Integer,
            bits: x,
            ..Default::default()
        };
        if x > i64::MAX as u64 {
            v.int_width = IntWidth::Uint64;
        }
        v
    }
    pub fn real(d: f64) -> Value {
        Value {
            ty: Type::Real,
            float: d,
            ..Default::default()
        }
    }
    pub fn real32(f: f32) -> Value {
        Value {
            ty: Type::Float32,
            float: f as f64,
            ..Default::default()
        }
    }
    pub fn string(s: &str) -> Value {
        Value::string_bytes(s.as_bytes())
    }
    pub fn string_bytes(b: &[u8]) -> Value {
        Value {
            ty: Type::String,
            bytes: b.to_vec(),
            ..Default::default()
        }
    }
    pub fn binary(data: &[u8]) -> Value {
        Value {
            ty: Type::Binary,
            bytes: data.to_vec(),
            ..Default::default()
        }
    }
    pub fn ext(type_code: i8, data: &[u8]) -> Value {
        Value {
            ty: Type::Ext,
            ext_type: type_code,
            bytes: data.to_vec(),
            ..Default::default()
        }
    }
    pub fn timestamp(seconds: i64) -> Value {
        Value {
            ty: Type::Timestamp,
            bits: seconds as u64,
            ts_nsec: 0,
            ..Default::default()
        }
    }
    pub fn timestamp_ns(seconds: i64, nanoseconds: u32) -> Value {
        Value {
            ty: Type::Timestamp,
            bits: seconds as u64,
            ts_nsec: nanoseconds,
            ..Default::default()
        }
    }

    fn fixed(width: IntWidth, bits: u64) -> Value {
        Value {
            ty: Type::Integer,
            bits,
            int_width: width,
            ..Default::default()
        }
    }
    pub fn int8(x: i8) -> Value {
        Value::fixed(IntWidth::Int8, x as i64 as u64)
    }
    pub fn int16(x: i16) -> Value {
        Value::fixed(IntWidth::Int16, x as i64 as u64)
    }
    pub fn int32(x: i32) -> Value {
        Value::fixed(IntWidth::Int32, x as i64 as u64)
    }
    pub fn int64(x: i64) -> Value {
        Value::fixed(IntWidth::Int64, x as u64)
    }
    pub fn uint8(x: u8) -> Value {
        Value::fixed(IntWidth::Uint8, x as u64)
    }
    pub fn uint16(x: u16) -> Value {
        Value::fixed(IntWidth::Uint16, x as u64)
    }
    pub fn uint32(x: u32) -> Value {
        Value::fixed(IntWidth::Uint32, x as u64)
    }
    pub fn uint64(x: u64) -> Value {
        Value::fixed(IntWidth::Uint64, x)
    }
}
