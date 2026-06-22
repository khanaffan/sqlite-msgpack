//! `Builder` — a streaming encoder that produces a [`Blob`].

use crate::blob::Blob;
use crate::encode as e;
use crate::value::Value;

/// Append msgpack elements in order, then finalise with [`Builder::build`].
#[derive(Default)]
pub struct Builder {
    buf: Vec<u8>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder { buf: Vec::new() }
    }

    // ── scalars ───────────────────────────────────────────────────────
    pub fn nil(&mut self) -> &mut Self {
        e::enc_nil(&mut self.buf);
        self
    }
    pub fn boolean(&mut self, v: bool) -> &mut Self {
        e::enc_bool(&mut self.buf, v);
        self
    }
    pub fn integer(&mut self, x: i64) -> &mut Self {
        e::enc_integer(&mut self.buf, x);
        self
    }
    pub fn unsigned_integer(&mut self, x: u64) -> &mut Self {
        e::enc_unsigned(&mut self.buf, x);
        self
    }
    pub fn real(&mut self, d: f64) -> &mut Self {
        e::enc_real(&mut self.buf, d);
        self
    }
    pub fn real32(&mut self, val: f32) -> &mut Self {
        e::enc_real32(&mut self.buf, val);
        self
    }
    pub fn string(&mut self, s: &str) -> &mut Self {
        e::enc_string(&mut self.buf, s.as_bytes());
        self
    }
    pub fn string_bytes(&mut self, s: &[u8]) -> &mut Self {
        e::enc_string(&mut self.buf, s);
        self
    }
    pub fn binary(&mut self, data: &[u8]) -> &mut Self {
        e::enc_binary(&mut self.buf, data);
        self
    }
    pub fn ext(&mut self, type_code: i8, data: &[u8]) -> &mut Self {
        e::enc_ext(&mut self.buf, type_code, data);
        self
    }

    // ── fixed-width integers ──────────────────────────────────────────
    pub fn int8(&mut self, x: i8) -> &mut Self {
        e::enc_int8(&mut self.buf, x as i64);
        self
    }
    pub fn int16(&mut self, x: i16) -> &mut Self {
        e::enc_int16(&mut self.buf, x as i64);
        self
    }
    pub fn int32(&mut self, x: i32) -> &mut Self {
        e::enc_int32(&mut self.buf, x as i64);
        self
    }
    pub fn int64(&mut self, x: i64) -> &mut Self {
        e::enc_int64(&mut self.buf, x);
        self
    }
    pub fn uint8(&mut self, x: u8) -> &mut Self {
        e::enc_uint8(&mut self.buf, x as u64);
        self
    }
    pub fn uint16(&mut self, x: u16) -> &mut Self {
        e::enc_uint16(&mut self.buf, x as u64);
        self
    }
    pub fn uint32(&mut self, x: u32) -> &mut Self {
        e::enc_uint32(&mut self.buf, x as u64);
        self
    }
    pub fn uint64(&mut self, x: u64) -> &mut Self {
        e::enc_uint64(&mut self.buf, x);
        self
    }

    // ── containers ────────────────────────────────────────────────────
    pub fn array_header(&mut self, count: u32) -> &mut Self {
        e::enc_array_header(&mut self.buf, count);
        self
    }
    pub fn map_header(&mut self, count: u32) -> &mut Self {
        e::enc_map_header(&mut self.buf, count);
        self
    }

    // ── embedding & timestamp ─────────────────────────────────────────
    pub fn raw(&mut self, data: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(data);
        self
    }
    pub fn raw_blob(&mut self, blob: &Blob) -> &mut Self {
        self.buf.extend_from_slice(blob.data());
        self
    }
    pub fn value(&mut self, v: &Value) -> &mut Self {
        e::encode_value(&mut self.buf, v);
        self
    }
    pub fn timestamp(&mut self, sec: i64) -> &mut Self {
        e::enc_timestamp(&mut self.buf, sec, 0);
        self
    }
    pub fn timestamp_ns(&mut self, sec: i64, nsec: u32) -> &mut Self {
        e::enc_timestamp(&mut self.buf, sec, nsec);
        self
    }

    // ── finalize ──────────────────────────────────────────────────────
    /// Finalise into a [`Blob`]. Takes `&self` so it can terminate a method
    /// chain (`Builder::new().integer(1).build()`).
    pub fn build(&self) -> Blob {
        Blob::from_vec(self.buf.clone())
    }
    pub fn len(&self) -> usize {
        self.buf.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// One-shot: encode a single [`Value`] into a [`Blob`].
    pub fn quote(v: &Value) -> Blob {
        let mut b = Builder::new();
        b.value(v);
        b.build()
    }
}
