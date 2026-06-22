//! Internal: encoding primitives (mirrors `msgpack_blob_encode.cpp`).

use crate::format as f;
use crate::value::{IntWidth, Type, Value};

pub fn enc_nil(out: &mut Vec<u8>) {
    out.push(f::MP_NIL);
}

pub fn enc_bool(out: &mut Vec<u8>, v: bool) {
    out.push(if v { f::MP_TRUE } else { f::MP_FALSE });
}

pub fn enc_integer(out: &mut Vec<u8>, x: i64) {
    if x >= 0 {
        if x <= 0x7f {
            out.push(x as u8);
        } else if x <= 0xff {
            out.push(f::MP_UINT8);
            out.push(x as u8);
        } else if x <= 0xffff {
            out.push(f::MP_UINT16);
            f::push16(out, x as u16);
        } else if x <= 0xffff_ffff {
            out.push(f::MP_UINT32);
            f::push32(out, x as u32);
        } else {
            out.push(f::MP_UINT64);
            f::push64(out, x as u64);
        }
    } else if x >= -32 {
        out.push(x as u8);
    } else if x >= -128 {
        out.push(f::MP_INT8);
        out.push(x as u8);
    } else if x >= -32768 {
        out.push(f::MP_INT16);
        f::push16(out, x as u16);
    } else if x >= -2147483648 {
        out.push(f::MP_INT32);
        f::push32(out, x as u32);
    } else {
        out.push(f::MP_INT64);
        f::push64(out, x as u64);
    }
}

pub fn enc_unsigned(out: &mut Vec<u8>, x: u64) {
    if x <= 0x7f {
        out.push(x as u8);
    } else if x <= 0xff {
        out.push(f::MP_UINT8);
        out.push(x as u8);
    } else if x <= 0xffff {
        out.push(f::MP_UINT16);
        f::push16(out, x as u16);
    } else if x <= 0xffff_ffff {
        out.push(f::MP_UINT32);
        f::push32(out, x as u32);
    } else {
        out.push(f::MP_UINT64);
        f::push64(out, x);
    }
}

pub fn enc_real(out: &mut Vec<u8>, d: f64) {
    out.push(f::MP_FLOAT64);
    f::push64(out, d.to_bits());
}

pub fn enc_real32(out: &mut Vec<u8>, val: f32) {
    out.push(f::MP_FLOAT32);
    f::push32(out, val.to_bits());
}

pub fn enc_string(out: &mut Vec<u8>, s: &[u8]) {
    let n = s.len();
    if n <= 31 {
        out.push(f::MP_FIXSTR_MASK | n as u8);
    } else if n <= 0xff {
        out.push(f::MP_STR8);
        out.push(n as u8);
    } else if n <= 0xffff {
        out.push(f::MP_STR16);
        f::push16(out, n as u16);
    } else {
        out.push(f::MP_STR32);
        f::push32(out, n as u32);
    }
    out.extend_from_slice(s);
}

pub fn enc_binary(out: &mut Vec<u8>, data: &[u8]) {
    let n = data.len();
    if n <= 0xff {
        out.push(f::MP_BIN8);
        out.push(n as u8);
    } else if n <= 0xffff {
        out.push(f::MP_BIN16);
        f::push16(out, n as u16);
    } else {
        out.push(f::MP_BIN32);
        f::push32(out, n as u32);
    }
    out.extend_from_slice(data);
}

pub fn enc_ext(out: &mut Vec<u8>, type_code: i8, data: &[u8]) {
    let n = data.len();
    match n {
        1 => out.push(f::MP_FIXEXT1),
        2 => out.push(f::MP_FIXEXT2),
        4 => out.push(f::MP_FIXEXT4),
        8 => out.push(f::MP_FIXEXT8),
        16 => out.push(f::MP_FIXEXT16),
        _ => {
            if n <= 0xff {
                out.push(f::MP_EXT8);
                out.push(n as u8);
            } else if n <= 0xffff {
                out.push(f::MP_EXT16);
                f::push16(out, n as u16);
            } else {
                out.push(f::MP_EXT32);
                f::push32(out, n as u32);
            }
        }
    }
    out.push(type_code as u8);
    out.extend_from_slice(data);
}

pub fn enc_int8(out: &mut Vec<u8>, x: i64) {
    out.push(f::MP_INT8);
    out.push(x as u8);
}
pub fn enc_int16(out: &mut Vec<u8>, x: i64) {
    out.push(f::MP_INT16);
    f::push16(out, x as u16);
}
pub fn enc_int32(out: &mut Vec<u8>, x: i64) {
    out.push(f::MP_INT32);
    f::push32(out, x as u32);
}
pub fn enc_int64(out: &mut Vec<u8>, x: i64) {
    out.push(f::MP_INT64);
    f::push64(out, x as u64);
}
pub fn enc_uint8(out: &mut Vec<u8>, x: u64) {
    out.push(f::MP_UINT8);
    out.push(x as u8);
}
pub fn enc_uint16(out: &mut Vec<u8>, x: u64) {
    out.push(f::MP_UINT16);
    f::push16(out, x as u16);
}
pub fn enc_uint32(out: &mut Vec<u8>, x: u64) {
    out.push(f::MP_UINT32);
    f::push32(out, x as u32);
}
pub fn enc_uint64(out: &mut Vec<u8>, x: u64) {
    out.push(f::MP_UINT64);
    f::push64(out, x);
}

pub fn enc_array_header(out: &mut Vec<u8>, count: u32) {
    if count <= 15 {
        out.push(f::MP_FIXARRAY_MASK | count as u8);
    } else if count <= 0xffff {
        out.push(f::MP_ARRAY16);
        f::push16(out, count as u16);
    } else {
        out.push(f::MP_ARRAY32);
        f::push32(out, count);
    }
}

pub fn enc_map_header(out: &mut Vec<u8>, count: u32) {
    if count <= 15 {
        out.push(f::MP_FIXMAP_MASK | count as u8);
    } else if count <= 0xffff {
        out.push(f::MP_MAP16);
        f::push16(out, count as u16);
    } else {
        out.push(f::MP_MAP32);
        f::push32(out, count);
    }
}

pub fn enc_timestamp(out: &mut Vec<u8>, sec: i64, nsec: u32) {
    if nsec == 0 && (0..=0xffff_ffff).contains(&sec) {
        out.push(f::MP_FIXEXT4);
        out.push(0xff);
        f::push32(out, sec as u32);
    } else if (0..=0x3_FFFF_FFFF).contains(&sec) {
        out.push(f::MP_FIXEXT8);
        out.push(0xff);
        f::push64(out, ((nsec as u64) << 34) | sec as u64);
    } else {
        out.push(f::MP_EXT8);
        out.push(12);
        out.push(0xff);
        f::push32(out, nsec);
        f::push64(out, sec as u64);
    }
}

pub fn encode_value(out: &mut Vec<u8>, v: &Value) {
    match v.get_type() {
        Type::Nil => enc_nil(out),
        Type::True => enc_bool(out, true),
        Type::False => enc_bool(out, false),
        Type::Integer => match v.int_width() {
            IntWidth::Int8 => enc_int8(out, v.as_i64()),
            IntWidth::Int16 => enc_int16(out, v.as_i64()),
            IntWidth::Int32 => enc_int32(out, v.as_i64()),
            IntWidth::Int64 => enc_int64(out, v.as_i64()),
            IntWidth::Uint8 => enc_uint8(out, v.as_u64()),
            IntWidth::Uint16 => enc_uint16(out, v.as_u64()),
            IntWidth::Uint32 => enc_uint32(out, v.as_u64()),
            IntWidth::Uint64 => enc_uint64(out, v.as_u64()),
            IntWidth::Auto => enc_integer(out, v.as_i64()),
        },
        Type::Real => enc_real(out, v.as_f64()),
        Type::Float32 => enc_real32(out, v.as_f32()),
        Type::String => enc_string(out, v.as_bytes()),
        Type::Binary => enc_binary(out, v.blob_data()),
        Type::Ext => enc_ext(out, v.ext_type(), v.blob_data()),
        Type::Timestamp => enc_timestamp(out, v.timestamp_seconds(), v.timestamp_nanoseconds()),
        // Array / Map are not directly encodable as a scalar Value (the C++
        // reference falls through to nil here).
        Type::Array | Type::Map => enc_nil(out),
    }
}
