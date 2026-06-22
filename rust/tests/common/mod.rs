//! Shared test support: a tiny std-only JSON reader (so the crate stays
//! dependency-free) plus helpers to replay the cross-language vectors.

#![allow(dead_code)]

use std::collections::BTreeMap;

use msgpack_blob::Value;

/// A minimal JSON value.
#[derive(Clone, Debug)]
pub enum Json {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Arr(Vec<Json>),
    Obj(BTreeMap<String, Json>),
}

impl Json {
    pub fn as_str(&self) -> &str {
        match self {
            Json::Str(s) => s,
            _ => panic!("expected string, got {:?}", self),
        }
    }
    pub fn as_f64(&self) -> f64 {
        match self {
            Json::Num(n) => *n,
            _ => panic!("expected number, got {:?}", self),
        }
    }
    pub fn as_i64(&self) -> i64 {
        self.as_f64() as i64
    }
    pub fn as_bool(&self) -> bool {
        match self {
            Json::Bool(b) => *b,
            _ => panic!("expected bool, got {:?}", self),
        }
    }
    pub fn as_arr(&self) -> &[Json] {
        match self {
            Json::Arr(a) => a,
            _ => panic!("expected array, got {:?}", self),
        }
    }
    pub fn get(&self, key: &str) -> &Json {
        match self {
            Json::Obj(m) => m.get(key).unwrap_or_else(|| panic!("missing key {}", key)),
            _ => panic!("expected object, got {:?}", self),
        }
    }
    pub fn has(&self, key: &str) -> bool {
        matches!(self, Json::Obj(m) if m.contains_key(key))
    }
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }
    fn value(&mut self) -> Json {
        self.ws();
        match self.b[self.i] {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => Json::Str(self.string()),
            b't' => {
                self.i += 4;
                Json::Bool(true)
            }
            b'f' => {
                self.i += 5;
                Json::Bool(false)
            }
            b'n' => {
                self.i += 4;
                Json::Null
            }
            _ => self.number(),
        }
    }
    fn object(&mut self) -> Json {
        let mut m = BTreeMap::new();
        self.i += 1; // {
        self.ws();
        if self.b[self.i] == b'}' {
            self.i += 1;
            return Json::Obj(m);
        }
        loop {
            self.ws();
            let key = self.string();
            self.ws();
            assert_eq!(self.b[self.i], b':');
            self.i += 1;
            let val = self.value();
            m.insert(key, val);
            self.ws();
            match self.b[self.i] {
                b',' => {
                    self.i += 1;
                }
                b'}' => {
                    self.i += 1;
                    break;
                }
                c => panic!("unexpected {} in object", c as char),
            }
        }
        Json::Obj(m)
    }
    fn array(&mut self) -> Json {
        let mut v = Vec::new();
        self.i += 1; // [
        self.ws();
        if self.b[self.i] == b']' {
            self.i += 1;
            return Json::Arr(v);
        }
        loop {
            v.push(self.value());
            self.ws();
            match self.b[self.i] {
                b',' => {
                    self.i += 1;
                }
                b']' => {
                    self.i += 1;
                    break;
                }
                c => panic!("unexpected {} in array", c as char),
            }
        }
        Json::Arr(v)
    }
    fn string(&mut self) -> String {
        assert_eq!(self.b[self.i], b'"');
        self.i += 1;
        let mut out: Vec<u8> = Vec::new();
        while self.b[self.i] != b'"' {
            let c = self.b[self.i];
            if c == b'\\' {
                self.i += 1;
                let esc = self.b[self.i];
                self.i += 1;
                match esc {
                    b'"' => out.push(b'"'),
                    b'\\' => out.push(b'\\'),
                    b'/' => out.push(b'/'),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0c),
                    b'u' => {
                        let mut cp = hex4(self.b, self.i);
                        self.i += 4;
                        if (0xd800..=0xdbff).contains(&cp)
                            && self.b[self.i] == b'\\'
                            && self.b[self.i + 1] == b'u'
                        {
                            let lo = hex4(self.b, self.i + 2);
                            if (0xdc00..=0xdfff).contains(&lo) {
                                self.i += 6;
                                cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
                            }
                        }
                        push_utf8(&mut out, cp as u32);
                    }
                    other => out.push(other),
                }
            } else {
                out.push(c);
                self.i += 1;
            }
        }
        self.i += 1; // closing "
        String::from_utf8(out).expect("vectors strings are valid UTF-8")
    }
    fn number(&mut self) -> Json {
        let start = self.i;
        while self.i < self.b.len()
            && matches!(
                self.b[self.i],
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'
            )
        {
            self.i += 1;
        }
        let s = std::str::from_utf8(&self.b[start..self.i]).unwrap();
        Json::Num(s.parse().unwrap())
    }
}

fn hex4(b: &[u8], off: usize) -> i32 {
    let mut v = 0;
    for j in 0..4 {
        let c = b[off + j];
        let h = match c {
            b'0'..=b'9' => (c - b'0') as i32,
            b'a'..=b'f' => (c - b'a' + 10) as i32,
            b'A'..=b'F' => (c - b'A' + 10) as i32,
            _ => 0,
        };
        v = (v << 4) | h;
    }
    v
}

fn push_utf8(out: &mut Vec<u8>, cp: u32) {
    if cp < 0x80 {
        out.push(cp as u8);
    } else if cp < 0x800 {
        out.push(0xc0 | (cp >> 6) as u8);
        out.push(0x80 | (cp & 0x3f) as u8);
    } else if cp < 0x10000 {
        out.push(0xe0 | (cp >> 12) as u8);
        out.push(0x80 | ((cp >> 6) & 0x3f) as u8);
        out.push(0x80 | (cp & 0x3f) as u8);
    } else {
        out.push(0xf0 | (cp >> 18) as u8);
        out.push(0x80 | ((cp >> 12) & 0x3f) as u8);
        out.push(0x80 | ((cp >> 6) & 0x3f) as u8);
        out.push(0x80 | (cp & 0x3f) as u8);
    }
}

pub fn parse_json(text: &str) -> Json {
    let mut p = Parser {
        b: text.as_bytes(),
        i: 0,
    };
    p.value()
}

/// Load and parse the shared cross-language vector file.
pub fn load_vectors() -> Json {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/vectors/blob_vectors.json"
    );
    let text = std::fs::read_to_string(path).expect("read blob_vectors.json");
    parse_json(&text)
}

pub fn hex_to_bytes(h: &str) -> Vec<u8> {
    let bytes = h.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let nib = |c: u8| -> u8 {
        match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => 0,
        }
    };
    let mut i = 0;
    while i + 1 < bytes.len() {
        out.push((nib(bytes[i]) << 4) | nib(bytes[i + 1]));
        i += 2;
    }
    out
}

pub fn bytes_to_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push_str(&format!("{:02x}", x));
    }
    s
}

/// Build a `Value` from a ValueSpec object (see `cpp/tests/gen_blob_vectors.cpp`).
pub fn build_value(spec: &Json) -> Value {
    match spec.get("k").as_str() {
        "nil" => Value::nil(),
        "bool" => Value::boolean(spec.get("v").as_bool()),
        "int" => Value::integer(spec.get("v").as_str().parse().unwrap()),
        "uint" => Value::unsigned_integer(spec.get("v").as_str().parse().unwrap()),
        "int8" => Value::int8(spec.get("v").as_str().parse().unwrap()),
        "int16" => Value::int16(spec.get("v").as_str().parse().unwrap()),
        "int32" => Value::int32(spec.get("v").as_str().parse().unwrap()),
        "int64" => Value::int64(spec.get("v").as_str().parse().unwrap()),
        "uint8" => Value::uint8(spec.get("v").as_str().parse().unwrap()),
        "uint16" => Value::uint16(spec.get("v").as_str().parse().unwrap()),
        "uint32" => Value::uint32(spec.get("v").as_str().parse().unwrap()),
        "uint64" => Value::uint64(spec.get("v").as_str().parse().unwrap()),
        "real" => Value::real(spec.get("v").as_f64()),
        "real32" => Value::real32(spec.get("v").as_f64() as f32),
        "str" => Value::string(spec.get("v").as_str()),
        "binary" => Value::binary(&hex_to_bytes(spec.get("hex").as_str())),
        "ext" => Value::ext(
            spec.get("type").as_i64() as i8,
            &hex_to_bytes(spec.get("hex").as_str()),
        ),
        "timestamp" => Value::timestamp_ns(
            spec.get("sec").as_str().parse().unwrap(),
            spec.get("nsec").as_i64() as u32,
        ),
        other => panic!("unknown spec kind {}", other),
    }
}
