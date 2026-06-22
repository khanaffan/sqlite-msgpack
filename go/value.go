package msgpackblob

// Type is the semantic type of a MessagePack element.
type Type int

// Type constants.
const (
	TypeNil Type = iota
	TypeTrue
	TypeFalse
	TypeInteger
	TypeReal
	TypeFloat32
	TypeString
	TypeBinary
	TypeArray
	TypeMap
	TypeExt
	TypeTimestamp
)

// String returns the human-readable label (matches the C++ type_str).
func (t Type) String() string {
	switch t {
	case TypeNil:
		return "null"
	case TypeTrue:
		return "true"
	case TypeFalse:
		return "false"
	case TypeInteger:
		return "integer"
	case TypeReal:
		return "real"
	case TypeFloat32:
		return "float32"
	case TypeString:
		return "text"
	case TypeBinary:
		return "binary"
	case TypeArray:
		return "array"
	case TypeMap:
		return "map"
	case TypeExt:
		return "ext"
	case TypeTimestamp:
		return "timestamp"
	}
	return "null"
}

// TypeStr returns the label for t (`"text"`, `"integer"`, …).
func TypeStr(t Type) string {
	return t.String()
}

// IntWidth is an integer encoding-width hint (forces a specific wire format).
type IntWidth int

// IntWidth constants.
const (
	WidthAuto IntWidth = iota
	WidthInt8
	WidthInt16
	WidthInt32
	WidthInt64
	WidthUint8
	WidthUint16
	WidthUint32
	WidthUint64
)

// Value is a decoded scalar or sub-blob value. Integer payloads are stored as
// raw 64-bit bits so the full signed/unsigned range round-trips exactly.
type Value struct {
	ty       Type
	bits     uint64 // integer bits / timestamp seconds
	float    float64
	bytes    []byte // string / binary / ext payload (no header)
	extType  int8
	tsNsec   uint32
	intWidth IntWidth
}

// ── accessors ─────────────────────────────────────────────────────────
func (v Value) Type() Type   { return v.ty }
func (v Value) IsNil() bool  { return v.ty == TypeNil }
func (v Value) AsBool() bool { return v.ty == TypeTrue }

func (v Value) AsInt64() int64 {
	switch v.ty {
	case TypeInteger, TypeTimestamp:
		return int64(v.bits)
	case TypeReal, TypeFloat32:
		return int64(v.float)
	case TypeTrue:
		return 1
	}
	return 0
}

func (v Value) AsUint64() uint64 {
	if v.ty == TypeInteger {
		return v.bits
	}
	return 0
}

func (v Value) AsFloat64() float64 {
	switch v.ty {
	case TypeReal, TypeFloat32:
		return v.float
	case TypeInteger:
		return float64(v.AsInt64())
	}
	return 0
}

func (v Value) AsFloat32() float32 {
	switch v.ty {
	case TypeFloat32, TypeReal:
		return float32(v.float)
	}
	return 0
}

// AsString returns the string payload as a Go string (byte-preserving — Go
// strings hold arbitrary bytes).
func (v Value) AsString() string {
	if v.ty == TypeString {
		return string(v.bytes)
	}
	return ""
}

// AsBytes returns the raw string payload bytes.
func (v Value) AsBytes() []byte {
	if v.ty == TypeString {
		return v.bytes
	}
	return nil
}

// BlobData returns the Binary/Ext payload (no header), or raw bytes for
// container values.
func (v Value) BlobData() []byte { return v.bytes }
func (v Value) BlobSize() int    { return len(v.bytes) }
func (v Value) ExtType() int8    { return v.extType }

func (v Value) TimestampSeconds() int64 {
	if v.ty == TypeTimestamp {
		return int64(v.bits)
	}
	return 0
}

func (v Value) TimestampNanoseconds() uint32 {
	if v.ty == TypeTimestamp {
		return v.tsNsec
	}
	return 0
}

func (v Value) IntWidth() IntWidth { return v.intWidth }

// ── constructors ──────────────────────────────────────────────────────

// Nil returns a nil Value.
func Nil() Value { return Value{ty: TypeNil} }

// Bool returns a boolean Value.
func Bool(b bool) Value {
	if b {
		return Value{ty: TypeTrue}
	}
	return Value{ty: TypeFalse}
}

// Int returns a compact-encoded integer Value.
func Int(x int64) Value { return Value{ty: TypeInteger, bits: uint64(x)} }

// Uint returns a compact-encoded unsigned integer Value.
func Uint(x uint64) Value {
	v := Value{ty: TypeInteger, bits: x}
	if x > uint64(^uint64(0)>>1) {
		v.intWidth = WidthUint64
	}
	return v
}

// Real returns a float64 Value.
func Real(d float64) Value { return Value{ty: TypeReal, float: d} }

// Real32 returns a float32 Value.
func Real32(f float32) Value { return Value{ty: TypeFloat32, float: float64(f)} }

// Str returns a string Value.
func Str(s string) Value { return Value{ty: TypeString, bytes: []byte(s)} }

// StrBytes returns a string Value from raw bytes (may be non-UTF-8).
func StrBytes(b []byte) Value { return Value{ty: TypeString, bytes: append([]byte(nil), b...)} }

// Bin returns a binary Value.
func Bin(data []byte) Value { return Value{ty: TypeBinary, bytes: append([]byte(nil), data...)} }

// Ext returns an ext Value.
func Ext(typeCode int8, data []byte) Value {
	return Value{ty: TypeExt, extType: typeCode, bytes: append([]byte(nil), data...)}
}

// Timestamp returns a timestamp Value (nanoseconds = 0).
func Timestamp(seconds int64) Value {
	return Value{ty: TypeTimestamp, bits: uint64(seconds)}
}

// TimestampNs returns a timestamp Value with nanoseconds.
func TimestampNs(seconds int64, nanoseconds uint32) Value {
	return Value{ty: TypeTimestamp, bits: uint64(seconds), tsNsec: nanoseconds}
}

func fixedInt(width IntWidth, bits uint64) Value {
	return Value{ty: TypeInteger, bits: bits, intWidth: width}
}

// Fixed-width integer constructors (force a specific wire encoding).
func Int8(x int8) Value     { return fixedInt(WidthInt8, uint64(int64(x))) }
func Int16(x int16) Value   { return fixedInt(WidthInt16, uint64(int64(x))) }
func Int32(x int32) Value   { return fixedInt(WidthInt32, uint64(int64(x))) }
func Int64(x int64) Value   { return fixedInt(WidthInt64, uint64(x)) }
func Uint8(x uint8) Value   { return fixedInt(WidthUint8, uint64(x)) }
func Uint16(x uint16) Value { return fixedInt(WidthUint16, uint64(x)) }
func Uint32(x uint32) Value { return fixedInt(WidthUint32, uint64(x)) }
func Uint64(x uint64) Value { return fixedInt(WidthUint64, x) }
