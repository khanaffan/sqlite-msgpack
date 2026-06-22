package msgpackblob

import "math"

func encNil(out *[]byte) { *out = append(*out, mpNil) }

func encBool(out *[]byte, v bool) {
	if v {
		*out = append(*out, mpTrue)
	} else {
		*out = append(*out, mpFalse)
	}
}

func encInteger(out *[]byte, x int64) {
	if x >= 0 {
		switch {
		case x <= 0x7f:
			*out = append(*out, byte(x))
		case x <= 0xff:
			*out = append(*out, mpUint8, byte(x))
		case x <= 0xffff:
			*out = append(*out, mpUint16)
			put16(out, uint16(x))
		case x <= 0xffffffff:
			*out = append(*out, mpUint32)
			put32(out, uint32(x))
		default:
			*out = append(*out, mpUint64)
			put64(out, uint64(x))
		}
	} else {
		switch {
		case x >= -32:
			*out = append(*out, byte(x))
		case x >= -128:
			*out = append(*out, mpInt8, byte(x))
		case x >= -32768:
			*out = append(*out, mpInt16)
			put16(out, uint16(x))
		case x >= -2147483648:
			*out = append(*out, mpInt32)
			put32(out, uint32(x))
		default:
			*out = append(*out, mpInt64)
			put64(out, uint64(x))
		}
	}
}

func encUnsigned(out *[]byte, x uint64) {
	switch {
	case x <= 0x7f:
		*out = append(*out, byte(x))
	case x <= 0xff:
		*out = append(*out, mpUint8, byte(x))
	case x <= 0xffff:
		*out = append(*out, mpUint16)
		put16(out, uint16(x))
	case x <= 0xffffffff:
		*out = append(*out, mpUint32)
		put32(out, uint32(x))
	default:
		*out = append(*out, mpUint64)
		put64(out, x)
	}
}

func encReal(out *[]byte, d float64) {
	*out = append(*out, mpFloat64)
	put64(out, math.Float64bits(d))
}

func encReal32(out *[]byte, f float32) {
	*out = append(*out, mpFloat32)
	put32(out, math.Float32bits(f))
}

func encString(out *[]byte, s []byte) {
	n := len(s)
	switch {
	case n <= 31:
		*out = append(*out, byte(mpFixstrMask|n))
	case n <= 0xff:
		*out = append(*out, mpStr8, byte(n))
	case n <= 0xffff:
		*out = append(*out, mpStr16)
		put16(out, uint16(n))
	default:
		*out = append(*out, mpStr32)
		put32(out, uint32(n))
	}
	*out = append(*out, s...)
}

func encBinary(out *[]byte, data []byte) {
	n := len(data)
	switch {
	case n <= 0xff:
		*out = append(*out, mpBin8, byte(n))
	case n <= 0xffff:
		*out = append(*out, mpBin16)
		put16(out, uint16(n))
	default:
		*out = append(*out, mpBin32)
		put32(out, uint32(n))
	}
	*out = append(*out, data...)
}

func encExt(out *[]byte, typeCode int8, data []byte) {
	n := len(data)
	switch n {
	case 1:
		*out = append(*out, mpFixext1)
	case 2:
		*out = append(*out, mpFixext2)
	case 4:
		*out = append(*out, mpFixext4)
	case 8:
		*out = append(*out, mpFixext8)
	case 16:
		*out = append(*out, mpFixext16)
	default:
		switch {
		case n <= 0xff:
			*out = append(*out, mpExt8, byte(n))
		case n <= 0xffff:
			*out = append(*out, mpExt16)
			put16(out, uint16(n))
		default:
			*out = append(*out, mpExt32)
			put32(out, uint32(n))
		}
	}
	*out = append(*out, byte(typeCode))
	*out = append(*out, data...)
}

func encInt8(out *[]byte, x int64)    { *out = append(*out, mpInt8, byte(x)) }
func encInt16(out *[]byte, x int64)   { *out = append(*out, mpInt16); put16(out, uint16(x)) }
func encInt32(out *[]byte, x int64)   { *out = append(*out, mpInt32); put32(out, uint32(x)) }
func encInt64(out *[]byte, x int64)   { *out = append(*out, mpInt64); put64(out, uint64(x)) }
func encUint8(out *[]byte, x uint64)  { *out = append(*out, mpUint8, byte(x)) }
func encUint16(out *[]byte, x uint64) { *out = append(*out, mpUint16); put16(out, uint16(x)) }
func encUint32(out *[]byte, x uint64) { *out = append(*out, mpUint32); put32(out, uint32(x)) }
func encUint64(out *[]byte, x uint64) { *out = append(*out, mpUint64); put64(out, x) }

func encArrayHeader(out *[]byte, count uint32) {
	switch {
	case count <= 15:
		*out = append(*out, byte(mpFixarrayMask|count))
	case count <= 0xffff:
		*out = append(*out, mpArray16)
		put16(out, uint16(count))
	default:
		*out = append(*out, mpArray32)
		put32(out, count)
	}
}

func encMapHeader(out *[]byte, count uint32) {
	switch {
	case count <= 15:
		*out = append(*out, byte(mpFixmapMask|count))
	case count <= 0xffff:
		*out = append(*out, mpMap16)
		put16(out, uint16(count))
	default:
		*out = append(*out, mpMap32)
		put32(out, count)
	}
}

func encTimestamp(out *[]byte, sec int64, nsec uint32) {
	if nsec == 0 && sec >= 0 && sec <= 0xffffffff {
		*out = append(*out, mpFixext4, 0xff)
		put32(out, uint32(sec))
	} else if sec >= 0 && sec <= 0x3FFFFFFFF {
		*out = append(*out, mpFixext8, 0xff)
		put64(out, (uint64(nsec)<<34)|uint64(sec))
	} else {
		*out = append(*out, mpExt8, 12, 0xff)
		put32(out, nsec)
		put64(out, uint64(sec))
	}
}

func encodeValue(out *[]byte, v Value) {
	switch v.ty {
	case TypeNil:
		encNil(out)
	case TypeTrue:
		encBool(out, true)
	case TypeFalse:
		encBool(out, false)
	case TypeInteger:
		switch v.intWidth {
		case WidthInt8:
			encInt8(out, v.AsInt64())
		case WidthInt16:
			encInt16(out, v.AsInt64())
		case WidthInt32:
			encInt32(out, v.AsInt64())
		case WidthInt64:
			encInt64(out, v.AsInt64())
		case WidthUint8:
			encUint8(out, v.AsUint64())
		case WidthUint16:
			encUint16(out, v.AsUint64())
		case WidthUint32:
			encUint32(out, v.AsUint64())
		case WidthUint64:
			encUint64(out, v.AsUint64())
		default:
			encInteger(out, v.AsInt64())
		}
	case TypeReal:
		encReal(out, v.AsFloat64())
	case TypeFloat32:
		encReal32(out, v.AsFloat32())
	case TypeString:
		encString(out, v.AsBytes())
	case TypeBinary:
		encBinary(out, v.BlobData())
	case TypeExt:
		encExt(out, v.ExtType(), v.BlobData())
	case TypeTimestamp:
		encTimestamp(out, v.TimestampSeconds(), v.TimestampNanoseconds())
	default:
		encNil(out)
	}
}
