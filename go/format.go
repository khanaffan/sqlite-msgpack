// Package msgpackblob is a pure-Go MessagePack Blob library.
//
// It is a zero-dependency port of the standalone C++ "msgpack" Blob API from
// sqlite-msgpack. It creates, queries, mutates and iterates MessagePack binary
// blobs and produces byte-identical output to the C++ library and the
// sqlite-msgpack SQLite extension, so blobs are fully interchangeable.
package msgpackblob

// MaxDepth is the maximum container nesting depth (matches the C++ library and
// the SQLite extension).
const MaxDepth = 200

// MaxOutput is the maximum output buffer size (64 MiB).
const MaxOutput = 64 * 1024 * 1024

// Version of the library.
const Version = "1.6.0"

// MessagePack format bytes.
const (
	mpNil      = 0xc0
	mpFalse    = 0xc2
	mpTrue     = 0xc3
	mpBin8     = 0xc4
	mpBin16    = 0xc5
	mpBin32    = 0xc6
	mpExt8     = 0xc7
	mpExt16    = 0xc8
	mpExt32    = 0xc9
	mpFloat32  = 0xca
	mpFloat64  = 0xcb
	mpUint8    = 0xcc
	mpUint16   = 0xcd
	mpUint32   = 0xce
	mpUint64   = 0xcf
	mpInt8     = 0xd0
	mpInt16    = 0xd1
	mpInt32    = 0xd2
	mpInt64    = 0xd3
	mpFixext1  = 0xd4
	mpFixext2  = 0xd5
	mpFixext4  = 0xd6
	mpFixext8  = 0xd7
	mpFixext16 = 0xd8
	mpStr8     = 0xd9
	mpStr16    = 0xda
	mpStr32    = 0xdb
	mpArray16  = 0xdc
	mpArray32  = 0xdd
	mpMap16    = 0xde
	mpMap32    = 0xdf

	mpFixmapMask   = 0x80
	mpFixarrayMask = 0x90
	mpFixstrMask   = 0xa0

	mpTimestampType = 0xff
)

// ── big-endian read helpers ─────────────────────────────────────────
func read16(a []byte, i int) uint32 {
	return uint32(a[i])<<8 | uint32(a[i+1])
}

func read32(a []byte, i int) uint32 {
	return uint32(a[i])<<24 | uint32(a[i+1])<<16 | uint32(a[i+2])<<8 | uint32(a[i+3])
}

func read64(a []byte, i int) uint64 {
	return uint64(read32(a, i))<<32 | uint64(read32(a, i+4))
}

// ── big-endian write helpers ────────────────────────────────────────
func put16(out *[]byte, v uint16) {
	*out = append(*out, byte(v>>8), byte(v))
}

func put32(out *[]byte, v uint32) {
	*out = append(*out, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func put64(out *[]byte, v uint64) {
	*out = append(*out, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// skipOne returns the offset just past one complete element starting at i, or 0
// on malformed / truncated input.
func skipOne(a []byte, n, i int) int {
	return skipOneD(a, n, i, 0)
}

func skipOneD(a []byte, n, i, depth int) int {
	if depth > MaxDepth {
		return 0
	}
	if i >= n {
		return 0
	}
	b := a[i]
	i++

	if b <= 0x7f {
		return i
	}
	if b >= 0xe0 {
		return i
	}

	switch b {
	case mpNil, mpFalse, mpTrue:
		return i
	case mpFloat32:
		if i+4 <= n {
			return i + 4
		}
		return 0
	case mpFloat64, mpInt64, mpUint64:
		if i+8 <= n {
			return i + 8
		}
		return 0
	case mpUint8, mpInt8:
		if i+1 <= n {
			return i + 1
		}
		return 0
	case mpUint16, mpInt16:
		if i+2 <= n {
			return i + 2
		}
		return 0
	case mpUint32, mpInt32:
		if i+4 <= n {
			return i + 4
		}
		return 0
	case mpBin8, mpStr8:
		if i+1 > n {
			return 0
		}
		sz := int(a[i])
		i++
		if sz <= n-i {
			return i + sz
		}
		return 0
	case mpBin16, mpStr16:
		if i+2 > n {
			return 0
		}
		sz := int(read16(a, i))
		i += 2
		if sz <= n-i {
			return i + sz
		}
		return 0
	case mpBin32, mpStr32:
		if i+4 > n {
			return 0
		}
		sz := int(read32(a, i))
		i += 4
		if sz <= n-i {
			return i + sz
		}
		return 0
	case mpFixext1:
		if i+2 <= n {
			return i + 2
		}
		return 0
	case mpFixext2:
		if i+3 <= n {
			return i + 3
		}
		return 0
	case mpFixext4:
		if i+5 <= n {
			return i + 5
		}
		return 0
	case mpFixext8:
		if i+9 <= n {
			return i + 9
		}
		return 0
	case mpFixext16:
		if i+17 <= n {
			return i + 17
		}
		return 0
	case mpExt8:
		if i+2 > n {
			return 0
		}
		sz := int(a[i])
		i += 2
		if sz <= n-i {
			return i + sz
		}
		return 0
	case mpExt16:
		if i+3 > n {
			return 0
		}
		sz := int(read16(a, i))
		i += 3
		if sz <= n-i {
			return i + sz
		}
		return 0
	case mpExt32:
		if i+5 > n {
			return 0
		}
		sz := int(read32(a, i))
		i += 5
		if sz <= n-i {
			return i + sz
		}
		return 0
	}

	// fixstr
	if b >= 0xa0 && b <= 0xbf {
		sz := int(b & 0x1f)
		if sz <= n-i {
			return i + sz
		}
		return 0
	}

	// fixarray
	if b >= 0x90 && b <= 0x9f {
		count := int(b & 0x0f)
		for j := 0; j < count; j++ {
			i = skipOneD(a, n, i, depth+1)
			if i == 0 {
				return 0
			}
		}
		return i
	}

	// fixmap
	if b >= 0x80 && b <= 0x8f {
		count := int(b & 0x0f)
		for j := 0; j < count; j++ {
			i = skipOneD(a, n, i, depth+1)
			if i == 0 {
				return 0
			}
			i = skipOneD(a, n, i, depth+1)
			if i == 0 {
				return 0
			}
		}
		return i
	}

	// array16/32
	if b == mpArray16 || b == mpArray32 {
		var count int
		if b == mpArray16 {
			if i+2 > n {
				return 0
			}
			count = int(read16(a, i))
			i += 2
		} else {
			if i+4 > n {
				return 0
			}
			count = int(read32(a, i))
			i += 4
		}
		for j := 0; j < count; j++ {
			i = skipOneD(a, n, i, depth+1)
			if i == 0 {
				return 0
			}
		}
		return i
	}

	// map16/32
	if b == mpMap16 || b == mpMap32 {
		var count int
		if b == mpMap16 {
			if i+2 > n {
				return 0
			}
			count = int(read16(a, i))
			i += 2
		} else {
			if i+4 > n {
				return 0
			}
			count = int(read32(a, i))
			i += 4
		}
		for j := 0; j < count; j++ {
			i = skipOneD(a, n, i, depth+1)
			if i == 0 {
				return 0
			}
			i = skipOneD(a, n, i, depth+1)
			if i == 0 {
				return 0
			}
		}
		return i
	}

	return 0
}
