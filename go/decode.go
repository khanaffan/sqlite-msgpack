package msgpackblob

import "math"

const (
	rcOK       = 0
	rcError    = 1
	rcNotFound = 2
)

func isValid(a []byte, n int) bool {
	if n == 0 {
		return false
	}
	return skipOne(a, n, 0) == n
}

func errorPosition(a []byte, n int) int {
	if n == 0 {
		return 0
	}
	if skipOne(a, n, 0) == n {
		return 0
	}
	for i := 0; i < n; {
		nxt := skipOne(a, n, i)
		if nxt == 0 {
			return i
		}
		i = nxt
	}
	return 0
}

func isTimestampExt(a []byte, n, i int) bool {
	if i >= n {
		return false
	}
	b := a[i]
	if b == mpFixext4 && i+6 <= n && a[i+1] == mpTimestampType {
		return true
	}
	if b == mpFixext8 && i+10 <= n && a[i+1] == mpTimestampType {
		return true
	}
	if b == mpExt8 && i+3 <= n && a[i+1] == 12 && a[i+2] == mpTimestampType {
		return true
	}
	return false
}

func decodeTimestamp(a []byte, n, i int) (int64, uint32, bool) {
	if i >= n {
		return 0, 0, false
	}
	b := a[i]
	if b == mpFixext4 && i+6 <= n && a[i+1] == mpTimestampType {
		return int64(read32(a, i+2)), 0, true
	}
	if b == mpFixext8 && i+10 <= n && a[i+1] == mpTimestampType {
		v := read64(a, i+2)
		return int64(v & 0x3FFFFFFFF), uint32(v >> 34), true
	}
	if b == mpExt8 && i+15 <= n && a[i+1] == 12 && a[i+2] == mpTimestampType {
		nsec := read32(a, i+3)
		sec := int64(read64(a, i+7))
		return sec, nsec, true
	}
	return 0, 0, false
}

func getType(a []byte, n, i int) Type {
	if i >= n {
		return TypeNil
	}
	b := a[i]
	if b == mpNil {
		return TypeNil
	}
	if b == mpTrue {
		return TypeTrue
	}
	if b == mpFalse {
		return TypeFalse
	}
	if b <= 0x7f || b >= 0xe0 {
		return TypeInteger
	}
	if b >= 0xa0 && b <= 0xbf {
		return TypeString
	}
	if b >= 0x90 && b <= 0x9f {
		return TypeArray
	}
	if b >= 0x80 && b <= 0x8f {
		return TypeMap
	}
	switch b {
	case mpUint8, mpUint16, mpUint32, mpUint64, mpInt8, mpInt16, mpInt32, mpInt64:
		return TypeInteger
	case mpFloat32:
		return TypeFloat32
	case mpFloat64:
		return TypeReal
	case mpStr8, mpStr16, mpStr32:
		return TypeString
	case mpBin8, mpBin16, mpBin32:
		return TypeBinary
	case mpArray16, mpArray32:
		return TypeArray
	case mpMap16, mpMap32:
		return TypeMap
	case mpExt8, mpExt16, mpExt32, mpFixext1, mpFixext2, mpFixext4, mpFixext8, mpFixext16:
		if isTimestampExt(a, n, i) {
			return TypeTimestamp
		}
		return TypeExt
	}
	return TypeNil
}

func getContainerCount(a []byte, n, i int) int64 {
	if i >= n {
		return -1
	}
	b := a[i]
	if b >= 0x90 && b <= 0x9f {
		return int64(b & 0x0f)
	}
	if b >= 0x80 && b <= 0x8f {
		return int64(b & 0x0f)
	}
	if b == mpArray16 && i+3 <= n {
		return int64(read16(a, i+1))
	}
	if b == mpArray32 && i+5 <= n {
		return int64(read32(a, i+1))
	}
	if b == mpMap16 && i+3 <= n {
		return int64(read16(a, i+1))
	}
	if b == mpMap32 && i+5 <= n {
		return int64(read32(a, i+1))
	}
	return -1
}

// stepKind classifies one parsed path step.
type stepKind int

const (
	stepEnd stepKind = iota
	stepErr
	stepKey
	stepIndex
)

func pathStep(zpath string, pi int) (stepKind, int, string, int64) {
	i := pi
	if i >= len(zpath) {
		return stepEnd, i, "", 0
	}
	c := zpath[i]
	if c == '.' {
		i++
		start := i
		for i < len(zpath) && zpath[i] != '.' && zpath[i] != '[' {
			i++
		}
		return stepKey, i, zpath[start:i], 0
	}
	if c == '[' {
		var idx int64
		hasDigit := false
		i++
		for i < len(zpath) && zpath[i] >= '0' && zpath[i] <= '9' {
			idx = idx*10 + int64(zpath[i]-'0')
			i++
			hasDigit = true
		}
		if !hasDigit || i >= len(zpath) || zpath[i] != ']' {
			return stepErr, i, "", 0
		}
		i++
		return stepIndex, i, "", idx
	}
	return stepErr, i, "", 0
}

// keyAt returns the key bytes for a map key at i, or nil.
func keyAt(a []byte, n, i int) ([]byte, bool) {
	kb := a[i]
	var klen, koff int
	switch {
	case kb >= 0xa0 && kb <= 0xbf:
		klen, koff = int(kb&0x1f), i+1
	case kb == mpStr8 && i+2 <= n:
		klen, koff = int(a[i+1]), i+2
	case kb == mpStr16 && i+3 <= n:
		klen, koff = int(read16(a, i+1)), i+3
	case kb == mpStr32 && i+5 <= n:
		klen, koff = int(read32(a, i+1)), i+5
	default:
		return nil, false
	}
	if klen > n-koff {
		return nil, false
	}
	return a[koff : koff+klen], true
}

func bytesEqual(x, y []byte) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// lookup resolves zpath to a byte range; returns (rc, iStart, iEnd).
func lookup(a []byte, n, iroot int, zpath string) (int, int, int) {
	if len(zpath) == 0 || zpath[0] != '$' {
		return rcError, 0, 0
	}
	icur := iroot
	pi := 1

	for {
		kind, npi, key, idx := pathStep(zpath, pi)
		pi = npi
		switch kind {
		case stepEnd:
			inext := skipOne(a, n, icur)
			iend := inext
			if iend == 0 {
				iend = n
			}
			if inext != 0 || icur == n {
				return rcOK, icur, iend
			}
			return rcError, icur, iend
		case stepErr:
			return rcError, 0, 0
		case stepIndex:
			if icur >= n {
				return rcNotFound, 0, 0
			}
			b := a[icur]
			var count int64
			var elemOff int
			if b >= 0x90 && b <= 0x9f {
				count, elemOff = int64(b&0x0f), icur+1
			} else if b == mpArray16 {
				if icur+3 > n {
					return rcError, 0, 0
				}
				count, elemOff = int64(read16(a, icur+1)), icur+3
			} else if b == mpArray32 {
				if icur+5 > n {
					return rcError, 0, 0
				}
				count, elemOff = int64(read32(a, icur+1)), icur+5
			} else {
				return rcNotFound, 0, 0
			}
			if idx < 0 || idx >= count {
				return rcNotFound, 0, 0
			}
			icur = elemOff
			for j := int64(0); j < idx; j++ {
				icur = skipOne(a, n, icur)
				if icur == 0 {
					return rcError, 0, 0
				}
			}
		case stepKey:
			if icur >= n {
				return rcNotFound, 0, 0
			}
			b := a[icur]
			var count, elemOff int
			if b >= 0x80 && b <= 0x8f {
				count, elemOff = int(b&0x0f), icur+1
			} else if b == mpMap16 {
				if icur+3 > n {
					return rcError, 0, 0
				}
				count, elemOff = int(read16(a, icur+1)), icur+3
			} else if b == mpMap32 {
				if icur+5 > n {
					return rcError, 0, 0
				}
				count, elemOff = int(read32(a, icur+1)), icur+5
			} else {
				return rcNotFound, 0, 0
			}
			keyBytes := []byte(key)
			icur = elemOff
			found := false
			for j := 0; j < count && !found; j++ {
				if icur >= n {
					return rcError, 0, 0
				}
				kstr, ok := keyAt(a, n, icur)
				valOff := skipOne(a, n, icur)
				if valOff == 0 {
					return rcError, 0, 0
				}
				if ok && bytesEqual(kstr, keyBytes) {
					icur = valOff
					found = true
				} else {
					icur = skipOne(a, n, valOff)
					if icur == 0 {
						return rcError, 0, 0
					}
				}
			}
			if !found {
				return rcNotFound, 0, 0
			}
		}
	}
}

func decodeElement(a []byte, n, istart, iend int) Value {
	if istart >= n || istart >= iend {
		return Nil()
	}
	b := a[istart]

	if b == mpNil {
		return Nil()
	}
	if b == mpFalse {
		return Bool(false)
	}
	if b == mpTrue {
		return Bool(true)
	}
	if b <= 0x7f {
		return Int(int64(b))
	}
	if b >= 0xe0 {
		return Int(int64(int8(b)))
	}

	switch b {
	case mpUint8:
		if istart+2 <= n {
			return Int(int64(a[istart+1]))
		}
	case mpUint16:
		if istart+3 <= n {
			return Int(int64(read16(a, istart+1)))
		}
	case mpUint32:
		if istart+5 <= n {
			return Int(int64(read32(a, istart+1)))
		}
	case mpUint64:
		if istart+9 <= n {
			return Uint(read64(a, istart+1))
		}
	case mpInt8:
		if istart+2 <= n {
			return Int(int64(int8(a[istart+1])))
		}
	case mpInt16:
		if istart+3 <= n {
			return Int(int64(int16(read16(a, istart+1))))
		}
	case mpInt32:
		if istart+5 <= n {
			return Int(int64(int32(read32(a, istart+1))))
		}
	case mpInt64:
		if istart+9 <= n {
			return Int(int64(read64(a, istart+1)))
		}
	case mpFloat32:
		if istart+5 <= n {
			return Real32(math.Float32frombits(read32(a, istart+1)))
		}
	case mpFloat64:
		if istart+9 <= n {
			return Real(math.Float64frombits(read64(a, istart+1)))
		}
	}

	// str
	var slen, soff int
	switch {
	case b >= 0xa0 && b <= 0xbf:
		slen, soff = int(b&0x1f), istart+1
	case b == mpStr8 && istart+2 <= n:
		slen, soff = int(a[istart+1]), istart+2
	case b == mpStr16 && istart+3 <= n:
		slen, soff = int(read16(a, istart+1)), istart+3
	case b == mpStr32 && istart+5 <= n:
		slen, soff = int(read32(a, istart+1)), istart+5
	}
	if soff != 0 {
		if slen > n-soff {
			slen = n - soff
		}
		return StrBytes(a[soff : soff+slen])
	}

	// bin
	var blen, boff int
	switch {
	case b == mpBin8 && istart+2 <= n:
		blen, boff = int(a[istart+1]), istart+2
	case b == mpBin16 && istart+3 <= n:
		blen, boff = int(read16(a, istart+1)), istart+3
	case b == mpBin32 && istart+5 <= n:
		blen, boff = int(read32(a, istart+1)), istart+5
	}
	if boff != 0 {
		if blen > n-boff {
			blen = n - boff
		}
		return Bin(a[boff : boff+blen])
	}

	// timestamp
	if sec, nsec, ok := decodeTimestamp(a, n, istart); ok {
		return TimestampNs(sec, nsec)
	}

	// ext
	var tc int8
	var elen, eoff int
	switch {
	case b == mpFixext1 && istart+3 <= n:
		tc, elen, eoff = int8(a[istart+1]), 1, istart+2
	case b == mpFixext2 && istart+4 <= n:
		tc, elen, eoff = int8(a[istart+1]), 2, istart+2
	case b == mpFixext4 && istart+6 <= n:
		tc, elen, eoff = int8(a[istart+1]), 4, istart+2
	case b == mpFixext8 && istart+10 <= n:
		tc, elen, eoff = int8(a[istart+1]), 8, istart+2
	case b == mpFixext16 && istart+18 <= n:
		tc, elen, eoff = int8(a[istart+1]), 16, istart+2
	case b == mpExt8 && istart+3 <= n:
		tc, elen, eoff = int8(a[istart+2]), int(a[istart+1]), istart+3
	case b == mpExt16 && istart+4 <= n:
		tc, elen, eoff = int8(a[istart+3]), int(read16(a, istart+1)), istart+4
	case b == mpExt32 && istart+6 <= n:
		tc, elen, eoff = int8(a[istart+5]), int(read32(a, istart+1)), istart+6
	}
	if eoff != 0 {
		if elen > n-eoff {
			elen = n - eoff
		}
		return Ext(tc, a[eoff:eoff+elen])
	}

	// containers → raw binary blob (includes header)
	return Bin(a[istart:iend])
}
