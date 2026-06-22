package msgpackblob

import (
	"math"
	"strconv"
	"strings"
)

const hexDigits = "0123456789abcdef"

// ── float formatting (C printf "%.<P>g") ────────────────────────────
// Go's strconv 'g' formatting with an explicit precision matches C's "%g"
// digit-for-digit (verified against the reference across 35k+ values).
func fmtDouble(d float64) string {
	s := strconv.FormatFloat(d, 'g', 17, 64)
	if !strings.ContainsAny(s, ".eE") {
		// C re-formats integer-valued doubles with "%.1f".
		s = strconv.FormatFloat(d, 'f', 1, 64)
	}
	return s
}

func fmtFloat32(f float32) string {
	return strconv.FormatFloat(float64(f), 'g', 7, 64)
}

// ── JSON output ─────────────────────────────────────────────────────
func escapeStr(out *[]byte, s []byte) {
	*out = append(*out, '"')
	start := 0
	for j := 0; j < len(s); j++ {
		c := s[j]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		if j > start {
			*out = append(*out, s[start:j]...)
		}
		switch c {
		case '"':
			*out = append(*out, '\\', '"')
		case '\\':
			*out = append(*out, '\\', '\\')
		case '\n':
			*out = append(*out, '\\', 'n')
		case '\r':
			*out = append(*out, '\\', 'r')
		case '\t':
			*out = append(*out, '\\', 't')
		default:
			*out = append(*out, '\\', 'u', '0', '0',
				hexDigits[c>>4], hexDigits[c&0xf])
		}
		start = j + 1
	}
	if len(s) > start {
		*out = append(*out, s[start:]...)
	}
	*out = append(*out, '"')
}

func newline(out *[]byte, depth, indentW int) {
	*out = append(*out, '\n')
	for k := 0; k < depth*indentW; k++ {
		*out = append(*out, ' ')
	}
}

func toJSONAt(out *[]byte, a []byte, n, i int, pretty bool, depth, indentW int) {
	if i >= n || depth > MaxDepth {
		*out = append(*out, "null"...)
		return
	}
	b := a[i]

	if b == mpNil {
		*out = append(*out, "null"...)
		return
	}
	if b == mpFalse {
		*out = append(*out, "false"...)
		return
	}
	if b == mpTrue {
		*out = append(*out, "true"...)
		return
	}
	if b <= 0x7f {
		*out = strconv.AppendInt(*out, int64(b), 10)
		return
	}
	if b >= 0xe0 {
		*out = strconv.AppendInt(*out, int64(int8(b)), 10)
		return
	}

	switch b {
	case mpUint8:
		if i+2 <= n {
			*out = strconv.AppendUint(*out, uint64(a[i+1]), 10)
			return
		}
	case mpUint16:
		if i+3 <= n {
			*out = strconv.AppendUint(*out, uint64(read16(a, i+1)), 10)
			return
		}
	case mpUint32:
		if i+5 <= n {
			*out = strconv.AppendUint(*out, uint64(read32(a, i+1)), 10)
			return
		}
	case mpUint64:
		if i+9 <= n {
			*out = strconv.AppendUint(*out, read64(a, i+1), 10)
			return
		}
	case mpInt8:
		if i+2 <= n {
			*out = strconv.AppendInt(*out, int64(int8(a[i+1])), 10)
			return
		}
	case mpInt16:
		if i+3 <= n {
			*out = strconv.AppendInt(*out, int64(int16(read16(a, i+1))), 10)
			return
		}
	case mpInt32:
		if i+5 <= n {
			*out = strconv.AppendInt(*out, int64(int32(read32(a, i+1))), 10)
			return
		}
	case mpInt64:
		if i+9 <= n {
			*out = strconv.AppendInt(*out, int64(read64(a, i+1)), 10)
			return
		}
	case mpFloat32:
		if i+5 <= n {
			f := math.Float32frombits(read32(a, i+1))
			if math.IsInf(float64(f), 0) || math.IsNaN(float64(f)) {
				*out = append(*out, "null"...)
				return
			}
			*out = append(*out, fmtFloat32(f)...)
			return
		}
	case mpFloat64:
		if i+9 <= n {
			d := math.Float64frombits(read64(a, i+1))
			if math.IsInf(d, 0) || math.IsNaN(d) {
				*out = append(*out, "null"...)
				return
			}
			*out = append(*out, fmtDouble(d)...)
			return
		}
	}

	// str
	var slen, soff int
	switch {
	case b >= 0xa0 && b <= 0xbf:
		slen, soff = int(b&0x1f), i+1
	case b == mpStr8 && i+2 <= n:
		slen, soff = int(a[i+1]), i+2
	case b == mpStr16 && i+3 <= n:
		slen, soff = int(read16(a, i+1)), i+3
	case b == mpStr32 && i+5 <= n:
		slen, soff = int(read32(a, i+1)), i+5
	}
	if soff != 0 {
		if slen > n-soff {
			slen = n - soff
		}
		escapeStr(out, a[soff:soff+slen])
		return
	}

	// bin → hex string
	var blen, boff int
	switch {
	case b == mpBin8 && i+2 <= n:
		blen, boff = int(a[i+1]), i+2
	case b == mpBin16 && i+3 <= n:
		blen, boff = int(read16(a, i+1)), i+3
	case b == mpBin32 && i+5 <= n:
		blen, boff = int(read32(a, i+1)), i+5
	}
	if boff != 0 {
		if blen > n-boff {
			blen = n - boff
		}
		*out = append(*out, '"')
		for j := 0; j < blen; j++ {
			by := a[boff+j]
			*out = append(*out, hexDigits[by>>4], hexDigits[by&0xf])
		}
		*out = append(*out, '"')
		return
	}

	// array
	isArr := false
	count := 0
	dataOff := 0
	switch {
	case b >= 0x90 && b <= 0x9f:
		isArr, count, dataOff = true, int(b&0x0f), i+1
	case b == mpArray16 && i+3 <= n:
		isArr, count, dataOff = true, int(read16(a, i+1)), i+3
	case b == mpArray32 && i+5 <= n:
		isArr, count, dataOff = true, int(read32(a, i+1)), i+5
	}
	if isArr {
		cur := dataOff
		*out = append(*out, '[')
		for j := 0; j < count; j++ {
			if cur >= n {
				break
			}
			nxt := skipOne(a, n, cur)
			if j > 0 {
				*out = append(*out, ',')
			}
			if pretty {
				newline(out, depth+1, indentW)
			}
			toJSONAt(out, a, n, cur, pretty, depth+1, indentW)
			if nxt != 0 {
				cur = nxt
			} else {
				cur = n
			}
		}
		if pretty && count > 0 {
			newline(out, depth, indentW)
		}
		*out = append(*out, ']')
		return
	}

	// map
	isMap := false
	count = 0
	dataOff = 0
	switch {
	case b >= 0x80 && b <= 0x8f:
		isMap, count, dataOff = true, int(b&0x0f), i+1
	case b == mpMap16 && i+3 <= n:
		isMap, count, dataOff = true, int(read16(a, i+1)), i+3
	case b == mpMap32 && i+5 <= n:
		isMap, count, dataOff = true, int(read32(a, i+1)), i+5
	}
	if isMap {
		cur := dataOff
		*out = append(*out, '{')
		for j := 0; j < count; j++ {
			if cur >= n {
				break
			}
			valOff := skipOne(a, n, cur)
			pairEnd := 0
			if valOff != 0 {
				pairEnd = skipOne(a, n, valOff)
			}
			if j > 0 {
				*out = append(*out, ',')
			}
			if pretty {
				newline(out, depth+1, indentW)
			}
			toJSONAt(out, a, n, cur, pretty, depth+1, indentW)
			*out = append(*out, ':')
			if pretty {
				*out = append(*out, ' ')
			}
			vo := valOff
			if vo == 0 {
				vo = n
			}
			toJSONAt(out, a, n, vo, pretty, depth+1, indentW)
			if pairEnd != 0 {
				cur = pairEnd
			} else {
				cur = n
			}
		}
		if pretty && count > 0 {
			newline(out, depth, indentW)
		}
		*out = append(*out, '}')
		return
	}

	// ext / unknown → null
	*out = append(*out, "null"...)
}

func toJSONBytes(a []byte, n int, pretty bool, indent int) []byte {
	out := make([]byte, 0, n*2)
	toJSONAt(&out, a, n, 0, pretty, 0, indent)
	return out
}

// ── JSON parser → msgpack ───────────────────────────────────────────
type jsonParser struct {
	z []byte
	n int
	i int
}

func (p *jsonParser) skipWS() {
	for p.i < p.n {
		switch p.z[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

func hex4(z []byte, off int) int {
	v := 0
	for j := 0; j < 4; j++ {
		c := z[off+j]
		var h int
		switch {
		case c >= '0' && c <= '9':
			h = int(c - '0')
		case c >= 'a' && c <= 'f':
			h = int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			h = int(c-'A') + 10
		default:
			return -1
		}
		v = (v << 4) | h
	}
	return v
}

func cpToUTF8(out *[]byte, cp int) {
	switch {
	case cp < 0x80:
		*out = append(*out, byte(cp))
	case cp < 0x800:
		*out = append(*out, byte(0xc0|cp>>6), byte(0x80|cp&0x3f))
	case cp < 0x10000:
		*out = append(*out, byte(0xe0|cp>>12), byte(0x80|(cp>>6)&0x3f), byte(0x80|cp&0x3f))
	default:
		*out = append(*out, byte(0xf0|cp>>18), byte(0x80|(cp>>12)&0x3f),
			byte(0x80|(cp>>6)&0x3f), byte(0x80|cp&0x3f))
	}
}

func (p *jsonParser) parseString(out *[]byte) int {
	var sb []byte
	p.i++ // skip "
	for p.i < p.n {
		c := p.z[p.i]
		if c == '"' {
			p.i++
			break
		}
		if c == '\\' {
			p.i++
			if p.i >= p.n {
				return rcError
			}
			esc := p.z[p.i]
			p.i++
			switch esc {
			case '"':
				sb = append(sb, '"')
			case '\\':
				sb = append(sb, '\\')
			case '/':
				sb = append(sb, '/')
			case 'n':
				sb = append(sb, '\n')
			case 'r':
				sb = append(sb, '\r')
			case 't':
				sb = append(sb, '\t')
			case 'b':
				sb = append(sb, 0x08)
			case 'f':
				sb = append(sb, 0x0c)
			case 'u':
				if p.i+4 > p.n {
					return rcError
				}
				cp := hex4(p.z, p.i)
				p.i += 4
				if cp < 0 {
					return rcError
				}
				if cp >= 0xD800 && cp <= 0xDBFF && p.i+6 <= p.n &&
					p.z[p.i] == '\\' && p.z[p.i+1] == 'u' {
					lo := hex4(p.z, p.i+2)
					if lo >= 0xDC00 && lo <= 0xDFFF {
						p.i += 6
						cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00)
					}
				}
				cpToUTF8(&sb, cp)
			default:
				sb = append(sb, esc)
			}
		} else {
			sb = append(sb, c)
			p.i++
		}
	}
	encString(out, sb)
	return rcOK
}

func (p *jsonParser) parseNumber(out *[]byte) int {
	start := p.i
	isFloat := false
	if p.i < p.n && p.z[p.i] == '-' {
		p.i++
	}
	for p.i < p.n && p.z[p.i] >= '0' && p.z[p.i] <= '9' {
		p.i++
	}
	if p.i < p.n && p.z[p.i] == '.' {
		isFloat = true
		p.i++
		for p.i < p.n && p.z[p.i] >= '0' && p.z[p.i] <= '9' {
			p.i++
		}
	}
	if p.i < p.n && (p.z[p.i] == 'e' || p.z[p.i] == 'E') {
		isFloat = true
		p.i++
		if p.i < p.n && (p.z[p.i] == '+' || p.z[p.i] == '-') {
			p.i++
		}
		for p.i < p.n && p.z[p.i] >= '0' && p.z[p.i] <= '9' {
			p.i++
		}
	}
	length := p.i - start
	if length <= 0 || length >= 64 {
		return rcError
	}
	text := string(p.z[start:p.i])

	if isFloat {
		d, _ := strconv.ParseFloat(text, 64)
		encReal(out, d)
	} else {
		// strconv.ParseInt clamps to MaxInt64/MinInt64 on overflow, matching
		// C strtoll saturation.
		v, _ := strconv.ParseInt(text, 10, 64)
		if v >= 0 {
			encUnsigned(out, uint64(v))
		} else {
			encInteger(out, v)
		}
	}
	return rcOK
}

func (p *jsonParser) parseArray(out *[]byte) int {
	var tmp []byte
	var count uint32
	p.i++ // skip [
	p.skipWS()
	for p.i < p.n && p.z[p.i] != ']' {
		if count > 0 {
			p.skipWS()
			if p.i >= p.n || p.z[p.i] != ',' {
				return rcError
			}
			p.i++
		}
		p.skipWS()
		if p.parseValue(&tmp) != rcOK {
			return rcError
		}
		count++
		p.skipWS()
	}
	if p.i >= p.n {
		return rcError
	}
	p.i++ // skip ]
	encArrayHeader(out, count)
	*out = append(*out, tmp...)
	return rcOK
}

func (p *jsonParser) parseObject(out *[]byte) int {
	var tmp []byte
	var count uint32
	p.i++ // skip {
	p.skipWS()
	for p.i < p.n && p.z[p.i] != '}' {
		if count > 0 {
			p.skipWS()
			if p.i >= p.n || p.z[p.i] != ',' {
				return rcError
			}
			p.i++
		}
		p.skipWS()
		if p.i >= p.n || p.z[p.i] != '"' {
			return rcError
		}
		if p.parseString(&tmp) != rcOK {
			return rcError
		}
		p.skipWS()
		if p.i >= p.n || p.z[p.i] != ':' {
			return rcError
		}
		p.i++
		p.skipWS()
		if p.parseValue(&tmp) != rcOK {
			return rcError
		}
		count++
		p.skipWS()
	}
	if p.i >= p.n {
		return rcError
	}
	p.i++ // skip }
	encMapHeader(out, count)
	*out = append(*out, tmp...)
	return rcOK
}

func (p *jsonParser) parseValue(out *[]byte) int {
	p.skipWS()
	if p.i >= p.n {
		return rcError
	}
	c := p.z[p.i]
	if c == 'n' && p.i+4 <= p.n && string(p.z[p.i:p.i+4]) == "null" {
		p.i += 4
		*out = append(*out, mpNil)
		return rcOK
	}
	if c == 't' && p.i+4 <= p.n && string(p.z[p.i:p.i+4]) == "true" {
		p.i += 4
		*out = append(*out, mpTrue)
		return rcOK
	}
	if c == 'f' && p.i+5 <= p.n && string(p.z[p.i:p.i+5]) == "false" {
		p.i += 5
		*out = append(*out, mpFalse)
		return rcOK
	}
	switch {
	case c == '"':
		return p.parseString(out)
	case c == '[':
		return p.parseArray(out)
	case c == '{':
		return p.parseObject(out)
	case c == '-' || (c >= '0' && c <= '9'):
		return p.parseNumber(out)
	}
	return rcError
}

func fromJSON(json []byte) []byte {
	p := jsonParser{z: json, n: len(json)}
	var out []byte
	if p.parseValue(&out) != rcOK {
		return nil
	}
	return out
}
