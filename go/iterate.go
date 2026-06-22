package msgpackblob

import "strconv"

// EachRow is a single row yielded by an Iterator.
type EachRow struct {
	Key     string // map key ("" for arrays / tree rows)
	Index   int64  // array/pair index (flat iteration only)
	Fullkey string // e.g. "$.users[0].name"
	Path    string // parent path
	ID      int    // byte offset in the blob
	Type    Type   // element type
	Value   Value  // element value
}

func keyStr(a []byte, n, i int) (string, bool) {
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
		return "", false
	}
	// Bounds guard (matches decode keyAt): reject a key length that runs past the
	// end of the blob. eachIter then defers to skipOne, which fails and stops
	// iteration — matching the C++ reference (no out-of-bounds read).
	if klen > n-koff {
		return "", false
	}
	return string(a[koff : koff+klen]), true
}

func containerInfo(a []byte, n, i int) (isArr, isMap bool, count, dataOff int) {
	b := a[i]
	switch {
	case b >= 0x90 && b <= 0x9f:
		return true, false, int(b & 0x0f), i + 1
	case b == mpArray16 && i+3 <= n:
		return true, false, int(read16(a, i+1)), i + 3
	case b == mpArray32 && i+5 <= n:
		return true, false, int(read32(a, i+1)), i + 5
	case b >= 0x80 && b <= 0x8f:
		return false, true, int(b & 0x0f), i + 1
	case b == mpMap16 && i+3 <= n:
		return false, true, int(read16(a, i+1)), i + 3
	case b == mpMap32 && i+5 <= n:
		return false, true, int(read32(a, i+1)), i + 5
	}
	return false, false, 0, 0
}

func eachIter(a []byte, n, icont int, zbase string) []EachRow {
	var rows []EachRow
	if icont >= n {
		return rows
	}
	isArr, isMap, count, dataOff := containerInfo(a, n, icont)
	if !isArr && !isMap {
		return rows
	}

	remaining := 0
	if dataOff <= n {
		remaining = n - dataOff
	}
	minBytes := 1
	if isMap {
		minBytes = 2
	}
	if count > remaining/minBytes+1 {
		return rows
	}

	cur := dataOff
	for j := 0; j < count; j++ {
		if cur >= n {
			break
		}
		if isArr {
			cEnd := skipOne(a, n, cur)
			if cEnd == 0 {
				break
			}
			rows = append(rows, EachRow{
				Index:   int64(j),
				Fullkey: zbase + "[" + strconv.Itoa(j) + "]",
				Path:    zbase,
				ID:      cur,
				Type:    getType(a, n, cur),
				Value:   decodeElement(a, n, cur, cEnd),
			})
			cur = cEnd
		} else {
			ks, ok := keyStr(a, n, cur)
			vOff := skipOne(a, n, cur)
			if vOff == 0 {
				break
			}
			pEnd := skipOne(a, n, vOff)
			if pEnd == 0 {
				break
			}
			key := ks
			if !ok {
				key = "?"
			}
			rows = append(rows, EachRow{
				Key:     key,
				Index:   int64(j),
				Fullkey: zbase + "." + key,
				Path:    zbase,
				ID:      vOff,
				Type:    getType(a, n, vOff),
				Value:   decodeElement(a, n, vOff, pEnd),
			})
			cur = pEnd
		}
	}
	return rows
}

func treeWalk(a []byte, n, ioff int, zfull, zparPath string, depth int, rows *[]EachRow) {
	if depth > MaxDepth || ioff >= n {
		return
	}
	iend := skipOne(a, n, ioff)
	if iend == 0 {
		return
	}

	*rows = append(*rows, EachRow{
		Fullkey: zfull,
		Path:    zparPath,
		ID:      ioff,
		Type:    getType(a, n, ioff),
		Value:   decodeElement(a, n, ioff, iend),
	})

	isArr, isMap, count, dataOff := containerInfo(a, n, ioff)
	if !isArr && !isMap {
		return
	}

	remaining := 0
	if dataOff <= n {
		remaining = n - dataOff
	}
	minBytes := 1
	if isMap {
		minBytes = 2
	}
	if count > remaining/minBytes+1 {
		return
	}

	cur := dataOff
	for j := 0; j < count; j++ {
		if cur >= n {
			break
		}
		if isArr {
			cEnd := skipOne(a, n, cur)
			if cEnd == 0 {
				break
			}
			treeWalk(a, n, cur, zfull+"["+strconv.Itoa(j)+"]", zfull, depth+1, rows)
			cur = cEnd
		} else {
			ks, ok := keyStr(a, n, cur)
			vOff := skipOne(a, n, cur)
			if vOff == 0 {
				break
			}
			pEnd := skipOne(a, n, vOff)
			if pEnd == 0 {
				break
			}
			key := ks
			if !ok {
				key = "?"
			}
			treeWalk(a, n, vOff, zfull+"."+key, zfull, depth+1, rows)
			cur = pEnd
		}
	}
}
