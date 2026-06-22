package msgpackblob

const (
	editSet      = 0
	editInsert   = 1
	editReplace  = 2
	editRemove   = 3
	editArrayIns = 4
)

func mapKey(a []byte, n, i int) ([]byte, bool) {
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
	// Bounds guard (matches decode keyAt): a truncated key length must not slice
	// past the end of the blob — the C++ reference defers to skipOne, which fails
	// the pair and aborts the edit gracefully.
	if klen > n-koff {
		return nil, false
	}
	return a[koff : koff+klen], true
}

func editMap(out *[]byte, a []byte, n, icur int, zkey []byte, zpath string, pi int, newBin []byte, mode int) int {
	if icur >= n {
		return rcError
	}
	b := a[icur]
	var count, dataOff int
	if b >= 0x80 && b <= 0x8f {
		count, dataOff = int(b&0x0f), icur+1
	} else if b == mpMap16 {
		if icur+3 > n {
			return rcError
		}
		count, dataOff = int(read16(a, icur+1)), icur+3
	} else if b == mpMap32 {
		if icur+5 > n {
			return rcError
		}
		count, dataOff = int(read32(a, icur+1)), icur+5
	} else {
		if mode == editReplace || mode == editRemove {
			iend := skipOne(a, n, icur)
			if iend != 0 {
				*out = append(*out, a[icur:iend]...)
			}
			return rcOK
		}
		return rcError
	}

	newCount := uint32(count)
	var tmp []byte
	cur2 := dataOff
	foundKey := false

	for j := 0; j < count; j++ {
		if cur2 >= n {
			return rcError
		}
		kstr, kok := mapKey(a, n, cur2)
		valOff := skipOne(a, n, cur2)
		if valOff == 0 {
			return rcError
		}
		pairEnd := skipOne(a, n, valOff)
		if pairEnd == 0 {
			return rcError
		}

		isMatch := kok && bytesEqual(kstr, zkey)

		if isMatch {
			foundKey = true
			if mode == editInsert {
				tmp = append(tmp, a[cur2:pairEnd]...)
			} else {
				var vbuf []byte
				rc, skip := editStep(&vbuf, a, n, valOff, zpath, pi, newBin, mode)
				if rc != rcOK {
					return rc
				}
				if skip {
					newCount--
				} else {
					tmp = append(tmp, a[cur2:valOff]...)
					tmp = append(tmp, vbuf...)
				}
			}
		} else {
			tmp = append(tmp, a[cur2:pairEnd]...)
		}
		cur2 = pairEnd
	}

	if !foundKey {
		if mode == editSet || mode == editInsert {
			kind, _, _, _ := pathStep(zpath, pi)
			if kind != stepEnd {
				iend := skipOne(a, n, icur)
				if iend != 0 {
					*out = append(*out, a[icur:iend]...)
				}
				return rcOK
			}
			encString(&tmp, zkey)
			tmp = append(tmp, newBin...)
			newCount++
		} else {
			iend := skipOne(a, n, icur)
			if iend != 0 {
				*out = append(*out, a[icur:iend]...)
			}
			return rcOK
		}
	}

	encMapHeader(out, newCount)
	*out = append(*out, tmp...)
	return rcOK
}

func editArray(out *[]byte, a []byte, n, icur int, stepIdx int64, zpath string, pi int, newBin []byte, mode int) int {
	if icur >= n {
		return rcError
	}
	b := a[icur]
	var count, dataOff int
	if b >= 0x90 && b <= 0x9f {
		count, dataOff = int(b&0x0f), icur+1
	} else if b == mpArray16 {
		if icur+3 > n {
			return rcError
		}
		count, dataOff = int(read16(a, icur+1)), icur+3
	} else if b == mpArray32 {
		if icur+5 > n {
			return rcError
		}
		count, dataOff = int(read32(a, icur+1)), icur+5
	} else {
		if mode == editReplace || mode == editRemove {
			iend := skipOne(a, n, icur)
			if iend != 0 {
				*out = append(*out, a[icur:iend]...)
			}
			return rcOK
		}
		return rcError
	}

	newCount := uint32(count)
	var tmp []byte
	cur2 := dataOff
	foundIt := false

	for j := 0; j < count; j++ {
		eEnd := skipOne(a, n, cur2)
		if eEnd == 0 {
			return rcError
		}

		if int64(j) == stepIdx {
			foundIt = true
			if mode == editArrayIns {
				tmp = append(tmp, newBin...)
				tmp = append(tmp, a[cur2:eEnd]...)
				newCount++
			} else if mode == editInsert {
				tmp = append(tmp, a[cur2:eEnd]...)
			} else {
				var ebuf []byte
				rc, skip := editStep(&ebuf, a, n, cur2, zpath, pi, newBin, mode)
				if rc != rcOK {
					return rc
				}
				if skip {
					newCount--
				} else {
					tmp = append(tmp, ebuf...)
				}
			}
		} else {
			tmp = append(tmp, a[cur2:eEnd]...)
		}
		cur2 = eEnd
	}

	if !foundIt {
		if mode == editArrayIns {
			tmp = append(tmp, newBin...)
			newCount++
		} else if (mode == editSet || mode == editInsert) && stepIdx == int64(count) {
			tmp = append(tmp, newBin...)
			newCount++
		} else if mode == editReplace || mode == editRemove {
			iend := skipOne(a, n, icur)
			if iend != 0 {
				*out = append(*out, a[icur:iend]...)
			}
			return rcOK
		} else {
			return rcNotFound
		}
	}

	encArrayHeader(out, newCount)
	*out = append(*out, tmp...)
	return rcOK
}

// editStep returns (rc, skip).
func editStep(out *[]byte, a []byte, n, icur int, zpath string, pi int, newBin []byte, mode int) (int, bool) {
	kind, npi, key, stepIdx := pathStep(zpath, pi)

	switch kind {
	case stepEnd:
		if mode == editRemove {
			return rcOK, true
		}
		if mode == editArrayIns {
			return rcError, false
		}
		if mode == editInsert {
			iend := skipOne(a, n, icur)
			if iend != 0 {
				*out = append(*out, a[icur:iend]...)
			}
			return rcOK, false
		}
		*out = append(*out, newBin...)
		return rcOK, false
	case stepErr:
		return rcError, false
	case stepKey:
		return editMap(out, a, n, icur, []byte(key), zpath, npi, newBin, mode), false
	default: // stepIndex
		return editArray(out, a, n, icur, stepIdx, zpath, npi, newBin, mode), false
	}
}

// applyEdit applies a path-targeted edit; returns (rc, outBytes).
func applyEdit(a []byte, n int, zpath string, newBin []byte, mode int) (int, []byte) {
	if len(zpath) == 0 || zpath[0] != '$' {
		return rcError, nil
	}
	var out []byte
	rc, _ := editStep(&out, a, n, 0, zpath, 1, newBin, mode)
	return rc, out
}

// ── merge patch (RFC 7386) ──────────────────────────────────────────

// mergePatch applies an RFC 7386 merge patch; returns (rc, outBytes).
func mergePatch(a []byte, n, ia int, p []byte, np, ip int) (int, []byte) {
	var out []byte
	rc := mergePatchInto(&out, a, n, ia, p, np, ip, 0)
	return rc, out
}

type patchEntry struct {
	keyOff, valOff, pairEnd int
	matched                 bool
}

func mergePatchInto(out *[]byte, a []byte, n, ia int, p []byte, np, ip, depth int) int {
	if ip >= np {
		return rcError
	}
	if depth > MaxDepth {
		return rcError
	}
	pb := p[ip]

	if pb == mpNil {
		*out = append(*out, mpNil)
		return rcOK
	}

	pIsMap := (pb >= 0x80 && pb <= 0x8f) || pb == mpMap16 || pb == mpMap32
	if !pIsMap {
		pEnd := skipOne(p, np, ip)
		if pEnd != 0 {
			*out = append(*out, p[ip:pEnd]...)
		}
		return rcOK
	}

	var ab byte
	if ia < n {
		ab = a[ia]
	}
	aIsMap := (ab >= 0x80 && ab <= 0x8f) || ab == mpMap16 || ab == mpMap32

	var pCount, pDataOff int
	if pb >= 0x80 && pb <= 0x8f {
		pCount, pDataOff = int(pb&0x0f), ip+1
	} else if pb == mpMap16 {
		if ip+3 > np {
			return rcError
		}
		pCount, pDataOff = int(read16(p, ip+1)), ip+3
	} else {
		if ip+5 > np {
			return rcError
		}
		pCount, pDataOff = int(read32(p, ip+1)), ip+5
	}

	aCount, aDataOff := 0, 0
	if aIsMap {
		if ab >= 0x80 && ab <= 0x8f {
			aCount, aDataOff = int(ab&0x0f), ia+1
		} else if ab == mpMap16 {
			if ia+3 > n {
				aIsMap = false
			} else {
				aCount, aDataOff = int(read16(a, ia+1)), ia+3
			}
		} else if ia+5 > n {
			aIsMap = false
		} else {
			aCount, aDataOff = int(read32(a, ia+1)), ia+5
		}
	}

	// Pre-scan patch keys.
	if pCount > (np-pDataOff)/2+1 {
		return rcError
	}
	pIdx := make([]patchEntry, 0, pCount)
	pc2 := pDataOff
	for k := 0; k < pCount; k++ {
		if pc2 >= np {
			return rcError
		}
		valOff := skipOne(p, np, pc2)
		if valOff == 0 {
			return rcError
		}
		pairEnd := skipOne(p, np, valOff)
		if pairEnd == 0 {
			return rcError
		}
		pIdx = append(pIdx, patchEntry{keyOff: pc2, valOff: valOff, pairEnd: pairEnd})
		pc2 = pairEnd
	}

	var tmp []byte
	var newCount uint32

	if aIsMap {
		ac := aDataOff
		for j := 0; j < aCount; j++ {
			if ac >= n {
				return rcError
			}
			kstr, kok := mapKey(a, n, ac)
			aValOff := skipOne(a, n, ac)
			if aValOff == 0 {
				return rcError
			}
			aPairEnd := skipOne(a, n, aValOff)
			if aPairEnd == 0 {
				return rcError
			}

			foundInPatch := false
			patchIsNil := false
			pMatchVal := 0
			for k := range pIdx {
				pkey, pok := mapKey(p, np, pIdx[k].keyOff)
				if pok && kok && bytesEqual(pkey, kstr) {
					foundInPatch = true
					pMatchVal = pIdx[k].valOff
					patchIsNil = pIdx[k].valOff < np && p[pIdx[k].valOff] == mpNil
					pIdx[k].matched = true
					break
				}
			}

			if foundInPatch && patchIsNil {
				// drop
			} else if foundInPatch {
				var mb []byte
				mrc := mergePatchInto(&mb, a, n, aValOff, p, np, pMatchVal, depth+1)
				if mrc == rcOK {
					tmp = append(tmp, a[ac:aValOff]...)
					tmp = append(tmp, mb...)
					newCount++
				}
			} else {
				tmp = append(tmp, a[ac:aPairEnd]...)
				newCount++
			}
			ac = aPairEnd
		}
	}

	for k := range pIdx {
		if !pIdx[k].matched && pIdx[k].valOff < np && p[pIdx[k].valOff] != mpNil {
			tmp = append(tmp, p[pIdx[k].keyOff:pIdx[k].pairEnd]...)
			newCount++
		}
	}

	encMapHeader(out, newCount)
	*out = append(*out, tmp...)
	return rcOK
}
