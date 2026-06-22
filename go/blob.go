package msgpackblob

import "encoding/hex"

// Blob is a MessagePack BLOB supporting read, mutation (copy-on-write) and JSON.
// The zero value is an empty (invalid) blob.
type Blob struct {
	data []byte
}

// NewBlob constructs a Blob from raw bytes (copies).
func NewBlob(data []byte) Blob {
	return Blob{data: append([]byte(nil), data...)}
}

// blobFromVec takes ownership of data without copying.
func blobFromVec(data []byte) Blob {
	return Blob{data: data}
}

// ── raw access ────────────────────────────────────────────────────────
func (b Blob) Data() []byte { return b.data }
func (b Blob) Size() int    { return len(b.data) }
func (b Blob) Empty() bool  { return len(b.data) == 0 }
func (b Blob) Hex() string  { return hex.EncodeToString(b.data) }

// ── validation ────────────────────────────────────────────────────────
func (b Blob) Valid() bool        { return isValid(b.data, len(b.data)) }
func (b Blob) ErrorPosition() int { return errorPosition(b.data, len(b.data)) }

// ── type inspection ───────────────────────────────────────────────────

// Type returns the type of the root element.
func (b Blob) Type() Type {
	if len(b.data) == 0 {
		return TypeNil
	}
	return getType(b.data, len(b.data), 0)
}

// TypeAt returns the type of the element at path.
func (b Blob) TypeAt(path string) Type {
	n := len(b.data)
	rc, istart, _ := lookup(b.data, n, 0, path)
	if rc != rcOK {
		return TypeNil
	}
	return getType(b.data, n, istart)
}

// TypeStr returns the label of the root element type.
func (b Blob) TypeStr() string { return b.Type().String() }

// TypeStrAt returns the label of the element type at path.
func (b Blob) TypeStrAt(path string) string { return b.TypeAt(path).String() }

// ── extraction ────────────────────────────────────────────────────────

// Extract decodes the element at path (nil Value if not found).
func (b Blob) Extract(path string) Value {
	n := len(b.data)
	rc, istart, iend := lookup(b.data, n, 0, path)
	if rc != rcOK {
		return Nil()
	}
	return decodeElement(b.data, n, istart, iend)
}

// ArrayLength returns the element count of the root container, or -1.
func (b Blob) ArrayLength() int64 {
	if len(b.data) == 0 {
		return -1
	}
	return getContainerCount(b.data, len(b.data), 0)
}

// ArrayLengthAt returns the element count of the container at path, or -1.
func (b Blob) ArrayLengthAt(path string) int64 {
	n := len(b.data)
	rc, istart, _ := lookup(b.data, n, 0, path)
	if rc != rcOK {
		return -1
	}
	return getContainerCount(b.data, n, istart)
}

// ── mutation (copy-on-write) ──────────────────────────────────────────
func (b Blob) apply(path string, value Value, mode int) Blob {
	var nb []byte
	encodeValue(&nb, value)
	rc, out := applyEdit(b.data, len(b.data), path, nb, mode)
	if rc == rcOK {
		return blobFromVec(out)
	}
	return b
}

// Set assigns value at path (creating it where appropriate).
func (b Blob) Set(path string, value Value) Blob { return b.apply(path, value, editSet) }

// SetBlob assigns an existing sub-blob at path (embedded verbatim).
func (b Blob) SetBlob(path string, sub Blob) Blob {
	rc, out := applyEdit(b.data, len(b.data), path, sub.data, editSet)
	if rc == rcOK {
		return blobFromVec(out)
	}
	return b
}

// Insert adds value at path only if it does not already exist.
func (b Blob) Insert(path string, value Value) Blob { return b.apply(path, value, editInsert) }

// Replace overwrites value at path only if it already exists.
func (b Blob) Replace(path string, value Value) Blob { return b.apply(path, value, editReplace) }

// ArrayInsert inserts value before the element at the array index path.
func (b Blob) ArrayInsert(path string, value Value) Blob { return b.apply(path, value, editArrayIns) }

// Remove deletes the element at path.
func (b Blob) Remove(path string) Blob {
	rc, out := applyEdit(b.data, len(b.data), path, nil, editRemove)
	if rc == rcOK {
		return blobFromVec(out)
	}
	return b
}

// Patch applies an RFC 7386 merge patch.
func (b Blob) Patch(merge Blob) Blob {
	rc, out := mergePatch(b.data, len(b.data), 0, merge.data, len(merge.data), 0)
	if rc == rcOK {
		return blobFromVec(out)
	}
	return b
}

// ── JSON conversion ───────────────────────────────────────────────────

// ToJSONBytes returns the byte-exact JSON serialisation (may contain non-UTF-8
// string bytes, identical to the C++ library output).
func (b Blob) ToJSONBytes() []byte {
	if len(b.data) == 0 {
		return []byte("null")
	}
	return toJSONBytes(b.data, len(b.data), false, 0)
}

// ToJSON returns the JSON serialisation as a string.
func (b Blob) ToJSON() string { return string(b.ToJSONBytes()) }

// ToJSONPrettyBytes returns the indented JSON serialisation as bytes.
func (b Blob) ToJSONPrettyBytes(indent int) []byte {
	if len(b.data) == 0 {
		return []byte("null")
	}
	if indent < 0 {
		indent = 0
	}
	if indent > 8 {
		indent = 8
	}
	return toJSONBytes(b.data, len(b.data), true, indent)
}

// ToJSONPretty returns the indented JSON serialisation as a string.
func (b Blob) ToJSONPretty(indent int) string { return string(b.ToJSONPrettyBytes(indent)) }

// FromJSONBytes parses JSON bytes into a msgpack blob.
func FromJSONBytes(json []byte) Blob { return blobFromVec(fromJSON(json)) }

// FromJSON parses a JSON string into a msgpack blob.
func FromJSON(json string) Blob { return blobFromVec(fromJSON([]byte(json))) }
