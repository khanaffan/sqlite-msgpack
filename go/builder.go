package msgpackblob

// Builder is a streaming encoder that produces a Blob. Methods return the
// Builder so calls can be chained; finalise with Build.
type Builder struct {
	buf []byte
}

// NewBuilder returns a new, empty Builder.
func NewBuilder() *Builder { return &Builder{} }

// ── scalars ───────────────────────────────────────────────────────────
func (b *Builder) Nil() *Builder                     { encNil(&b.buf); return b }
func (b *Builder) Boolean(v bool) *Builder           { encBool(&b.buf, v); return b }
func (b *Builder) Integer(x int64) *Builder          { encInteger(&b.buf, x); return b }
func (b *Builder) UnsignedInteger(x uint64) *Builder { encUnsigned(&b.buf, x); return b }
func (b *Builder) Real(d float64) *Builder           { encReal(&b.buf, d); return b }
func (b *Builder) Real32(f float32) *Builder         { encReal32(&b.buf, f); return b }
func (b *Builder) String(s string) *Builder          { encString(&b.buf, []byte(s)); return b }
func (b *Builder) StringBytes(s []byte) *Builder     { encString(&b.buf, s); return b }
func (b *Builder) Binary(data []byte) *Builder       { encBinary(&b.buf, data); return b }

// Ext appends an ext element.
func (b *Builder) Ext(typeCode int8, data []byte) *Builder { encExt(&b.buf, typeCode, data); return b }

// ── fixed-width integers ──────────────────────────────────────────────
func (b *Builder) Int8(x int8) *Builder     { encInt8(&b.buf, int64(x)); return b }
func (b *Builder) Int16(x int16) *Builder   { encInt16(&b.buf, int64(x)); return b }
func (b *Builder) Int32(x int32) *Builder   { encInt32(&b.buf, int64(x)); return b }
func (b *Builder) Int64(x int64) *Builder   { encInt64(&b.buf, x); return b }
func (b *Builder) Uint8(x uint8) *Builder   { encUint8(&b.buf, uint64(x)); return b }
func (b *Builder) Uint16(x uint16) *Builder { encUint16(&b.buf, uint64(x)); return b }
func (b *Builder) Uint32(x uint32) *Builder { encUint32(&b.buf, uint64(x)); return b }
func (b *Builder) Uint64(x uint64) *Builder { encUint64(&b.buf, x); return b }

// ── containers ────────────────────────────────────────────────────────
func (b *Builder) ArrayHeader(count uint32) *Builder { encArrayHeader(&b.buf, count); return b }
func (b *Builder) MapHeader(count uint32) *Builder   { encMapHeader(&b.buf, count); return b }

// ── embedding & timestamp ─────────────────────────────────────────────
func (b *Builder) Raw(data []byte) *Builder     { b.buf = append(b.buf, data...); return b }
func (b *Builder) RawBlob(blob Blob) *Builder   { b.buf = append(b.buf, blob.data...); return b }
func (b *Builder) Value(v Value) *Builder       { encodeValue(&b.buf, v); return b }
func (b *Builder) Timestamp(sec int64) *Builder { encTimestamp(&b.buf, sec, 0); return b }

// TimestampNs appends a timestamp with nanoseconds.
func (b *Builder) TimestampNs(sec int64, nsec uint32) *Builder {
	encTimestamp(&b.buf, sec, nsec)
	return b
}

// ── finalize ──────────────────────────────────────────────────────────

// Build consumes the accumulated bytes and returns a Blob.
func (b *Builder) Build() Blob { return blobFromVec(b.buf) }

// Len returns the number of bytes accumulated so far.
func (b *Builder) Len() int { return len(b.buf) }

// Quote encodes a single Value into a Blob.
func Quote(v Value) Blob {
	var b Builder
	b.Value(v)
	return b.Build()
}
