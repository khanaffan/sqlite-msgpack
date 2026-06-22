package msgpackblob

// Iterator is a cursor over a container's children, supporting flat (each) and
// recursive (tree) modes — mirroring the SQLite extension's msgpack_each and
// msgpack_tree table-valued functions.
type Iterator struct {
	blob      Blob
	base      string
	recursive bool
	rows      []EachRow
	cursor    int
	populated bool
}

// NewIterator creates an iterator over the container at path. When recursive is
// true it walks the whole subtree (tree); otherwise it yields direct children
// (each).
func NewIterator(blob Blob, path string, recursive bool) *Iterator {
	if path == "" {
		path = "$"
	}
	return &Iterator{blob: blob, base: path, recursive: recursive, cursor: -1}
}

func (it *Iterator) populate() {
	if it.populated {
		return
	}
	it.populated = true
	it.rows = nil

	a := it.blob.data
	n := len(a)
	if n == 0 {
		return
	}

	iroot := 0
	if it.base != "$" {
		rc, istart, _ := lookup(a, n, 0, it.base)
		if rc != rcOK {
			return
		}
		iroot = istart
	}

	if it.recursive {
		treeWalk(a, n, iroot, it.base, it.base, 0, &it.rows)
	} else {
		it.rows = eachIter(a, n, iroot, it.base)
	}
}

// Next advances the cursor; returns false past the last row.
func (it *Iterator) Next() bool {
	it.populate()
	it.cursor++
	return it.cursor < len(it.rows)
}

// Current returns the row at the cursor (valid after Next returns true).
func (it *Iterator) Current() EachRow {
	return it.rows[it.cursor]
}

// Reset rewinds the cursor to before the first row.
func (it *Iterator) Reset() { it.cursor = -1 }

// Rows returns all rows as a slice.
func (it *Iterator) Rows() []EachRow {
	it.populate()
	return it.rows
}
