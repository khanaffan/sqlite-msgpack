package msgpackblob_test

// Replay the shared cross-language vectors (tests/vectors/blob_vectors.json).
// Generated from the C++ reference implementation, so passing them proves the
// Go port is byte-identical.

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	mb "github.com/khanaffan/sqlite-msgpack/go"
)

type specT struct {
	K    string          `json:"k"`
	V    json.RawMessage `json:"v"`
	Hex  string          `json:"hex"`
	Type int             `json:"type"`
	Sec  string          `json:"sec"`
	Nsec int64           `json:"nsec"`
	JSON string          `json:"json"`
}

type iterRow struct {
	Key     *string `json:"key"`
	Index   *int64  `json:"index"`
	Fullkey string  `json:"fullkey"`
	Path    string  `json:"path"`
	ID      int     `json:"id"`
	Type    string  `json:"type"`
}

type vectorsT struct {
	FromJSON []struct {
		JSON string `json:"json"`
		Hex  string `json:"hex"`
	} `json:"from_json"`
	ToJSON []struct {
		Hex  string `json:"hex"`
		JSON string `json:"json"`
	} `json:"to_json"`
	ToJSONPretty []struct {
		Hex    string `json:"hex"`
		Indent int    `json:"indent"`
		JSON   string `json:"json"`
	} `json:"to_json_pretty"`
	Typed []struct {
		Spec specT  `json:"spec"`
		Hex  string `json:"hex"`
	} `json:"typed"`
	Mutate []struct {
		Base  string `json:"base"`
		Op    string `json:"op"`
		Path  string `json:"path"`
		Spec  specT  `json:"spec"`
		Patch string `json:"patch"`
		Hex   string `json:"hex"`
	} `json:"mutate"`
	Extract []struct {
		Base  string `json:"base"`
		Path  string `json:"path"`
		Type  string `json:"type"`
		Vjson string `json:"vjson"`
	} `json:"extract"`
	ArrayLength []struct {
		Base string `json:"base"`
		Path string `json:"path"`
		Len  int64  `json:"len"`
	} `json:"array_length"`
	Iterate []struct {
		Base      string    `json:"base"`
		Path      string    `json:"path"`
		Recursive bool      `json:"recursive"`
		Rows      []iterRow `json:"rows"`
	} `json:"iterate"`
}

func loadVectors(t *testing.T) vectorsT {
	t.Helper()
	raw, err := os.ReadFile("../tests/vectors/blob_vectors.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v vectorsT
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	return v
}

func mustHex(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

func parseI64(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func parseU64(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func buildValue(spec specT) mb.Value {
	str := func() string {
		var s string
		if err := json.Unmarshal(spec.V, &s); err != nil {
			panic(err)
		}
		return s
	}
	switch spec.K {
	case "nil":
		return mb.Nil()
	case "bool":
		var b bool
		_ = json.Unmarshal(spec.V, &b)
		return mb.Bool(b)
	case "int":
		return mb.Int(parseI64(str()))
	case "uint":
		return mb.Uint(parseU64(str()))
	case "int8":
		return mb.Int8(int8(parseI64(str())))
	case "int16":
		return mb.Int16(int16(parseI64(str())))
	case "int32":
		return mb.Int32(int32(parseI64(str())))
	case "int64":
		return mb.Int64(parseI64(str()))
	case "uint8":
		return mb.Uint8(uint8(parseU64(str())))
	case "uint16":
		return mb.Uint16(uint16(parseU64(str())))
	case "uint32":
		return mb.Uint32(uint32(parseU64(str())))
	case "uint64":
		return mb.Uint64(parseU64(str()))
	case "real":
		var f float64
		_ = json.Unmarshal(spec.V, &f)
		return mb.Real(f)
	case "real32":
		var f float64
		_ = json.Unmarshal(spec.V, &f)
		return mb.Real32(float32(f))
	case "str":
		return mb.Str(str())
	case "binary":
		return mb.Bin(mustHex(spec.Hex))
	case "ext":
		return mb.Ext(int8(spec.Type), mustHex(spec.Hex))
	case "timestamp":
		return mb.TimestampNs(parseI64(spec.Sec), uint32(spec.Nsec))
	}
	panic("unknown spec kind " + spec.K)
}

func TestFromJSON(t *testing.T) {
	for _, c := range loadVectors(t).FromJSON {
		if got := mb.FromJSON(c.JSON).Hex(); got != c.Hex {
			t.Errorf("from_json %q: got %s want %s", c.JSON, got, c.Hex)
		}
	}
}

func TestToJSON(t *testing.T) {
	for _, c := range loadVectors(t).ToJSON {
		if got := mb.NewBlob(mustHex(c.Hex)).ToJSON(); got != c.JSON {
			t.Errorf("to_json %s: got %q want %q", c.Hex, got, c.JSON)
		}
	}
}

func TestToJSONPretty(t *testing.T) {
	for _, c := range loadVectors(t).ToJSONPretty {
		if got := mb.NewBlob(mustHex(c.Hex)).ToJSONPretty(c.Indent); got != c.JSON {
			t.Errorf("to_json_pretty %s: got %q want %q", c.Hex, got, c.JSON)
		}
	}
}

func TestTyped(t *testing.T) {
	for _, c := range loadVectors(t).Typed {
		if got := mb.Quote(buildValue(c.Spec)).Hex(); got != c.Hex {
			t.Errorf("typed %+v: got %s want %s", c.Spec, got, c.Hex)
		}
	}
}

func TestMutate(t *testing.T) {
	for _, c := range loadVectors(t).Mutate {
		base := mb.FromJSON(c.Base)
		var r mb.Blob
		switch c.Op {
		case "set":
			r = base.Set(c.Path, buildValue(c.Spec))
		case "insert":
			r = base.Insert(c.Path, buildValue(c.Spec))
		case "replace":
			r = base.Replace(c.Path, buildValue(c.Spec))
		case "array_insert":
			r = base.ArrayInsert(c.Path, buildValue(c.Spec))
		case "remove":
			r = base.Remove(c.Path)
		case "set_blob":
			r = base.SetBlob(c.Path, mb.FromJSON(c.Spec.JSON))
		case "patch":
			r = base.Patch(mb.FromJSON(c.Patch))
		default:
			t.Fatalf("unknown op %s", c.Op)
		}
		if got := r.Hex(); got != c.Hex {
			t.Errorf("mutate %s %s %s: got %s want %s", c.Op, c.Base, c.Path, got, c.Hex)
		}
	}
}

func TestExtract(t *testing.T) {
	for _, c := range loadVectors(t).Extract {
		blob := mb.FromJSON(c.Base)
		if got := blob.TypeStrAt(c.Path); got != c.Type {
			t.Errorf("extract type %s %s: got %s want %s", c.Base, c.Path, got, c.Type)
		}
		if got := mb.Quote(blob.Extract(c.Path)).ToJSON(); got != c.Vjson {
			t.Errorf("extract value %s %s: got %q want %q", c.Base, c.Path, got, c.Vjson)
		}
	}
}

func TestArrayLength(t *testing.T) {
	for _, c := range loadVectors(t).ArrayLength {
		var got int64
		if c.Path == "$" {
			got = mb.FromJSON(c.Base).ArrayLength()
		} else {
			got = mb.FromJSON(c.Base).ArrayLengthAt(c.Path)
		}
		if got != c.Len {
			t.Errorf("array_length %s %s: got %d want %d", c.Base, c.Path, got, c.Len)
		}
	}
}

func TestIterate(t *testing.T) {
	for _, c := range loadVectors(t).Iterate {
		rows := mb.NewIterator(mb.FromJSON(c.Base), c.Path, c.Recursive).Rows()
		if len(rows) != len(c.Rows) {
			t.Fatalf("iterate %s %s: got %d rows want %d", c.Base, c.Path, len(rows), len(c.Rows))
		}
		for i, exp := range c.Rows {
			got := rows[i]
			if got.Fullkey != exp.Fullkey || got.Path != exp.Path || got.ID != exp.ID || got.Type.String() != exp.Type {
				t.Errorf("iterate row %d: got %+v want %+v", i, got, exp)
			}
			if exp.Key != nil && (got.Key != *exp.Key || got.Index != *exp.Index) {
				t.Errorf("iterate row %d key/index: got %q/%d want %q/%d", i, got.Key, got.Index, *exp.Key, *exp.Index)
			}
		}
	}
}
