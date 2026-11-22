package ring

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// Every fixed-width kind written through the direct path and read back
// through the core's checked getters: the bytes land where the core
// expects them, nulls and the width refusals included.
func TestDirectStoresReadBackThroughCore(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: false},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "u16", Type: arrow.PrimitiveTypes.Uint16, Nullable: false},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true},
		{Name: "xyz", Type: arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float64), Nullable: false},
		{Name: "servo", Type: arrow.StructOf(arrow.Field{Name: "pos", Type: arrow.PrimitiveTypes.Int32}), Nullable: false},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	r, err := NewRing(schema, 64, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	leaf := func(p string) int32 {
		i, err := r.Leaf(p)
		if err != nil {
			t.Fatal(err)
		}
		return i
	}
	i8, i32, u16, f32, f64, b, ts, id, xyz, pos, s := leaf("i8"), leaf("i32"), leaf("u16"), leaf("f32"),
		leaf("f64"), leaf("b"), leaf("ts"), leaf("id"), leaf("xyz"), leaf("servo.pos"), leaf("s")
	xyzBytes := make([]byte, 24)
	for i, v := range []float64{1.5, -2.5, 3.25} {
		binary.LittleEndian.PutUint64(xyzBytes[i*8:], math.Float64bits(v))
	}
	for row := int64(0); row < 100; row++ { // past the ring's wrap
		if _, err := r.RowBegin(0); err != nil {
			t.Fatal(err)
		}
		must := func(err error) {
			if err != nil {
				t.Fatalf("row %d: %v", row, err)
			}
		}
		must(r.SetI64(i8, -5))
		if row%2 == 0 {
			must(r.SetI64(i32, row*1000))
		} else {
			must(r.SetNull(i32))
		}
		must(r.SetU64(u16, 65535))
		must(r.SetF64(f32, 0.5))
		if row%3 == 0 {
			must(r.SetF64(f64, float64(row)*0.25))
		}
		must(r.SetBool(b, row%2 == 1))
		must(r.SetI64(ts, 1700000000000000+row))
		must(r.SetBytes(id, []byte{1, 2, 3, byte(row)}))
		must(r.SetList(xyz, xyzBytes, 3))
		must(r.SetI64(pos, int64(row)))
		must(r.SetString(s, "row"))
		if _, err := r.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	for _, seq := range []int64{r.OldestSeq(), 64, 77, r.NewestSeq()} {
		check := func(name string, got, want any, null, wantNull bool, err error) {
			if err != nil || null != wantNull || (!null && got != want) {
				t.Fatalf("%s@%d: got %v null=%v err=%v, want %v null=%v", name, seq, got, null, err, want, wantNull)
			}
		}
		v, n, err := r.GetI64(seq, i8)
		check("i8", v, int64(-5), n, false, err)
		v, n, err = r.GetI64(seq, i32)
		check("i32", v, seq*1000, n, seq%2 == 1, err)
		u, n, err := r.GetU64(seq, u16)
		check("u16", u, uint64(65535), n, false, err)
		f, n, err := r.GetF64(seq, f32)
		check("f32", f, 0.5, n, false, err)
		f, n, err = r.GetF64(seq, f64)
		check("f64", f, float64(seq)*0.25, n, seq%3 != 0, err)
		bv, n, err := r.GetBool(seq, b)
		check("b", bv, seq%2 == 1, n, false, err)
		v, n, err = r.GetI64(seq, ts)
		check("ts", v, int64(1700000000000000+seq), n, false, err)
		v, n, err = r.GetI64(seq, pos)
		check("servo.pos", v, seq, n, false, err)
		bs, n, err := r.GetBytes(seq, s)
		check("s", string(bs), "row", n, false, err)
	}
	// the refusals, with the core's status names
	if _, err := r.RowBegin(0); err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"i8 overflow":       r.SetI64(i8, 300),
		"u16 negative":      r.SetI64(u16, -1),
		"int into float":    r.SetI64(f64, 1),
		"float into int":    r.SetF64(i8, 1.0),
		"id wrong width":    r.SetBytes(id, []byte{1}),
		"xyz wrong count":   r.SetList(xyz, xyzBytes, 2),
		"null not nullable": r.SetNull(i8),
		"leaf out of range": r.SetI64(99, 1),
	} {
		if err == nil {
			t.Fatalf("%s: must refuse", name)
		}
	}
	if e, ok := r.SetI64(i8, 300).(*Error); !ok || e.Status != "width" {
		t.Fatalf("overflow status: %v", r.SetI64(i8, 300))
	}
	if e, ok := r.SetNull(i8).(*Error); !ok || e.Status != "not-nullable" {
		t.Fatalf("not-nullable status: %v", r.SetNull(i8))
	}
}

func BenchmarkSetF64(b *testing.B) {
	r, _ := NewRing(testSchema(), 4096, nil)
	defer r.Close()
	x, _ := r.Leaf("x")
	r.RowBegin(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.SetF64(x, 0.5)
	}
}

func BenchmarkSetF64ViaCore(b *testing.B) {
	r, _ := NewRing(testSchema(), 4096, nil)
	defer r.Close()
	x, _ := r.Leaf("x")
	r.RowBegin(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.setF64ViaCore(x, 0.5)
	}
}
