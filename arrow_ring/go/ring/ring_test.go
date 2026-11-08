package ring

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
}

func fill(t *testing.T, r *Ring, start, count int) {
	t.Helper()
	n, _ := r.Leaf("n")
	x, _ := r.Leaf("x")
	s, _ := r.Leaf("s")
	for i := start; i < start+count; i++ {
		if _, err := r.RowBegin(time.Second); err != nil {
			t.Fatal(err)
		}
		if err := r.SetI64(n, int64(i)); err != nil {
			t.Fatal(err)
		}
		if err := r.SetF64(x, float64(i)*0.5); err != nil {
			t.Fatal(err)
		}
		if err := r.SetString(s, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestRowWriteReadBack(t *testing.T) {
	r, err := NewRing(testSchema(), 256, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fill(t, r, 0, 100)
	if r.NewestSeq() != 99 || r.OldestSeq() != 0 {
		t.Fatalf("window [%d,%d]", r.OldestSeq(), r.NewestSeq())
	}
	n, _ := r.Leaf("n")
	x, _ := r.Leaf("x")
	s, _ := r.Leaf("s")
	for _, seq := range []int64{0, 42, 99} {
		v, null, err := r.GetI64(seq, n)
		if err != nil || null || v != seq {
			t.Fatalf("n@%d: %d %v %v", seq, v, null, err)
		}
		f, _, _ := r.GetF64(seq, x)
		if f != float64(seq)*0.5 {
			t.Fatalf("x@%d: %v", seq, f)
		}
		b, _, err := r.GetBytes(seq, s)
		if err != nil || string(b) != fmt.Sprintf("row-%d", seq) {
			t.Fatalf("s@%d: %q %v", seq, b, err)
		}
	}
}

func TestWrapAndStale(t *testing.T) {
	r, err := NewRing(testSchema(), 64, &Options{LeaseMarginS: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fill(t, r, 0, 200) // 3x capacity: seqs 0..135 recycled
	n, _ := r.Leaf("n")
	if _, _, err := r.GetI64(5, n); !IsStale(err) {
		t.Fatalf("recycled seq must be stale, got %v", err)
	}
	v, _, err := r.GetI64(199, n)
	if err != nil || v != 199 {
		t.Fatalf("newest: %d %v", v, err)
	}
	if r.OldestSeq() != 200-64 {
		t.Fatalf("oldest %d", r.OldestSeq())
	}
}

func TestAppendRecordBulk(t *testing.T) {
	r, err := NewRing(testSchema(), 512, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	mem := memory.DefaultAllocator
	b := array.NewRecordBuilder(mem, testSchema())
	defer b.Release()
	for i := 0; i < 300; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.Float64Builder).Append(float64(i) * 0.5)
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf("row-%d", i))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	if err := r.AppendRecord(rec); err != nil {
		t.Fatal(err)
	}
	if r.NewestSeq() != 299 {
		t.Fatalf("newest %d", r.NewestSeq())
	}
	nl, _ := r.Leaf("n")
	sl, _ := r.Leaf("s")
	v, _, _ := r.GetI64(150, nl)
	bs, _, _ := r.GetBytes(150, sl)
	if v != 150 || string(bs) != "row-150" {
		t.Fatalf("row 150: %d %q", v, bs)
	}
}

func TestStaticRelationZeroCopyOut(t *testing.T) {
	r, err := NewRing(testSchema(), 512, &Options{LeaseMarginS: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fill(t, r, 0, 400)
	rs, err := r.StaticRelation(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Lo != 0 || rs.Hi != 400 {
		t.Fatalf("window [%d,%d)", rs.Lo, rs.Hi)
	}
	recs, err := rs.Records()
	if err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	next := int64(0)
	for _, rec := range recs {
		ncol := rec.Column(0).(*array.Int64)
		scol := rec.Column(2).(*array.String)
		for i := 0; i < ncol.Len(); i++ {
			if ncol.Value(i) != next ||
				scol.Value(i) != fmt.Sprintf("row-%d", next) {
				t.Fatalf("row %d: %d %q", next, ncol.Value(i), scol.Value(i))
			}
			next++
		}
		total += int64(rec.NumRows())
	}
	if total != 400 {
		t.Fatalf("total %d", total)
	}
	for _, rec := range recs {
		rec.Release()
	}
	rs.Release()
}

func TestPinStallsWriterLoudly(t *testing.T) {
	r, err := NewRing(testSchema(), 64, &Options{LeaseMarginS: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fill(t, r, 0, 64) // full
	rs, err := r.StaticRelation(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// the pin covers seq 0; allocating seq 64 must recycle it -> stall
	_, err = r.RowBegin(30 * time.Millisecond)
	if !IsPinStalled(err) {
		t.Fatalf("expected pin-stall, got %v", err)
	}
	rs.Release()
	if _, err := r.RowBegin(time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Pin(lo, hi) holds the rows named and no others: the writer recycles
// everything below lo and meets the pin only when it reaches lo.
func TestPinRangeHoldsOnlyItsRows(t *testing.T) {
	r, err := NewRing(testSchema(), 64, &Options{LeaseMarginS: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fill(t, r, 0, 64) // full
	rs, err := r.Pin(60, 64)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Lo != 60 || rs.Hi != 64 {
		t.Fatalf("pin [60,64) got [%d,%d)", rs.Lo, rs.Hi)
	}
	fill(t, r, 64, 60) // seq 64..123 recycle 0..59: no pin in the way
	if _, err = r.RowBegin(30 * time.Millisecond); !IsPinStalled(err) {
		t.Fatalf("seq 124 must stall on the pin at 60, got %v", err)
	}
	rs.Release()
	if _, err := r.RowBegin(time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	// lo below the oldest row is raised to it; hi above committed is lowered.
	rs, err = r.Pin(0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Lo != r.OldestSeq() || rs.Hi != r.NewestSeq()+1 {
		t.Fatalf("clamped pin got [%d,%d), window [%d,%d)", rs.Lo, rs.Hi, r.OldestSeq(), r.NewestSeq()+1)
	}
	rs.Release()
	if _, err := r.Pin(10, 5); err == nil {
		t.Fatal("hi < lo must be refused")
	}
}

func TestWireMapScatterGather(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "cnt", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "pos", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	r, err := NewRing(schema, 128, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	m, err := r.NewWireMap([]WireEntry{
		{Path: "cnt", Offset: 0, Size: 2},
		{Path: "pos", Offset: 2, Size: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i := 0; i < 5; i++ {
		frame := make([]byte, 10)
		frame[0] = byte(i)
		binary.LittleEndian.PutUint64(frame[2:], math.Float64bits(float64(i)*1.5))
		if _, err := r.RowBegin(time.Second); err != nil {
			t.Fatal(err)
		}
		if err := r.FromWire(m, frame); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	pl, _ := r.Leaf("pos")
	v, _, _ := r.GetF64(3, pl)
	if v != 4.5 {
		t.Fatalf("pos@3 = %v", v)
	}
	frame, err := r.ToWire(m, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	if frame[0] != 3 {
		t.Fatalf("cnt gathered %d", frame[0])
	}
}

func TestStructSchemaFlattensToLeaves(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tv", Type: arrow.PrimitiveTypes.Float64},
		{Name: "servo", Type: arrow.StructOf(
			arrow.Field{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "vel", Type: arrow.PrimitiveTypes.Int64},
		)},
	}, nil)
	r, err := NewRing(schema, 64, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	pos, err := r.Leaf("servo.pos")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.RowBegin(time.Second); err != nil {
		t.Fatal(err)
	}
	tv, _ := r.Leaf("tv")
	vel, _ := r.Leaf("servo.vel")
	r.SetF64(tv, 1.5)
	r.SetI64(pos, 100)
	r.SetI64(vel, -3)
	if _, err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	v, _, _ := r.GetI64(0, pos)
	if v != 100 {
		t.Fatalf("servo.pos = %d", v)
	}
}

func TestAppendRecordAtGapAdvance(t *testing.T) {
	r, err := NewRing(testSchema(), 64, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	mem := memory.DefaultAllocator
	mk := func(start, n int) arrow.RecordBatch {
		b := array.NewRecordBuilder(mem, testSchema())
		defer b.Release()
		for i := start; i < start+n; i++ {
			b.Field(0).(*array.Int64Builder).Append(int64(i))
			b.Field(1).(*array.Float64Builder).Append(float64(i) * 0.5)
			b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf("row-%d", i))
		}
		rec := b.NewRecordBatch()
		return rec
	}
	rec := mk(0, 10)
	if err := r.AppendRecordAt(rec, 500); err != nil { // fresh jump-start
		t.Fatal(err)
	}
	rec.Release()
	if r.OldestSeq() != 500 || r.NewestSeq() != 509 {
		t.Fatalf("window [%d,%d]", r.OldestSeq(), r.NewestSeq())
	}
	rec = mk(10, 5)
	if err := r.AppendRecordAt(rec, 600); err != nil { // GAP
		t.Fatal(err)
	}
	rec.Release()
	if r.OldestSeq() != 600 || r.NewestSeq() != 604 {
		t.Fatalf("post-gap window [%d,%d]", r.OldestSeq(), r.NewestSeq())
	}
	n, _ := r.Leaf("n")
	if _, _, err := r.GetI64(505, n); !IsStale(err) {
		t.Fatalf("pre-gap read must be stale, got %v", err)
	}
	v, _, err := r.GetI64(602, n)
	if err != nil || v != 12 {
		t.Fatalf("post-gap read: %d %v", v, err)
	}
	rec = mk(0, 1)
	if err := r.AppendRecordAt(rec, 100); err == nil { // regression
		t.Fatal("seq regression must refuse")
	}
	rec.Release()
}
