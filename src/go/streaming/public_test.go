package streaming_test

// The package as a program outside it has it: the calls of the package,
// no process object, no ring.

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/neofactory/streaming-parquet/src/go/streaming"
)

func TestThePublicCalls(t *testing.T) {
	store := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	s, err := streaming.CreateStream("pub/calls", schema, streaming.StreamOptions{
		Store: store, Memory: &streaming.MemoryOptions{Rows: 100}})
	if err != nil {
		t.Fatal(err)
	}
	defer streaming.Close()

	if err := streaming.SetLogger(nil); err == nil ||
		!strings.Contains(err.Error(), "call SetLogger first") {
		t.Fatalf("SetLogger after a stream was not refused: %v", err)
	}
	if _, err := s.Column("nope"); err == nil {
		t.Fatal("a column the stream does not have was accepted")
	}
	n, _ := s.Column("n")
	x, _ := s.Column("x")

	// a cursor made now gives the rows committed after it
	cursor := s.Cursor()
	for i := int64(0); i < 10; i++ {
		row, err := s.MutableRow(0)
		if err != nil {
			t.Fatal(err)
		}
		if row.RowNumber() != i {
			t.Fatalf("row %d has the number %d", i, row.RowNumber())
		}
		row.SetI64(n, i)
		row.SetF64(x, float64(i)*0.5)
	}
	if count, err := s.Commit(); err != nil || count != 10 {
		t.Fatalf("commit: %d rows, %v", count, err)
	}

	newest, ok := s.NewestRow()
	if !ok || newest.RowNumber() != 9 {
		t.Fatalf("newest row: %v %d", ok, newest.RowNumber())
	}
	if v, null, err := newest.F64(x); err != nil || null || v != 4.5 {
		t.Fatalf("x of the newest row: %v %v %v", v, null, err)
	}
	for want := int64(0); want < 10; want++ {
		row, ok := cursor.Next()
		if !ok {
			t.Fatalf("the cursor ended before row %d", want)
		}
		if v, _, _ := row.I64(n); v != want || row.RowNumber() != want {
			t.Fatalf("the cursor gave n=%d, row %d; want %d", v, row.RowNumber(), want)
		}
	}
	if _, ok := cursor.Next(); ok {
		t.Fatal("the cursor gave a row past the newest")
	}

	// many rows at once, committed by the call
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 11, 12}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{5, 5.5, 6}, nil)
	rec := b.NewRecordBatch()
	b.Release()
	if err := s.Append(rec); err != nil {
		t.Fatal(err)
	}
	rec.Release()
	added, err := cursor.ArrowTable()
	if err != nil || added.NumRows() != 3 {
		t.Fatalf("the cursor's table of the appended rows: %v", err)
	}
	added.Release()

	if got := s.MemoryState(); got.Rows != 13 || got.RowsUnarchived != 13 || got.Capacity != 100 {
		t.Fatalf("memory state: %+v", got)
	}
	all, err := s.ArrowTable()
	if err != nil || all.NumRows() != 13 || all.NumCols() != 2 {
		t.Fatalf("every row: %v", err)
	}
	all.Release()
	last, err := s.ArrowTableNewest(4, "x")
	if err != nil || last.NumRows() != 4 || last.NumCols() != 1 ||
		last.Schema().Field(0).Name != "x" {
		t.Fatalf("the newest 4 rows of x: %v", err)
	}
	if v := last.Column(0).Data().Chunk(0).(*array.Float64).Value(0); v != 4.5 {
		t.Fatalf("the first of the newest 4 rows has x=%v", v)
	}
	last.Release()
	if _, err := s.ArrowTable("nope"); err == nil {
		t.Fatal("a table of a column the stream does not have was made")
	}

	// a reader in the same process reads the stream in place
	a, err := streaming.Connect("pub/calls", streaming.ConnectOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.WaitConnected(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if row, ok := a.NewestRow(); !ok || row.RowNumber() != 12 {
		t.Fatalf("the reader's newest row: %v %d", ok, row.RowNumber())
	}
	if got := a.MemoryState(); got.Rows != 13 {
		t.Fatalf("the reader's memory state: %+v", got)
	}
	names, err := streaming.ListStreams(store)
	if err != nil || len(names) != 1 || names[0] != "pub/calls" {
		t.Fatalf("ListStreams: %v %v", names, err)
	}

	// Close ends the reader and the stream; a call after it starts again
	if err := streaming.Close(); err != nil {
		t.Fatal(err)
	}
	if a.State() != "closed" {
		t.Fatalf("after Close the reader's state is %q", a.State())
	}
	if err := streaming.SetLogger(nil); err != nil {
		t.Fatalf("SetLogger after Close: %v", err)
	}
}
