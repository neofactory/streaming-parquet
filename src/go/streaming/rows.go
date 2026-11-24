package streaming

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/neofactory/streaming-parquet/src/go/ring"
)

// The rows of a stream: how a program writes them and reads them. The
// ring that holds them is not part of the API.

// Column names one column of a stream for the row calls: the answer of
// Stream.Column or Attached.Column. A column inside a struct has a
// dotted path ("pos.x").
type Column int32

// WireEntry maps Size bytes at byte Offset of a packed frame to the
// fixed-width column at Path.
type WireEntry = ring.WireEntry

// WireMap is a set of WireEntry made ready for a stream (Stream.WireMap).
type WireMap = ring.WireMap

func columnOf(r *ring.Ring, who, path string) (Column, error) {
	if r == nil {
		return 0, fmt.Errorf("%s: Column(%q): not connected yet, so the "+
			"stream's columns are not known", who, path)
	}
	leaf, err := r.Leaf(path)
	if err != nil {
		return 0, fmt.Errorf("%s: Column(%q): %w", who, path, err)
	}
	return Column(leaf), nil
}

// ---- writing ---------------------------------------------------------------

// Schema is the stream's Arrow schema.
func (s *Stream) Schema() *arrow.Schema { return s.ring.Schema() }

// Column answers the column at path, for the setters and getters.
func (s *Stream) Column(path string) (Column, error) {
	return columnOf(s.ring, "stream "+s.Name, path)
}

// MutableRow is the row a writer is filling: the answer of
// Stream.MutableRow. Set its columns, then Commit the stream. A column
// that takes null and is not set is null.
type MutableRow struct {
	r      *ring.Ring
	number int64
}

// MutableRow begins the next row. ONE goroutine writes a stream. When
// the memory is full and a reader holds the oldest rows, it waits up to
// block for the reader to let go, and then returns an error.
func (s *Stream) MutableRow(block time.Duration) (MutableRow, error) {
	n, err := s.ring.RowBegin(block)
	return MutableRow{s.ring, n}, err
}

// RowNumber is the number of this row in the writer's run: the first
// row a writer commits is 0.
func (m MutableRow) RowNumber() int64 { return m.number }

func (m MutableRow) SetI64(c Column, v int64) error     { return m.r.SetI64(int32(c), v) }
func (m MutableRow) SetU64(c Column, v uint64) error    { return m.r.SetU64(int32(c), v) }
func (m MutableRow) SetF64(c Column, v float64) error   { return m.r.SetF64(int32(c), v) }
func (m MutableRow) SetBool(c Column, v bool) error     { return m.r.SetBool(int32(c), v) }
func (m MutableRow) SetBytes(c Column, v []byte) error  { return m.r.SetBytes(int32(c), v) }
func (m MutableRow) SetString(c Column, v string) error { return m.r.SetString(int32(c), v) }
func (m MutableRow) SetNull(c Column) error             { return m.r.SetNull(int32(c)) }

// SetList sets a list column: elems is the bytes of nElems elements.
func (m MutableRow) SetList(c Column, elems []byte, nElems int64) error {
	return m.r.SetList(int32(c), elems, nElems)
}

// FromWire puts one packed frame into the row, as the map says.
func (m MutableRow) FromWire(wm *WireMap, frame []byte) error {
	return m.r.FromWire(wm, frame)
}

// WireMap makes a map from the bytes of a packed frame to columns, for
// MutableRow.FromWire. Close it when done with it.
func (s *Stream) WireMap(entries []WireEntry) (*WireMap, error) {
	return s.ring.NewWireMap(entries)
}

// Append writes many rows at once, the rows of an Arrow record batch,
// and commits them: readers see them at once, as after Commit.
func (s *Stream) Append(rec arrow.RecordBatch) error {
	err := s.ring.AppendRecord(rec)
	s.committed()
	return err
}

// ---- reading ---------------------------------------------------------------

// Row is one committed row in memory. Its getters answer the value,
// and whether it is null. A row stays readable until the memory is
// used again for newer rows; a getter then returns an error.
type Row struct {
	r      *ring.Ring
	number int64
}

// RowNumber is the number of this row in its writer's run: the first
// row a writer commits is 0.
func (w Row) RowNumber() int64 { return w.number }

func (w Row) I64(c Column) (v int64, null bool, err error)   { return w.r.GetI64(w.number, int32(c)) }
func (w Row) U64(c Column) (v uint64, null bool, err error)  { return w.r.GetU64(w.number, int32(c)) }
func (w Row) F64(c Column) (v float64, null bool, err error) { return w.r.GetF64(w.number, int32(c)) }
func (w Row) Bool(c Column) (v bool, null bool, err error)   { return w.r.GetBool(w.number, int32(c)) }
func (w Row) Bytes(c Column) (v []byte, null bool, err error) {
	return w.r.GetBytes(w.number, int32(c))
}

func newestRow(r *ring.Ring) (Row, bool) {
	if r == nil {
		return Row{}, false
	}
	n := r.NewestSeq()
	if n < 0 {
		return Row{}, false
	}
	return Row{r, n}, true
}

// Cursor is a reader's place in a stream's memory. Next gives the rows
// committed since the last call, one at a time; ArrowTable gives them
// as one table. It starts after the newest row at the time it was made.
// Rows that the memory used again before this reader saw them are
// counted in Lost. When the writer starts again (a new run), the cursor
// goes on from the oldest row of the new run.
type Cursor struct {
	source func() *ring.Ring
	ring   *ring.Ring
	next   int64
	lost   int64
}

func newCursor(source func() *ring.Ring) *Cursor {
	c := &Cursor{source: source, ring: source()}
	if c.ring != nil {
		c.next = c.ring.NewestSeq() + 1
	}
	return c
}

// current is the memory the cursor reads now, nil with none yet.
func (c *Cursor) current() *ring.Ring {
	r := c.source()
	if r != c.ring {
		c.ring, c.next = r, 0
		if r != nil {
			c.next = r.OldestSeq()
		}
	}
	if r == nil {
		return nil
	}
	if oldest := r.OldestSeq(); oldest > c.next {
		c.lost += oldest - c.next
		c.next = oldest
	}
	return r
}

// Next answers the next row this cursor has not given, and false when
// there is none yet.
func (c *Cursor) Next() (Row, bool) {
	r := c.current()
	if r == nil || c.next > r.NewestSeq() {
		return Row{}, false
	}
	row := Row{r, c.next}
	c.next++
	return row, true
}

// Lost is the count of rows this cursor never gave, because the memory
// was used again before it reached them.
func (c *Cursor) Lost() int64 { return c.lost }

// ArrowTable gives the rows this cursor has not given as one table, no
// copy, and moves past them. Release the table when done with it: it
// holds its rows in memory, and the writer waits for a table that
// holds the oldest rows of a full memory.
func (c *Cursor) ArrowTable(columns ...string) (arrow.Table, error) {
	r := c.current()
	if r == nil {
		return emptyTable(nil, columns)
	}
	hi := r.NewestSeq() + 1
	t, reached, err := tableOf(r, c.next, hi, columns)
	if err != nil {
		return nil, err
	}
	c.next = reached
	return t, nil
}

func emptyTable(schema *arrow.Schema, columns []string) (arrow.Table, error) {
	if schema == nil {
		schema = arrow.NewSchema(nil, nil)
	}
	return tableColumns(array.NewTableFromRecords(schema, nil), columns)
}

// tableOf is the rows [lo, hi) of r as one table, with no copy: the
// table pins exactly its rows until it is released. It also answers
// the row number after the table's last row.
func tableOf(r *ring.Ring, lo, hi int64, columns []string) (arrow.Table, int64, error) {
	if hi <= lo {
		t, err := emptyTable(r.Schema(), columns)
		return t, lo, err
	}
	rs, err := r.Pin(lo, hi)
	if err != nil {
		return nil, lo, err
	}
	if rs.Hi <= rs.Lo {
		rs.Release()
		t, err := emptyTable(r.Schema(), columns)
		return t, lo, err
	}
	recs, err := rs.Records()
	if err != nil {
		rs.Release()
		return nil, lo, err
	}
	reached := rs.Hi
	// the pin now lives exactly as long as the records do
	rs.Detach()
	schema := r.Schema()
	if len(recs) > 0 {
		schema = recs[0].Schema()
	}
	t := array.NewTableFromRecords(schema, recs)
	for _, rec := range recs {
		rec.Release() // the table holds them
	}
	out, err := tableColumns(t, columns)
	return out, reached, err
}

// tableColumns answers t with the named columns only, in the order
// given; every column when none is named. It takes t: the caller
// releases the answer only.
func tableColumns(t arrow.Table, columns []string) (arrow.Table, error) {
	if len(columns) == 0 {
		return t, nil
	}
	defer t.Release()
	fields := make([]arrow.Field, len(columns))
	cols := make([]arrow.Column, len(columns))
	for i, name := range columns {
		idx := t.Schema().FieldIndices(name)
		if len(idx) == 0 {
			return nil, fmt.Errorf("ArrowTable: the stream has no column %q", name)
		}
		fields[i] = t.Schema().Field(idx[0])
		cols[i] = *t.Column(idx[0])
	}
	return array.NewTable(arrow.NewSchema(fields, nil), cols, t.NumRows()), nil
}

// newestOf is the newest `rows` rows of r as one table.
func newestOf(r *ring.Ring, rows int64, columns []string) (arrow.Table, error) {
	if r == nil {
		return emptyTable(nil, columns)
	}
	hi := r.NewestSeq() + 1
	lo := r.OldestSeq()
	if rows < 0 {
		rows = 0
	}
	if hi-rows > lo {
		lo = hi - rows
	}
	t, _, err := tableOf(r, lo, hi, columns)
	return t, err
}

// MemoryState is what a stream, or a reader, holds in memory right now.
type MemoryState struct {
	Rows           int64 // committed rows readable from memory
	RowsUnarchived int64 // of those, rows not yet written to a file (0 for a reader that writes none)
	Capacity       int64 // rows the memory can hold
}

func rowsIn(r *ring.Ring) int64 {
	if r == nil {
		return 0
	}
	newest := r.NewestSeq()
	if newest < 0 {
		return 0
	}
	return newest - r.OldestSeq() + 1
}

// ---- a stream reads the same way a reader does -------------------------------

// NewestRow is the newest committed row, and false while there is none.
func (s *Stream) NewestRow() (Row, bool) { return newestRow(s.ring) }

// Cursor answers a cursor on the stream's memory: the rows committed
// from now on.
func (s *Stream) Cursor() *Cursor {
	return newCursor(func() *ring.Ring { return s.ring })
}

// unarchived is the count of committed rows not yet written to a file:
// all of them for a stream that writes none.
func (s *Stream) unarchived() int64 {
	committed := s.ring.NewestSeq() + 1
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.arch == nil || !s.arch.recording() {
		return committed - s.ring.OldestSeq()
	}
	n := committed - s.arch.wp.planned()
	if n < 0 {
		n = 0
	}
	if held := committed - s.ring.OldestSeq(); n > held {
		n = held
	}
	return n
}

// ArrowTable gives the rows in memory that are not yet in a file as
// one table, no copy; on a stream that writes no files, every row in
// memory. columns names a subset. Release the table when done with it:
// it holds its rows in memory, and the writer waits for a table that
// holds the oldest rows of a full memory.
func (s *Stream) ArrowTable(columns ...string) (arrow.Table, error) {
	return newestOf(s.ring, s.unarchived(), columns)
}

// ArrowTableNewest gives the newest `rows` rows in memory as one table
// (fewer when memory holds fewer), as ArrowTable does.
func (s *Stream) ArrowTableNewest(rows int64, columns ...string) (arrow.Table, error) {
	return newestOf(s.ring, rows, columns)
}

// MemoryState answers what the stream holds in memory right now.
func (s *Stream) MemoryState() MemoryState {
	return MemoryState{Rows: rowsIn(s.ring), RowsUnarchived: s.unarchived(),
		Capacity: s.ringRows}
}

// ---- a reader -----------------------------------------------------------------

// current is the memory the reader reads now: nil before its first
// connection.
func (a *Attached) current() *ring.Ring {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ring
}

// Schema is the stream's Arrow schema (of the columns this reader
// takes), and nil before the first connection.
func (a *Attached) Schema() *arrow.Schema {
	if r := a.current(); r != nil {
		return r.Schema()
	}
	return nil
}

// Column answers the column at path, for the getters. It is an error
// before the first connection: the columns are not known yet
// (WaitConnected).
func (a *Attached) Column(path string) (Column, error) {
	return columnOf(a.current(), "reader of "+a.Name, path)
}

// NewestRow is the newest row this reader has, and false while it has
// none or is not connected yet.
func (a *Attached) NewestRow() (Row, bool) { return newestRow(a.current()) }

// Cursor answers a cursor on this reader's memory: the rows that
// arrive from now on.
func (a *Attached) Cursor() *Cursor { return newCursor(a.current) }

// ArrowTable gives every row this reader has in memory as one table,
// no copy. columns names a subset. Release the table when done with it.
func (a *Attached) ArrowTable(columns ...string) (arrow.Table, error) {
	r := a.current()
	if r == nil {
		return emptyTable(nil, columns)
	}
	t, _, err := tableOf(r, r.OldestSeq(), r.NewestSeq()+1, columns)
	return t, err
}

// ArrowTableNewest gives the newest `rows` rows this reader has in
// memory as one table (fewer when it has fewer).
func (a *Attached) ArrowTableNewest(rows int64, columns ...string) (arrow.Table, error) {
	return newestOf(a.current(), rows, columns)
}

// MemoryState answers what this reader holds in memory right now.
func (a *Attached) MemoryState() MemoryState {
	return MemoryState{Rows: rowsIn(a.current()), Capacity: a.capacity}
}

// WaitConnected waits until the reader has its first connection, so
// that the stream's columns are known, or returns an error after
// timeout that says the reader's state.
func (a *Attached) WaitConnected(timeout time.Duration) error {
	_, err := a.waitRing(timeout)
	return err
}
