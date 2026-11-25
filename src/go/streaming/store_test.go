package streaming

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A Go writer's ticket says what the store holds: the newest stored
// row, and each unfinished file with its newest checkpoint version, its
// first row and its row counts. A finished file leaves the ticket; the
// stored row stays.
func TestTicketSaysWhatTheStoreHolds(t *testing.T) {
	root := t.TempDir()
	tel, err := newProcess(processConfig{Store: root})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err := tel.CreateStream("t/store", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400},
		Store: root, Parquet: &ParquetOptions{RowGroupRows: 100, CheckpointSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	ticket := func() *Ticket {
		tk, err := ticketsOf(tel).get("t/store")
		if err != nil {
			t.Fatal(err)
		}
		return tk
	}
	stored := func(row int64) func() bool {
		return func() bool {
			tk := ticket()
			return tk != nil && tk.StoredRow != nil && *tk.StoredRow == row
		}
	}
	// before any row: the store holds nothing of this run
	waitUntil(t, "the first ticket", func() bool { return ticket() != nil })
	if tk := ticket(); tk.StoredRow == nil || *tk.StoredRow != -1 || len(tk.Files) != 0 ||
		tk.KeepSeconds != 600 || tk.StoredAt != 0 {
		t.Fatalf("the ticket before any row: %+v", tk)
	}
	// 130 rows: one row group of 100 final, 30 in the tail checkpoint
	writeRows(t, s, 0, 100)
	writeRows(t, s, 100, 30)
	waitUntil(t, "row 129 stored", func() bool {
		s.Commit() // the checkpoint is due by time
		return stored(129)()
	})
	tk := ticket()
	if len(tk.Files) != 1 || tk.StoredAt == 0 {
		t.Fatalf("the ticket with one open file: %+v", tk)
	}
	f := tk.Files[0]
	dir := filepath.Join(root, "streams", "t", "store") + "/"
	if !strings.HasPrefix(f.Checkpoint, dir) || !strings.HasSuffix(f.Checkpoint, ".ckpt") ||
		f.FirstRow == nil || *f.FirstRow != 0 || f.FinalRows == nil || *f.FinalRows != 100 ||
		f.Rows == nil || *f.Rows != 130 {
		t.Fatalf("the open file in the ticket: %+v (first %v final %v rows %v)", f,
			f.FirstRow, f.FinalRows, f.Rows)
	}
	// a cut finishes the file: it leaves the ticket, the stored row stays
	s.Cut()
	waitUntil(t, "the finished file leaves the ticket", func() bool {
		return len(ticket().Files) == 0
	})
	if !stored(129)() {
		t.Fatalf("the stored row after the cut: %+v", ticket().StoredRow)
	}
	// the next file begins at row 130
	writeRows(t, s, 130, 20)
	waitUntil(t, "row 149 stored", func() bool {
		s.Commit()
		return stored(149)()
	})
	tk = ticket()
	if len(tk.Files) != 1 || *tk.Files[0].FirstRow != 130 || *tk.Files[0].Rows != 20 ||
		*tk.Files[0].FinalRows != 0 {
		t.Fatalf("the second file in the ticket: %+v", tk.Files)
	}
}

// ticketsOf is where the tickets of a test process's store are.
func ticketsOf(t *process) ticketStore {
	_, tickets, _ := t.resolve("")
	return tickets
}

// Where the files go is the store, and nowhere else: a stream with no
// store is refused parquet, a store that is a prefix inside a bucket is
// refused, and a reader that writes files needs a store of its own.
func TestFilesGoToTheStore(t *testing.T) {
	tel, err := newProcess(processConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	refused := func(what string, err error, part string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), part) {
			t.Fatalf("%s: %v, want it refused with %q", what, err, part)
		}
	}
	_, err = tel.CreateStream("t/nostore", testSchema(), StreamOptions{Parquet: &ParquetOptions{}})
	refused("parquet with no store", err, "the stream has no store")
	_, err = tel.CreateStream("t/prefix", testSchema(), StreamOptions{Store: "s3://bucket/some/prefix"})
	refused("a prefix inside a bucket", err, "a store is a whole bucket")

	store := t.TempDir()
	s, err := tel.CreateStream("t/where", testSchema(), StreamOptions{Store: store,
		Memory: &MemoryOptions{Rows: 400}, Parquet: &ParquetOptions{}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(store, "streams", "t", "where", "*.parquet"))
	if len(files) != 1 {
		t.Fatalf("the files under the store's streams/t/where/: %v", files)
	}

	_, err = tel.Connect("t/where", ConnectOptions{Store: store, Parquet: &ParquetOptions{}})
	refused("a reader with parquet and no store for it", err, "needs ConnectOptions.ParquetStore")
	_, err = tel.Connect("t/where", ConnectOptions{Store: store, Parquet: &ParquetOptions{},
		ParquetStore: "file://" + store + "/"})
	refused("a reader whose files would go to the writer's folder", err, "a store of its own")
}

// A reader's own files: the rows it receives, with the writer's row
// numbers, written from a StartParquet on; a cut, the state and the
// durable row number as a stream has them; and one reader for a folder.
func TestAReaderWritesFilesOfItsOwn(t *testing.T) {
	tel, err := newProcess(processConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	store, copies := t.TempDir(), t.TempDir()
	s, err := tel.CreateStream("t/copy", testSchema(), StreamOptions{Store: store,
		Memory: &MemoryOptions{Rows: 400}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := tel.Connect("t/copy", ConnectOptions{Store: store, ParquetStore: copies,
		Memory: &MemoryOptions{Rows: 400}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.WaitConnected(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	received := func(n int64) func() bool {
		return func() bool { row, ok := a.NewestRow(); return ok && row.RowNumber() >= n }
	}
	writeRows(t, s, 0, 10)
	waitUntil(t, "10 rows received", received(9))
	if st := a.ArchiveState(); st.File != "" || a.DurableRowNumber() != -1 {
		t.Fatalf("before StartParquet: %+v, durable %d", st, a.DurableRowNumber())
	}
	// the mark: rows 10 and later, with the label, by the default options
	if err := a.StartParquet("job", &ParquetOptions{CheckpointSeconds: 0.05}); err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 10, 10)
	waitUntil(t, "row 19 stored", func() bool { return a.DurableRowNumber() == 19 })
	if st := a.ArchiveState(); st.Failing || st.File == "" || !strings.Contains(st.Location, copies) {
		t.Fatalf("while it writes: %+v", st)
	}
	if err := a.CutLabel("next"); err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 20, 5)
	waitUntil(t, "row 24 stored", func() bool { return a.DurableRowNumber() == 24 })

	// a second reader of the same folder is refused, and says who has it
	b, err := tel.Connect("t/copy", ConnectOptions{Store: store, ParquetStore: copies,
		Memory: &MemoryOptions{Rows: 400}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.WaitConnected(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := b.StartParquet("b", nil); err == nil || !strings.Contains(err.Error(), "writer.lock") {
		t.Fatalf("a second reader of the folder: %v, want it refused", err)
	}
	if st := b.ArchiveState(); !st.Failing {
		t.Fatalf("the refused reader's state: %+v", st)
	}
	b.Close()

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(copies, "streams", "t", "copy", "*.parquet"))
	if len(files) != 2 || !strings.Contains(filepath.Base(files[0]), "job") ||
		!strings.Contains(filepath.Base(files[1]), "next") {
		t.Fatalf("the reader's files: %v", files)
	}
	if _, err := os.Stat(filepath.Join(copies, "tickets")); err == nil {
		t.Fatal("a reader's files got a ticket")
	}
	plain, err := tel.Connect("t/copy", ConnectOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.StartParquet("x", nil); err == nil ||
		!strings.Contains(err.Error(), "no store for files") {
		t.Fatalf("a reader with no ParquetStore: %v, want it refused", err)
	}
}
