package streaming

import (
	"path/filepath"
	"strings"
	"testing"
)

// A Go writer's ticket says what the store holds: the newest stored
// row, and each unfinished file with its newest checkpoint version, its
// first row and its row counts. A finished file leaves the ticket; the
// stored row stays.
func TestTicketSaysWhatTheStoreHolds(t *testing.T) {
	root := t.TempDir()
	tel, err := Init(Config{Store: root})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err := tel.CreateStream("t/store", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400},
		Parquet: &ParquetOptions{BaseDir: root, RowGroupRows: 100, CheckpointSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	ticket := func() *Ticket {
		tk, err := tel.tickets.get("t/store")
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
