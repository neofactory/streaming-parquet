package streaming

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/pqc"
)

// Durable: the first seq not yet in the archive. A consumer that must
// acknowledge its input only once it is durable compares a row's seq
// against it after Publish; the checkpoint cadence moves it.
func TestDurableFollowsTheCheckpoint(t *testing.T) {
	tel, err := Init(Config{FlightAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	if tel.FlightPort() == 0 {
		t.Fatal("a configured Flight address did not bind")
	}
	s, err := tel.CreateStream("t/durable", testSchema(),
		StreamOptions{RingRows: 2000, Parquet: true,
			Archive: ParquetOptions{BaseDir: t.TempDir(), CheckpointSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Durable(); got != 0 {
		t.Fatalf("before any row: durable %d, want 0", got)
	}
	writeRows(t, s, 0, 10)
	// Not yet: the checkpoint cadence has not elapsed.
	if got := s.Durable(); got != 0 {
		t.Fatalf("right after commit: durable %d, want 0", got)
	}
	deadline := time.Now().Add(3 * time.Second)
	for s.Durable() < 10 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if _, err := s.Publish(); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.Durable(); got != 10 {
		t.Fatalf("after the checkpoint: durable %d, want 10", got)
	}
	writeRows(t, s, 10, 5)
	if err := s.StopParquet(); err != nil {
		t.Fatal(err)
	}
	if got := s.Durable(); got != 15 {
		t.Fatalf("after stop: durable %d, want 15", got)
	}
}

// The ring holds two row groups: either size follows from the other,
// DuckDB's row group when neither is given, and a ring under two row
// groups is refused by CreateStream and Attach.
func TestRingHoldsTwoRowGroups(t *testing.T) {
	for _, c := range []struct{ ring, group, wantRing, wantGroup int64 }{
		{0, 0, 245_760, 122_880},
		{0, 500, 1000, 500},
		{1000, 0, 1000, 500},
		{3000, 1000, 3000, 1000},
	} {
		ring, group, err := ringSizes("t", c.ring, c.group)
		if err != nil || ring != c.wantRing || group != c.wantGroup {
			t.Fatalf("ringSizes(%d, %d) = %d, %d, %v; want %d, %d",
				c.ring, c.group, ring, group, err, c.wantRing, c.wantGroup)
		}
	}
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	_, err = tel.CreateStream("t/small", testSchema(), StreamOptions{RingRows: 1999,
		Archive: ParquetOptions{BaseDir: t.TempDir(), RowGroupRows: 1000}})
	if err == nil || !strings.Contains(err.Error(), "less than two row groups") {
		t.Fatalf("CreateStream: %v, want the ring refused", err)
	}
	if _, err := tel.Attach("t/small", AttachOptions{RingRows: 1999,
		Archive: ParquetOptions{RowGroupRows: 1000}}); err == nil {
		t.Fatal("Attach accepted a ring under two row groups")
	}
}

// A checkpoint publishes new rows only: with nothing committed since the
// last one, the next interval writes no checkpoint.
func TestCheckpointSkipsWhenNothingNew(t *testing.T) {
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	dir := t.TempDir()
	s, err := tel.CreateStream("t/idle", testSchema(), StreamOptions{RingRows: 2000, Parquet: true,
		Archive: ParquetOptions{BaseDir: dir, CheckpointSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	deadline := time.Now().Add(3 * time.Second)
	for s.Durable() < 10 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if _, err := s.Publish(); err != nil {
			t.Fatal(err)
		}
	}
	ckpt := func() time.Time {
		m, err := filepath.Glob(filepath.Join(dir, "streams", "t", "idle", "*.ckpt"))
		if err != nil || len(m) != 1 {
			t.Fatalf("checkpoint files %v, %v", m, err)
		}
		fi, err := os.Stat(m[0])
		if err != nil {
			t.Fatal(err)
		}
		return fi.ModTime()
	}
	before := ckpt()
	for i := 0; i < 10; i++ { // ten intervals with nothing new
		time.Sleep(60 * time.Millisecond)
		if _, err := s.Publish(); err != nil {
			t.Fatal(err)
		}
	}
	if after := ckpt(); !after.Equal(before) {
		t.Fatalf("an idle stream rewrote its checkpoint (%v -> %v)", before, after)
	}
}

// A file is cut when its row groups reach FileCutBytes: the next row
// group begins a new file, and no row is lost at the cut.
func TestFileIsCutByBytes(t *testing.T) {
	was := FileCutBytes
	FileCutBytes = 4000
	defer func() { FileCutBytes = was }()
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	dir := t.TempDir()
	s, err := tel.CreateStream("t/bytecut", testSchema(),
		StreamOptions{RingRows: 400, Parquet: true,
			Archive: ParquetOptions{BaseDir: dir, RowGroupRows: 200, Compression: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 12; k++ { // 12 row groups of 200 rows
		writeRows(t, s, k*200, 200)
		if _, err := s.Publish(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.StopParquet(); err != nil {
		t.Fatal(err)
	}
	var files []string
	rows := 0
	err = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && strings.HasSuffix(p, ".parquet") && !info.IsDir() {
			files = append(files, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 3 {
		t.Fatalf("12 row groups with a cut at 4,000 bytes made %d file(s): %v", len(files), files)
	}
	for _, f := range files {
		n, err := parquetRows(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		rows += n
	}
	if rows != 2400 {
		t.Fatalf("the files hold %d rows, want 2400", rows)
	}
}

// parquetRows is the row count of the parquet file at path.
func parquetRows(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	r, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	defer r.Close()
	return int(r.NumRows()), nil
}

// A writer's ticket says when a reader may call it dead: three beats,
// at least 30 seconds, by the one rule in the core. A ticket that does
// not say gets the rule for a writer with no beat.
func TestTicketSaysWhenItIsStale(t *testing.T) {
	root := t.TempDir()
	tel, err := Init(Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err := tel.CreateStream("t/stale", testSchema(), StreamOptions{RingRows: 2000,
		Archive: ParquetOptions{BaseDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var tk *Ticket
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if tk, err = tel.tickets.get("t/stale"); err != nil {
			t.Fatal(err)
		}
		if tk != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if tk == nil {
		t.Fatal("no ticket after 5 s")
	}
	if want := 3 * heartbeatInterval.Seconds(); tk.StaleAfter != want || tk.staleAfter() != want {
		t.Fatalf("stale_after: the ticket says %v, want three beats = %v", tk.StaleAfter, want)
	}
	if got := (&Ticket{Name: "t/old"}).staleAfter(); got != 30 {
		t.Fatalf("a ticket that names no stale_after: %v s, want 30", got)
	}
	if got := pqc.DeadAfter(5 * time.Second); got != 30*time.Second {
		t.Fatalf("a 5 s beat: dead after %v, want 30 s", got)
	}
}
