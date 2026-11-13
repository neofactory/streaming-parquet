package streaming

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/neofactory/streaming-parquet/arrow_ring/go/ring"
	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/pqc"
)

// Durable: the first seq not yet in the archive. A consumer that must
// acknowledge its input only once it is durable compares a row's seq
// against it; the checkpoint cadence moves it, with no call from the
// producer.
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
		StreamOptions{RingRows: 2000,
			Parquet: &ParquetOptions{BaseDir: t.TempDir(), CheckpointSeconds: 0.05}})
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
	// no call from here: the stream's goroutine writes the checkpoint
	waitUntil(t, "the checkpoint", func() bool { return s.Durable() >= 10 })
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
		Parquet: &ParquetOptions{BaseDir: t.TempDir(), RowGroupRows: 1000}})
	if err == nil || !strings.Contains(err.Error(), "less than two row groups") {
		t.Fatalf("CreateStream: %v, want the ring refused", err)
	}
	if _, err := tel.Attach("t/small", AttachOptions{RingRows: 1999,
		Parquet: &ParquetOptions{RowGroupRows: 1000}}); err == nil {
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
	s, err := tel.CreateStream("t/idle", testSchema(), StreamOptions{RingRows: 2000,
		Parquet: &ParquetOptions{BaseDir: dir, CheckpointSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	// no call from here: the stream's goroutine writes the checkpoint
	waitUntil(t, "the checkpoint", func() bool { return s.Durable() >= 10 })
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
		StreamOptions{RingRows: 400,
			Parquet: &ParquetOptions{BaseDir: dir, RowGroupRows: 200, Compression: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 12; k++ { // 12 row groups of 200 rows
		writeRows(t, s, k*200, 200)
		// a ring of two row groups: let each group be written
		want := int64(k+1) * 200
		waitUntil(t, "the row group", func() bool { return s.Durable() >= want })
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
	s, err := tel.CreateStream("t/stale", testSchema(), StreamOptions{RingRows: 2000})
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

// fileRows reads the n column of every finished parquet file in dir,
// keyed by file name.
func fileRows(t *testing.T, dir string) map[string][]int64 {
	t.Helper()
	out := map[string][]int64{}
	names, _ := filepath.Glob(filepath.Join(dir, "*.parquet"))
	for _, name := range names {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		r, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		var rows []int64
		for g := 0; g < r.NumRowGroups(); g++ {
			col, err := r.RowGroup(g).Column(0)
			if err != nil {
				t.Fatal(err)
			}
			cr := col.(*file.Int64ColumnChunkReader)
			buf := make([]int64, r.RowGroup(g).NumRows())
			got, _, err := cr.ReadBatch(int64(len(buf)), buf, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, buf[:got]...)
		}
		r.Close()
		out[filepath.Base(name)] = rows
	}
	return out
}

// StartParquet on a recording stream is a mark: the open file ends
// there with every earlier row in it, and the new file holds the rows
// committed after.
func TestStartParquetIsAMark(t *testing.T) {
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	dir := t.TempDir()
	s, err := tel.CreateStream("t/mark", testSchema(), StreamOptions{RingRows: 400,
		Parquet: &ParquetOptions{BaseDir: dir}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 25)
	if err := s.StartParquet("B", nil); err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 25, 15)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got := fileRows(t, filepath.Join(dir, "streams", "t", "mark"))
	if len(got) != 2 {
		t.Fatalf("files: %v, want two", got)
	}
	for name, rows := range got {
		lo, hi := int64(0), int64(25)
		if strings.HasSuffix(name, "-B.parquet") {
			lo, hi = 25, 40
		}
		if int64(len(rows)) != hi-lo || rows[0] != lo || rows[len(rows)-1] != hi-1 {
			t.Fatalf("%s holds %v, want [%d, %d)", name, rows, lo, hi)
		}
	}
}

// A stream created with no parquet options writes no file until a
// StartParquet gives them, and then only the rows committed after.
func TestStartParquetOnAStreamCreatedWithNone(t *testing.T) {
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	dir := t.TempDir()
	s, err := tel.CreateStream("t/late", testSchema(), StreamOptions{RingRows: 400})
	if err != nil {
		t.Fatal(err)
	}
	if s.ParquetOptions() != nil {
		t.Fatal("a stream created with none has parquet options")
	}
	writeRows(t, s, 0, 25)
	if err := s.StartParquet("X", nil); err == nil ||
		!strings.Contains(err.Error(), "needs ParquetOptions") {
		t.Fatalf("StartParquet with no options: %v, want it refused", err)
	}
	if err := s.StartParquet("X", &ParquetOptions{BaseDir: dir, RowGroupRows: 300}); err == nil ||
		!strings.Contains(err.Error(), "less than two row groups") {
		t.Fatalf("a row group of 300 in a ring of 400: %v, want it refused", err)
	}
	if err := s.StartParquet("DBG", &ParquetOptions{BaseDir: dir}); err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 25, 15)
	if err := s.StopParquet(); err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 40, 5) // off again: in no file
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got := fileRows(t, filepath.Join(dir, "streams", "t", "late"))
	if len(got) != 1 {
		t.Fatalf("files: %v, want one", got)
	}
	for name, rows := range got {
		if len(rows) != 15 || rows[0] != 25 || rows[14] != 39 {
			t.Fatalf("%s holds %v, want [25, 40)", name, rows)
		}
	}
}

// A write fault shows in ArchiveState, not as the result of a call, and
// clears when a write works again.
func TestArchiveStateShowsAWriteFault(t *testing.T) {
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	dir := t.TempDir()
	s, err := tel.CreateStream("t/fault", testSchema(), StreamOptions{RingRows: 2000,
		Parquet: &ParquetOptions{BaseDir: dir, CheckpointSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	if st := s.ArchiveState(); st.Failing || st.Location == "" {
		t.Fatalf("a new stream: %+v", st)
	}
	folder := filepath.Join(dir, "streams", "t", "fault")
	if err := os.Chmod(folder, 0o500); err != nil { // no new file can be made
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	waitUntil(t, "the fault", func() bool { return s.ArchiveState().Failing })
	if st := s.ArchiveState(); st.Error == "" || s.Durable() != 0 {
		t.Fatalf("failing: %+v, durable %d", st, s.Durable())
	}
	if err := os.Chmod(folder, 0o700); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the recovery", func() bool {
		return !s.ArchiveState().Failing && s.Durable() == 10
	})
}

// An internal stream has no ticket and is not served; Publish and
// Unpublish change a running stream.
func TestPublishedOrInternal(t *testing.T) {
	root := t.TempDir()
	tel, err := Init(Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err := tel.CreateStream("t/int", testSchema(),
		StreamOptions{RingRows: 400, Internal: true})
	if err != nil {
		t.Fatal(err)
	}
	listed := func() bool {
		names, _ := tel.ListStreams()
		for _, n := range names {
			if n == "t/int" {
				return true
			}
		}
		return false
	}
	served := func() bool {
		tel.flight.mu.Lock()
		defer tel.flight.mu.Unlock()
		return tel.flight.streams["t/int"] != nil
	}
	writeRows(t, s, 0, 10)
	if s.Published() || listed() || served() {
		t.Fatalf("an internal stream: published %v, ticket %v, served %v",
			s.Published(), listed(), served())
	}
	s.Publish()
	waitUntil(t, "the ticket", listed)
	if !s.Published() || !served() {
		t.Fatal("after Publish the stream is not served")
	}
	a, err := tel.Attach("t/int", AttachOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	waitUntil(t, "a reader", func() bool { return a.State() == "streaming" })
	s.Unpublish()
	if s.Published() || listed() || served() {
		t.Fatalf("after Unpublish: published %v, ticket %v, served %v",
			s.Published(), listed(), served())
	}
	writeRows(t, s, 10, 5) // the stream itself goes on
	if got := s.Ring().NewestSeq(); got != 14 {
		t.Fatalf("newest seq %d, want 14", got)
	}
}

// Commit pins the rows of a write that is due. A writer that comes a
// whole ring around to rows not yet written waits at that pin, and no
// row is lost; when the store works again every row is written.
func TestWriterWaitsForRowsNotYetWritten(t *testing.T) {
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	dir := t.TempDir()
	s, err := tel.CreateStream("t/wait", testSchema(), StreamOptions{RingRows: 400,
		Parquet: &ParquetOptions{BaseDir: dir, RowGroupRows: 200}})
	if err != nil {
		t.Fatal(err)
	}
	folder := filepath.Join(dir, "streams", "t", "wait")
	if err := os.Chmod(folder, 0o500); err != nil { // the store fails
		t.Fatal(err)
	}
	defer os.Chmod(folder, 0o700)
	writeRows(t, s, 0, 200)   // one row group due: pinned, its write fails
	writeRows(t, s, 200, 200) // a second: the ring is full of unwritten rows
	waitUntil(t, "the fault", func() bool { return s.ArchiveState().Failing })
	_, err = s.Ring().RowBegin(50 * time.Millisecond)
	if !ring.IsPinStalled(err) {
		t.Fatalf("a row into a ring of unwritten rows: %v, want the writer to wait at the pin", err)
	}
	if st := s.ArchiveState(); st.LostRows != 0 || s.Durable() != 0 {
		t.Fatalf("while the store fails: %+v, durable %d", st, s.Durable())
	}
	if err := os.Chmod(folder, 0o700); err != nil { // the store works again
		t.Fatal(err)
	}
	waitUntil(t, "every row written", func() bool { return s.Durable() == 400 })
	writeRows(t, s, 400, 10) // and the writer goes on
	if st := s.ArchiveState(); st.Failing || st.LostRows != 0 {
		t.Fatalf("after the store recovered: %+v", st)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, got := range fileRows(t, folder) {
		rows += len(got)
	}
	if rows != 410 {
		t.Fatalf("the files hold %d rows, want 410", rows)
	}
}
