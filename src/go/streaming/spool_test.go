package streaming

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/neofactory/streaming-parquet/src/go/pqc"
)

// fakeBucket is a directory that plays a bucket: every call on it fails
// while down is set, as a bucket that cannot be reached does.
type fakeBucket struct {
	dir  *pqc.DirSink
	down *atomic.Bool
}

var errAway = errors.New("the bucket cannot be reached")

func (b *fakeBucket) Put(name string, data []byte) error {
	if b.down.Load() {
		return errAway
	}
	return b.dir.Put(name, data)
}
func (b *fakeBucket) Get(name string, offset, length int64) ([]byte, error) {
	if b.down.Load() {
		return nil, errAway
	}
	return b.dir.Get(name, offset, length)
}
func (b *fakeBucket) Size(name string) (int64, error) {
	if b.down.Load() {
		return 0, errAway
	}
	return b.dir.Size(name)
}
func (b *fakeBucket) Exists(name string) bool { return !b.down.Load() && b.dir.Exists(name) }
func (b *fakeBucket) Delete(name string) error {
	if b.down.Load() {
		return errAway
	}
	return b.dir.Delete(name)
}
func (b *fakeBucket) List(prefix string) ([]string, error) {
	if b.down.Load() {
		return nil, errAway
	}
	return b.dir.List(prefix)
}
func (b *fakeBucket) Assemble(dest string, sources []pqc.Source) error {
	if b.down.Load() {
		return errAway
	}
	return b.dir.Assemble(dest, sources)
}
func (b *fakeBucket) PartMin() uint64 { return 0 }
func (b *fakeBucket) Sub(prefix string) pqc.Sink {
	return &fakeBucket{dir: b.dir.Sub(prefix).(*pqc.DirSink), down: b.down}
}
func (b *fakeBucket) URI() string {
	return "s3://fake-bucket/" + strings.TrimPrefix(b.dir.URI(), "file://")
}
func (b *fakeBucket) GetVersion(name string) ([]byte, string, error) {
	if b.down.Load() {
		return nil, "", errAway
	}
	return b.dir.GetVersion(name)
}
func (b *fakeBucket) PutIfVersion(name string, data []byte, version string) (string, error) {
	if b.down.Load() {
		return "", errAway
	}
	return b.dir.PutIfVersion(name, data, version)
}

const fakeStore = "s3://fake-bucket"

// withFakeBucket makes a process whose store "s3://fake-bucket" is a
// directory: its files through the fake bucket, its tickets as files.
func withFakeBucket(t *testing.T) (*process, *fakeBucket, string) {
	t.Helper()
	tel, err := newProcess(processConfig{})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ds, _ := pqc.NewDirSink(dir)
	bucket := &fakeBucket{dir: ds, down: &atomic.Bool{}}
	tel.sinks[fakeStore] = bucket
	tel.stores[fakeStore] = &fileStore{base: filepath.Join(dir, "tickets")}
	return tel, bucket, dir
}

func parquetFiles(t *testing.T, folder string) []string {
	t.Helper()
	found, _ := filepath.Glob(filepath.Join(folder, "*.parquet"))
	sort.Strings(found)
	return found
}

// A bucket store is written through the spool. The upload copies it;
// while the bucket is away the writer goes on, a file ends locally and
// waits, the ticket says only what the bucket holds; when the bucket is
// back the upload catches up, and both copies end with the same files.
func TestSpoolAndUpload(t *testing.T) {
	tel, bucket, bucketDir := withFakeBucket(t)
	defer tel.Close()
	spool := t.TempDir()
	s, err := tel.CreateStream("t/spool", testSchema(), StreamOptions{Store: fakeStore, Spool: spool,
		Memory:  &MemoryOptions{Rows: 400},
		Parquet: &ParquetOptions{CheckpointSeconds: 0.05, UploadSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	stored := func() int64 {
		tk, err := ticketsOf2(tel).get("t/spool")
		if err != nil || tk == nil || tk.StoredRow == nil {
			return -2
		}
		return *tk.StoredRow
	}
	if st := s.UploadState(); st == nil || st.Failing {
		t.Fatalf("upload state at start: %+v", st)
	}
	writeRows(t, s, 0, 10)
	waitUntil(t, "row 9 in the spool", func() bool { return s.DurableRowNumber() == 9 })
	waitUntil(t, "row 9 in the bucket, by the ticket", func() bool { return stored() == 9 })
	if tk, _ := ticketsOf2(tel).get("t/spool"); tk.UploadSeconds != 0.05 ||
		len(tk.Files) != 1 || !strings.HasPrefix(tk.Files[0].Checkpoint, "s3://fake-bucket/") {
		t.Fatalf("the ticket of an uploaded stream: %+v", tk)
	}
	if st := s.ArchiveState(); !strings.HasPrefix(st.Location, "file://"+spool) &&
		!strings.HasPrefix(st.Location, spool) {
		t.Fatalf("the first copy is the spool; state says %q", st.Location)
	}

	// the bucket goes away
	bucket.down.Store(true)
	writeRows(t, s, 10, 10)
	waitUntil(t, "row 19 in the spool", func() bool { return s.DurableRowNumber() == 19 })
	waitUntil(t, "the upload says it fails", func() bool { return s.UploadState().Failing })
	if st := s.ArchiveState(); st.Failing {
		t.Fatalf("the files fail while only the bucket is away: %+v", st)
	}
	bucket.down.Store(false) // to read the ticket: its store is the same directory
	if got := stored(); got != 9 {
		bucket.down.Store(true)
		t.Fatalf("the ticket's stored row while the bucket was away: %d, want 9", got)
	}
	bucket.down.Store(true)
	s.Cut() // the file ends locally, and waits for the bucket
	writeRows(t, s, 20, 5)
	waitUntil(t, "row 24 in the spool", func() bool { return s.DurableRowNumber() == 24 })
	waitUntil(t, "one file pending", func() bool { return len(s.UploadState().Pending) == 1 })
	if st := s.UploadState(); !st.Failing || st.Error == "" {
		t.Fatalf("upload state during the outage: %+v", st)
	}

	// the bucket is back
	bucket.down.Store(false)
	waitUntil(t, "the upload recovered and nothing is pending", func() bool {
		st := s.UploadState()
		return !st.Failing && len(st.Pending) == 0
	})
	waitUntil(t, "row 24 in the bucket, by the ticket", func() bool { return stored() == 24 })
	waitCatalog(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	local := parquetFiles(t, filepath.Join(spool, "streams", "t", "spool"))
	far := parquetFiles(t, filepath.Join(bucketDir, "streams", "t", "spool"))
	if len(local) != 2 || len(far) != 2 {
		t.Fatalf("finished files: %v in the spool, %v in the bucket", local, far)
	}
	for i := range local {
		a, _ := os.ReadFile(local[i])
		b, _ := os.ReadFile(far[i])
		if filepath.Base(local[i]) != filepath.Base(far[i]) || len(a) == 0 || !bytes.Equal(a, b) {
			t.Fatalf("%s and %s differ", local[i], far[i])
		}
	}
	if _, err := os.Stat(filepath.Join(bucketDir, "streams", "t", "spool", catalogObject)); err != nil {
		t.Fatalf("the catalog is not in the bucket: %v", err)
	}
	if _, err := os.Stat(filepath.Join(spool, "streams", "t", "spool", catalogObject)); err == nil {
		t.Fatal("a catalog was written in the spool")
	}
}

// ticketsOf2 is the ticket store of the fake bucket.
func ticketsOf2(t *process) ticketStore { return t.stores[fakeStore] }

type spoolRow struct {
	N int64   `parquet:"n"`
	X float64 `parquet:"x"`
}

// leftover writes a file of 500 rows into sink as an earlier run left
// it: versions and objects, not assembled.
func leftover(t *testing.T, sink pqc.Sink, name string) {
	t.Helper()
	w, err := pqc.NewWriter(name, sink, 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]spoolRow, 500)
	for i := range rows {
		rows[i] = spoolRow{int64(i), float64(i) * 0.5}
	}
	if _, err := pqc.WriteRows(w, rows); err != nil {
		t.Fatal(err)
	}
	w.Free()
}

// THE SWEEP AT START, with a spool. A file an earlier run left in the
// spool is sent and ended by the first passes. A file whose local copy
// was assembled, and whose bucket copy was not, is ended from the
// bucket's own objects.
func TestSpoolSweepAtStart(t *testing.T) {
	tel, _, bucketDir := withFakeBucket(t)
	defer tel.Close()
	spool := t.TempDir()
	local, _ := pqc.NewDirSink(filepath.Join(spool, "streams", "t", "left"))
	far, _ := pqc.NewDirSink(filepath.Join(bucketDir, "streams", "t", "left"))
	// one: in the spool only, not assembled
	leftover(t, local, "20260101-000000.parquet")
	// two: sent to the bucket, assembled in the spool, the crash before
	// the bucket's assembly
	leftover(t, local, "20260101-000001.parquet")
	names, _ := local.List("20260101-000001.parquet.")
	for _, n := range names {
		data, _ := local.Get(n, 0, -1)
		if err := far.Put(n, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pqc.Finalize("20260101-000001.parquet", local); err != nil {
		t.Fatal(err)
	}

	s, err := tel.CreateStream("t/left", testSchema(), StreamOptions{Store: fakeStore, Spool: spool,
		Memory:  &MemoryOptions{Rows: 400},
		Parquet: &ParquetOptions{UploadSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "both leftovers finished in the bucket", func() bool {
		return far.Exists("20260101-000000.parquet") && far.Exists("20260101-000001.parquet") &&
			len(s.UploadState().Pending) == 0
	})
	for _, name := range []string{"20260101-000000.parquet", "20260101-000001.parquet"} {
		a, _ := local.Get(name, 0, -1)
		b, _ := far.Get(name, 0, -1)
		if len(a) == 0 || !bytes.Equal(a, b) {
			t.Fatalf("%s: %d bytes in the spool, %d in the bucket", name, len(a), len(b))
		}
	}
	left, _ := local.List("")
	for _, n := range left {
		if strings.Contains(n, ".ckpt") {
			t.Fatalf("the spool still has %s", n)
		}
	}
	waitCatalog(t, s, 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// A stream with no store writes its files to the spool alone: no
// upload, no ticket, no catalog.
func TestNoStoreWritesToTheSpool(t *testing.T) {
	tel, err := newProcess(processConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	spool := t.TempDir()
	s, err := tel.CreateStream("t/alone", testSchema(), StreamOptions{Spool: spool,
		Memory: &MemoryOptions{Rows: 400}, Parquet: &ParquetOptions{}})
	if err != nil {
		t.Fatal(err)
	}
	if s.UploadState() != nil {
		t.Fatal("a stream with no store has an upload")
	}
	writeRows(t, s, 0, 10)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	folder := filepath.Join(spool, "streams", "t", "alone")
	if got := parquetFiles(t, folder); len(got) != 1 {
		t.Fatalf("the spool's files: %v", got)
	}
	if _, err := os.Stat(filepath.Join(folder, catalogObject)); err == nil {
		t.Fatal("a stream with no store wrote a catalog")
	}
	if _, err := os.Stat(filepath.Join(spool, "tickets")); err == nil {
		t.Fatal("a stream with no store wrote a ticket")
	}
}
