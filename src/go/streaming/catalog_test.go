package streaming

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func waitCatalog(t *testing.T, s *Stream, files int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := s.ArchiveState()
		if st.CatalogError != "" {
			t.Fatalf("catalog: %s", st.CatalogError)
		}
		if st.CatalogFiles == files {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the catalog lists %d file(s), want %d", st.CatalogFiles, files)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func checkpointsOf(t *testing.T, folder string) int {
	t.Helper()
	entries, err := os.ReadDir(folder)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".ckpt") {
			n++
		}
	}
	return n
}

// Every finished file is listed in catalog.sqlite beside the files. A
// file stays in the ticket until a published catalog lists it; then the
// ticket names the catalog's version and not the file. Its checkpoints
// stay for the keep time. The next start finds the finished files
// beside their checkpoints and its catalog pass has nothing to add.
func TestCatalog(t *testing.T) {
	dir := t.TempDir()
	tel, err := newProcess(processConfig{Store: dir})
	if err != nil {
		t.Fatal(err)
	}
	s, err := tel.CreateStream("t/cat", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400},
		Store: dir, Parquet: &ParquetOptions{}})
	if err != nil {
		t.Fatal(err)
	}
	folder := filepath.Join(dir, "streams", "t", "cat")
	published := filepath.Join(folder, catalogObject)
	for k := 0; k < 3; k++ {
		writeRows(t, s, k*10, 10)
		s.Cut()
		writeRows(t, s, k*10+10, 0) // a commit: the cut is done
		waitCatalog(t, s, k+1)
	}
	st, err := os.Stat(published)
	if err != nil {
		t.Fatal(err)
	}
	// the ticket: the catalog's version, and no finished file
	deadline := time.Now().Add(10 * time.Second)
	for {
		tk, err := ticketsOf(tel).get("t/cat")
		if err == nil && tk != nil && len(tk.Files) == 0 && tk.CatalogEtag != "" {
			st, _ = os.Stat(published)
			want := versionText(st)
			if tk.CatalogEtag != want {
				t.Fatalf("the ticket names catalog %q, the store has %q", tk.CatalogEtag, want)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the ticket still names files or no catalog: %+v", tk)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := checkpointsOf(t, folder); n < 3 {
		t.Fatalf("%d checkpoint version(s) beside the files, want one or more "+
			"for each of the 3: kept for the keep time", n)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	tel.Close()
	before, _ := os.Stat(published)

	tel, err = newProcess(processConfig{Store: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err = tel.CreateStream("t/cat", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400},
		Store: dir, Parquet: &ParquetOptions{}})
	if err != nil {
		t.Fatal(err)
	}
	waitCatalog(t, s, 3)
	after, _ := os.Stat(published)
	if versionText(before) != versionText(after) {
		t.Fatalf("a pass with nothing to add published the catalog again")
	}
	writeRows(t, s, 0, 7)
	if err := s.Close(); err != nil { // the last file is listed before the stream goes
		t.Fatal(err)
	}
	if st := s.ArchiveState(); st.CatalogFiles != 4 || st.CatalogError != "" {
		t.Fatalf("after close the catalog lists %d file(s), error %q; want 4",
			st.CatalogFiles, st.CatalogError)
	}
	if got := len(fileRows(t, folder)); got != 4 {
		t.Fatalf("%d files, want 4", got)
	}
}

func versionText(st os.FileInfo) string {
	return itoa(st.Size()) + "-" + itoa(st.ModTime().UnixNano())
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

// The dated copies of the catalog: one when the catalog first exists,
// another when it changed and the interval passed, none while it is
// the same; and none at all when the options say so.
func TestCatalogDatedCopies(t *testing.T) {
	copies := func(dir, name string) []string {
		found, _ := filepath.Glob(filepath.Join(dir, "streams", filepath.FromSlash(name), "catalog.2*.sqlite"))
		return found
	}
	dir := t.TempDir()
	tel, err := newProcess(processConfig{Store: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err := tel.CreateStream("t/copies", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400},
		Parquet: &ParquetOptions{CheckpointSeconds: 0.05, CatalogCopySeconds: 1.2}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	s.Cut()
	waitCatalog(t, s, 1)
	waitUntil(t, "the first dated copy", func() bool { return len(copies(dir, "t/copies")) == 1 })
	// the catalog changes, the interval has not passed: no copy yet
	writeRows(t, s, 10, 10)
	s.Cut()
	waitCatalog(t, s, 2)
	if n := len(copies(dir, "t/copies")); n != 1 {
		t.Fatalf("%d copies before the interval passed", n)
	}
	// the interval passes; a write pokes the catalog: the second copy
	time.Sleep(1300 * time.Millisecond)
	writeRows(t, s, 20, 10)
	waitUntil(t, "the second dated copy", func() bool { return len(copies(dir, "t/copies")) == 2 })
	// nothing changes: no third copy, however long
	time.Sleep(1300 * time.Millisecond)
	writeRows(t, s, 30, 10)
	time.Sleep(300 * time.Millisecond)
	if n := len(copies(dir, "t/copies")); n != 2 {
		t.Fatalf("%d copies of a catalog that changed twice", n)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	none, err := tel.CreateStream("t/nocopies", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400},
		Parquet: &ParquetOptions{CatalogCopySeconds: -1}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, none, 0, 10)
	none.Cut()
	waitCatalog(t, none, 1)
	if err := none.Close(); err != nil {
		t.Fatal(err)
	}
	if n := len(copies(dir, "t/nocopies")); n != 0 {
		t.Fatalf("%d copies with the copies off", n)
	}
}
