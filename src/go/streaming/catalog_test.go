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
		Parquet: &ParquetOptions{BaseDir: dir}})
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
		Parquet: &ParquetOptions{BaseDir: dir}})
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
