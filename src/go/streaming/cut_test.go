package streaming

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Cut ends the open file at the last committed row and the next row
// begins a new one: every row is in one file, once. CutLabel names the
// next file.
func TestCut(t *testing.T) {
	tel, err := newProcess(processConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	dir := t.TempDir()
	s, err := tel.CreateStream("t/cut", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400},
		Parquet: &ParquetOptions{BaseDir: dir}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	s.Cut()
	s.Cut() // nothing new: no empty file
	writeRows(t, s, 10, 10)
	s.CutLabel("P")
	writeRows(t, s, 20, 10)
	s.CutLabel("")
	writeRows(t, s, 30, 5)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got := fileRows(t, filepath.Join(dir, "streams", "t", "cut"))
	if len(got) != 4 {
		t.Fatalf("files: %v, want four", got)
	}
	var firsts []int
	for name, rows := range got {
		for i, r := range rows {
			if r != rows[0]+int64(i) {
				t.Fatalf("%s: rows %v are not in order", name, rows)
			}
		}
		labeled := strings.Contains(name, "-P")
		if labeled != (rows[0] == 20) || (labeled && len(rows) != 10) {
			t.Fatalf("%s holds %v: the label P belongs to rows 20 to 29", name, rows)
		}
		firsts = append(firsts, int(rows[0])*100+len(rows))
	}
	sort.Ints(firsts)
	want := []int{0*100 + 10, 10*100 + 10, 20*100 + 10, 30*100 + 5}
	for i := range want {
		if firsts[i] != want[i] {
			t.Fatalf("files hold %v (first row * 100 + rows), want %v", firsts, want)
		}
	}
	if st := s.ArchiveState(); st.Failing || st.LostRows != 0 {
		t.Fatalf("archive state after the cuts: %+v", st)
	}
}

// Cut on a stream that writes no files does nothing.
func TestCutWhenNotRecording(t *testing.T) {
	tel, err := newProcess(processConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err := tel.CreateStream("t/nocut", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	s.Cut()
	s.CutLabel("X")
	if st := s.ArchiveState(); st.Failing || st.File != "" {
		t.Fatalf("a cut on a stream with no files: %+v", st)
	}
}
