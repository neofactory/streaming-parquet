package pqc

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type Row struct {
	N int64   `parquet:"n"`
	X float64 `parquet:"x"`
	S string  `parquet:"s"`
}

func makeRows(start, count int) []Row {
	rows := make([]Row, count)
	for i := range rows {
		n := start + i
		rows[i] = Row{N: int64(n), X: float64(n) * 0.5, S: "row-" + itoa(n)}
	}
	return rows
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func checkExact(t *testing.T, rows []Row, want int) {
	t.Helper()
	if len(rows) != want {
		t.Fatalf("rows: got %d want %d", len(rows), want)
	}
	for i, r := range rows {
		if r.N != int64(i) || r.S != "row-"+itoa(i) {
			t.Fatalf("row %d wrong: %+v", i, r)
		}
	}
}

func TestLifecycle(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewDirSink(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter("a.parquet", sink, 64*1024, true)
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 8; k++ {
		if _, err := WriteRows(w, makeRows(k*500, 500)); err != nil {
			t.Fatal(err)
		}
	}
	if w.RowsFinal() != 4000 {
		t.Fatalf("RowsFinal %d", w.RowsFinal())
	}
	// live read mid-write, from the checkpoint alone
	r, err := OpenReader(w.CkptName(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if r.Watermark != 4000 {
		t.Fatalf("watermark %d", r.Watermark)
	}
	live, err := r.Compose()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := ReadRows[Row](live)
	if err != nil {
		t.Fatal(err)
	}
	checkExact(t, rows, 4000)
	if err := CloseTyped[Row](w); err != nil {
		t.Fatal(err)
	}
	names, _ := sink.List("")
	if len(names) != 1 || names[0] != "a.parquet" {
		t.Fatalf("residue: %v", names)
	}
	final, _ := os.ReadFile(filepath.Join(dir, "a.parquet"))
	rows, err = ReadRows[Row](final)
	if err != nil {
		t.Fatal(err)
	}
	checkExact(t, rows, 4000)
}

func TestProvisionalTailAndCrash(t *testing.T) {
	dir := t.TempDir()
	sink, _ := NewDirSink(dir)
	w, err := NewWriter("b.parquet", sink, 5<<20, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteRows(w, makeRows(0, 100)); err != nil {
		t.Fatal(err)
	}
	wm, err := CheckpointRows(w, makeRows(100, 7))
	if err != nil {
		t.Fatal(err)
	}
	if wm != 107 {
		t.Fatalf("watermark %d", wm)
	}
	w.Free() // crash: no Close
	status, err := Finalize("b.parquet", sink)
	if err != nil || status != "recovered" {
		t.Fatalf("finalize: %v %v", status, err)
	}
	final, _ := os.ReadFile(filepath.Join(dir, "b.parquet"))
	rows, err := ReadRows[Row](final)
	if err != nil {
		t.Fatal(err)
	}
	checkExact(t, rows, 107) // the tail was NOT lost
	// idempotent
	if status, _ := Finalize("b.parquet", sink); status != "done" {
		t.Fatalf("re-finalize: %v", status)
	}
}

func TestEmptyFile(t *testing.T) {
	sink, _ := NewDirSink(t.TempDir())
	w, err := NewWriter("e.parquet", sink, 5<<20, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := CloseTyped[Row](w); err != nil {
		t.Fatal(err)
	}
	b, _ := sink.Get("e.parquet", 0, -1)
	rows, err := ReadRows[Row](b)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected empty, got %d", len(rows))
	}
}

// A sub-sink's folder is made on first write; before that it lists
// empty, as an S3 prefix does (and as the Python DirSink does).
func TestDirSinkSubListsEmptyBeforeFirstWrite(t *testing.T) {
	sink, _ := NewDirSink(t.TempDir())
	sub := sink.Sub("streams/a/b")
	names, err := sub.List("")
	if err != nil || len(names) != 0 {
		t.Fatalf("unwritten sub-sink: got %v, %v; want empty, nil", names, err)
	}
	if err := sub.Put("x.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	names, err = sub.List("")
	if err != nil || len(names) != 1 || names[0] != "x.parquet" {
		t.Fatalf("after one put: got %v, %v; want [x.parquet], nil", names, err)
	}
}

func TestRefusals(t *testing.T) {
	sink, _ := NewDirSink(t.TempDir())
	if _, err := NewWriter("nope.txt", sink, 1024, false); err == nil {
		t.Fatal("must refuse non-.parquet name")
	}
	w, _ := NewWriter("c.parquet", sink, 5<<20, true)
	if _, err := WriteRows(w, makeRows(0, 10)); err != nil {
		t.Fatal(err)
	}
	w.Free()
	// corrupt checkpoint refuses
	cobj := w.CkptName()
	raw, _ := sink.Get(cobj, 0, -1)
	raw[len(raw)/2] ^= 0xFF
	if err := sink.Put(cobj, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(cobj, sink); err == nil {
		t.Fatal("corrupt checkpoint must refuse")
	}
	// mispaired refuses
	raw[len(raw)/2] ^= 0xFF
	if err := sink.Put(CkptObject("z.parquet", 1), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := Finalize("z.parquet", sink); err == nil {
		t.Fatal("mispaired checkpoint must refuse")
	}
}

func TestChunkRetryAfterFailedPut(t *testing.T) {
	sink, _ := NewDirSink(t.TempDir())
	failing := &failFirstPut{Sink: sink}
	w, err := NewWriter("f.parquet", failing, 4096, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteRows(w, makeRows(0, 2000)); err == nil {
		t.Fatal("injected chunk-PUT failure must surface")
	}
	// retry: the same pending chunk ships now
	if _, err := WriteRows(w, makeRows(2000, 100)); err != nil {
		t.Fatal(err)
	}
	if err := CloseTyped[Row](w); err != nil {
		t.Fatal(err)
	}
	b, _ := sink.Get("f.parquet", 0, -1)
	rows, err := ReadRows[Row](b)
	if err != nil {
		t.Fatal(err)
	}
	checkExact(t, rows, 2100)
}

type failFirstPut struct {
	Sink
	fired bool
}

func (f *failFirstPut) Put(name string, data []byte) error {
	if !f.fired && containsChunks(name) {
		f.fired = true
		return os.ErrPermission
	}
	return f.Sink.Put(name, data)
}

func containsChunks(name string) bool {
	for i := 0; i+8 <= len(name); i++ {
		if name[i:i+8] == ".chunks/" {
			return true
		}
	}
	return false
}

// A checkpoint stores only its new rows: one object for each, merged in
// steps, and the checkpoint object holds the list and the footer. The
// file reads back exactly after every checkpoint.
func TestCheckpointStoresOnlyNewRows(t *testing.T) {
	dir := t.TempDir()
	sink, _ := NewDirSink(dir)
	w, err := NewWriter("n.parquet", sink, 64<<10, false)
	if err != nil {
		t.Fatal(err)
	}
	before := map[string]bool{}
	for k := 1; k <= 120; k++ {
		wm, err := CheckpointRows(w, makeRows(0, k*50))
		if err != nil {
			t.Fatalf("checkpoint %d: %v", k, err)
		}
		if wm != uint64(k*50) {
			t.Fatalf("checkpoint %d: watermark %d", k, wm)
		}
		r, err := OpenReader(w.CkptName(), sink)
		if err != nil {
			t.Fatal(err)
		}
		file, err := r.Compose()
		if err != nil {
			t.Fatal(err)
		}
		rows, err := ReadRows[Row](file)
		if err != nil {
			t.Fatal(err)
		}
		checkExact(t, rows, k*50)
		// one new object for this checkpoint: its new rows, or a merge
		objects, _ := sink.List("n.parquet.chunks/")
		fresh := 0
		for _, o := range objects {
			if !before[o] {
				fresh++
				before[o] = true
			}
		}
		if fresh != 1 {
			t.Fatalf("checkpoint %d stored %d objects", k, fresh)
		}
		// the unfinished rows stay in few objects
		if len(r.Manifest) > 40 {
			t.Fatalf("checkpoint %d: %d objects in the list", k, len(r.Manifest))
		}
		// the data region is the footer alone: nothing but the list
		// and the footer in the checkpoint object
		if r.dataLen > 16<<10 {
			t.Fatalf("checkpoint %d: a data region of %d bytes", k, r.dataLen)
		}
	}
	// a tail never gets shorter
	if _, err := CheckpointRows(w, makeRows(0, 10)); err == nil || !strings.Contains(err.Error(), "never gets shorter") {
		t.Fatalf("a shorter tail: %v", err)
	}
	// the final rows replace the objects of the tail
	if _, err := WriteRows(w, makeRows(0, 6000)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(w.CkptName(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Manifest) != 1 {
		t.Fatalf("after the final rows: %d objects in the list", len(r.Manifest))
	}
	if err := CloseTyped[Row](w); err != nil {
		t.Fatal(err)
	}
	final, _ := os.ReadFile(filepath.Join(dir, "n.parquet"))
	rows, err := ReadRows[Row](final)
	if err != nil {
		t.Fatal(err)
	}
	checkExact(t, rows, 6000)
	if left, _ := sink.List(""); len(left) != 1 {
		t.Fatalf("left in the store: %v", left)
	}
	if _, err := os.Stat(filepath.Join(dir, "n.parquet.chunks")); !os.IsNotExist(err) {
		t.Fatal("the objects' folder is left behind, empty")
	}
}
