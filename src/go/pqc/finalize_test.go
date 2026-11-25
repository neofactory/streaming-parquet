package pqc

import (
	"fmt"
	"strings"
	"testing"
)

// watched is a Sink that fails the Get of one object, and records the
// deletes in their order.
type watched struct {
	Sink
	failGet string
	deletes []string
}

func (w *watched) Get(name string, offset, length int64) ([]byte, error) {
	if name == w.failGet {
		return nil, fmt.Errorf("injected: %s cannot be read now", name)
	}
	return w.Sink.Get(name, offset, length)
}

func (w *watched) Delete(name string) error {
	w.deletes = append(w.deletes, name)
	return w.Sink.Delete(name)
}

// twoVersions writes a file with two checkpoint versions in a new
// directory, and leaves it unfinished.
func twoVersions(t *testing.T) (*watched, []Version) {
	t.Helper()
	dir, _ := NewDirSink(t.TempDir())
	w, err := NewWriter("f.parquet", dir, 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := WriteRows(w, makeRows(i*1500, 1500)); err != nil {
			t.Fatal(err)
		}
	}
	w.Free()
	versions, err := CkptVersions(dir, "f.parquet")
	if err != nil || len(versions) != 2 {
		t.Fatalf("versions: %v, %v; want two", versions, err)
	}
	return &watched{Sink: dir}, versions
}

// A version that cannot be READ now is not a version that is not a
// checkpoint: the assembly stops, and nothing is deleted. (An older
// version has fewer rows, and the deletes would take the newer one.)
func TestFinalizeStopsWhenAVersionCannotBeRead(t *testing.T) {
	sink, versions := twoVersions(t)
	sink.failGet = versions[1].Object
	if _, err := Finalize("f.parquet", sink); err == nil ||
		!strings.Contains(err.Error(), "cannot be read now") {
		t.Fatalf("Finalize: %v; want the read's own error", err)
	}
	if sink.Exists("f.parquet") || len(sink.deletes) != 0 {
		t.Fatalf("after a failed read: file there %v, deletes %v; want neither",
			sink.Exists("f.parquet"), sink.deletes)
	}
	// the read works again: the file is made from the newest version
	sink.failGet = ""
	if status, err := Finalize("f.parquet", sink); err != nil || status != "recovered" {
		t.Fatalf("Finalize: %q, %v", status, err)
	}
	if last := sink.deletes[len(sink.deletes)-1]; last != versions[1].Object {
		t.Fatalf("the last delete is %s; want the version in use, %s", last, versions[1].Object)
	}
	left, _ := sink.List("")
	if len(left) != 1 || left[0] != "f.parquet" {
		t.Fatalf("left: %v; want the file alone", left)
	}
	if status, err := Finalize("f.parquet", sink); err != nil || status != "done" {
		t.Fatalf("Finalize again: %q, %v; want done", status, err)
	}
}

// A newest version whose bytes are not a checkpoint is passed over,
// with a line for the log; the one before it makes the file, and is
// deleted last.
func TestFinalizePassesOverAVersionThatIsNotACheckpoint(t *testing.T) {
	sink, versions := twoVersions(t)
	if err := sink.Put(versions[1].Object, []byte("not a checkpoint")); err != nil {
		t.Fatal(err)
	}
	var lines []string
	SetWarn(func(text string) { lines = append(lines, text) })
	defer SetWarn(nil)
	if status, err := Finalize("f.parquet", sink); err != nil || status != "recovered" {
		t.Fatalf("Finalize: %q, %v", status, err)
	}
	if len(lines) != 1 || !strings.Contains(lines[0], versions[1].Object) ||
		!strings.Contains(lines[0], "assembling from "+versions[0].Object) {
		t.Fatalf("log lines: %q", lines)
	}
	n := len(sink.deletes)
	if n < 2 || sink.deletes[n-1] != versions[0].Object || sink.deletes[n-2] != versions[1].Object {
		t.Fatalf("deletes end with %v; want the bad version, then the one in use", sink.deletes[max(0, n-2):])
	}
}

// With the checkpoints kept, nothing is deleted; the discard deletes
// later, and refuses while the finished file is not there.
func TestDiscardCheckpoints(t *testing.T) {
	sink, versions := twoVersions(t)
	if err := DiscardCheckpoints("f.parquet", sink); err == nil ||
		!strings.Contains(err.Error(), "only copy") {
		t.Fatalf("discard with no finished file: %v; want a refusal", err)
	}
	if status, err := FinalizeKeep("f.parquet", sink); err != nil || status != "recovered" {
		t.Fatalf("FinalizeKeep: %q, %v", status, err)
	}
	if len(sink.deletes) != 0 {
		t.Fatalf("kept, but deleted %v", sink.deletes)
	}
	if err := DiscardCheckpoints("f.parquet", sink); err != nil {
		t.Fatal(err)
	}
	if last := sink.deletes[len(sink.deletes)-1]; last != versions[1].Object {
		t.Fatalf("the last delete is %s; want the newest version", last)
	}
	left, _ := sink.List("")
	if len(left) != 1 || left[0] != "f.parquet" {
		t.Fatalf("left: %v; want the file alone", left)
	}
}

func TestPlanParts(t *testing.T) {
	src := func(names ...string) []Source {
		var out []Source
		for _, n := range names {
			out = append(out, Source{n, -1, -1})
		}
		return out
	}
	show := func(parts []part) string {
		var b strings.Builder
		for _, p := range parts {
			kind := "upload"
			if p.copy {
				kind = "copy"
			}
			fmt.Fprintf(&b, "%s", kind)
			for _, s := range p.sources {
				fmt.Fprintf(&b, " %s", s.Name)
			}
			b.WriteString("; ")
		}
		return b.String()
	}
	const big, small = 5000, 100
	for _, c := range []struct {
		names []string
		sizes []int64
		want  string
	}{
		{[]string{"a", "b", "c", "d"}, []int64{big, big, small, small}, "copy a; copy b; upload c d; "},
		{[]string{"a"}, []int64{big}, "copy a; "},
		{[]string{"a", "b"}, []int64{small, small}, "upload a b; "},
		// a small object before a large one: the run takes the large one in
		{[]string{"a", "b", "c"}, []int64{small, big, small}, "upload a b; upload c; "},
		{nil, nil, ""},
	} {
		parts, err := planParts(src(c.names...), c.sizes, 4096)
		if err != nil || show(parts) != c.want {
			t.Fatalf("planParts(%v): %q, %v; want %q", c.sizes, show(parts), err, c.want)
		}
	}
	many := make([]int64, MaxParts+1)
	for i := range many {
		many[i] = 10
	}
	if _, err := planParts(make([]Source, len(many)), many, 10); err == nil ||
		!strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("more parts than S3 takes: %v; want a refusal", err)
	}
}
