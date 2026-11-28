package pqc

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// outage is a sink whose writes fail while down is set.
type outage struct {
	Sink
	down bool
	puts int
}

func (o *outage) Put(name string, data []byte) error {
	if o.down {
		return errors.New("the remote cannot be reached")
	}
	o.puts++
	return o.Sink.Put(name, data)
}

func (o *outage) Assemble(dest string, sources []Source) error {
	if o.down {
		return errors.New("the remote cannot be reached")
	}
	return o.Sink.Assemble(dest, sources)
}

// The upload, by the core's rules: an outage loses nothing that was
// sent, an object is sent once, the ticket is told at each version, and
// the file ends in both copies with the same bytes.
func TestUpload(t *testing.T) {
	local, _ := NewDirSink(t.TempDir())
	far, _ := NewDirSink(t.TempDir())
	remote := &outage{Sink: far}
	w, err := NewWriter("m.parquet", local, 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewUpload("m.parquet", local, remote, SupersededKeep)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Free()
	var told []uint64
	stored := func(object string, rows uint64) { told = append(told, rows) }

	if st, err := m.Run(true, false, stored); err != nil || st.State != "idle" {
		t.Fatalf("nothing local yet: %+v %v", st, err)
	}
	if _, err := WriteRows(w, makeRows(0, 2000)); err != nil {
		t.Fatal(err)
	}
	// the remote is down: the pass fails, and nothing is told
	remote.down = true
	if _, err := m.Run(true, false, stored); err == nil {
		t.Fatal("a pass with the remote down did not fail")
	}
	remote.down = false
	st, err := m.Run(true, false, stored)
	if err != nil || st.State != "uploaded" || st.Chunks == 0 || st.Rows != 2000 {
		t.Fatalf("the first version: %+v %v", st, err)
	}
	if len(told) != 1 || told[0] != 2000 || m.LastObject() != st.Object {
		t.Fatalf("told %v, last object %q, status %+v", told, m.LastObject(), st)
	}
	sent := remote.puts
	if st, err := m.Run(true, false, stored); err != nil || st.State != "synced" || remote.puts != sent {
		t.Fatalf("the same version again: %+v %v, %d puts more", st, err, remote.puts-sent)
	}
	// more rows: only the new objects go, each once
	if _, err := WriteRows(w, makeRows(2000, 1500)); err != nil {
		t.Fatal(err)
	}
	st, err = m.Run(true, false, stored)
	if err != nil || st.State != "uploaded" || st.Rows != 3500 {
		t.Fatalf("the second version: %+v %v", st, err)
	}
	names, _ := far.List("")
	seen := map[string]int{}
	for _, n := range names {
		seen[n]++
	}
	for n, k := range seen {
		if k != 1 {
			t.Fatalf("%s is on the remote %d times", n, k)
		}
	}

	// THE END OF A FILE: the writer's last checkpoint, then a pass with
	// the remote down at its assembly: the local file is made, the remote
	// one waits; the next pass finishes it from the version in hand
	if _, err := w.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	w.Free()
	remote.down = true
	if _, err := m.Run(false, false, stored); err == nil {
		t.Fatal("the end of the file with the remote down did not fail")
	}
	if !local.Exists("m.parquet") || far.Exists("m.parquet") {
		t.Fatalf("after the failed pass: local %v, remote %v", local.Exists("m.parquet"), far.Exists("m.parquet"))
	}
	remote.down = false
	st, err = m.Run(false, false, stored)
	if err != nil || !st.Done || st.State != "local-finalized" {
		t.Fatalf("the pass that ends the file: %+v %v", st, err)
	}
	a, _ := local.Get("m.parquet", 0, -1)
	b, _ := far.Get("m.parquet", 0, -1)
	if len(a) == 0 || !bytes.Equal(a, b) {
		t.Fatalf("the two finished files differ: %d and %d bytes", len(a), len(b))
	}
	left, _ := far.List("")
	for _, n := range left {
		if strings.Contains(n, ".ckpt") || strings.Contains(n, ".chunks/") {
			t.Fatalf("the remote still holds %s after the file ended", n)
		}
	}
	if st, err := m.Run(false, false, stored); err != nil || !st.Done {
		t.Fatalf("done stays done: %+v %v", st, err)
	}

	// the sweep's table is the core's
	if SweepLocal(false, true, true) != SweepUpload || SweepLocal(false, false, true) != SweepFinish ||
		SweepLocal(true, false, true) != SweepAwaitCatalog || SweepLocal(true, true, true) != SweepDiscard ||
		SweepRemote(false, true, true) != SweepNothing || SweepRemote(false, true, false) != SweepUpload {
		t.Fatal("the sweep's answers are not the core's")
	}
	// and so are the names of versions
	if CkptObject("x.parquet", 7) != "x.parquet.000007.ckpt" {
		t.Fatal(CkptObject("x.parquet", 7))
	}
	if n, seq, ok := ParseCkptObject("s3://b/streams/a/x.parquet.000007.ckpt"); !ok || seq != 7 ||
		n != "s3://b/streams/a/x.parquet" {
		t.Fatalf("parse: %q %d %v", n, seq, ok)
	}
	if _, _, ok := ParseCkptObject("x.parquet.chunks/000001"); ok {
		t.Fatal("a chunk's name was taken for a version")
	}
}
