package pqc

import (
	"bytes"
	"strings"
	"testing"

	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/s3fake"
)

// The Go side of the Python contract suite: the S3 sink against the
// in-process fake, then the writer's whole lifecycle on it.

func TestS3SinkContract(t *testing.T) {
	fake := s3fake.New()
	defer fake.Close()
	sink := NewS3Sink(fake.Client(), "b", "/runs/")
	if sink.Prefix != "runs/" {
		t.Fatalf("prefix rooted as %q", sink.Prefix)
	}
	if sink.Exists("x") {
		t.Fatal("exists before put")
	}
	if _, err := sink.Size("x"); err == nil || !IsNotFound(err) {
		t.Fatalf("size of an absent object: %v", err)
	}
	if err := sink.Put("x", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if raw, ok := fake.Object("b", "runs/x"); !ok || string(raw) != "0123456789" {
		t.Fatalf("stored under the prefix: %q %v", raw, ok)
	}
	whole, err := sink.Get("x", 0, -1)
	if err != nil || string(whole) != "0123456789" {
		t.Fatalf("whole get %q %v", whole, err)
	}
	part, err := sink.Get("x", 3, 4)
	if err != nil || string(part) != "3456" {
		t.Fatalf("ranged get %q %v", part, err)
	}
	tail, err := sink.Get("x", 7, -1)
	if err != nil || string(tail) != "789" {
		t.Fatalf("open-ended get %q %v", tail, err)
	}
	if _, err := sink.Get("x", 10, -1); err == nil {
		t.Fatal("offset past the end must fail")
	}
	if n, err := sink.Size("x"); err != nil || n != 10 {
		t.Fatalf("size %d %v", n, err)
	}
	// Immutable objects: a put replaces the whole object atomically.
	if err := sink.Put("x", []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if n, _ := sink.Size("x"); n != 2 {
		t.Fatalf("replace left size %d", n)
	}
	// Listing: by prefix, sorted, names relative to the root.
	for _, n := range []string{"a.parquet.chunks/000002", "a.parquet.chunks/000001", "a.parquet.ckpt", "b.parquet"} {
		if err := sink.Put(n, []byte("z")); err != nil {
			t.Fatal(err)
		}
	}
	names, err := sink.List("a.parquet.chunks/")
	if err != nil || strings.Join(names, ",") != "a.parquet.chunks/000001,a.parquet.chunks/000002" {
		t.Fatalf("list %v %v", names, err)
	}
	all, _ := sink.List("")
	if len(all) != 5 {
		t.Fatalf("list all: %v", all)
	}
	if err := sink.Delete("b.parquet"); err != nil || sink.Exists("b.parquet") {
		t.Fatalf("delete: %v, exists %v", err, sink.Exists("b.parquet"))
	}
	if err := sink.Delete("b.parquet"); err != nil {
		t.Fatalf("deleting an absent object is not an error: %v", err)
	}

	// Assembly: sources under the part minimum travel together, as one
	// uploaded part. S3 cannot join small objects, and refuses a part
	// under the minimum unless it is the last.
	if err := sink.Assemble("joined", []Source{{Name: "a.parquet.ckpt", Offset: -1, Length: -1}, {Name: "x", Offset: -1, Length: -1}}); err != nil {
		t.Fatalf("two small sources: %v", err)
	}
	one, _ := sink.Get("a.parquet.ckpt", 0, -1)
	two, _ := sink.Get("x", 0, -1)
	if joined, err := sink.Get("joined", 0, -1); err != nil || !bytes.Equal(joined, append(append([]byte{}, one...), two...)) {
		t.Fatalf("two small sources joined: %q %v", joined, err)
	}
	if err := sink.Delete("joined"); err != nil {
		t.Fatal(err)
	}
	// A source that is absent: refused, and the upload is aborted.
	if err := sink.Assemble("out", []Source{{Name: "a.parquet.ckpt", Offset: -1, Length: -1}, {Name: "absent", Offset: -1, Length: -1}}); err == nil {
		t.Fatal("an assembly with an absent source succeeded")
	}
	if fake.OpenUploads() != 0 {
		t.Fatal("a refused assembly left its multipart upload open")
	}
	// A proper assembly: a 5 MiB chunk, then a ranged tail, concatenated
	// in order, appearing atomically as one object.
	big := bytes.Repeat([]byte("c"), MinPart)
	if err := sink.Put("big", big); err != nil {
		t.Fatal(err)
	}
	if err := sink.Put("tail", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Assemble("out", []Source{{Name: "big", Offset: -1, Length: -1}, {Name: "tail", Offset: 2, Length: 5}}); err != nil {
		t.Fatal(err)
	}
	got, err := sink.Get("out", 0, -1)
	if err != nil || len(got) != MinPart+5 || !bytes.Equal(got[:MinPart], big) || string(got[MinPart:]) != "23456" {
		t.Fatalf("assembled %d bytes, %v", len(got), err)
	}
	if fake.OpenUploads() != 0 {
		t.Fatal("a completed assembly left its multipart upload open")
	}
	// A missing source aborts the upload and fails loudly.
	if err := sink.Assemble("out2", []Source{{Name: "big", Offset: -1, Length: -1}, {Name: "nope", Offset: -1, Length: -1}}); err == nil {
		t.Fatal("a missing source must fail")
	}
	if fake.OpenUploads() != 0 || sink.Exists("out2") {
		t.Fatal("a failed assembly left an upload open or a partial object")
	}
}

// TestLifecycleOnS3 is TestLifecycle on the S3 sink: the chunk size is
// the part minimum, so a small file is one tail at finalize, which the
// multipart rules allow as the last part.
func TestLifecycleOnS3(t *testing.T) {
	fake := s3fake.New()
	defer fake.Close()
	sink := NewS3Sink(fake.Client(), "b", "streams/t")
	if _, err := NewWriter("a.parquet", sink, 64*1024, true); err == nil || !strings.Contains(err.Error(), "below the sink's part minimum") {
		t.Fatalf("a chunk size under the part minimum must be refused: %v", err)
	}
	w, err := NewWriter("a.parquet", sink, MinPart, true)
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
	final, err := sink.Get("a.parquet", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = ReadRows[Row](final)
	if err != nil {
		t.Fatal(err)
	}
	checkExact(t, rows, 4000)
	if fake.OpenUploads() != 0 {
		t.Fatal("finalize left a multipart upload open")
	}
}
