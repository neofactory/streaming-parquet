package pqc

import (
	"errors"
	"testing"

	"github.com/neofactory/streaming-parquet/src/go/s3fake"
)

// The contract of Versioned, the same for a directory and for a bucket:
// an absent object has the version ""; a put names the version it
// replaces and is refused with ErrChanged when the object is another.
func versionedContract(t *testing.T, vs Versioned, sink Sink) {
	t.Helper()
	if b, v, err := vs.GetVersion("c"); err != nil || v != "" || b != nil {
		t.Fatalf("an absent object: %q %q %v", b, v, err)
	}
	if _, err := vs.PutIfVersion("c", []byte("x"), "some-version"); !errors.Is(err, ErrChanged) {
		t.Fatalf("a put over a version of an absent object: %v", err)
	}
	v1, err := vs.PutIfVersion("c", []byte("one"), "")
	if err != nil || v1 == "" {
		t.Fatalf("the first put: %q %v", v1, err)
	}
	if _, err := vs.PutIfVersion("c", []byte("x"), ""); !errors.Is(err, ErrChanged) {
		t.Fatalf("a create-only put over an object: %v", err)
	}
	b, v, err := vs.GetVersion("c")
	if err != nil || string(b) != "one" || v != v1 {
		t.Fatalf("get after put: %q %q (put said %q) %v", b, v, v1, err)
	}
	v2, err := vs.PutIfVersion("c", []byte("second"), v1)
	if err != nil || v2 == "" || v2 == v1 {
		t.Fatalf("the second put: %q after %q, %v", v2, v1, err)
	}
	if _, err := vs.PutIfVersion("c", []byte("x"), v1); !errors.Is(err, ErrChanged) {
		t.Fatalf("a put over a replaced version: %v", err)
	}
	if b, _ := sink.Get("c", 0, -1); string(b) != "second" {
		t.Fatalf("a refused put changed the object: %q", b)
	}
}

func TestVersionedDir(t *testing.T) {
	d, err := NewDirSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	versionedContract(t, d, d)
}

func TestVersionedS3(t *testing.T) {
	fake := s3fake.New()
	defer fake.Close()
	s := NewS3Sink(fake.Client(), "b", "runs")
	versionedContract(t, s, s)
}
