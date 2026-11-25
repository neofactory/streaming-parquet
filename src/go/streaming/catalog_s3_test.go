package streaming

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/neofactory/streaming-parquet/src/go/pqc"
)

// The catalog on a real bucket: the conditional upload and the footer
// reads as S3 does them. Runs only when SP_TEST_S3_BUCKET names a
// bucket; it writes under streams/test/go-catalog-check/ there, and
// the stream's ticket at tickets/test/go-catalog-check.json, and
// deletes every object it wrote.
func TestCatalogRealS3(t *testing.T) {
	bucket := os.Getenv("SP_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("SP_TEST_S3_BUCKET is not set")
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	root := pqc.NewS3Sink(s3.NewFromConfig(cfg), bucket, "")
	const name = "test/go-catalog-check"
	mine := root.Sub("streams/" + name)
	left, err := mine.List("")
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("streams/%s/ is not empty: %v", name, left)
	}
	defer func() {
		names, err := mine.List("")
		if err != nil {
			t.Errorf("cleanup: %v", err)
		}
		for _, n := range names {
			if err := mine.Delete(n); err != nil {
				t.Errorf("cleanup %s: %v", n, err)
			}
		}
		ticket := "tickets/" + name + ".json"
		if root.Exists(ticket) { // a stream that did not close leaves it
			if err := root.Delete(ticket); err != nil {
				t.Errorf("cleanup %s: %v", ticket, err)
			}
		}
		names, _ = mine.List("")
		t.Logf("deleted the test's objects; %d left under streams/%s/", len(names), name)
	}()

	run := func(first, files, want int) {
		tel, err := newProcess(processConfig{})
		if err != nil {
			t.Fatal(err)
		}
		defer tel.Close()
		t00 := time.Now()
		// no spool: the bucket is the first copy, and the catalog's
		// conditional upload is what this test is about
		s, err := tel.CreateStream(name, testSchema(), StreamOptions{Store: "s3://" + bucket,
			NoSpool: true, Memory: &MemoryOptions{Rows: 400}, Parquet: &ParquetOptions{}})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("create took %.1f s", time.Since(t00).Seconds())
		t0 := time.Now()
		for k := 0; k < files; k++ {
			writeRows(t, s, first+k*10, 10)
			s.Cut()
			waitCatalog(t, s, want-files+k+1)
			t.Logf("file %d listed %.1f s after the run's start", k+1, time.Since(t0).Seconds())
		}
		t1 := time.Now()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("close took %.1f s", time.Since(t1).Seconds())
	}
	run(0, 2, 2)
	run(20, 1, 3) // a second writer: the stored catalog gains a file
	names, err := mine.List("")
	if err != nil {
		t.Fatal(err)
	}
	finished := 0
	for _, n := range names {
		if strings.HasSuffix(n, ".parquet") {
			finished++
		}
	}
	if finished != 3 || !mine.Exists(catalogObject) {
		t.Fatalf("the store holds %v", names)
	}
}

// A spool mirrored to a real bucket: the uploads, the assembly in the
// bucket and the catalog as S3 does them. Runs only when
// SP_TEST_S3_BUCKET names a bucket; it writes under
// streams/test/go-spool-check/ there, and the stream's ticket at
// tickets/test/go-spool-check.json, and deletes every object it wrote.
func TestSpoolRealS3(t *testing.T) {
	bucket := os.Getenv("SP_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("SP_TEST_S3_BUCKET is not set")
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	root := pqc.NewS3Sink(s3.NewFromConfig(cfg), bucket, "")
	const name = "test/go-spool-check"
	mine := root.Sub("streams/" + name)
	if left, err := mine.List(""); err != nil || len(left) != 0 {
		t.Fatalf("streams/%s/ is not empty, or cannot be listed: %v %v", name, left, err)
	}
	defer func() {
		names, err := mine.List("")
		if err != nil {
			t.Errorf("cleanup: %v", err)
		}
		for _, n := range names {
			if err := mine.Delete(n); err != nil {
				t.Errorf("cleanup %s: %v", n, err)
			}
		}
		ticket := "tickets/" + name + ".json"
		if root.Exists(ticket) {
			if err := root.Delete(ticket); err != nil {
				t.Errorf("cleanup %s: %v", ticket, err)
			}
		}
		names, _ = mine.List("")
		t.Logf("deleted the test's objects; %d left under streams/%s/", len(names), name)
	}()

	tel, err := newProcess(processConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	spool := t.TempDir()
	s, err := tel.CreateStream(name, testSchema(), StreamOptions{Store: "s3://" + bucket, Spool: spool,
		Memory:  &MemoryOptions{Rows: 2000},
		Parquet: &ParquetOptions{CheckpointSeconds: 0.5, ShipSeconds: 1}})
	if err != nil {
		t.Fatal(err)
	}
	wait := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out: %s; mirror %+v, files %+v", what, s.MirrorState(), s.ArchiveState())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	t0 := time.Now()
	writeRows(t, s, 0, 300)
	wait("row 299 in the spool", func() bool { return s.DurableRowNumber() == 299 })
	wait("the first version in the bucket", func() bool {
		names, err := mine.List("")
		if err != nil {
			return false
		}
		for _, n := range names {
			if strings.HasSuffix(n, ".ckpt") {
				return true
			}
		}
		return false
	})
	t.Logf("the first version was in the bucket %.1f s after the rows", time.Since(t0).Seconds())
	s.Cut()
	writeRows(t, s, 300, 200)
	wait("the first file finished in the bucket and listed", func() bool {
		st := s.ArchiveState()
		return st.CatalogFiles == 1 && len(s.MirrorState().Pending) == 0
	})
	if st := s.MirrorState(); st.Failing {
		t.Fatalf("mirror: %+v", st)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	names, err := mine.List("")
	if err != nil {
		t.Fatal(err)
	}
	finished := 0
	for _, n := range names {
		if strings.HasSuffix(n, ".parquet") {
			finished++
			local, _ := os.ReadFile(filepath.Join(spool, "streams", filepath.FromSlash(name), n))
			far, err := mine.Get(n, 0, -1)
			if err != nil || len(local) == 0 || !bytes.Equal(local, far) {
				t.Fatalf("%s: %d bytes in the spool, %d in the bucket (%v)", n, len(local), len(far), err)
			}
		}
	}
	if finished != 2 || !mine.Exists(catalogObject) {
		t.Fatalf("the bucket holds %v", names)
	}
}
