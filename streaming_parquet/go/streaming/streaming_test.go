package streaming

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/pqc"
)

func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
}

func writeRows(t *testing.T, s *Stream, start, count int) {
	t.Helper()
	r := s.Ring()
	n, _ := r.Leaf("n")
	x, _ := r.Leaf("x")
	sl, _ := r.Leaf("s")
	for i := start; i < start+count; i++ {
		if _, err := r.RowBegin(time.Second); err != nil {
			t.Fatal(err)
		}
		r.SetI64(n, int64(i))
		r.SetF64(x, float64(i)*0.5)
		r.SetString(sl, fmt.Sprintf("row-%d", i))
	}
	if _, err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(); err != nil {
		t.Fatal(err)
	}
}

// One process, no discovery: create a stream, attach by direct ticket, the
// attached ring fills over real Flight on loopback.
func TestLoopbackAttach(t *testing.T) {
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err := tel.CreateStream("t/loop", testSchema(),
		StreamOptions{RingRows: 1000,
			Archive: ArchiveOptions{BaseDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := tel.Attach("t/loop", AttachOptions{
		Ticket:  &Ticket{Name: "t/loop", Host: "127.0.0.1", Port: tel.FlightPort()},
		Archive: ArchiveOptions{BaseDir: t.TempDir()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	// the ring appears at connect (schema learned via GetFlightInfo)
	if _, err := a.WaitRing(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 5; k++ {
		writeRows(t, s, k*100, 100)
		time.Sleep(20 * time.Millisecond)
	}
	r, err := a.WaitRing(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.NewestSeq() < 499-100 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	// at least the later rows arrived (early ones may predate connect)
	n, _ := r.Leaf("n")
	sl, _ := r.Leaf("s")
	last := r.NewestSeq()
	if last < 0 {
		t.Fatal("no rows arrived")
	}
	v, _, err := r.GetI64(last, n)
	if err != nil {
		t.Fatal(err)
	}
	if v != 499 {
		t.Fatalf("last row n=%d (want 499)", v)
	}
	b, _, _ := r.GetBytes(last, sl)
	if string(b) != "row-499" {
		t.Fatalf("last row s=%q", b)
	}
}

// The symmetry Paul asked for: an ATTACHED process archives to
// parquet, exactly as a creating process would.
func TestAttachedArchivesParquet(t *testing.T) {
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	pubDir, subDir := t.TempDir(), t.TempDir()
	s, err := tel.CreateStream("t/arch", testSchema(),
		StreamOptions{RingRows: 2000,
			Archive: ArchiveOptions{BaseDir: pubDir}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := tel.Attach("t/arch", AttachOptions{
		Ticket:   &Ticket{Name: "t/arch", Host: "127.0.0.1", Port: tel.FlightPort()},
		RingRows: 5000, // the lease at this test's rate needs more than two 200-row groups
		Archive: ArchiveOptions{BaseDir: subDir, RowGroupRows: 200,
			CheckpointSeconds: 0.05},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.StartParquet("SYM"); err != nil { // pre-connect: arms
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // let the subscriber connect
	for k := 0; k < 6; k++ {
		writeRows(t, s, k*150, 150)
		time.Sleep(30 * time.Millisecond)
	}
	r, err := a.WaitRing(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.NewestSeq() < 899-150 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	got := r.NewestSeq() + 1
	if got < 600 {
		t.Fatalf("attached ring only has %d rows", got)
	}
	if err := a.StopParquet(); err != nil {
		t.Fatal(err)
	}
	// the attached side produced an ordinary finalized parquet file
	pqDir := filepath.Join(subDir, "streams", "t", "arch")
	entries, err := os.ReadDir(pqDir)
	if err != nil {
		t.Fatal(err)
	}
	var file string
	for _, e := range entries {
		if strings.Contains(e.Name(), "SYM") &&
			strings.HasSuffix(e.Name(), ".parquet") {
			file = filepath.Join(pqDir, e.Name())
		}
		if strings.HasSuffix(e.Name(), ".ckpt") {
			t.Fatalf("checkpoint not finalized: %s", e.Name())
		}
	}
	if file == "" {
		t.Fatalf("no labeled parquet file in %s: %v", pqDir, entries)
	}
	type Row struct {
		N int64   `parquet:"n"`
		X float64 `parquet:"x"`
		S string  `parquet:"s"`
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := pqc.ReadRows[Row](b)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(rows)) < got-150 || len(rows) == 0 {
		t.Fatalf("archived %d rows, ring had %d", len(rows), got)
	}
	// contiguous ascending n, ending at the last ring row
	for i := 1; i < len(rows); i++ {
		if rows[i].N != rows[i-1].N+1 {
			t.Fatalf("gap at %d: %d -> %d", i, rows[i-1].N, rows[i].N)
		}
	}
	if rows[len(rows)-1].S != fmt.Sprintf("row-%d", rows[len(rows)-1].N) {
		t.Fatalf("content mismatch at tail")
	}
}
