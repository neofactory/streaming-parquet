package streaming

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Durable: the first seq not yet in the archive. A consumer that must
// acknowledge its input only once it is durable compares a row's seq
// against it after Publish; the checkpoint cadence moves it.
func TestDurableFollowsTheCheckpoint(t *testing.T) {
	tel, err := Init(Config{FlightAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	if tel.FlightPort() == 0 {
		t.Fatal("a configured Flight address did not bind")
	}
	s, err := tel.CreateStream("t/durable", testSchema(),
		StreamOptions{RingRows: 2000, Parquet: true,
			Archive: ArchiveOptions{BaseDir: t.TempDir(), CheckpointSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Durable(); got != 0 {
		t.Fatalf("before any row: durable %d, want 0", got)
	}
	writeRows(t, s, 0, 10)
	// Not yet: the checkpoint cadence has not elapsed.
	if got := s.Durable(); got != 0 {
		t.Fatalf("right after commit: durable %d, want 0", got)
	}
	deadline := time.Now().Add(3 * time.Second)
	for s.Durable() < 10 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if _, err := s.Publish(); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.Durable(); got != 10 {
		t.Fatalf("after the checkpoint: durable %d, want 10", got)
	}
	writeRows(t, s, 10, 5)
	if err := s.StopParquet(); err != nil {
		t.Fatal(err)
	}
	if got := s.Durable(); got != 15 {
		t.Fatalf("after stop: durable %d, want 15", got)
	}
}

// The ring holds two row groups: either size follows from the other,
// DuckDB's row group when neither is given, and a ring under two row
// groups is refused by CreateStream and Attach.
func TestRingHoldsTwoRowGroups(t *testing.T) {
	for _, c := range []struct{ ring, group, wantRing, wantGroup int64 }{
		{0, 0, 245_760, 122_880},
		{0, 500, 1000, 500},
		{1000, 0, 1000, 500},
		{3000, 1000, 3000, 1000},
	} {
		ring, group, err := ringSizes("t", c.ring, c.group)
		if err != nil || ring != c.wantRing || group != c.wantGroup {
			t.Fatalf("ringSizes(%d, %d) = %d, %d, %v; want %d, %d",
				c.ring, c.group, ring, group, err, c.wantRing, c.wantGroup)
		}
	}
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	_, err = tel.CreateStream("t/small", testSchema(), StreamOptions{RingRows: 1999,
		Archive: ArchiveOptions{BaseDir: t.TempDir(), RowGroupRows: 1000}})
	if err == nil || !strings.Contains(err.Error(), "less than two row groups") {
		t.Fatalf("CreateStream: %v, want the ring refused", err)
	}
	if _, err := tel.Attach("t/small", AttachOptions{RingRows: 1999,
		Archive: ArchiveOptions{RowGroupRows: 1000}}); err == nil {
		t.Fatal("Attach accepted a ring under two row groups")
	}
}

// A checkpoint publishes new rows only: with nothing committed since the
// last one, the next interval writes no checkpoint.
func TestCheckpointSkipsWhenNothingNew(t *testing.T) {
	tel, err := Init(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	dir := t.TempDir()
	s, err := tel.CreateStream("t/idle", testSchema(), StreamOptions{RingRows: 2000, Parquet: true,
		Archive: ArchiveOptions{BaseDir: dir, CheckpointSeconds: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	deadline := time.Now().Add(3 * time.Second)
	for s.Durable() < 10 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if _, err := s.Publish(); err != nil {
			t.Fatal(err)
		}
	}
	ckpt := func() time.Time {
		m, err := filepath.Glob(filepath.Join(dir, "streams", "t", "idle", "*.ckpt"))
		if err != nil || len(m) != 1 {
			t.Fatalf("checkpoint files %v, %v", m, err)
		}
		fi, err := os.Stat(m[0])
		if err != nil {
			t.Fatal(err)
		}
		return fi.ModTime()
	}
	before := ckpt()
	for i := 0; i < 10; i++ { // ten intervals with nothing new
		time.Sleep(60 * time.Millisecond)
		if _, err := s.Publish(); err != nil {
			t.Fatal(err)
		}
	}
	if after := ckpt(); !after.Equal(before) {
		t.Fatalf("an idle stream rewrote its checkpoint (%v -> %v)", before, after)
	}
}
