package ring

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// keep is a slog handler that keeps the records it is given.
type keep struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (k *keep) Enabled(context.Context, slog.Level) bool { return true }
func (k *keep) Handle(_ context.Context, r slog.Record) error {
	k.mu.Lock()
	k.recs = append(k.recs, r)
	k.mu.Unlock()
	return nil
}
func (k *keep) WithAttrs([]slog.Attr) slog.Handler { return k }
func (k *keep) WithGroup(string) slog.Handler      { return k }
func (k *keep) got() []slog.Record {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]slog.Record(nil), k.recs...)
}

func oversized(t *testing.T, r *Ring) {
	t.Helper()
	s, _ := r.Leaf("s")
	if _, err := r.RowBegin(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.SetString(s, strings.Repeat("B", 1000)); err != nil {
		t.Fatal(err)
	}
}

// A line the core makes comes back in the call's report and goes to the
// ring's logger, as a warning, with the time the core made it.
func TestCoreLineGoesToTheLogger(t *testing.T) {
	k := &keep{}
	r, err := NewRing(testSchema(), 64, &Options{SegmentBytes: 256, Logger: slog.New(k)})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fill(t, r, 0, 3)
	if n := len(k.got()); n != 0 {
		t.Fatalf("values that fit made %d lines", n)
	}
	t0 := time.Now()
	oversized(t, r)
	t1 := time.Now()
	recs := k.got()
	if len(recs) != 1 {
		t.Fatalf("an oversized value made %d lines, want 1", len(recs))
	}
	rec := recs[0]
	if rec.Level != slog.LevelWarn || !strings.Contains(rec.Message, "oversized value: 1000 elements") {
		t.Fatalf("the line: %v %q", rec.Level, rec.Message)
	}
	if rec.Time.Before(t0.Add(-time.Millisecond)) || rec.Time.After(t1.Add(time.Millisecond)) {
		t.Fatalf("the line's time %v is not between %v and %v", rec.Time, t0, t1)
	}
}

// With HoldLines the writer's goroutine logs nothing: the lines wait
// for LogLines.
func TestHeldLinesWaitForLogLines(t *testing.T) {
	k := &keep{}
	r, err := NewRing(testSchema(), 64, &Options{SegmentBytes: 256, Logger: slog.New(k), HoldLines: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.LinesHeld() {
		t.Fatal("lines held before any was made")
	}
	oversized(t, r)
	oversized(t, r)
	if n := len(k.got()); n != 0 {
		t.Fatalf("the writer logged %d held lines", n)
	}
	if !r.LinesHeld() {
		t.Fatal("no lines held after two were made")
	}
	r.LogLines()
	recs := k.got()
	if len(recs) != 2 || !strings.Contains(recs[1].Message, "(#2)") {
		t.Fatalf("LogLines logged %d lines", len(recs))
	}
	r.LogLines()
	if len(k.got()) != 2 || r.LinesHeld() {
		t.Fatal("a line was logged twice")
	}
	oversized(t, r)
	lines := r.TakeLines()
	if len(lines) != 1 || lines[0].Level != slog.LevelWarn || !strings.Contains(lines[0].Text, "(#3)") {
		t.Fatalf("TakeLines: %+v", lines)
	}
}

// The core does not wait for a pin: RowBegin does, for as long as it
// was told, and then says which pin.
func TestRowBeginWaitsHere(t *testing.T) {
	r, err := NewRing(testSchema(), 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fill(t, r, 0, 8)
	rs, err := r.Pin(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	_, err = r.RowBegin(0)
	if !IsPinStalled(err) || time.Since(t0) > 20*time.Millisecond {
		t.Fatalf("no wait asked: %v after %v", err, time.Since(t0))
	}
	if !strings.Contains(err.Error(), "blocked by a pin on [0, 1)") {
		t.Fatalf("the stall does not name the pin: %v", err)
	}
	t0 = time.Now()
	_, err = r.RowBegin(60 * time.Millisecond)
	if !IsPinStalled(err) || time.Since(t0) < 60*time.Millisecond {
		t.Fatalf("a 60 ms wait: %v after %v", err, time.Since(t0))
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		rs.Release()
	}()
	t0 = time.Now()
	if _, err := r.RowBegin(2 * time.Second); err != nil {
		t.Fatalf("the pin was released: %v", err)
	}
	if d := time.Since(t0); d < 20*time.Millisecond || d > time.Second {
		t.Fatalf("the row came after %v, want about 30 ms", d)
	}
	if n := r.NewestSeq(); n != 7 {
		t.Fatalf("the blocked calls took rows: newest %d", n)
	}
}
