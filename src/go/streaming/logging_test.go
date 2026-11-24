package streaming

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// The library's lines go to the *slog.Logger of the Config, each with
// its level: a milestone is INFO, a condition to look at is WARN.
func TestLoggerOfTheConfig(t *testing.T) {
	var out lockedBuffer
	logger := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo}))
	tel, err := Init(Config{Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	s, err := tel.CreateStream("t/log", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400},
		Parquet: &ParquetOptions{BaseDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 0, 10)
	s.Cut()
	waitCatalog(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"level=INFO", "[t/log] catalog: listed 1 file(s), 1 in all",
		"[t/log] no store configured",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("the logger did not get %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "level=ERROR") {
		t.Fatalf("an error line in a run with no fault:\n%s", got)
	}
}
