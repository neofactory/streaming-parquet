package streaming

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A second writer of a stream on this server is refused, and the
// refusal names the holder; the stream is free once the holder closes.
func TestSecondWriterOnThisServerIsRefused(t *testing.T) {
	root := t.TempDir()
	first, err := Init(Config{Store: root})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Init(Config{Store: root})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	s, err := first.CreateStream("t/own", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = second.CreateStream("t/own", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400}})
	if err == nil {
		t.Fatal("the second writer was not refused")
	}
	for _, want := range []string{`CreateStream("t/own")`,
		"another process on this server writes this stream",
		fmt.Sprintf("pid %d", os.Getpid()), "writer.lock"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal lacks %q: %v", want, err)
		}
	}
	s.Close()
	s2, err := second.CreateStream("t/own", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400}})
	if err != nil {
		t.Fatalf("after the holder closed: %v", err)
	}
	s2.Close()
}

// An internal stream that writes no files takes no lock: two processes
// can each have one of the same name. Publish takes the lock.
func TestInternalStreamTakesNoLock(t *testing.T) {
	root := t.TempDir()
	a, _ := Init(Config{Store: root})
	defer a.Close()
	b, _ := Init(Config{Store: root})
	defer b.Close()
	sa, err := a.CreateStream("t/in", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400}, Internal: true})
	if err != nil {
		t.Fatal(err)
	}
	sb, err := b.CreateStream("t/in", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400}, Internal: true})
	if err != nil {
		t.Fatalf("a second internal stream of the name: %v", err)
	}
	if err := sa.Publish(); err != nil {
		t.Fatal(err)
	}
	if err := sb.Publish(); err == nil ||
		!strings.Contains(err.Error(), "another process on this server") {
		t.Fatalf("Publish while another writer holds the stream: %v", err)
	}
	if sb.Published() {
		t.Fatal("a refused Publish left the stream published")
	}
}

// A fresh ticket from another server refuses, naming it; a stale one is
// taken over.
func TestTicketOfAnotherServer(t *testing.T) {
	root := t.TempDir()
	tel, err := Init(Config{Store: root})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	path := filepath.Join(root, "tickets", "t", "other.json")
	os.MkdirAll(filepath.Dir(path), 0o755)
	ticket := func(age float64) {
		now := float64(time.Now().UnixNano()) / 1e9
		text := fmt.Sprintf(`{"name": "t/other", "host": "10.9.9.9", "port": 4242, `+
			`"ts": %f, "run": "2026-10-26T00:00:00.000000Z"}`, now-age)
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ticket(5)
	_, err = tel.CreateStream("t/other", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400}})
	if err == nil || !strings.Contains(err.Error(),
		"the server at 10.9.9.9:4242 writes this stream") {
		t.Fatalf("a fresh ticket from another server: %v", err)
	}
	ticket(3600)
	s, err := tel.CreateStream("t/other", testSchema(), StreamOptions{Memory: &MemoryOptions{Rows: 400}})
	if err != nil {
		t.Fatalf("a stale ticket was not taken over: %v", err)
	}
	s.Close()
}
