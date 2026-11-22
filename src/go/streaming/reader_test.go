package streaming

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func waitState(t *testing.T, a *Attached, want string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for a.State() != want {
		if time.Now().After(deadline) {
			t.Fatalf("state %q (%s), want %q", a.State(), a.Reason(), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A reader of a name whose folder does not exist is refused at once;
// a name in a folder that exists, and a name with no folder, wait.
func TestAttachFolderRule(t *testing.T) {
	root := t.TempDir()
	tel, err := Init(Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()

	_, err = tel.Attach("no/such/place", AttachOptions{})
	if err == nil {
		t.Fatal("Attach waited for a name in no folder")
	}
	for _, part := range []string{`Attach("no/such/place"): the folder no/such does not exist in the store`,
		"made the first time a writer of a stream under no/such runs",
		"check the path"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("the refusal %q lacks %q", err, part)
		}
	}

	a, err := tel.Attach("alone", AttachOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if a.State() != "no-ticket" || !strings.Contains(a.Reason(), "no ticket for alone") {
		t.Fatalf("state %q, reason %q", a.State(), a.Reason())
	}
	a.Close()

	// files in the folder and no ticket: a wrong last part waits
	if err := os.MkdirAll(filepath.Join(root, "streams", "rd", "old", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	a, err = tel.Attach("rd/old/y", AttachOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if a.State() != "no-ticket" {
		t.Fatalf("state %q", a.State())
	}
	a.Close()
}

// A waiting reader finds a stream this process creates later and reads
// its ring in place (an internal stream: no ticket, no Flight). When
// the stream closes the reader waits again, and finds the next one.
func TestAttachFindsLocalStream(t *testing.T) {
	tel, err := Init(Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Close()
	other, err := tel.CreateStream("rd/late/other", testSchema(),
		StreamOptions{RingRows: 400, Internal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()

	a, err := tel.Attach("rd/late/s", AttachOptions{}) // the folder exists in this process alone
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if a.State() != "no-ticket" {
		t.Fatalf("state %q", a.State())
	}
	for round := 0; round < 2; round++ {
		s, err := tel.CreateStream("rd/late/s", testSchema(),
			StreamOptions{RingRows: 400, Internal: true})
		if err != nil {
			t.Fatal(err)
		}
		writeRows(t, s, 0, 10+round)
		waitState(t, a, "streaming")
		if a.Reason() != "" {
			t.Fatalf("reason %q while streaming", a.Reason())
		}
		r := a.Ring()
		if r == nil || r.NewestSeq() != int64(9+round) {
			t.Fatalf("round %d: the reader's ring is not the stream's", round)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		waitState(t, a, "no-ticket")
		if a.Ring() != nil {
			t.Fatal("the reader kept the ring of a closed stream")
		}
	}
}
