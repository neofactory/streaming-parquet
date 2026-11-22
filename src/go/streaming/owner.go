package streaming

// One writer per stream: the ownership check a stream makes before it
// is published or writes a file.
//
// The decisions and their messages are the stream core's (sp.h, THE
// OWNERSHIP DECISION), the same for every host language. This file
// makes the lock file call and reads the ticket.
//
// The lock lives on local disk: under a directory root, else under the
// default spool, the same place a Python writer on this server uses, so
// a Python writer and a Go writer of one stream refuse each other.

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
)

// ownership is the held lock. The operating system releases it when
// the process ends.
type ownership struct {
	path string
	f    *os.File
}

func (o *ownership) release() {
	if o != nil && o.f != nil {
		o.f.Close() // closing the descriptor drops the lock
		o.f = nil
	}
}

// lockDir is the local directory lock files live under.
func (t *Telemetry) lockDir() string {
	root := strings.TrimPrefix(t.root, "file://")
	if t.root != "" && !strings.Contains(root, "://") {
		return root
	}
	name := "unknown"
	if u, err := user.Current(); err == nil {
		name = u.Username
	}
	return filepath.Join("/var/tmp", "streaming-parquet", name)
}

// claim owns the stream name, or returns an error that names who does.
func (t *Telemetry) claim(name, run string) (*ownership, error) {
	who := fmt.Sprintf("CreateStream(%q)", name)
	path := filepath.Join(t.lockDir(), "streams", filepath.FromSlash(name), "writer.lock")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("%s: the lock file's directory: %w", who, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("%s: the lock file: %w", who, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		holder := make([]byte, 4096)
		n, _ := f.ReadAt(holder, 0)
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.New(lockRefusal(who, path, holder[:n]))
		}
		return nil, fmt.Errorf("%s: the lock on %s: %w", who, path, err)
	}
	host := localIP()
	f.Truncate(0)
	f.WriteAt(lockRecord(os.Getpid(), host, t.flight.port, run), 0)
	owned := &ownership{path: path, f: f}
	if t.tickets == nil {
		return owned, nil
	}
	ticket, err := t.tickets.get(name)
	if err != nil {
		// an unreachable store at start never delays the stream: this
		// server's lock still holds
		t.log.Warn(ownershipUnread(name, t.root, err))
		return owned, nil
	}
	decision, message := ownershipDecision(who, name, t.root, ticket, host)
	switch decision {
	case ownRefused:
		owned.release()
		return nil, errors.New(message)
	case ownTakeOver:
		t.log.Warn(message)
	}
	return owned, nil
}
