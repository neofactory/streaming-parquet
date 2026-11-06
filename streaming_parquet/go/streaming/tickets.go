package streaming

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Ticket is the discovery record, wire-compatible with the Python
// module: one JSON object at <root>/tickets/<name>.json, rewritten
// every heartbeat with a fresh Ts; deleted on clean close. A missing
// ticket means "no publisher"; a stale Ts means "dead publisher".
type Ticket struct {
	Name string  `json:"name"`
	Host string  `json:"host"`
	Port int     `json:"port"`
	Ts   float64 `json:"ts"`
	Run  string  `json:"run,omitempty"` // the writer's start time, UTC
	//                 text to the microsecond; changes on respawn, so
	//                 subscribers resync instead of livelocking on the
	//                 restarted seq axis; every file records it as sp_run
	Archive string `json:"archive,omitempty"` // where the history lives
}

const (
	heartbeatInterval = 60 * time.Second // readers call it dead after 2.25 min (staleThresholdS)
	// a ticket older than this is a dead publisher, not a live one
	staleThresholdS = 135.0
	// how often a feeder re-reads the ticket while unconnected
	ticketPollInterval = time.Second
)

// ticketStore is the discovery backend behind a root URI: file roots
// (file:// or a bare path) and s3://bucket[/prefix] roots (tickets_s3.go),
// the same two the Python module serves. Any other scheme is a loud
// error — attach with a direct Ticket instead.
type ticketStore interface {
	put(name string, t Ticket) error
	get(name string) (*Ticket, error) // nil, nil = no ticket there
	delete(name string) error
	list() ([]string, error)
}

func newTicketStore(root string) (ticketStore, error) {
	if strings.HasPrefix(root, "file://") {
		return &fileStore{base: filepath.Join(
			strings.TrimPrefix(root, "file://"), "tickets")}, nil
	}
	if bucket, prefix, ok := parseS3Root(root); ok {
		return newDefaultS3Store(bucket, prefix)
	}
	if strings.Contains(root, "://") {
		return nil, fmt.Errorf("ticket root %q: scheme not implemented "+
			"(file://, s3:// and bare paths; use AttachOptions."+
			"Ticket for direct dialing)", root)
	}
	return &fileStore{base: filepath.Join(root, "tickets")}, nil
}

// fileStore keeps tickets as files under <dir>/tickets/. Writes are
// atomic (temp + rename) so a poller never reads a half ticket.
type fileStore struct{ base string }

func (f *fileStore) path(name string) string {
	return filepath.Join(f.base, filepath.FromSlash(name)+".json")
}

func (f *fileStore) put(name string, t Ticket) error {
	path := f.path(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (f *fileStore) get(name string) (*Ticket, error) {
	b, err := os.ReadFile(f.path(name))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var t Ticket
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("ticket %s: %w", name, err)
	}
	return &t, nil
}

func (f *fileStore) delete(name string) error {
	err := os.Remove(f.path(name))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (f *fileStore) list() ([]string, error) {
	var names []string
	err := filepath.WalkDir(f.base,
		func(path string, d os.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".json") {
				return nil
			}
			rel, err := filepath.Rel(f.base, path)
			if err != nil {
				return err
			}
			names = append(names,
				filepath.ToSlash(strings.TrimSuffix(rel, ".json")))
			return nil
		})
	return names, err
}

// runStart returns a run's identity: the time the writer started the
// stream, UTC, to the microsecond, as text of fixed width (sorts in time
// order) -- the same form the Python module writes.
func runStart() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
}

// localIP returns the address other machines can reach us at (same
// approach as the Python module: the source address of an outbound
// probe; falls back to loopback).
func localIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
