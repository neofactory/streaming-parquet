package streaming

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Ticket is a stream's ticket: one JSON object at
// <root>/tickets/<name>.json, written by the stream's writer and
// deleted on a clean close. A missing ticket means no writer; a stale
// one means a writer that died. Its members, their kinds, its text and
// the stale rule are the stream core's (sp.h is the spec); this struct
// is the Go form. A text member that is "" and a number that is 0 are
// members the writer did not know; StoredRow is a pointer because -1 is
// a value ("no row stored").
type Ticket struct {
	Name string
	Host string
	Port int
	Ts   float64 // when the ticket was written
	Run  string  // the writer's start time, UTC to the microsecond;
	//              every file it writes records it as sp_run

	BeatSeconds       float64 // the longest gap between two writes of the ticket
	StaleAfter        float64 // seconds after Ts at which a reader calls the writer dead
	CheckpointSeconds float64
	ShipSeconds       float64
	StoredRow         *int64 // the newest row of this run the store holds; -1 = none
	StoredAt          float64
	Files             []TicketFile // the files not finished in the store
	KeepSeconds       float64
	CatalogEtag       string
	Revision          int64 // counts this run's writes of the ticket

	Archive string // where the files are; writers before 2026-11 wrote it
}

// TicketFile is one file not finished in the store: the path of its
// newest checkpoint version, the stream row number of its first row,
// the rows in its finished row groups, and its rows. A nil number is
// one the writer did not know.
type TicketFile struct {
	Checkpoint string
	FirstRow   *int64
	FinalRows  *int64
	Rows       *int64
}

const (
	// the beat: the ticket is rewritten this often; readers call the
	// writer dead three beats after the last one (Ticket.StaleAfter)
	heartbeatInterval = 60 * time.Second
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
	b, err := ticketText(&t)
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
	t, err := ticketFromText(b)
	if err != nil {
		return nil, fmt.Errorf("ticket %s: %w", name, err)
	}
	return t, nil
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
