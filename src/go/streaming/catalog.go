package streaming

// The catalog of a stream's finished files (sp.h: THE CATALOG): one
// object in the store, <store>/streams/<name>/catalog.sqlite, a DuckLake
// 1.0 catalog in SQLite. The core writes it; this file does the reads
// and writes, the same way the Python host does (_catalog.py).
//
// sp_catalog.c is a CHECKED-IN COPY of ../../core/sp_catalog.c, as
// sp_core.c is of sp.c. It needs the system's SQLite library and header
// (Ubuntu: libsqlite3-dev).
//
// A pass brings the catalog's bytes from the store, lists the stream's
// finished files, reads the footer of each file the catalog lacks,
// gives it to the core, and publishes the new bytes only if the stored
// object is still the version it read. The files are the source of
// truth: every pass adds each finished file the catalog does not list.
//
// A finished file stays in the ticket, with its checkpoints in the
// store, until a published catalog lists it. Then the ticket is written
// without it and with the catalog's version, and its checkpoints are
// deleted once the keep time has passed.
//
// A pass runs on a goroutine of its own. A stream has at most one pass
// running; a file that finishes meanwhile owes another. Close waits a
// few seconds for the pass, then stops it between two files: nothing is
// published, and the next start's pass does the work.

/*
#cgo LDFLAGS: -lsqlite3
#include <stdlib.h>
#include "sp.h"
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/neofactory/streaming-parquet/src/go/pqc"
)

// catalogObject is the catalog's name in the stream's folder.
const catalogObject = "catalog.sqlite"

// catalogTailBytes: a file's footer is looked for in this many of its
// last bytes; a longer footer takes a second read.
const catalogTailBytes = 64 * 1024

// catalogCloseWait: how long Close waits for a pass before it stops it.
const catalogCloseWait = 3 * time.Second

var errCatalogStopped = errors.New("stopped by close")

type catalog struct {
	name   string
	folder string // the stream's folder in the store, ending in "/"
	sink   pqc.Sink
	vs     pqc.Versioned
	store  *storeState
	log    *slog.Logger
	// writeTicket writes the stream's ticket now; false when it could
	// not. Nil: the stream has no ticket.
	writeTicket func() bool

	mu         sync.Mutex
	wanted     bool          // a pass is owed
	running    bool          // a pass goroutine is alive
	stopped    bool          // Close: no pass starts, a running one stops
	done       chan struct{} // closed when the running pass ends
	etag       string        // the published catalog's version; "" = none
	ticketEtag string        // the version the ticket was last given
	files      int           // files the published catalog lists
	errText    string        // the last pass's failure; "" when fine
	awaiting   map[string]bool
	dropOwed   bool // files left the record; the ticket is not yet written without them

	// The dated copies of the catalog, catalog.<YYYYMMDD-HHMMSS>.sqlite
	// beside it. When one is due is the core's rule (sp.h: DATED COPIES OF
	// THE CATALOG). Under mu, but for the pass goroutine's own steps.
	copySeconds float64 // the interval; negative = no copies
	copyScanned bool    // the folder was looked at for the newest copy
	newestCopy  string  // its name, "" for none
	copyWall    float64 // the time its name says
	copySeen    string  // the catalog's version at the last copy or check

	cat *C.SpCatalog // the pass goroutine's alone
}

func newCatalog(name, folder string, sink pqc.Sink, store *storeState, writeTicket func() bool, log *slog.Logger) *catalog {
	vs, ok := sink.(pqc.Versioned)
	if !ok {
		return nil // a sink that cannot publish by version: no catalog
	}
	return &catalog{name: name, folder: folder, sink: sink, vs: vs, store: store,
		writeTicket: writeTicket, awaiting: map[string]bool{}, wanted: true, log: log,
		copySeconds: defaultCatalogCopySeconds}
}

// defaultCatalogCopySeconds: a dated copy a day (the Python
// ParquetOptions.catalog_copy_seconds default).
const defaultCatalogCopySeconds = 86400.0

// setCopySeconds sets the interval of the dated copies: 0 = a day,
// negative = no copies.
func (c *catalog) setCopySeconds(seconds float64) {
	if seconds == 0 {
		seconds = defaultCatalogCopySeconds
	}
	c.mu.Lock()
	c.copySeconds = seconds
	c.mu.Unlock()
}

// copyDue: a dated copy is due, by the core's rule. Caller holds mu. No
// I/O.
func (c *catalog) copyDue() bool {
	if c.copySeconds < 0 || c.etag == "" {
		return false
	}
	has, changed := C.int(0), C.int(0)
	if c.newestCopy != "" {
		has = 1
	}
	if c.etag != c.copySeen {
		changed = 1
	}
	return C.sp_catalog_copy_due(C.double(c.copySeconds), has, C.double(c.copyWall), changed) != 0
}

const copyStamp = "20060102-150405"

// copyPass makes a dated copy of the published catalog when one is due
// and the catalog differs from the newest copy. The pass goroutine's.
func (c *catalog) copyPass() error {
	c.mu.Lock()
	off, scanned := c.copySeconds < 0, c.copyScanned
	c.mu.Unlock()
	if off {
		return nil
	}
	if !scanned {
		names, err := c.sink.List("catalog.")
		if err != nil {
			return err
		}
		newest, wall := "", 0.0
		for _, n := range names {
			stamp, ok := strings.CutPrefix(n, "catalog.")
			stamp, ok2 := strings.CutSuffix(stamp, ".sqlite")
			if !ok || !ok2 {
				continue
			}
			when, err := time.ParseInLocation(copyStamp, stamp, time.Local)
			if err != nil {
				continue // catalog.sqlite itself, or another file
			}
			if w := float64(when.Unix()); newest == "" || w > wall {
				newest, wall = n, w
			}
		}
		c.mu.Lock()
		c.copyScanned, c.newestCopy, c.copyWall = true, newest, wall
		c.mu.Unlock()
	}
	c.mu.Lock()
	due, etag, newest := c.copyDue(), c.etag, c.newestCopy
	c.mu.Unlock()
	if !due {
		return nil
	}
	now, _, err := c.vs.GetVersion(catalogObject)
	if err != nil {
		return err
	}
	if newest != "" {
		was, _, err := c.vs.GetVersion(newest)
		if err != nil {
			return err
		}
		if bytes.Equal(now, was) {
			c.mu.Lock()
			c.copySeen = etag // unchanged since that copy
			c.mu.Unlock()
			return nil
		}
	}
	when := time.Now()
	name := "catalog." + when.Format(copyStamp) + ".sqlite"
	if err := c.sink.Put(name, now); err != nil {
		return err
	}
	c.mu.Lock()
	c.newestCopy, c.copyWall, c.copySeen = name, float64(when.Unix()), etag
	c.mu.Unlock()
	c.log.Info(fmt.Sprintf("[%s] catalog: dated copy %s%s", c.name, c.folder, name))
	return nil
}

func (c *catalog) state() (files int, errText string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.files, c.errText
}

// await: the file is finished in the store, its checkpoints kept. It
// stays in the ticket until the catalog lists it.
func (c *catalog) await(file string) {
	c.mu.Lock()
	c.awaiting[file] = true
	c.wanted = true
	c.mu.Unlock()
	c.store.awaits(file)
	c.poke()
}

// poke starts a pass if one is owed and none runs.
func (c *catalog) poke() {
	c.mu.Lock()
	if c.running || c.stopped || !(c.wanted || c.copyDue()) {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.done = make(chan struct{})
	c.mu.Unlock()
	go c.run()
}

func (c *catalog) run() {
	for {
		c.mu.Lock()
		c.wanted = false
		c.mu.Unlock()
		names, err := c.pass()
		if err == nil {
			c.listed(names)
			err = c.copyPass()
		}
		c.mu.Lock()
		switch {
		case err == nil:
			if c.errText != "" {
				c.log.Info(fmt.Sprintf("[%s] catalog recovered", c.name))
			}
			c.errText = ""
		case errors.Is(err, errCatalogStopped):
			c.log.Info(fmt.Sprintf("[%s] catalog pass %v; the next start's "+
				"pass finishes it", c.name, err))
		default:
			if c.errText == "" {
				c.log.Error(fmt.Sprintf("[%s] catalog failing: %v", c.name, err))
			}
			c.errText = err.Error()
			c.wanted = true // the next finished file or checkpoint tries again
		}
		again := err == nil && c.wanted && !c.stopped
		if !again {
			c.running = false
			close(c.done)
		}
		c.mu.Unlock()
		if !again {
			return
		}
	}
}

// drop forgets the catalog in memory: it may hold files never
// published, and the next pass starts again from the store's.
func (c *catalog) drop() {
	if c.cat != nil {
		C.sp_catalog_free(c.cat)
		c.cat = nil
	}
}

// pass answers the names of the files the published catalog lists.
func (c *catalog) pass() (map[string]bool, error) {
	var names map[string]bool
	var err error
	for try := 0; try < 3; try++ {
		names, err = c.passOnce()
		if err == nil {
			return names, nil
		}
		c.drop()
		if !errors.Is(err, pqc.ErrChanged) {
			return nil, err
		}
		c.log.Warn(fmt.Sprintf("[%s] catalog: %v since this writer's last "+
			"upload; fetching it again", c.name, err))
	}
	return nil, fmt.Errorf("the published catalog changed under three passes "+
		"in a row: another writer is updating it (%w)", err)
}

func (c *catalog) passOnce() (map[string]bool, error) {
	var r report
	if c.cat == nil {
		data, etag, err := c.vs.GetVersion(catalogObject)
		if err != nil {
			return nil, err
		}
		cf := C.CString(c.folder)
		var p *C.uint8_t
		if len(data) > 0 {
			p = (*C.uint8_t)(unsafe.Pointer(&data[0]))
		}
		rc := C.sp_catalog_open(p, C.size_t(len(data)), cf, &c.cat, &r.c)
		C.free(unsafe.Pointer(cf))
		if rc != 0 {
			c.cat = nil
			return nil, fmt.Errorf("%s%s is not a catalog this library reads: %v",
				c.folder, catalogObject, r.err())
		}
		c.mu.Lock()
		c.etag = etag
		c.mu.Unlock()
	}
	all, err := c.sink.List("")
	if err != nil {
		return nil, err
	}
	added := 0
	for _, n := range all {
		if !strings.HasSuffix(n, ".parquet") || strings.Contains(n, "/") {
			continue
		}
		cp := C.CString(c.folder + n)
		has := C.sp_catalog_lists(c.cat, cp) != 0
		C.free(unsafe.Pointer(cp))
		if has {
			continue
		}
		c.mu.Lock()
		stopped := c.stopped
		c.mu.Unlock()
		if stopped {
			return nil, fmt.Errorf("%w after %d file(s)", errCatalogStopped, added)
		}
		if err := c.add(n); err != nil {
			return nil, err
		}
		added++
	}
	names := map[string]bool{}
	count := int(C.sp_catalog_files(c.cat))
	for i := 0; i < count; i++ {
		var p *C.char
		if C.sp_catalog_file(c.cat, C.int64_t(i), &p, &r.c) != 0 {
			return nil, r.err()
		}
		path := C.GoString(p)
		C.sp_free(unsafe.Pointer(p))
		names[path[strings.LastIndexByte(path, '/')+1:]] = true
	}
	if added == 0 {
		c.mu.Lock()
		published := c.etag != ""
		c.files = count
		c.mu.Unlock()
		if !published {
			return map[string]bool{}, nil // no catalog in the store lists them
		}
		return names, nil
	}
	var n C.size_t
	var p *C.uint8_t
	if C.sp_catalog_bytes(c.cat, &p, &n, &r.c) != 0 {
		return nil, r.err()
	}
	data := C.GoBytes(unsafe.Pointer(p), C.int(n))
	C.sp_free(unsafe.Pointer(p))
	c.mu.Lock()
	etag := c.etag
	c.mu.Unlock()
	etag, err = c.vs.PutIfVersion(catalogObject, data, etag)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.etag, c.files = etag, count
	c.mu.Unlock()
	c.log.Info(fmt.Sprintf("[%s] catalog: listed %d file(s), %d in all; "+
		"published %s%s (%.1f MB)", c.name, added, count, c.folder,
		catalogObject, float64(len(data))/1e6))
	return names, nil
}

// add reads the footer of the file `name` and lists the file.
func (c *catalog) add(name string) error {
	var r report
	size, err := c.sink.Size(name)
	if err != nil {
		return err
	}
	off := size - catalogTailBytes
	if off < 0 {
		off = 0
	}
	tail, err := c.sink.Get(name, off, -1)
	if err != nil {
		return err
	}
	if len(tail) < 8 {
		return fmt.Errorf("%s%s: %d bytes is not a parquet file", c.folder, name, len(tail))
	}
	var length C.int64_t
	if C.sp_parquet_footer_length((*C.uint8_t)(unsafe.Pointer(&tail[0])),
		C.size_t(len(tail)), &length, &r.c) != 0 {
		return fmt.Errorf("%s%s: %v", c.folder, name, r.err())
	}
	n := int64(length)
	var footer []byte
	if n+8 <= int64(len(tail)) {
		footer = tail[int64(len(tail))-8-n : len(tail)-8]
	} else {
		footer, err = c.sink.Get(name, size-8-n, n)
		if err != nil {
			return err
		}
	}
	if int64(len(footer)) != n || n == 0 {
		return fmt.Errorf("%s%s: the footer is %d bytes and the store gave %d",
			c.folder, name, n, len(footer))
	}
	cp := C.CString(c.folder + name)
	defer C.free(unsafe.Pointer(cp))
	if C.sp_catalog_add(c.cat, cp, C.int64_t(size), (*C.uint8_t)(unsafe.Pointer(&footer[0])),
		C.size_t(len(footer)), &r.c) != 0 {
		return r.err()
	}
	return nil
}

// listed: a published catalog lists `names`. Each awaiting file among
// them leaves the ticket, the ticket is written, and only then do its
// checkpoints count as superseded.
func (c *catalog) listed(names map[string]bool) {
	c.mu.Lock()
	var files []string
	for n := range c.awaiting {
		if names[n] {
			files = append(files, n)
		}
	}
	newer := c.etag != c.ticketEtag
	c.ticketEtag = c.etag
	if c.store.listed(files, c.etag) {
		c.dropOwed = true
	}
	owed := c.dropOwed
	c.mu.Unlock()
	if owed || newer {
		if c.writeTicket != nil && !c.writeTicket() {
			return // ticket not written: delete nothing yet
		}
	}
	if owed {
		c.mu.Lock()
		c.dropOwed = false
		c.mu.Unlock()
		c.store.dropped()
	}
	c.discardDue()
}

// discardDue deletes the checkpoints of the files the ticket dropped at
// least the keep time ago.
func (c *catalog) discardDue() {
	for _, n := range c.store.due() {
		c.mu.Lock()
		mine := c.awaiting[n]
		c.mu.Unlock()
		if !mine {
			continue
		}
		if err := pqc.DiscardCheckpoints(n, c.sink); err != nil {
			c.log.Error(fmt.Sprintf("[%s] deleting the checkpoints of %s after "+
				"the ticket dropped it: %v; the next checkpoint retries", c.name, n, err))
			continue
		}
		c.store.discarded(n)
		c.mu.Lock()
		delete(c.awaiting, n)
		c.mu.Unlock()
	}
}

// finish: the stream is closing. The catalog lists the last file when
// it can within `wait`; otherwise the pass stops between two files.
func (c *catalog) finish(wait time.Duration) {
	c.poke()
	c.mu.Lock()
	done, running := c.done, c.running
	c.mu.Unlock()
	if running {
		select {
		case <-done:
		case <-time.After(wait):
		}
	}
	c.mu.Lock()
	c.stopped = true
	done, running = c.done, c.running
	c.mu.Unlock()
	if running {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			c.log.Warn(fmt.Sprintf("[%s] catalog: the pass did not stop in 30 s", c.name))
			return // the pass still holds the catalog in memory
		}
	}
	c.drop()
}
