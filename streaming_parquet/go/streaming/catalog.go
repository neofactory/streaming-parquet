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
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/pqc"
)

// catalogObject is the catalog's name in the stream's folder.
const catalogObject = "catalog.sqlite"

// catalogTailBytes: a file's footer is looked for in this many of its
// last bytes; a longer footer takes a second read.
const catalogTailBytes = 64 * 1024

// catalogCloseWait: how long Close waits for a pass before it stops it.
const catalogCloseWait = 3 * time.Second

const catalogErrLen = 512

var errCatalogStopped = errors.New("stopped by close")

type catalog struct {
	name   string
	folder string // the stream's folder in the store, ending in "/"
	sink   pqc.Sink
	vs     pqc.Versioned
	store  *storeState
	// writeTicket writes the stream's ticket now; false when it could
	// not. Nil: the stream has no ticket.
	writeTicket func() bool

	mu       sync.Mutex
	wanted   bool          // a pass is owed
	running  bool          // a pass goroutine is alive
	stopped  bool          // Close: no pass starts, a running one stops
	done     chan struct{} // closed when the running pass ends
	etag     string        // the published catalog's version; "" = none
	files    int           // files the published catalog lists
	errText  string        // the last pass's failure; "" when fine
	awaiting map[string]bool
	dropOwed bool // files left the record; the ticket is not yet written without them

	cat *C.SpCatalog // the pass goroutine's alone
}

func newCatalog(name, folder string, sink pqc.Sink, store *storeState, writeTicket func() bool) *catalog {
	vs, ok := sink.(pqc.Versioned)
	if !ok {
		return nil // a sink that cannot publish by version: no catalog
	}
	return &catalog{name: name, folder: folder, sink: sink, vs: vs, store: store,
		writeTicket: writeTicket, awaiting: map[string]bool{}, wanted: true}
}

// version is the published catalog's version, for the ticket.
func (c *catalog) version() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.etag
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
	if c.running || c.stopped || !c.wanted {
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
		}
		c.mu.Lock()
		switch {
		case err == nil:
			if c.errText != "" {
				fmt.Fprintf(os.Stderr, "[%s] catalog recovered\n", c.name)
			}
			c.errText = ""
		case errors.Is(err, errCatalogStopped):
			fmt.Fprintf(os.Stderr, "[%s] catalog pass %v; the next start's "+
				"pass finishes it\n", c.name, err)
		default:
			if c.errText == "" {
				fmt.Fprintf(os.Stderr, "[%s] catalog failing: %v\n", c.name, err)
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

func wallNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }

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
		fmt.Fprintf(os.Stderr, "[%s] catalog: %v since this writer's last "+
			"upload; fetching it again\n", c.name, err)
	}
	return nil, fmt.Errorf("the published catalog changed under three passes "+
		"in a row: another writer is updating it (%w)", err)
}

func (c *catalog) passOnce() (map[string]bool, error) {
	var eb [catalogErrLen]C.char
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
		c.cat = C.sp_catalog_open(p, C.size_t(len(data)), cf, C.double(wallNow()), &eb[0], catalogErrLen)
		C.free(unsafe.Pointer(cf))
		if c.cat == nil {
			return nil, fmt.Errorf("%s%s is not a catalog this library reads: %s",
				c.folder, catalogObject, C.GoString(&eb[0]))
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
		p := C.sp_catalog_file(c.cat, C.int64_t(i))
		if p == nil {
			break
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
	p := C.sp_catalog_bytes(c.cat, C.double(wallNow()), &n, &eb[0], catalogErrLen)
	if p == nil {
		return nil, fmt.Errorf("%s", C.GoString(&eb[0]))
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
	fmt.Fprintf(os.Stderr, "[%s] catalog: listed %d file(s), %d in all; "+
		"published %s%s (%.1f MB)\n", c.name, added, count, c.folder,
		catalogObject, float64(len(data))/1e6)
	return names, nil
}

// add reads the footer of the file `name` and lists the file.
func (c *catalog) add(name string) error {
	var eb [catalogErrLen]C.char
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
	n := int64(C.sp_parquet_footer_length((*C.uint8_t)(unsafe.Pointer(&tail[0])),
		C.size_t(len(tail)), &eb[0], catalogErrLen))
	if n < 0 {
		return fmt.Errorf("%s%s: %s", c.folder, name, C.GoString(&eb[0]))
	}
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
		C.size_t(len(footer)), &eb[0], catalogErrLen) != 0 {
		return fmt.Errorf("%s", C.GoString(&eb[0]))
	}
	return nil
}

// listed: a published catalog lists `names`. Each awaiting file among
// them leaves the ticket, the ticket is written, and only then do its
// checkpoints count as superseded.
func (c *catalog) listed(names map[string]bool) {
	c.mu.Lock()
	for n := range c.awaiting {
		if names[n] && c.store.listed(n) {
			c.dropOwed = true
		}
	}
	owed := c.dropOwed
	c.mu.Unlock()
	if owed {
		if c.writeTicket != nil && !c.writeTicket() {
			return // ticket not written: delete nothing yet
		}
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
			fmt.Fprintf(os.Stderr, "[%s] deleting the checkpoints of %s after "+
				"the ticket dropped it: %v; the next checkpoint retries\n", c.name, n, err)
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
			fmt.Fprintf(os.Stderr, "[%s] catalog: the pass did not stop in 30 s\n", c.name)
			return // the pass still holds the catalog in memory
		}
	}
	c.drop()
}
