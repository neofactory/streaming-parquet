package pqc

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Source is one assembly input: Offset/Length of -1 mean the whole
// object. The final source may be any length; earlier ones must
// satisfy the sink's part rules (S3: >= 5 MiB).
type Source struct {
	Name   string
	Offset int64
	Length int64
}

// Sink is where chunks, checkpoints, and finished files live — the
// same contract as the Python sinks (DirSink / S3Sink).
type Sink interface {
	Put(name string, data []byte) error
	Get(name string, offset, length int64) ([]byte, error)
	Size(name string) (int64, error)
	Exists(name string) bool
	Delete(name string) error
	List(prefix string) ([]string, error)
	Assemble(dest string, sources []Source) error
	PartMin() uint64
	Sub(prefix string) Sink // the same sink rooted deeper (streams/<name>/)
	URI() string            // where this sink's names resolve, for tickets and logs
}

// DirSink is a local directory: atomic replace via rename, assembly
// by concatenation, fsync before every rename (the power-loss story).
type DirSink struct{ Root string }

func NewDirSink(root string) (*DirSink, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &DirSink{Root: root}, nil
}

func (d *DirSink) PartMin() uint64 { return 0 }

// Sub roots a DirSink deeper; the directory is made on first write.
func (d *DirSink) Sub(prefix string) Sink {
	return &DirSink{Root: filepath.Join(d.Root, filepath.FromSlash(strings.Trim(prefix, "/")))}
}

func (d *DirSink) URI() string {
	abs, err := filepath.Abs(d.Root)
	if err != nil {
		return d.Root
	}
	return abs
}

func (d *DirSink) path(name string) (string, error) {
	p := filepath.Join(d.Root, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	return p, nil
}

func writeDurable(path string, write func(*os.File) error) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := write(f); err == nil {
		err = f.Sync()
	} else {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (d *DirSink) Put(name string, data []byte) error {
	p, err := d.path(name)
	if err != nil {
		return err
	}
	return writeDurable(p, func(f *os.File) error {
		_, err := f.Write(data)
		return err
	})
}

func (d *DirSink) Get(name string, offset, length int64) ([]byte, error) {
	p := filepath.Join(d.Root, name)
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	if offset > int64(len(b)) {
		return nil, fmt.Errorf("%s: offset past end", name)
	}
	b = b[offset:]
	if length >= 0 && length < int64(len(b)) {
		b = b[:length]
	}
	return b, nil
}

func (d *DirSink) Size(name string) (int64, error) {
	st, err := os.Stat(filepath.Join(d.Root, name))
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (d *DirSink) Exists(name string) bool {
	_, err := os.Stat(filepath.Join(d.Root, name))
	return err == nil
}

func (d *DirSink) Delete(name string) error {
	err := os.Remove(filepath.Join(d.Root, name))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (d *DirSink) List(prefix string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(d.Root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			if p == d.Root && errors.Is(err, fs.ErrNotExist) {
				return nil // nothing written here yet: lists empty, as an S3 prefix does
			}
			return err
		}
		if e.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(d.Root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, prefix) && !strings.HasSuffix(rel, ".tmp") {
			out = append(out, rel)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func (d *DirSink) Assemble(dest string, sources []Source) error {
	p, err := d.path(dest)
	if err != nil {
		return err
	}
	return writeDurable(p, func(f *os.File) error {
		for _, s := range sources {
			off := s.Offset
			if off < 0 {
				off = 0
			}
			b, err := d.Get(s.Name, off, s.Length)
			if err != nil {
				return err
			}
			if _, err := f.Write(b); err != nil {
				return err
			}
		}
		return nil
	})
}
