package pqc

import (
	"bytes"

	"github.com/parquet-go/parquet-go"
)

// The typed convenience layer: Go structs in, mini parquet files out
// (parquet-go does the encoding; the C core never sees a Go type).
//
//	type Row struct {
//	    N int64   `parquet:"n"`
//	    X float64 `parquet:"x"`
//	    S string  `parquet:"s"`
//	}
//	w, _ := pqc.NewWriter("run.parquet", sink, 5<<20, true)
//	pqc.WriteRows(w, rows)          // final row group + checkpoint
//	pqc.CheckpointRows(w, pending)  // provisional tail rides
//	pqc.CloseTyped[Row](w)          // -> ordinary run.parquet

// EncodeRows encodes rows as one complete mini parquet file.
func EncodeRows[T any](rows []T) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// WriteRows appends rows as a final row group (implies a checkpoint
// when the writer was built with checkpointEveryWrite).
func WriteRows[T any](w *Writer, rows []T) (int64, error) {
	mini, err := EncodeRows(rows)
	if err != nil {
		return 0, err
	}
	return w.AddGroupMini(mini)
}

// CheckpointRows publishes a checkpoint whose provisional tail is
// `tail` (nil/empty = claim final rows only).
func CheckpointRows[T any](w *Writer, tail []T) (uint64, error) {
	if len(tail) == 0 {
		return w.Checkpoint(nil)
	}
	mini, err := EncodeRows(tail)
	if err != nil {
		return 0, err
	}
	return w.Checkpoint(mini)
}

// CloseTyped closes the writer; the type parameter supplies the
// schema for the empty-file case.
func CloseTyped[T any](w *Writer) error {
	empty, err := EncodeRows[T](nil)
	if err != nil {
		return err
	}
	return w.Close(empty)
}

// ReadRows decodes a complete parquet file's bytes (a finished file,
// or Reader.Compose() of a live one).
func ReadRows[T any](file []byte) ([]T, error) {
	return parquet.Read[T](bytes.NewReader(file), int64(len(file)))
}
