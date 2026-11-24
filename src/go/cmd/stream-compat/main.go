// stream-compat: the Go side of the Python<->Go cross-compatibility
// suite for streaming_parquet (run_go_streaming_compat.py drives it).
//
//	stream-compat serve <broker> <name> <rows> <hold_s>
//	    create a stream, announce by root ticket, write rows in
//	    bursts (100 rows / 20 ms) publishing each, then hold the
//	    stream open <hold_s> seconds and exit cleanly
//	stream-compat attach <broker> <name> <min_rows> [pqdir label]
//	    attach by root discovery, optionally StartParquet(label)
//	    immediately (pre-connect arming), wait until the local ring's
//	    newest row is at least <min_rows>-1, verify contiguity of the
//	    received window, print "<first> <last>"; with pqdir, stop the
//	    recording and print "file=<path>" too
package main

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/neofactory/streaming-parquet/src/go/streaming"
)

func schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
}

// the rich-type schema for the cross-language type matrix: nullable
// float64 with NaN, nullable unicode strings, timestamps, int64 lists
func schemaRich() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "t", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}, nil)
}

func richF(n int64) (float64, bool) { // value, isNull
	if n%5 == 0 {
		return 0, true
	}
	if n%7 == 3 {
		return math.NaN(), false
	}
	return float64(n) * 0.5, false
}

func richS(n int64) (string, bool) {
	if n%4 == 0 {
		return "", true
	}
	return fmt.Sprintf("u\u2713%d", n), false
}

// numeric-only twin for cross-generation tests (the v1 module is
// numpy-based; the production streams it served were numeric)
func schemaNX() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
}

// waitRows waits until the reader has the row numbered minRows-1, and
// answers every row it holds in memory as one table.
func waitRows(a *streaming.Attached, minRows int) arrow.Table {
	if err := a.WaitConnected(15 * time.Second); err != nil {
		die(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		newest, ok := a.NewestRow()
		if ok && newest.RowNumber()+1 >= int64(minRows) {
			break
		}
		if time.Now().After(deadline) {
			die(fmt.Errorf("timeout: the newest row is %d, want >= %d "+
				"(state %s)", newest.RowNumber(), minRows-1, a.State()))
		}
		time.Sleep(20 * time.Millisecond)
	}
	table, err := a.ArrowTable()
	if err != nil {
		die(err)
	}
	return table
}

// rowsOf calls each for every row of the table, the oldest first.
func rowsOf(t arrow.Table, each func(rec arrow.RecordBatch, i int)) {
	rdr := array.NewTableReader(t, 0)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			each(rec, i)
		}
	}
}

func column(rec arrow.RecordBatch, name string) arrow.Array {
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		die(fmt.Errorf("the table has no column %q", name))
	}
	return rec.Column(idx[0])
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "stream-compat:", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: stream-compat serve|attach ...")
		os.Exit(2)
	}
	// every line of the library, for the test that reads this program's
	// standard error
	logger := slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := streaming.SetLogger(logger); err != nil {
		die(err)
	}
	defer streaming.Close()
	store := os.Args[2]
	name := os.Args[3]
	switch os.Args[1] {
	case "serve-rich":
		rows, _ := strconv.Atoi(os.Args[4])
		hold, _ := strconv.ParseFloat(os.Args[5], 64)
		s, err := streaming.CreateStream(name, schemaRich(),
			streaming.StreamOptions{Store: store, Memory: &streaming.MemoryOptions{Rows: int64(rows) * 2}})
		if err != nil {
			die(err)
		}
		nl, _ := s.Column("n")
		fl, _ := s.Column("f")
		sl, _ := s.Column("s")
		tl, _ := s.Column("t")
		gl, _ := s.Column("tags")
		time.Sleep(1500 * time.Millisecond)
		for i := int64(0); i < int64(rows); {
			for k := 0; k < 50 && i < int64(rows); k++ {
				row, err := s.MutableRow(time.Second)
				if err != nil {
					die(err)
				}
				row.SetI64(nl, i)
				if v, isNull := richF(i); isNull {
					row.SetNull(fl)
				} else {
					row.SetF64(fl, v)
				}
				if v, isNull := richS(i); isNull {
					row.SetNull(sl)
				} else {
					row.SetString(sl, v)
				}
				row.SetI64(tl, i*1_000_000)
				elems := make([]byte, 0, 24)
				for j := int64(0); j < i%3; j++ {
					var b [8]byte
					for x := 0; x < 8; x++ {
						b[x] = byte(uint64(i) >> (8 * x))
					}
					elems = append(elems, b[:]...)
				}
				row.SetList(gl, elems, i%3)
				i++
			}
			if _, err := s.Commit(); err != nil {
				die(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(time.Duration(hold * float64(time.Second)))
		fmt.Println("served", rows)
	case "attach-rich":
		minRows, _ := strconv.Atoi(os.Args[4])
		a, err := streaming.Connect(name, streaming.ConnectOptions{Store: store, Memory: &streaming.MemoryOptions{Rows: 20000}})
		if err != nil {
			die(err)
		}
		defer a.Close()
		table := waitRows(a, minRows)
		defer table.Release()
		nulls, nans := 0, 0
		lo, hi := int64(-1), int64(-1)
		rowsOf(table, func(rec arrow.RecordBatch, i int) {
			n := column(rec, "n").(*array.Int64).Value(i)
			if lo < 0 {
				lo = n
			} else if n != hi+1 {
				die(fmt.Errorf("gap: n %d -> %d", hi, n))
			}
			hi = n
			fcol := column(rec, "f").(*array.Float64)
			ef, en := richF(n)
			if fcol.IsNull(i) != en {
				die(fmt.Errorf("f null mismatch at n=%d", n))
			}
			if !en {
				f := fcol.Value(i)
				if math.IsNaN(ef) != math.IsNaN(f) ||
					(!math.IsNaN(ef) && f != ef) {
					die(fmt.Errorf("f@%d = %v want %v", n, f, ef))
				}
				if math.IsNaN(f) {
					nans++
				}
			} else {
				nulls++
			}
			scol := column(rec, "s")
			es, esn := richS(n)
			if scol.IsNull(i) != esn || (!esn && scol.(array.StringLike).Value(i) != es) {
				die(fmt.Errorf("s@%d = %q null=%v want %q null=%v",
					n, scol.ValueStr(i), scol.IsNull(i), es, esn))
			}
			tv := int64(column(rec, "t").(*array.Timestamp).Value(i))
			if tv != n*1_000_000 {
				die(fmt.Errorf("t@%d = %d", n, tv))
			}
		})
		// a row's number in the stream is its n: the writer counted from 0
		if newest, ok := a.NewestRow(); !ok || newest.RowNumber() < hi {
			die(fmt.Errorf("the newest row is before the table's last, n=%d", hi))
		}
		fmt.Printf("%d %d %d %d\n", lo, hi, nulls, nans)
	case "serve-nx":
		rows, _ := strconv.Atoi(os.Args[4])
		hold, _ := strconv.ParseFloat(os.Args[5], 64)
		s, err := streaming.CreateStream(name, schemaNX(),
			streaming.StreamOptions{Store: store, Memory: &streaming.MemoryOptions{Rows: int64(rows) * 2}})
		if err != nil {
			die(err)
		}
		nl, _ := s.Column("n")
		xl, _ := s.Column("x")
		time.Sleep(1500 * time.Millisecond)
		for i := 0; i < rows; {
			for k := 0; k < 100 && i < rows; k++ {
				row, err := s.MutableRow(time.Second)
				if err != nil {
					die(err)
				}
				row.SetI64(nl, int64(i))
				row.SetF64(xl, float64(i)*0.5)
				i++
			}
			if _, err := s.Commit(); err != nil {
				die(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(time.Duration(hold * float64(time.Second)))
		fmt.Println("served", rows)
	case "attach-nx":
		minRows, _ := strconv.Atoi(os.Args[4])
		a, err := streaming.Connect(name, streaming.ConnectOptions{Store: store})
		if err != nil {
			die(err)
		}
		defer a.Close()
		table := waitRows(a, minRows)
		defer table.Release()
		firstN, lastN := int64(-1), int64(-1)
		rowsOf(table, func(rec arrow.RecordBatch, i int) {
			v := column(rec, "n").(*array.Int64).Value(i)
			if firstN < 0 {
				firstN = v
			} else if v != lastN+1 {
				die(fmt.Errorf("gap: n %d -> %d", lastN, v))
			}
			lastN = v
			if x := column(rec, "x").(*array.Float64).Value(i); x != float64(v)*0.5 {
				die(fmt.Errorf("x@n=%d is %v", v, x))
			}
		})
		fmt.Printf("%d %d\n", firstN, lastN)
	case "serve":
		rows, _ := strconv.Atoi(os.Args[4])
		hold, _ := strconv.ParseFloat(os.Args[5], 64)
		o := streaming.StreamOptions{Store: store, Memory: &streaming.MemoryOptions{Rows: int64(rows) * 2}}
		if len(os.Args) > 6 && os.Args[6] == "pq" {
			// files under the root, where a reader of the root finds them
			o.Parquet = &streaming.ParquetOptions{BaseDir: os.Args[2],
				RowGroupRows: 500, CheckpointSeconds: 0.1}
		}
		s, err := streaming.CreateStream(name, schema(), o)
		if err != nil {
			die(err)
		}
		nl, _ := s.Column("n")
		xl, _ := s.Column("x")
		sl, _ := s.Column("s")
		time.Sleep(1500 * time.Millisecond) // let subscribers find us
		for i := 0; i < rows; {
			for k := 0; k < 100 && i < rows; k++ {
				row, err := s.MutableRow(time.Second)
				if err != nil {
					die(err)
				}
				row.SetI64(nl, int64(i))
				row.SetF64(xl, float64(i)*0.5)
				row.SetString(sl, fmt.Sprintf("row-%d", i))
				i++
			}
			if _, err := s.Commit(); err != nil {
				die(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(time.Duration(hold * float64(time.Second)))
		fmt.Println("served", rows)
	case "attach":
		minRows, _ := strconv.Atoi(os.Args[4])
		var pqdir, label string
		if len(os.Args) > 6 {
			pqdir, label = os.Args[5], os.Args[6]
		}
		var parquet *streaming.ParquetOptions
		if pqdir != "" {
			parquet = &streaming.ParquetOptions{BaseDir: pqdir,
				RowGroupRows: 500, CheckpointSeconds: 0.1}
		}
		a, err := streaming.Connect(name, streaming.ConnectOptions{Store: store, Parquet: parquet})
		if err != nil {
			die(err)
		}
		defer a.Close()
		if pqdir != "" {
			if err := a.StartParquet(label); err != nil {
				die(err)
			}
		}
		table := waitRows(a, minRows)
		defer table.Release()
		firstN, lastN := int64(-1), int64(-1)
		last := ""
		rowsOf(table, func(rec arrow.RecordBatch, i int) {
			v := column(rec, "n").(*array.Int64).Value(i)
			if firstN < 0 {
				firstN = v
			} else if v != lastN+1 {
				die(fmt.Errorf("gap: n %d -> %d", lastN, v))
			}
			lastN = v
			last = column(rec, "s").(array.StringLike).Value(i)
		})
		if last != fmt.Sprintf("row-%d", lastN) {
			die(fmt.Errorf("s of the last row (n=%d) is %q", lastN, last))
		}
		out := fmt.Sprintf("%d %d", firstN, lastN)
		if pqdir != "" {
			if err := a.StopParquet(); err != nil {
				die(err)
			}
			dir := filepath.Join(pqdir, "streams", filepath.FromSlash(name))
			entries, err := os.ReadDir(dir)
			if err != nil {
				die(err)
			}
			for _, e := range entries {
				if strings.Contains(e.Name(), label) &&
					strings.HasSuffix(e.Name(), ".parquet") {
					out += " file=" + filepath.Join(dir, e.Name())
				}
			}
		}
		fmt.Println(out)
	default:
		os.Exit(2)
	}
}
