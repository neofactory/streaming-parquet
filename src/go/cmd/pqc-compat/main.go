// pqc-compat: the Go side of the Python<->Go cross-compatibility
// suite (run_go_compat.py drives it).
//
//	pqc-compat write <dir> <name> <rows>   write groups + a tail
//	    checkpoint, then EXIT WITHOUT CLOSE (a crashed writer:
//	    chunks + checkpoint left for anyone to finalize)
//	pqc-compat finalize <dir> <name>       finalize a checkpoint
//	    (whoever wrote it)
//	pqc-compat read <dir> <file>           read a finished file OR a
//	    live checkpoint; print "<rows> <sum-of-n>"
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/pqc"
)

type Row struct {
	N int64   `parquet:"n"`
	X float64 `parquet:"x"`
	S string  `parquet:"s"`
}

func makeRows(start, count int) []Row {
	rows := make([]Row, count)
	for i := range rows {
		n := start + i
		rows[i] = Row{N: int64(n), X: float64(n) * 0.5,
			S: fmt.Sprintf("row-%d", n)}
	}
	return rows
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "pqc-compat:", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: pqc-compat write|finalize|read ...")
		os.Exit(2)
	}
	sink, err := pqc.NewDirSink(os.Args[2])
	if err != nil {
		die(err)
	}
	switch os.Args[1] {
	case "write":
		name := os.Args[3]
		total, err := strconv.Atoi(os.Args[4])
		if err != nil {
			die(err)
		}
		w, err := pqc.NewWriter(name, sink, 32*1024, true)
		if err != nil {
			die(err)
		}
		i := 0
		for i < total-40 { // final groups, 500 rows each (last partial)
			n := total - 40 - i
			if n > 500 {
				n = 500
			}
			if _, err := pqc.WriteRows(w, makeRows(i, n)); err != nil {
				die(err)
			}
			i += n
		}
		// the last 40 rows ride as a provisional tail; then "crash"
		if _, err := pqc.CheckpointRows(w, makeRows(i, total-i)); err != nil {
			die(err)
		}
		w.Free()
	case "finalize":
		status, err := pqc.Finalize(os.Args[3], sink)
		if err != nil {
			die(err)
		}
		fmt.Println(status)
	case "read":
		name := os.Args[3]
		var file []byte
		if sink.Exists(name) {
			file, err = sink.Get(name, 0, -1)
		} else {
			var r *pqc.Reader
			var vs []pqc.Version
			vs, err = pqc.CkptVersions(sink, name)
			if err == nil && len(vs) == 0 {
				err = fmt.Errorf("%s: neither the file nor a checkpoint", name)
			}
			if err == nil {
				r, err = pqc.OpenReader(vs[len(vs)-1].Object, sink)
			}
			if err == nil {
				file, err = r.Compose()
			}
		}
		if err != nil {
			die(err)
		}
		rows, err := pqc.ReadRows[Row](file)
		if err != nil {
			die(err)
		}
		var sum int64
		for i, r := range rows {
			if r.N != int64(i) {
				die(fmt.Errorf("row %d: n=%d (content wrong)", i, r.N))
			}
			sum += r.N
		}
		fmt.Printf("%d %d\n", len(rows), sum)
	default:
		os.Exit(2)
	}
}
