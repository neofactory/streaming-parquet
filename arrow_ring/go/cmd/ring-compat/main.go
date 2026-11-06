// ring-compat: the Go side of the Python<->Go cross-compatibility
// suite for arrow_ring (run_go_compat.py drives it).
//
//	ring-compat write-ipc <file> <rows>   fill a Go ring, pin a record
//	    set, write it as an Arrow IPC stream file (zero-copy export)
//	ring-compat read-ipc <file>           read a pyarrow-written IPC
//	    stream into a Go ring (AppendRecord), verify via row getters,
//	    print "<rows> <sum-of-n>"
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/neofactory/streaming-parquet/arrow_ring/go/ring"

	"github.com/apache/arrow-go/v18/arrow"
)

func schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "servo", Type: arrow.StructOf(
			arrow.Field{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "vel", Type: arrow.PrimitiveTypes.Int64},
		)},
	}, nil)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "ring-compat:", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: ring-compat write-ipc|read-ipc ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "write-ipc":
		rows, err := strconv.Atoi(os.Args[3])
		if err != nil {
			die(err)
		}
		r, err := ring.NewRing(schema(), int64(rows)*2, nil)
		if err != nil {
			die(err)
		}
		defer r.Close()
		n, _ := r.Leaf("n")
		x, _ := r.Leaf("x")
		s, _ := r.Leaf("s")
		pos, _ := r.Leaf("servo.pos")
		vel, _ := r.Leaf("servo.vel")
		for i := 0; i < rows; i++ {
			if _, err := r.RowBegin(time.Second); err != nil {
				die(err)
			}
			r.SetI64(n, int64(i))
			r.SetF64(x, float64(i)*0.5)
			r.SetString(s, fmt.Sprintf("row-%d", i))
			r.SetI64(pos, int64(i)*10)
			r.SetI64(vel, int64(-i))
		}
		if _, err := r.Commit(); err != nil {
			die(err)
		}
		rs, err := r.StaticRelation(time.Second)
		if err != nil {
			die(err)
		}
		defer rs.Release()
		recs, err := rs.Records()
		if err != nil {
			die(err)
		}
		f, err := os.Create(os.Args[2])
		if err != nil {
			die(err)
		}
		w := ipc.NewWriter(f, ipc.WithSchema(schema()))
		for _, rec := range recs {
			if err := w.Write(rec); err != nil {
				die(err)
			}
			rec.Release()
		}
		if err := w.Close(); err != nil {
			die(err)
		}
		if err := f.Close(); err != nil {
			die(err)
		}
	case "read-ipc":
		f, err := os.Open(os.Args[2])
		if err != nil {
			die(err)
		}
		rd, err := ipc.NewReader(f)
		if err != nil {
			die(err)
		}
		r, err := ring.NewRing(rd.Schema(), 1<<16, nil)
		if err != nil {
			die(err)
		}
		defer r.Close()
		for rd.Next() {
			if err := r.AppendRecord(rd.RecordBatch()); err != nil {
				die(err)
			}
		}
		rd.Release()
		n, _ := r.Leaf("n")
		s, _ := r.Leaf("s")
		pos, _ := r.Leaf("servo.pos")
		total := r.NewestSeq() + 1
		var sum int64
		for seq := int64(0); seq < total; seq++ {
			v, _, err := r.GetI64(seq, n)
			if err != nil || v != seq {
				die(fmt.Errorf("n@%d = %d (%v)", seq, v, err))
			}
			b, _, err := r.GetBytes(seq, s)
			if err != nil || string(b) != fmt.Sprintf("row-%d", seq) {
				die(fmt.Errorf("s@%d = %q (%v)", seq, b, err))
			}
			p, _, err := r.GetI64(seq, pos)
			if err != nil || p != seq*10 {
				die(fmt.Errorf("servo.pos@%d = %d (%v)", seq, p, err))
			}
			sum += v
		}
		fmt.Printf("%d %d\n", total, sum)
	default:
		os.Exit(2)
	}
}
