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
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/neofactory/streaming-parquet/streaming_parquet/go/streaming"
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

func die(err error) {
	fmt.Fprintln(os.Stderr, "stream-compat:", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: stream-compat serve|attach ...")
		os.Exit(2)
	}
	tel, err := streaming.Init(streaming.Config{Root: os.Args[2]})
	if err != nil {
		die(err)
	}
	defer tel.Close()
	name := os.Args[3]
	switch os.Args[1] {
	case "serve-rich":
		rows, _ := strconv.Atoi(os.Args[4])
		hold, _ := strconv.ParseFloat(os.Args[5], 64)
		s, err := tel.CreateStream(name, schemaRich(),
			streaming.StreamOptions{RingRows: int64(rows) * 2})
		if err != nil {
			die(err)
		}
		r := s.Ring()
		nl, _ := r.Leaf("n")
		fl, _ := r.Leaf("f")
		sl, _ := r.Leaf("s")
		tl, _ := r.Leaf("t")
		gl, _ := r.Leaf("tags")
		time.Sleep(1500 * time.Millisecond)
		for i := int64(0); i < int64(rows); {
			for k := 0; k < 50 && i < int64(rows); k++ {
				if _, err := r.RowBegin(time.Second); err != nil {
					die(err)
				}
				r.SetI64(nl, i)
				if v, isNull := richF(i); isNull {
					r.SetNull(fl)
				} else {
					r.SetF64(fl, v)
				}
				if v, isNull := richS(i); isNull {
					r.SetNull(sl)
				} else {
					r.SetString(sl, v)
				}
				r.SetI64(tl, i*1_000_000)
				elems := make([]byte, 0, 24)
				for j := int64(0); j < i%3; j++ {
					var b [8]byte
					for x := 0; x < 8; x++ {
						b[x] = byte(uint64(i) >> (8 * x))
					}
					elems = append(elems, b[:]...)
				}
				r.SetList(gl, elems, i%3)
				i++
			}
			if _, err := r.Commit(); err != nil {
				die(err)
			}
			if _, err := s.Publish(); err != nil {
				die(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(time.Duration(hold * float64(time.Second)))
		fmt.Println("served", rows)
	case "attach-rich":
		minRows, _ := strconv.Atoi(os.Args[4])
		a, err := tel.Attach(name, streaming.AttachOptions{RingRows: 20000})
		if err != nil {
			die(err)
		}
		defer a.Close()
		r, err := a.WaitRing(15 * time.Second)
		if err != nil {
			die(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for r.NewestSeq()+1 < int64(minRows) {
			if time.Now().After(deadline) {
				die(fmt.Errorf("timeout at %d rows", r.NewestSeq()+1))
			}
			time.Sleep(20 * time.Millisecond)
		}
		nl, _ := r.Leaf("n")
		fl, _ := r.Leaf("f")
		sl, _ := r.Leaf("s")
		tl, _ := r.Leaf("t")
		nulls, nans := 0, 0
		lo, hi := r.OldestSeq(), r.NewestSeq()
		for seq := lo; seq <= hi; seq++ {
			n, _, err := r.GetI64(seq, nl)
			if err != nil || n != seq {
				die(fmt.Errorf("n@%d=%d (%v)", seq, n, err))
			}
			f, isNull, err := r.GetF64(seq, fl)
			if err != nil {
				die(err)
			}
			ef, en := richF(n)
			if isNull != en {
				die(fmt.Errorf("f null mismatch at n=%d", n))
			}
			if !en {
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
			b, sNull, err := r.GetBytes(seq, sl)
			if err != nil {
				die(err)
			}
			es, esn := richS(n)
			if sNull != esn || (!esn && string(b) != es) {
				die(fmt.Errorf("s@%d = %q null=%v want %q null=%v",
					n, b, sNull, es, esn))
			}
			tv, _, err := r.GetI64(seq, tl)
			if err != nil || tv != n*1_000_000 {
				die(fmt.Errorf("t@%d = %d (%v)", n, tv, err))
			}
		}
		fmt.Printf("%d %d %d %d\n", lo, hi, nulls, nans)
	case "serve-nx":
		rows, _ := strconv.Atoi(os.Args[4])
		hold, _ := strconv.ParseFloat(os.Args[5], 64)
		s, err := tel.CreateStream(name, schemaNX(),
			streaming.StreamOptions{RingRows: int64(rows) * 2})
		if err != nil {
			die(err)
		}
		r := s.Ring()
		nl, _ := r.Leaf("n")
		xl, _ := r.Leaf("x")
		time.Sleep(1500 * time.Millisecond)
		for i := 0; i < rows; {
			for k := 0; k < 100 && i < rows; k++ {
				if _, err := r.RowBegin(time.Second); err != nil {
					die(err)
				}
				r.SetI64(nl, int64(i))
				r.SetF64(xl, float64(i)*0.5)
				i++
			}
			if _, err := r.Commit(); err != nil {
				die(err)
			}
			if _, err := s.Publish(); err != nil {
				die(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(time.Duration(hold * float64(time.Second)))
		fmt.Println("served", rows)
	case "attach-nx":
		minRows, _ := strconv.Atoi(os.Args[4])
		a, err := tel.Attach(name, streaming.AttachOptions{})
		if err != nil {
			die(err)
		}
		defer a.Close()
		r, err := a.WaitRing(15 * time.Second)
		if err != nil {
			die(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for r.NewestSeq()+1 < int64(minRows) {
			if time.Now().After(deadline) {
				die(fmt.Errorf("timeout: ring has %d rows, want >= %d "+
					"(state %s)", r.NewestSeq()+1, minRows, a.State()))
			}
			time.Sleep(20 * time.Millisecond)
		}
		nl, _ := r.Leaf("n")
		xl, _ := r.Leaf("x")
		lo, hi := r.OldestSeq(), r.NewestSeq()
		var firstN, lastN int64
		for seq := lo; seq <= hi; seq++ {
			v, _, err := r.GetI64(seq, nl)
			if err != nil {
				die(fmt.Errorf("n@%d: %w", seq, err))
			}
			if seq == lo {
				firstN = v
			} else if v != lastN+1 {
				die(fmt.Errorf("gap at seq %d: n %d -> %d", seq, lastN, v))
			}
			lastN = v
			x, _, err := r.GetF64(seq, xl)
			if err != nil || x != float64(v)*0.5 {
				die(fmt.Errorf("x@%d = %v (n=%d, %v)", seq, x, v, err))
			}
		}
		fmt.Printf("%d %d\n", firstN, lastN)
	case "serve":
		rows, _ := strconv.Atoi(os.Args[4])
		hold, _ := strconv.ParseFloat(os.Args[5], 64)
		s, err := tel.CreateStream(name, schema(),
			streaming.StreamOptions{RingRows: int64(rows) * 2})
		if err != nil {
			die(err)
		}
		r := s.Ring()
		nl, _ := r.Leaf("n")
		xl, _ := r.Leaf("x")
		sl, _ := r.Leaf("s")
		time.Sleep(1500 * time.Millisecond) // let subscribers find us
		for i := 0; i < rows; {
			for k := 0; k < 100 && i < rows; k++ {
				if _, err := r.RowBegin(time.Second); err != nil {
					die(err)
				}
				r.SetI64(nl, int64(i))
				r.SetF64(xl, float64(i)*0.5)
				r.SetString(sl, fmt.Sprintf("row-%d", i))
				i++
			}
			if _, err := r.Commit(); err != nil {
				die(err)
			}
			if _, err := s.Publish(); err != nil {
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
		a, err := tel.Attach(name, streaming.AttachOptions{Parquet: parquet})
		if err != nil {
			die(err)
		}
		defer a.Close()
		if pqdir != "" {
			if err := a.StartParquet(label); err != nil {
				die(err)
			}
		}
		r, err := a.WaitRing(15 * time.Second)
		if err != nil {
			die(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for r.NewestSeq()+1 < int64(minRows) {
			if time.Now().After(deadline) {
				die(fmt.Errorf("timeout: ring has %d rows, want >= %d "+
					"(state %s)", r.NewestSeq()+1, minRows, a.State()))
			}
			time.Sleep(20 * time.Millisecond)
		}
		nl, _ := r.Leaf("n")
		sl, _ := r.Leaf("s")
		lo, hi := r.OldestSeq(), r.NewestSeq()
		var firstN, lastN int64
		for seq := lo; seq <= hi; seq++ {
			v, _, err := r.GetI64(seq, nl)
			if err != nil {
				die(fmt.Errorf("n@%d: %w", seq, err))
			}
			if seq == lo {
				firstN = v
			} else if v != lastN+1 {
				die(fmt.Errorf("gap at seq %d: n %d -> %d", seq, lastN, v))
			}
			lastN = v
			if seq == hi {
				b, _, err := r.GetBytes(seq, sl)
				if err != nil || string(b) != fmt.Sprintf("row-%d", v) {
					die(fmt.Errorf("s@%d = %q (%v)", seq, b, err))
				}
			}
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
