// leak-bisect: isolate which component grows RSS on string streams
// (found by the torture rig; the growth survives the pool-spares fix
// and the explicit stream release).
//
//	leak-bisect <stage> <seconds>
//	  ingest    ring + string AppendRecord churn only
//	  relation  ingest + StaticRelation/Records/Release churn
//	  encode    relation + pqarrow encode of the records
//
// Prints "t=<s> rss=<MB> heap=<MB>" every 5s. RSS from getrusage
// (darwin: max RSS in bytes); heap from Go runtime. C-side growth
// shows in rss but not heap.
package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/neofactory/streaming-parquet/arrow_ring/go/ring"
)

func rssMB() float64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Maxrss) / (1 << 20)
}

func heapMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1 << 20)
}

func main() {
	stage := os.Args[1]
	seconds, _ := strconv.Atoi(os.Args[2])
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
	r, err := ring.NewRing(schema, 72000, &ring.Options{RateHint: 12000})
	if err != nil {
		panic(err)
	}
	defer r.Close()
	mem := memory.DefaultAllocator
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy))

	makeRec := func(start, count int64) arrow.RecordBatch {
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()
		nb := b.Field(0).(*array.Int64Builder)
		sb := b.Field(1).(*array.StringBuilder)
		for i := start; i < start+count; i++ {
			nb.Append(i)
			sb.Append(fmt.Sprintf("row-%d-%s", i,
				string(bytes.Repeat([]byte{'a'}, int(i%97)))))
		}
		return b.NewRecordBatch()
	}

	var n int64
	t0 := time.Now()
	last := time.Now()
	for time.Since(t0) < time.Duration(seconds)*time.Second {
		rec := makeRec(n, 1200) // ~12k rows/s at 100ms cadence
		if err := r.AppendRecord(rec); err != nil {
			panic(err)
		}
		rec.Release()
		n += 1200

		if stage == "relation" || stage == "encode" {
			rs, err := r.StaticRelation(time.Second)
			if err == nil {
				recs, err := rs.Records()
				if err != nil {
					panic(err)
				}
				if stage == "encode" {
					var buf bytes.Buffer
					fw, err := pqarrow.NewFileWriter(schema, &buf, props,
						pqarrow.DefaultWriterProps())
					if err != nil {
						panic(err)
					}
					for _, rec := range recs {
						if err := fw.WriteBuffered(rec); err != nil {
							panic(err)
						}
					}
					fw.Close()
				}
				for _, rec := range recs {
					rec.Release()
				}
				rs.Release()
			}
		}
		if time.Since(last) > 5*time.Second {
			last = time.Now()
			fmt.Printf("t=%3.0f rss=%6.1fMB heap=%6.1fMB rows=%d\n",
				time.Since(t0).Seconds(), rssMB(), heapMB(), n)
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("FINAL rss=%.1fMB heap=%.1fMB\n", rssMB(), heapMB())
}
