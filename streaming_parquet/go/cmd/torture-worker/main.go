// torture-worker: Go roles for the cross-stack torture rig
// (run_torture.py in streaming-parquet-integration orchestrates; see it
// for the scenario).
//
//	torture-worker pub <root> <name> <kind> <rate> <seed> <archdir|-> <seconds> <status>
//	torture-worker sub <root> <name> <kind> <rate> <seed> <archdir|-> <seconds> <status>
//
// kind: num (n int64, x float64=n*0.5) | str (adds s utf8 deterministic)
// Publishers write n=0,1,2,... at ~rate rows/s in 20ms bursts and
// Publish each burst; with an archive dir they churn parquet sessions
// (Start/StopParquet with labels) every 45-180s. Subscribers attach by
// root-ticket discovery and continuously verify contiguity: within an epoch n
// must ascend by exactly 1; n restarting at a lower value is an EPOCH
// (publisher respawn), counted, not a failure; falling behind the ring
// (stale) is a RESYNC, counted. A real gap is a failure. Status JSON
// is written atomically every 2s: rows/epochs/gaps/resyncs/errors.
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/neofactory/streaming-parquet/streaming_parquet/go/streaming"
)

type status struct {
	Role    string `json:"role"`
	Rows    int64  `json:"rows"`
	Epochs  int64  `json:"epochs"`
	Gaps    int64  `json:"gaps"`
	Resyncs int64  `json:"resyncs"`
	Stalls  int64  `json:"stalls"`
	Errors  int64  `json:"errors"`
	Files   int64  `json:"files"`
	Note    string `json:"note"`
	Ts      int64  `json:"ts"`
}

var stPath string
var st status

func writeStatus() {
	st.Ts = time.Now().Unix()
	b, _ := json.Marshal(st)
	tmp := stPath + ".tmp"
	os.WriteFile(tmp, b, 0o644)
	os.Rename(tmp, stPath)
}

func die(err error) {
	st.Errors++
	st.Note = err.Error()
	writeStatus()
	fmt.Fprintln(os.Stderr, "torture-worker:", err)
	os.Exit(1)
}

func schemaFor(kind string) *arrow.Schema {
	fields := []arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
	}
	if kind == "str" {
		fields = append(fields,
			arrow.Field{Name: "s", Type: arrow.BinaryTypes.String})
	}
	return arrow.NewSchema(fields, nil)
}

func strFor(n int64) string {
	pad := int(n % 97)
	b := make([]byte, 0, 24+pad)
	b = append(b, fmt.Sprintf("row-%d-", n)...)
	for i := 0; i < pad; i++ {
		b = append(b, 'a')
	}
	return string(b)
}

func main() {
	if len(os.Args) < 10 {
		fmt.Fprintln(os.Stderr, "usage: torture-worker pub|sub <root> "+
			"<name> <kind> <rate> <seed> <archdir|-> <seconds> <status>")
		os.Exit(2)
	}
	role, root, name, kind := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	rate, _ := strconv.Atoi(os.Args[5])
	seed, _ := strconv.ParseInt(os.Args[6], 10, 64)
	archdir := os.Args[7]
	seconds, _ := strconv.ParseFloat(os.Args[8], 64)
	stPath = os.Args[9]
	st.Role = "go-" + role
	rng := rand.New(rand.NewSource(seed))

	stop := make(chan os.Signal, 2)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	deadline := time.Now().Add(time.Duration(seconds * float64(time.Second)))

	tel, err := streaming.Init(streaming.Config{Root: root})
	if err != nil {
		die(err)
	}
	defer tel.Close()

	switch role {
	case "pub":
		runPub(tel, name, kind, rate, rng, archdir, deadline, stop)
	case "sub":
		runSub(tel, name, kind, rate, rng, archdir, deadline, stop)
	default:
		os.Exit(2)
	}
	writeStatus()
}

func runPub(tel *streaming.Telemetry, name, kind string, rate int,
	rng *rand.Rand, archdir string, deadline time.Time,
	stop chan os.Signal) {
	s, err := tel.CreateStream(name, schemaFor(kind),
		streaming.StreamOptions{RingRows: int64(rate) * 6})
	if err != nil {
		die(err)
	}
	session := 0
	if archdir != "-" {
		if err := s.StartParquet(fmt.Sprintf("s%03d", session),
			&streaming.ParquetOptions{BaseDir: archdir,
				RowGroupSeconds: 3, CheckpointSeconds: 0.7,
				RowGroupRows: int64(rate) * 3}); err != nil {
			die(err)
		}
	}
	nextChurn := time.Now().Add(time.Duration(45+rng.Intn(135)) * time.Second)
	r := s.Ring()
	nl, _ := r.Leaf("n")
	xl, _ := r.Leaf("x")
	sl := int32(-1)
	if kind == "str" {
		sl, _ = r.Leaf("s")
	}
	var n int64
	burst := rate / 50 // 20ms cadence
	if burst < 1 {
		burst = 1
	}
	lastStatus := time.Now()
	t0 := time.Now()
	for time.Now().Before(deadline) {
		select {
		case <-stop:
			goto done
		default:
		}
		for k := 0; k < burst; k++ {
			if _, err := r.RowBegin(2 * time.Second); err != nil {
				// pin stall is loud and survivable; count and retry
				st.Stalls++
				time.Sleep(100 * time.Millisecond)
				k--
				continue
			}
			r.SetI64(nl, n)
			r.SetF64(xl, float64(n)*0.5)
			if sl >= 0 {
				r.SetString(sl, strFor(n))
			}
			n++
		}
		if _, err := r.Commit(); err != nil {
			die(err)
		}
		if _, err := s.Publish(); err != nil {
			st.Errors++
			st.Note = err.Error()
		}
		st.Rows = n
		if archdir != "-" && time.Now().After(nextChurn) {
			if err := s.StopParquet(); err != nil {
				st.Errors++
				st.Note = "stop: " + err.Error()
			}
			st.Files++
			session++
			if err := s.StartParquet(fmt.Sprintf("s%03d", session), nil); err != nil {
				die(err)
			}
			nextChurn = time.Now().Add(
				time.Duration(45+rng.Intn(135)) * time.Second)
		}
		if time.Since(lastStatus) > 2*time.Second {
			writeStatus()
			lastStatus = time.Now()
		}
		ahead := float64(n)/float64(rate) - time.Since(t0).Seconds()
		if ahead > 0.005 {
			time.Sleep(time.Duration(ahead * float64(time.Second)))
		}
	}
done:
	if archdir != "-" {
		if err := s.StopParquet(); err != nil {
			st.Errors++
		}
		st.Files++
	}
	s.Close()
}

func runSub(tel *streaming.Telemetry, name, kind string, rate int,
	rng *rand.Rand, archdir string, deadline time.Time,
	stop chan os.Signal) {
	var parquet *streaming.ParquetOptions
	if archdir != "-" {
		parquet = &streaming.ParquetOptions{BaseDir: archdir,
			RowGroupSeconds: 3, CheckpointSeconds: 0.7,
			RowGroupRows: int64(rate) * 3}
	}
	a, err := tel.Attach(name, streaming.AttachOptions{
		RingRows: int64(rate) * 6, Parquet: parquet})
	if err != nil {
		die(err)
	}
	defer a.Close()
	session := 0
	if archdir != "-" {
		if err := a.StartParquet(fmt.Sprintf("a%03d", session)); err != nil {
			die(err)
		}
	}
	nextChurn := time.Now().Add(time.Duration(45+rng.Intn(135)) * time.Second)
	var lastSeq, lastN int64 = -1, -1
	lastStatus := time.Now()
	for time.Now().Before(deadline) {
		select {
		case <-stop:
			goto done
		case <-time.After(100 * time.Millisecond):
		}
		r := a.Ring()
		if r == nil {
			continue
		}
		nl, err := r.Leaf("n")
		if err != nil {
			continue
		}
		newest := r.NewestSeq()
		if newest < 0 {
			continue
		}
		oldest := r.OldestSeq()
		start := lastSeq + 1
		if start < oldest { // fell behind (SIGSTOP chaos etc.)
			st.Resyncs++
			start = oldest
			lastN = -2 // unknown; next row starts a fresh check run
		}
		for seq := start; seq <= newest; seq++ {
			v, _, err := r.GetI64(seq, nl)
			if err != nil {
				st.Resyncs++
				lastN = -2
				break
			}
			switch {
			case lastN < 0:
				// first contact (mid-stream join) or resync landing
			case v == lastN+1:
				// contiguous
			case v <= lastN:
				st.Epochs++ // publisher respawned (n restarted)
			default:
				st.Gaps++ // a real hole: unexplained loss
			}
			lastN = v
			lastSeq = seq
			st.Rows++
		}
		if archdir != "-" && time.Now().After(nextChurn) {
			if err := a.StopParquet(); err != nil {
				st.Errors++
				st.Note = "stop: " + err.Error()
			}
			st.Files++
			session++
			if err := a.StartParquet(fmt.Sprintf("a%03d", session)); err != nil {
				st.Errors++
				st.Note = "start: " + err.Error()
			}
			nextChurn = time.Now().Add(
				time.Duration(45+rng.Intn(135)) * time.Second)
		}
		if time.Since(lastStatus) > 2*time.Second {
			writeStatus()
			lastStatus = time.Now()
		}
	}
done:
	if archdir != "-" {
		if err := a.StopParquet(); err != nil {
			st.Errors++
		}
		st.Files++
	}
}
