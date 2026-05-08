// bench_tcp.go — TCP load-generator daemon (B안)
//
// Lifecycle:
//   1. Listen on TCP control port 9092.
//   2. On "RUN <rps> <duration_sec> <msg_size> [<conns>]" line:
//        spawn N persistent TCP connections to the upstream (sidecar:9091
//        by default, override via BENCH_TARGET env), each running an
//        open-loop pacer. Each request sends msg_size bytes and reads
//        msg_size bytes back. Latency = wall time of that round trip.
//      Reply: "OK <rps_achieved> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>"
//      Same wire format as bench_dpumesh.
//   3. Connections are torn down at the end of each run (so each RUN starts
//      fresh and is reproducible). The control port persists across runs.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ctrlPortDefault = 9092
	waitTimeoutMs   = 5000
)

var target = "sidecar:9091"

func nowSec() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// One worker: maintain one TCP connection, fire requests at scheduled times.
func worker(
	id int, target string,
	budget int64, intervalSec float64,
	startAt float64, msgSize int,
	stop *atomic.Bool,
	ok, fail *atomic.Int64,
	samples []float64,
) int {
	conn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		fail.Add(budget)
		return 0
	}
	defer conn.Close()

	// Disable Nagle for accurate per-request latency
	if tc, ok2 := conn.(*net.TCPConn); ok2 {
		_ = tc.SetNoDelay(true)
	}

	tx := make([]byte, msgSize)
	for i := range tx {
		tx[i] = byte('A' + (i & 0xf))
	}
	rx := make([]byte, msgSize)

	written := 0
	for i := int64(0); i < budget; i++ {
		if stop.Load() {
			break
		}
		// wrk2-style scheduled-time semantics. Latency starts at the tick this
		// request was supposed to fire, not when this goroutine actually got
		// to it. Fixes coordinated omission: under saturation, scheduled is
		// in the past, sleep is skipped, and the captured latency includes
		// the queuing wait — what a constant-rate client would actually see.
		scheduled := startAt + float64(i)*intervalSec
		now := nowSec()
		if scheduled > now {
			d := time.Duration((scheduled - now) * float64(time.Second))
			time.Sleep(d)
		}

		t0 := scheduled
		_ = conn.SetDeadline(time.Now().Add(time.Duration(waitTimeoutMs) * time.Millisecond))

		if _, err := conn.Write(tx); err != nil {
			fail.Add(1)
			return written
		}
		if _, err := io.ReadFull(conn, rx); err != nil {
			fail.Add(1)
			return written
		}

		latUs := (nowSec() - t0) * 1e6
		if written < len(samples) {
			samples[written] = latUs
			written++
		}
		ok.Add(1)
	}
	return written
}

func runTest(connOut net.Conn, rps, dur, msgSize, conns int) {
	if rps < 1 || dur < 1 || msgSize < 1 {
		fmt.Fprintln(connOut, "ERR invalid args (need rps>=1 dur>=1 size>=1)")
		return
	}
	nWorkers := conns
	if nWorkers <= 0 {
		nWorkers = rps / 100
		if nWorkers < 1 {
			nWorkers = 1
		}
	}
	totalBudget := int64(rps) * int64(dur)
	perWorker := totalBudget / int64(nWorkers)
	remainder := totalBudget % int64(nWorkers)
	intervalSec := float64(nWorkers) / float64(rps)

	fmt.Fprintf(os.Stderr, "[bench-tcp] RUN rps=%d dur=%d size=%d conns=%d (workers=%d)\n",
		rps, dur, msgSize, conns, nWorkers)

	var stop atomic.Bool
	var ok, fail atomic.Int64
	var wg sync.WaitGroup

	allSamples := make([][]float64, nWorkers)
	written := make([]int, nWorkers)

	startAt := nowSec() + 0.05

	for i := 0; i < nWorkers; i++ {
		budget := perWorker
		if int64(i) < remainder {
			budget++
		}
		allSamples[i] = make([]float64, budget)
		wg.Add(1)
		go func(idx int, b int64) {
			defer wg.Done()
			written[idx] = worker(idx, target, b, intervalSec,
				startAt, msgSize, &stop, &ok, &fail, allSamples[idx])
		}(i, budget)
	}

	// Hard deadline = dur + grace
	go func() {
		time.Sleep(time.Duration(dur+5) * time.Second)
		stop.Store(true)
	}()
	wg.Wait()

	wall := nowSec() - startAt
	if wall < 1e-6 {
		wall = 1e-6
	}

	// Pool samples
	var pooled []float64
	for i := 0; i < nWorkers; i++ {
		pooled = append(pooled, allSamples[i][:written[i]]...)
	}
	sort.Float64s(pooled)

	pct := func(p float64) float64 {
		if len(pooled) == 0 {
			return 0
		}
		idx := int(p * float64(len(pooled)-1))
		if idx >= len(pooled) {
			idx = len(pooled) - 1
		}
		return pooled[idx]
	}

	rpsAch := float64(ok.Load()) / wall
	mbS := rpsAch * float64(msgSize) * 2.0 / (1024.0 * 1024.0)
	reply := fmt.Sprintf("OK %.1f %.1f %.1f %.1f %d %d %.2f\n",
		rpsAch, pct(0.50), pct(0.99), pct(0.999),
		ok.Load(), fail.Load(), mbS)
	fmt.Fprint(connOut, reply)
	fmt.Fprintf(os.Stderr, "[bench-tcp] DONE %s", reply)
}

func parseRun(line string) (rps, dur, size, conns int, err error) {
	tokens := strings.Fields(line)
	if len(tokens) < 4 || tokens[0] != "RUN" {
		err = fmt.Errorf("bad command")
		return
	}
	if rps, err = strconv.Atoi(tokens[1]); err != nil {
		return
	}
	if dur, err = strconv.Atoi(tokens[2]); err != nil {
		return
	}
	if size, err = strconv.Atoi(tokens[3]); err != nil {
		return
	}
	if len(tokens) >= 5 {
		conns, err = strconv.Atoi(tokens[4])
	}
	return
}

func handleCtrl(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	line, err := r.ReadString('\n')
	if err != nil && line == "" {
		return
	}
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "PING") {
		fmt.Fprintln(c, "PONG")
		return
	}
	rps, dur, size, conns, err := parseRun(line)
	if err != nil {
		fmt.Fprintln(c, "ERR bad command (use: RUN <rps> <dur> <size> [<conns>])")
		return
	}
	runTest(c, rps, dur, size, conns)
}

func main() {
	port := flag.Int("ctrl-port", ctrlPortDefault, "control TCP port")
	flag.Parse()

	if t := os.Getenv("BENCH_TARGET"); t != "" {
		target = t
	}
	fmt.Fprintf(os.Stderr, "[bench-tcp] target=%s\n", target)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "[bench-tcp] control LISTEN on :%d\n", *port)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		handleCtrl(c)
	}
}
