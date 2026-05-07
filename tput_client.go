// tput_client.go - High-throughput Thrift load generator (Go)
//
// Drop-in replacement for test_thrift.py throughput mode.
// Designed to NOT be the bottleneck so server-side ceiling is measurable:
//   - One goroutine per TCP connection (cheap; thousands feasible)
//   - Auto-sized connection pool: enough that no single conn saturates
//   - TCP_NODELAY on every conn (no Nagle batching)
//   - Phase-separated: dial all sockets first, then synchronized start
//   - Wall-clock corrected throughput (no nominal-duration bias)
//   - wrk2-style coordinated-omission corrected latency
//
// Build:  go build -o tput_client tput_client.go
// Run:    ./tput_client -host=10.x.x.x -port=9091 -rps=21000 -duration=10
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

// Thrift TBinaryProtocol type IDs — must match test_thrift.py exactly.
const (
	tStop   byte = 0
	tI32    byte = 8
	tI64    byte = 10
	tString byte = 11
	tMap    byte = 13
)

// buildFrame constructs the wire-level Thrift framed call for ComposeUniqueId.
// Frame format (matches test_thrift.py byte-for-byte):
//   [4B frame_len][version=0x80010001][4B name_len]["ComposeUniqueId"][4B seq_id]
//   [args: i64 req_id, i32 post_type, map<str,str> carrier, T_STOP]
// Frame size: 59 bytes baseline, or 68 + padBytes when padBytes > 0.
func buildFrame(method string, padBytes int) []byte {
	var p []byte

	// f1: T_I64 field=1 value=12345
	p = append(p, tI64, 0x00, 0x01)
	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], 12345)
	p = append(p, b8[:]...)

	// f2: T_I32 field=2 value=1
	p = append(p, tI32, 0x00, 0x02)
	var b4 [4]byte
	binary.BigEndian.PutUint32(b4[:], 1)
	p = append(p, b4[:]...)

	// f3: T_MAP field=3 keytype=string valtype=string
	p = append(p, tMap, 0x00, 0x03, tString, tString)
	var cnt [4]byte
	if padBytes > 0 {
		binary.BigEndian.PutUint32(cnt[:], 1)
		p = append(p, cnt[:]...)
		var keyLen, valLen [4]byte
		binary.BigEndian.PutUint32(keyLen[:], 1)
		binary.BigEndian.PutUint32(valLen[:], uint32(padBytes))
		p = append(p, keyLen[:]...)
		p = append(p, 'p')
		p = append(p, valLen[:]...)
		padBuf := make([]byte, padBytes)
		for i := range padBuf {
			padBuf[i] = 'X'
		}
		p = append(p, padBuf...)
	} else {
		p = append(p, cnt[:]...) // count=0
	}
	p = append(p, tStop)

	// Header: version + type=Call(1), name_len, name, seq_id=0
	var hdr []byte
	hdr = append(hdr, 0x80, 0x01, 0x00, 0x01)
	var nl [4]byte
	binary.BigEndian.PutUint32(nl[:], uint32(len(method)))
	hdr = append(hdr, nl[:]...)
	hdr = append(hdr, []byte(method)...)
	hdr = append(hdr, 0x00, 0x00, 0x00, 0x00)

	body := append(hdr, p...)
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(body)))
	copy(out[4:], body)
	return out
}

func dialNoDelay(addr string) (*net.TCPConn, error) {
	c, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	tc := c.(*net.TCPConn)
	_ = tc.SetNoDelay(true)
	return tc, nil
}

type sample struct {
	corrected float64 // ms (sched_ts → recv_done)
	raw       float64 // ms (send_ts  → recv_done)
	respWire  int     // bytes on wire for response (4B frame_len + body)
}

type wstate struct {
	conn    *net.TCPConn
	indices []int // global request indices owned by this worker
	samples []sample
	failNet int // network/IO errors (connection broken, read deadline)
	failExc int // server-side TApplicationException (gateway / dpumesh failure)
}

// runSchedule drives one connection through its assigned request indices.
// ts(idx) = base + idx*interval — computed at fire time so all workers share clock.
//
// Result classification per request:
//   ok        — Thrift Reply (msg_type=0x02)         → counted in samples
//   failExc   — Thrift Exception (msg_type=0x03)     → connection alive, keep using
//   failNet   — TCP error / read timeout / EOF       → redial connection
func runSchedule(s *wstate, addr string, base time.Time, interval time.Duration, frame []byte) {
	var resp4 [4]byte
	samples := make([]sample, 0, len(s.indices))
	failNet := 0
	failExc := 0
	bodyBuf := make([]byte, 0, 1024)

	// Per-request read deadline. Gateway's RESPONSE_TIMEOUT_MS is 30s, so allow
	// 35s for the round-trip; anything longer is a hung path, count as failNet.
	const readDeadline = 35 * time.Second

	for k, idx := range s.indices {
		ts := base.Add(time.Duration(idx) * interval)
		if d := time.Until(ts); d > 0 {
			time.Sleep(d)
		}
		sendTs := time.Now()
		netErr := false
		excErr := false
		var respLen uint32

		_ = s.conn.SetReadDeadline(time.Now().Add(readDeadline))

		if _, err := s.conn.Write(frame); err != nil {
			netErr = true
		} else if _, err := io.ReadFull(s.conn, resp4[:]); err != nil {
			netErr = true
		} else {
			respLen = binary.BigEndian.Uint32(resp4[:])
			if respLen > 16*1024*1024 || respLen < 4 {
				netErr = true
			} else {
				if cap(bodyBuf) < int(respLen) {
					bodyBuf = make([]byte, respLen)
				} else {
					bodyBuf = bodyBuf[:respLen]
				}
				if _, err := io.ReadFull(s.conn, bodyBuf); err != nil {
					netErr = true
				} else {
					// Thrift binary protocol header in body[0..3]:
					//   bytes 0..1 = version 0x8001
					//   bytes 2..3 = message type (LE on wire = upper byte 0x00,
					//                lower byte = 0x02 Reply / 0x03 Exception)
					// gateway/send_thrift_exception writes 0x80 0x01 0x00 0x03.
					// Real unique-id-service Reply writes 0x80 0x01 0x00 0x02.
					if bodyBuf[3] == 0x03 {
						excErr = true
					}
				}
			}
		}
		now := time.Now()

		switch {
		case !netErr && !excErr:
			samples = append(samples, sample{
				corrected: float64(now.Sub(ts).Microseconds()) / 1000.0,
				raw:       float64(now.Sub(sendTs).Microseconds()) / 1000.0,
				respWire:  int(4 + respLen),
			})
		case excErr:
			// Server-side failure (e.g. ENQUEUE rejected, DPUmesh timeout).
			// Connection is still healthy — keep using it.
			failExc++
		case netErr:
			failNet++
			_ = s.conn.Close()
			c, derr := dialNoDelay(addr)
			if derr != nil {
				// Cannot recover — count remaining indices as network-failed.
				failNet += len(s.indices) - k - 1
				s.samples = samples
				s.failNet = failNet
				s.failExc = failExc
				return
			}
			s.conn = c
		}
	}
	s.samples = samples
	s.failNet = failNet
	s.failExc = failExc
}

func percentile(s []float64, p float64) float64 {
	if len(s) == 0 {
		return 0
	}
	idx := int(float64(len(s)) * p / 100.0)
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

func mean(s []float64) float64 {
	if len(s) == 0 {
		return 0
	}
	var sum float64
	for _, v := range s {
		sum += v
	}
	return sum / float64(len(s))
}

func main() {
	host := flag.String("host", "127.0.0.1", "Gateway host")
	port := flag.Int("port", 9091, "Gateway port")
	rps := flag.Int("rps", 1000, "Target RPS")
	dur := flag.Int("duration", 10, "Duration seconds")
	msgSize := flag.Int("msg-size", 8192, "Total DMA size in bytes, 128-aligned (min 128, max 8192)")
	numConns := flag.Int("conns", 0, "Number of TCP connections (0=auto: rps/25, min 64, max 8192)")
	flag.Parse()

	if *numConns == 0 {
		// Auto-size so per-conn load is well under saturation.
		// Assume per-request ~10ms → 100 RPS/conn ceiling.
		// Target ~25 RPS/conn = 4× headroom.
		n := *rps / 25
		if n < 64 {
			n = 64
		}
		if n > 8192 {
			n = 8192
		}
		*numConns = n
	}

	// msg-size = total DMA bytes the gateway will issue per request:
	//   DMA = align_up_128( TCP wire frame length )
	// msg-size must be 128-aligned and ≤ DPA_DMA_COPY_MAX (8192).
	const dmaAlign = 128
	const dmaMax = 8192
	if *msgSize < dmaAlign {
		fmt.Fprintf(os.Stderr, "msg-size %d below DMA alignment (%d); bumping to %d\n",
			*msgSize, dmaAlign, dmaAlign)
		*msgSize = dmaAlign
	}
	if *msgSize > dmaMax {
		fmt.Fprintf(os.Stderr, "msg-size %d exceeds DPA_DMA_COPY_MAX (%d); capping to %d\n",
			*msgSize, dmaMax, dmaMax)
		*msgSize = dmaMax
	}
	if *msgSize%dmaAlign != 0 {
		aligned := ((*msgSize) + dmaAlign - 1) &^ (dmaAlign - 1)
		fmt.Fprintf(os.Stderr, "msg-size %d not %d-aligned; rounding up to %d\n",
			*msgSize, dmaAlign, aligned)
		*msgSize = aligned
	}

	wireSize := *msgSize
	pad := 0
	if wireSize > 68 {
		pad = wireSize - 68
	} else if wireSize < 59 {
		fmt.Fprintf(os.Stderr, "msg-size %d → wire %d < baseline frame 59; using 59\n",
			*msgSize, wireSize)
		wireSize = 59
	}
	frame := buildFrame("ComposeUniqueId", pad)
	actualWire := len(frame)
	dmaActual := (actualWire + dmaAlign - 1) &^ (dmaAlign - 1)

	total := (*rps) * (*dur)
	interval := time.Second / time.Duration(*rps)
	addr := fmt.Sprintf("%s:%d", *host, *port)

	fmt.Println("============================================================")
	fmt.Printf("  Throughput Test (Go) — %s\n", addr)
	fmt.Println("============================================================")
	fmt.Printf("  Target RPS:   %d\n", *rps)
	fmt.Printf("  Duration:     %ds\n", *dur)
	fmt.Printf("  Connections:  %d\n", *numConns)
	fmt.Printf("  Total reqs:   %d\n", total)
	fmt.Printf("  DMA size:     %d bytes  (= %d wire, 128-aligned)\n",
		dmaActual, actualWire)
	fmt.Printf("  TCP wire:     %d bytes  (Thrift frame, pad=%d)\n", actualWire, pad)
	fmt.Printf("  Interval:     %v\n", interval)
	fmt.Println("============================================================")
	fmt.Println()

	// Interleaved index assignment: worker k gets indices {k, k+nc, k+2nc, ...}
	// This spreads each worker's load uniformly across the test window.
	states := make([]wstate, *numConns)
	for i := 0; i < total; i++ {
		cidx := i % (*numConns)
		states[cidx].indices = append(states[cidx].indices, i)
	}

	// Phase 1: dial all sockets with bounded concurrency to avoid SYN flood
	// against a listen backlog of 128 (gateway.c default).
	fmt.Printf("Connecting %d sockets...\n", *numConns)
	dialStart := time.Now()
	var dialWg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i := range states {
		if len(states[i].indices) == 0 {
			continue
		}
		dialWg.Add(1)
		go func(idx int) {
			defer dialWg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			c, err := dialNoDelay(addr)
			if err != nil {
				return
			}
			states[idx].conn = c
		}(i)
	}
	dialWg.Wait()
	dialDur := time.Since(dialStart)

	connected := 0
	for i := range states {
		if states[i].conn != nil {
			connected++
		}
	}
	fmt.Printf("Connected %d/%d in %v\n\n", connected, *numConns, dialDur)
	if connected == 0 {
		fmt.Fprintln(os.Stderr, "ERROR: no connections established")
		os.Exit(1)
	}

	// Phase 2: launch workers, all racing toward the same shared base time.
	// 500ms cushion lets every goroutine reach its sleep-until-base point
	// before the first scheduled request, so workers don't shift their
	// schedule due to startup jitter. wallStart = base (not Now) so the
	// 500ms warmup is excluded from the throughput measurement window —
	// otherwise wall RPS reads ~1-2% low (e.g. 44.3K instead of 45K at
	// 30s × 45K target).
	base := time.Now().Add(500 * time.Millisecond)
	var wg sync.WaitGroup
	wallStart := base
	for i := range states {
		if states[i].conn == nil {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer states[idx].conn.Close()
			runSchedule(&states[idx], addr, base, interval, frame)
		}(i)
	}
	wg.Wait()
	wallDur := time.Since(wallStart)

	// Aggregate
	var corrected, raws []float64
	var ok, failNet, failExc int64
	var totalRespWire, totalRespDma int64
	for i := range states {
		for _, s := range states[i].samples {
			corrected = append(corrected, s.corrected)
			raws = append(raws, s.raw)
			totalRespWire += int64(s.respWire)
			respDma := (s.respWire + dmaAlign - 1) &^ (dmaAlign - 1)
			totalRespDma += int64(respDma)
		}
		ok += int64(len(states[i].samples))
		failNet += int64(states[i].failNet)
		failExc += int64(states[i].failExc)
	}
	fail := failNet + failExc
	sort.Float64s(corrected)
	sort.Float64s(raws)

	nominalDur := float64(*dur)
	actualDur := wallDur.Seconds()
	rpsNom := float64(ok) / nominalDur
	rpsWall := float64(ok) / actualDur

	// TX (request) bandwidth
	dmaTxBps := float64(ok) * float64(dmaActual) / actualDur
	wireTxBps := float64(ok) * float64(actualWire) / actualDur
	// RX (response) bandwidth — sums actual per-request response sizes
	dmaRxBps := float64(totalRespDma) / actualDur
	wireRxBps := float64(totalRespWire) / actualDur
	// Total bidirectional bandwidth
	dmaBps := dmaTxBps + dmaRxBps
	wireBps := wireTxBps + wireRxBps

	avgRespWire := 0.0
	avgRespDma := 0.0
	if ok > 0 {
		avgRespWire = float64(totalRespWire) / float64(ok)
		avgRespDma = float64(totalRespDma) / float64(ok)
	}

	fmt.Println("============================================================")
	fmt.Println("  Results")
	fmt.Println("============================================================")
	denom := ok + fail
	if denom == 0 {
		denom = 1
	}
	fmt.Printf("  Requests:     %d OK, %d failed (%.1f%% ok)\n",
		ok, fail, 100*float64(ok)/float64(denom))
	fmt.Printf("                  · %d Thrift exception (server-side fail: ENQUEUE/timeout)\n", failExc)
	fmt.Printf("                  · %d network error (TCP / read timeout)\n", failNet)
	fmt.Printf("  Wall-clock:   %.2fs (nominal %ds)\n", actualDur, *dur)
	fmt.Printf("  Actual RPS:   %.1f (nominal-dur basis)\n", rpsNom)
	fmt.Printf("  Actual RPS:   %.1f (wall-clock basis) <- real throughput\n", rpsWall)
	fmt.Println()
	fmt.Println("  DMA bandwidth (req + resp, wall-clock):")
	fmt.Printf("    TX (req):   %.2f MB/s, %.3f Gbps  (%dB DMA/req)\n",
		dmaTxBps/1024/1024, dmaTxBps*8/1e9, dmaActual)
	fmt.Printf("    RX (resp):  %.2f MB/s, %.3f Gbps  (avg %.0fB DMA/resp)\n",
		dmaRxBps/1024/1024, dmaRxBps*8/1e9, avgRespDma)
	fmt.Printf("    Total:      %.2f MB/s, %.3f Gbps\n",
		dmaBps/1024/1024, dmaBps*8/1e9)
	fmt.Println("  TCP bandwidth (req + resp, wall-clock):")
	fmt.Printf("    TX (req):   %.2f MB/s, %.3f Gbps  (%dB wire/req)\n",
		wireTxBps/1024/1024, wireTxBps*8/1e9, actualWire)
	fmt.Printf("    RX (resp):  %.2f MB/s, %.3f Gbps  (avg %.0fB wire/resp)\n",
		wireRxBps/1024/1024, wireRxBps*8/1e9, avgRespWire)
	fmt.Printf("    Total:      %.2f MB/s, %.3f Gbps\n",
		wireBps/1024/1024, wireBps*8/1e9)

	if len(corrected) > 0 {
		fmt.Println()
		fmt.Println("  Latency (corrected for coordinated omission):")
		fmt.Printf("    Avg:    %.2f ms\n", mean(corrected))
		fmt.Printf("    P50:    %.2f ms\n", percentile(corrected, 50))
		fmt.Printf("    P95:    %.2f ms\n", percentile(corrected, 95))
		fmt.Printf("    P99:    %.2f ms\n", percentile(corrected, 99))
		fmt.Printf("    Max:    %.2f ms\n", corrected[len(corrected)-1])

		fmt.Println()
		fmt.Println("  Raw RTT (uncorrected):")
		fmt.Printf("    Avg:    %.2f ms\n", mean(raws))
		fmt.Printf("    P50:    %.2f ms\n", percentile(raws, 50))
		fmt.Printf("    P95:    %.2f ms\n", percentile(raws, 95))
		fmt.Printf("    P99:    %.2f ms\n", percentile(raws, 99))
		fmt.Printf("    Max:    %.2f ms\n", raws[len(raws)-1])
	}
	fmt.Println("============================================================")

	if fail > 0 {
		os.Exit(2)
	}
}
