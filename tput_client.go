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
}

type wstate struct {
	conn    *net.TCPConn
	indices []int // global request indices owned by this worker
	samples []sample
	fail    int
}

// runSchedule drives one connection through its assigned request indices.
// ts(idx) = base + idx*interval — computed at fire time so all workers share clock.
func runSchedule(s *wstate, addr string, base time.Time, interval time.Duration, frame []byte) {
	var resp4 [4]byte
	samples := make([]sample, 0, len(s.indices))
	fail := 0
	bodyBuf := make([]byte, 0, 1024)

	for k, idx := range s.indices {
		ts := base.Add(time.Duration(idx) * interval)
		if d := time.Until(ts); d > 0 {
			time.Sleep(d)
		}
		sendTs := time.Now()
		ok := true

		if _, err := s.conn.Write(frame); err != nil {
			ok = false
		} else if _, err := io.ReadFull(s.conn, resp4[:]); err != nil {
			ok = false
		} else {
			rl := binary.BigEndian.Uint32(resp4[:])
			if rl > 16*1024*1024 {
				ok = false
			} else {
				if cap(bodyBuf) < int(rl) {
					bodyBuf = make([]byte, rl)
				} else {
					bodyBuf = bodyBuf[:rl]
				}
				if _, err := io.ReadFull(s.conn, bodyBuf); err != nil {
					ok = false
				}
			}
		}
		now := time.Now()

		if ok {
			samples = append(samples, sample{
				corrected: float64(now.Sub(ts).Microseconds()) / 1000.0,
				raw:       float64(now.Sub(sendTs).Microseconds()) / 1000.0,
			})
		} else {
			fail++
			_ = s.conn.Close()
			c, derr := dialNoDelay(addr)
			if derr != nil {
				// Cannot recover — count remaining indices as failed
				fail += len(s.indices) - k - 1
				s.samples = samples
				s.fail = fail
				return
			}
			s.conn = c
		}
	}
	s.samples = samples
	s.fail = fail
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
	msgSize := flag.Int("msg-size", 8192, "Frame size in bytes (>=59)")
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

	pad := 0
	if *msgSize > 68 {
		pad = *msgSize - 68
	} else if *msgSize < 59 {
		*msgSize = 59
	}
	frame := buildFrame("ComposeUniqueId", pad)
	actualSize := len(frame)

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
	fmt.Printf("  Msg size:     %d bytes (pad=%d)\n", actualSize, pad)
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

	// Phase 2: launch workers, all racing toward the same shared base time
	base := time.Now().Add(500 * time.Millisecond)
	var wg sync.WaitGroup
	wallStart := time.Now()
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
	var ok, fail int64
	for i := range states {
		for _, s := range states[i].samples {
			corrected = append(corrected, s.corrected)
			raws = append(raws, s.raw)
		}
		ok += int64(len(states[i].samples))
		fail += int64(states[i].fail)
	}
	sort.Float64s(corrected)
	sort.Float64s(raws)

	nominalDur := float64(*dur)
	actualDur := wallDur.Seconds()
	rpsNom := float64(ok) / nominalDur
	rpsWall := float64(ok) / actualDur
	bps := float64(ok) * float64(actualSize) / actualDur

	fmt.Println("============================================================")
	fmt.Println("  Results")
	fmt.Println("============================================================")
	denom := ok + fail
	if denom == 0 {
		denom = 1
	}
	fmt.Printf("  Requests:     %d OK, %d failed (%.1f%% ok)\n",
		ok, fail, 100*float64(ok)/float64(denom))
	fmt.Printf("  Wall-clock:   %.2fs (nominal %ds)\n", actualDur, *dur)
	fmt.Printf("  Actual RPS:   %.1f (nominal-dur basis)\n", rpsNom)
	fmt.Printf("  Actual RPS:   %.1f (wall-clock basis) <- real throughput\n", rpsWall)
	fmt.Printf("  Bandwidth:    %.2f MB/s, %.3f Gbps (wall-clock)\n",
		bps/1024/1024, bps*8/1e9)

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
