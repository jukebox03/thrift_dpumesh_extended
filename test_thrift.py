#!/usr/bin/env python3
import socket
import struct
import sys
import threading
import time

# Thrift TBinaryProtocol types
T_STOP   = 0
T_I32    = 8
T_I64    = 10
T_STRING = 11
T_MAP    = 13

def build_thrift_framed_call(method_name, args_payload, seq_id=0):
    """Wraps a TBinaryProtocol call in a Thrift Frame (4-byte length)."""
    header = struct.pack('>HH', 0x8001, 1) # Version(0x8001), Type(Call=1)
    name_bytes = method_name.encode('ascii')
    header += struct.pack('>I', len(name_bytes)) + name_bytes
    header += struct.pack('>I', seq_id)
    
    payload = header + args_payload
    frame = struct.pack('>I', len(payload)) + payload
    return frame

def build_compose_unique_id_args(req_id=12345, post_type=1, pad_bytes=0):
    """Builds the binary payload for UniqueIdService.ComposeUniqueId arguments.
    pad_bytes: extra bytes to add via carrier map entries to inflate message size."""
    f1 = struct.pack('>bHq', T_I64, 1, req_id)
    f2 = struct.pack('>bHi', T_I32, 2, post_type)

    if pad_bytes > 0:
        # Add a single map entry with key="p" and value=pad_string to carrier map
        pad_str = b'X' * pad_bytes
        map_data = struct.pack('>I', len(b'p')) + b'p' + struct.pack('>I', len(pad_str)) + pad_str
        f3 = struct.pack('>bHbbI', T_MAP, 3, T_STRING, T_STRING, 1) + map_data
    else:
        f3 = struct.pack('>bHbbI', T_MAP, 3, T_STRING, T_STRING, 0) # empty map

    stop = struct.pack('>b', T_STOP)
    return f1 + f2 + f3 + stop

def send_request(host, port, thread_id):
    """Single request worker."""
    req_id = 1000 + thread_id
    args = build_compose_unique_id_args(req_id=req_id)
    frame = build_thrift_framed_call("ComposeUniqueId", args, seq_id=thread_id)
    
    print(f"[Thread-{thread_id}] Connecting to {host}:{port}...")
    try:
        with socket.create_connection((host, port), timeout=10) as s:
            s.sendall(frame)
            
            # Read response frame length (4 bytes)
            resp_len_bytes = s.recv(4)
            if not resp_len_bytes:
                print(f"[Thread-{thread_id}] [!] Connection closed")
                return False
            
            resp_len = struct.unpack('>I', resp_len_bytes)[0]
            resp_payload = b""
            while len(resp_payload) < resp_len:
                chunk = s.recv(resp_len - len(resp_payload))
                if not chunk: break
                resp_payload += chunk
            
            print(f"[Thread-{thread_id}] [+] Received {len(resp_payload)} bytes response. (hex: {resp_payload[:16].hex()}...)")
            return True
    except Exception as e:
        print(f"[Thread-{thread_id}] [!] Error: {e}")
        return False

def calc_pad_for_target(target_frame_size):
    """Calculate pad_bytes needed to make frame exactly target_frame_size bytes.
    Base frame (no padding, empty map) = 59 bytes.
    Adding 1 map entry with key 'p' adds 9 bytes overhead: frame = 68 + pad_bytes.
    For targets <= 68, use pad_bytes=0 (frame stays at 59B or 68B)."""
    if target_frame_size <= 59:
        return 0  # no padding possible below base size
    return target_frame_size - 68

def send_sized_request(host, port, target_size, seq_id=0, timeout=10):
    """Send a request with a specific total frame size. Returns (success, actual_size)."""
    pad = calc_pad_for_target(target_size)
    args = build_compose_unique_id_args(req_id=9000 + seq_id, pad_bytes=pad)
    frame = build_thrift_framed_call("ComposeUniqueId", args, seq_id=seq_id)
    actual = len(frame)

    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.sendall(frame)

            resp_len_bytes = b""
            while len(resp_len_bytes) < 4:
                chunk = s.recv(4 - len(resp_len_bytes))
                if not chunk:
                    return False, actual
                resp_len_bytes += chunk

            resp_len = struct.unpack('>I', resp_len_bytes)[0]
            resp_payload = b""
            while len(resp_payload) < resp_len:
                chunk = s.recv(resp_len - len(resp_payload))
                if not chunk:
                    break
                resp_payload += chunk

            return True, actual
    except Exception as e:
        print(f"  Error: {e}")
        return False, actual

def run_size_test(host, port):
    """Test DMA with various message sizes around 128-byte boundary."""
    print(f"\n{'='*60}")
    print(f"  DMA Size Boundary Test — {host}:{port}")
    print(f"{'='*60}")

    targets = [59, 100, 128, 256, 512, 1024, 2048, 4096, 8192,
               16384, 32768, 65536, 131072]
    results = []

    for i, target in enumerate(targets):
        ok, actual = send_sized_request(host, port, target, seq_id=i, timeout=15)
        status = "OK" if ok else "FAIL"
        results.append((target, actual, ok))
        print(f"  [{status:4s}] target={target:5d}B  actual_frame={actual:5d}B")
        time.sleep(0.3)

    print(f"\n{'='*60}")
    print(f"  Results Summary")
    print(f"{'='*60}")
    pass_count = sum(1 for _, _, ok in results if ok)
    fail_count = len(results) - pass_count
    for target, actual, ok in results:
        mark = "PASS" if ok else "FAIL"
        print(f"  {mark}  {actual:5d}B")
    print(f"\n  Total: {pass_count} passed, {fail_count} failed out of {len(results)}")

    if fail_count > 0:
        # Find boundary
        last_pass = 0
        first_fail = 0
        for target, actual, ok in results:
            if ok:
                last_pass = actual
            elif first_fail == 0:
                first_fail = actual
        if last_pass and first_fail:
            print(f"  >>> DMA boundary: last PASS={last_pass}B, first FAIL={first_fail}B")

    return fail_count == 0

def stress_worker(host, port, thread_id, num_requests, results_list, timeout=10, pad_bytes=0):
    """Send num_requests sequentially on one connection per request."""
    ok = 0
    fail = 0
    for i in range(num_requests):
        req_id = thread_id * 100000 + i
        args = build_compose_unique_id_args(req_id=req_id, pad_bytes=pad_bytes)
        frame = build_thrift_framed_call("ComposeUniqueId", args, seq_id=req_id % 65536)
        try:
            with socket.create_connection((host, port), timeout=timeout) as s:
                s.sendall(frame)
                resp_len_bytes = b""
                while len(resp_len_bytes) < 4:
                    chunk = s.recv(4 - len(resp_len_bytes))
                    if not chunk:
                        raise ConnectionError("connection closed")
                    resp_len_bytes += chunk
                resp_len = struct.unpack('>I', resp_len_bytes)[0]
                resp_payload = b""
                while len(resp_payload) < resp_len:
                    chunk = s.recv(resp_len - len(resp_payload))
                    if not chunk:
                        break
                    resp_payload += chunk
                ok += 1
        except Exception as e:
            fail += 1
            if fail <= 3:
                print(f"  [Thread-{thread_id}] req#{i} error: {e}")
    results_list[thread_id] = (ok, fail)


def run_stress_test(host, port, num_threads, num_requests, pad_bytes=0):
    """High-load stress test: num_threads concurrent threads, each sending num_requests."""
    total_target = num_threads * num_requests
    # Calculate actual frame size for display
    sample_args = build_compose_unique_id_args(req_id=0, pad_bytes=pad_bytes)
    sample_frame = build_thrift_framed_call("ComposeUniqueId", sample_args, seq_id=0)
    frame_size = len(sample_frame)

    print(f"\n{'='*60}")
    print(f"  Stress Test — {host}:{port}")
    print(f"  {num_threads} threads x {num_requests} requests = {total_target} total")
    print(f"  Message size: {frame_size} bytes (pad={pad_bytes})")
    print(f"{'='*60}\n")

    results_list = [None] * num_threads
    threads = []
    t0 = time.time()

    for i in range(num_threads):
        t = threading.Thread(target=stress_worker,
                             args=(host, port, i, num_requests, results_list),
                             kwargs={'pad_bytes': pad_bytes})
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    elapsed = time.time() - t0
    total_ok = sum(r[0] for r in results_list if r)
    total_fail = sum(r[1] for r in results_list if r)

    print(f"\n{'='*60}")
    print(f"  Stress Test Results")
    print(f"{'='*60}")
    print(f"  Threads:    {num_threads}")
    print(f"  Msg size:   {frame_size} bytes")
    print(f"  Succeeded:  {total_ok}/{total_target}")
    print(f"  Failed:     {total_fail}/{total_target}")
    print(f"  Duration:   {elapsed:.2f}s")
    if elapsed > 0:
        print(f"  Throughput: {total_ok / elapsed:.1f} req/s")
        print(f"  Bandwidth:  {total_ok * frame_size / elapsed / 1024 / 1024:.1f} MB/s")
    print(f"  Success rate: {total_ok * 100 / max(total_target, 1):.1f}%")

    if total_fail > 0:
        print(f"\n  Per-thread breakdown:")
        for i, r in enumerate(results_list):
            if r and r[1] > 0:
                print(f"    Thread-{i}: {r[0]} ok, {r[1]} fail")

    return total_fail == 0


# ---------------------------------------------------------------------------
# Throughput mode — wrk2-style constant-rate load generator
# ---------------------------------------------------------------------------

def _recv_exact(s, n):
    """Receive exactly n bytes from socket."""
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed")
        buf += chunk
    return buf


def throughput_worker(host, port, thread_id, schedule, results_queue,
                      frame, timeout=10):
    """Each thread owns a persistent connection and fires requests at scheduled times.
    schedule: list of absolute timestamps (time.monotonic based) when to send.
    results_queue: list to append (scheduled_time, actual_send_time, rtt, ok) tuples.
    Coordinated omission: latency = response_time - scheduled_time (not send_time).
    """
    local_results = []
    sock = None

    def ensure_conn():
        nonlocal sock
        if sock is not None:
            return
        sock = socket.create_connection((host, port), timeout=timeout)

    for sched_ts in schedule:
        # Wait until scheduled time
        now = time.monotonic()
        if sched_ts > now:
            time.sleep(sched_ts - now)

        send_ts = time.monotonic()
        ok = False
        try:
            ensure_conn()
            sock.sendall(frame)
            resp_len_bytes = _recv_exact(sock, 4)
            resp_len = struct.unpack('>I', resp_len_bytes)[0]
            _recv_exact(sock, resp_len)
            ok = True
        except Exception:
            # Reconnect on next attempt
            if sock:
                try: sock.close()
                except: pass
                sock = None
        rtt = time.monotonic() - send_ts
        # Coordinated-omission corrected latency: from scheduled time
        corrected_latency = time.monotonic() - sched_ts
        local_results.append((corrected_latency, rtt, ok))

    if sock:
        try: sock.close()
        except: pass

    results_queue[thread_id] = local_results


def run_throughput_test(host, port, target_rps, duration_sec, pad_bytes=0,
                        num_threads=None):
    """wrk2-style constant-rate throughput test.
    - Distributes requests evenly across threads
    - Measures coordinated-omission corrected latency
    - Reports percentile histogram
    """
    if num_threads is None:
        # Auto: 1 thread per 50 RPS, min 1, max 256
        num_threads = max(1, min(256, target_rps // 50 + 1))

    total_requests = target_rps * duration_sec
    interval = 1.0 / target_rps if target_rps > 0 else 1.0

    # Build frame once
    pad = pad_bytes
    args = build_compose_unique_id_args(req_id=0, pad_bytes=pad)
    frame = build_thrift_framed_call("ComposeUniqueId", args, seq_id=0)
    frame_size = len(frame)

    print(f"\n{'='*60}")
    print(f"  Throughput Test (wrk2-style) — {host}:{port}")
    print(f"{'='*60}")
    print(f"  Target RPS:   {target_rps}")
    print(f"  Duration:     {duration_sec}s")
    print(f"  Threads:      {num_threads}")
    print(f"  Total reqs:   {total_requests}")
    print(f"  Msg size:     {frame_size} bytes (pad={pad})")
    print(f"{'='*60}\n")

    # Build per-thread schedules (interleaved assignment)
    base_time = time.monotonic() + 0.5  # 500ms warmup
    schedules = [[] for _ in range(num_threads)]
    for i in range(total_requests):
        t = base_time + i * interval
        schedules[i % num_threads].append(t)

    results_queue = [None] * num_threads
    threads = []

    for i in range(num_threads):
        t = threading.Thread(target=throughput_worker,
                             args=(host, port, i, schedules[i], results_queue,
                                   frame))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Aggregate results
    all_latencies = []  # coordinated-omission corrected
    all_rtts = []       # raw RTT
    ok_count = 0
    fail_count = 0

    for thread_results in results_queue:
        if not thread_results:
            continue
        for corrected, rtt, ok in thread_results:
            if ok:
                all_latencies.append(corrected * 1000)  # ms
                all_rtts.append(rtt * 1000)              # ms
                ok_count += 1
            else:
                fail_count += 1

    actual_duration = duration_sec
    actual_rps = ok_count / actual_duration if actual_duration > 0 else 0

    print(f"{'='*60}")
    print(f"  Results")
    print(f"{'='*60}")
    print(f"  Requests:     {ok_count} OK, {fail_count} failed "
          f"({ok_count * 100 / max(ok_count + fail_count, 1):.1f}%)")
    print(f"  Actual RPS:   {actual_rps:.1f}")
    print(f"  Bandwidth:    {ok_count * frame_size / actual_duration / 1024 / 1024:.2f} MB/s")

    if all_latencies:
        all_latencies.sort()
        all_rtts.sort()
        n = len(all_latencies)

        def pct(arr, p):
            idx = min(int(len(arr) * p / 100), len(arr) - 1)
            return arr[idx]

        print(f"\n  Latency (corrected for coordinated omission):")
        print(f"    Avg:    {sum(all_latencies) / n:.2f} ms")
        print(f"    P50:    {pct(all_latencies, 50):.2f} ms")
        print(f"    P95:    {pct(all_latencies, 95):.2f} ms")
        print(f"    P99:    {pct(all_latencies, 99):.2f} ms")
        print(f"    Max:    {all_latencies[-1]:.2f} ms")

        print(f"\n  Raw RTT (uncorrected):")
        print(f"    Avg:    {sum(all_rtts) / n:.2f} ms")
        print(f"    P50:    {pct(all_rtts, 50):.2f} ms")
        print(f"    P95:    {pct(all_rtts, 95):.2f} ms")
        print(f"    P99:    {pct(all_rtts, 99):.2f} ms")
        print(f"    Max:    {all_rtts[-1]:.2f} ms")

    print(f"{'='*60}")
    return fail_count == 0


def build_compose_unique_id_args_2entry(first_val_len):
    """Builds args with 2 carrier map entries.
    Entry 1: key='k', value='V'*first_val_len (variable size)
    Entry 2: key='x', value='y' (fixed, structural bytes must survive DMA)
    This ensures structural Thrift bytes (type/length fields) are placed
    at different offsets depending on first_val_len."""
    f1 = struct.pack('>bHq', T_I64, 1, 9999)
    f2 = struct.pack('>bHi', T_I32, 2, 1)

    # Map header: 2 entries
    map_hdr = struct.pack('>bHbbI', T_MAP, 3, T_STRING, T_STRING, 2)
    # Entry 1
    key1 = b'k'
    val1 = b'V' * first_val_len
    e1 = struct.pack('>I', len(key1)) + key1 + struct.pack('>I', len(val1)) + val1
    # Entry 2 (small, structural bytes must be correct)
    key2 = b'x'
    val2 = b'y'
    e2 = struct.pack('>I', len(key2)) + key2 + struct.pack('>I', len(val2)) + val2
    stop = struct.pack('>b', T_STOP)
    return f1 + f2 + map_hdr + e1 + e2 + stop


def run_boundary_test(host, port):
    """Binary search for exact DMA boundary using 2-entry carrier maps.
    Entry 2's structural bytes move past the boundary as entry 1 grows."""
    print(f"\n{'='*60}")
    print(f"  DMA Boundary Test (2-entry carrier map) — {host}:{port}")
    print(f"{'='*60}")

    # First: compare single-entry vs 2-entry at same size (~140B)
    print(f"\n  --- Control: single-entry padding vs 2-entry at ~140B ---")

    # Single entry 140B (padding, should pass if padding theory correct)
    pad = calc_pad_for_target(140)
    args_single = build_compose_unique_id_args(req_id=8000, pad_bytes=pad)
    frame_single = build_thrift_framed_call("ComposeUniqueId", args_single, seq_id=100)
    ok_s, sz_s = send_sized_request(host, port, 140, seq_id=100, timeout=10)
    print(f"  Single-entry 140B: {'PASS' if ok_s else 'FAIL'} (actual={sz_s}B)")
    time.sleep(0.3)

    # 2-entry ~140B (first_val_len chosen so frame ≈ 140B)
    # frame = 78 + first_val_len, so first_val_len = 62 → frame = 140B
    args_2e = build_compose_unique_id_args_2entry(first_val_len=62)
    frame_2e = build_thrift_framed_call("ComposeUniqueId", args_2e, seq_id=101)
    actual_2e = len(frame_2e)
    try:
        with socket.create_connection((host, port), timeout=10) as s:
            s.sendall(frame_2e)
            resp = b""
            while len(resp) < 4:
                chunk = s.recv(4 - len(resp))
                if not chunk: break
                resp += chunk
            if len(resp) == 4:
                rlen = struct.unpack('>I', resp)[0]
                rpay = b""
                while len(rpay) < rlen:
                    chunk = s.recv(rlen - len(rpay))
                    if not chunk: break
                    rpay += chunk
                ok_2e = True
            else:
                ok_2e = False
    except:
        ok_2e = False
    print(f"  2-entry   {actual_2e}B: {'PASS' if ok_2e else 'FAIL'}")
    time.sleep(0.3)

    # Sweep: 2-entry maps with increasing first_val_len
    # frame = 78 + first_val_len
    # entry2 key_len starts at frame byte 67 + first_val_len
    print(f"\n  --- Sweep: 2-entry frames from 80B to 200B ---")
    print(f"  (entry2 structural bytes move past DMA boundary)")
    last_pass = 0
    first_fail = 0
    results = []

    for fvl in list(range(0, 130, 2)):  # frame = 78 to 208 in 2B steps
        args = build_compose_unique_id_args_2entry(first_val_len=fvl)
        frame = build_thrift_framed_call("ComposeUniqueId", args, seq_id=200+fvl)
        fsz = len(frame)
        entry2_offset = 67 + fvl  # frame byte where entry2 key_len starts
        try:
            with socket.create_connection((host, port), timeout=10) as s:
                s.sendall(frame)
                resp = b""
                while len(resp) < 4:
                    chunk = s.recv(4 - len(resp))
                    if not chunk: break
                    resp += chunk
                if len(resp) == 4:
                    rlen = struct.unpack('>I', resp)[0]
                    rpay = b""
                    while len(rpay) < rlen:
                        chunk = s.recv(rlen - len(rpay))
                        if not chunk: break
                        rpay += chunk
                    ok = True
                else:
                    ok = False
        except:
            ok = False
        status = "PASS" if ok else "FAIL"
        results.append((fsz, entry2_offset, ok))
        print(f"  [{status}] frame={fsz:4d}B  entry2_at_byte={entry2_offset:4d}")
        if ok:
            last_pass = fsz
        elif first_fail == 0:
            first_fail = fsz
        time.sleep(0.2)

        # Stop after 5 consecutive failures
        recent_fails = sum(1 for _, _, o in results[-5:] if not o)
        if recent_fails >= 5:
            print(f"  (stopping after 5 consecutive failures)")
            break

    print(f"\n  --- Summary ---")
    if last_pass and first_fail:
        print(f"  Last PASS: {last_pass}B")
        print(f"  First FAIL: {first_fail}B")
        print(f"  >>> DMA boundary between {last_pass}B and {first_fail}B")
    elif first_fail == 0:
        print(f"  All passed! No DMA boundary found in range.")
    else:
        print(f"  First failure at {first_fail}B")

    return first_fail == 0


def parse_size_str(s):
    """Parse size string like '128K', '1M', '4096' into bytes."""
    s = s.strip().upper()
    if s.endswith('K') or s.endswith('KB'):
        return int(s.rstrip('KB')) * 1024
    elif s.endswith('M') or s.endswith('MB'):
        return int(s.rstrip('MB')) * 1024 * 1024
    return int(s)


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <host> [port] [num_threads|size|stress|throughput]")
        print(f"       {sys.argv[0]} <host> [port] size                              — DMA size boundary test")
        print(f"       {sys.argv[0]} <host> [port] stress [T] [N] [SIZE]             — stress test (T threads x N reqs)")
        print(f"       {sys.argv[0]} <host> [port] throughput [RPS] [DUR] [SIZE] [T]  — wrk2-style constant-rate test")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9091

    # "size" mode: run size boundary test
    if len(sys.argv) > 3 and sys.argv[3] == "size":
        ok = run_size_test(host, port)
        sys.exit(0 if ok else 1)

    # "boundary" mode: 2-entry carrier map DMA boundary test
    if len(sys.argv) > 3 and sys.argv[3] == "boundary":
        ok = run_boundary_test(host, port)
        sys.exit(0 if ok else 1)

    # "stress" mode: high-load stress test
    if len(sys.argv) > 3 and sys.argv[3] == "stress":
        num_threads = int(sys.argv[4]) if len(sys.argv) > 4 else 10
        num_requests = int(sys.argv[5]) if len(sys.argv) > 5 else 100
        msg_size = parse_size_str(sys.argv[6]) if len(sys.argv) > 6 else 0
        # Convert target frame size to pad_bytes
        pad = calc_pad_for_target(msg_size) if msg_size > 0 else 0
        ok = run_stress_test(host, port, num_threads, num_requests, pad_bytes=pad)
        sys.exit(0 if ok else 1)

    # "throughput" mode: wrk2-style constant-rate load test
    if len(sys.argv) > 3 and sys.argv[3] == "throughput":
        rps = int(sys.argv[4]) if len(sys.argv) > 4 else 100
        duration = int(sys.argv[5]) if len(sys.argv) > 5 else 10
        msg_size = parse_size_str(sys.argv[6]) if len(sys.argv) > 6 else 8192
        threads = int(sys.argv[7]) if len(sys.argv) > 7 else None
        pad = calc_pad_for_target(msg_size) if msg_size > 0 else 0
        ok = run_throughput_test(host, port, rps, duration, pad_bytes=pad,
                                num_threads=threads)
        sys.exit(0 if ok else 1)

    num_threads = int(sys.argv[3]) if len(sys.argv) > 3 else 1

    print(f"[*] Starting {num_threads} request thread(s) to {host}:{port}...")

    threads = []
    results = [False] * num_threads

    def worker(idx):
        results[idx] = send_request(host, port, idx)

    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
        time.sleep(0.1) # Stagger starts slightly

    for t in threads:
        t.join()

    success_count = sum(1 for r in results if r)
    print(f"\n[*] Summary: {success_count}/{num_threads} requests succeeded.")

    if success_count > 0:
        print("[SUCCESS] DPU data exchange confirmed via Request Threads!")
        sys.exit(0)
    else:
        print("[FAILURE] No requests succeeded.")
        sys.exit(1)

if __name__ == "__main__":
    main()
