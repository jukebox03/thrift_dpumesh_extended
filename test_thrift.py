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

    targets = [59, 100, 126, 127, 128, 129, 130, 256, 512, 1024]
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

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <host> [port] [num_threads|size]")
        print(f"       {sys.argv[0]} <host> [port] size   — run DMA size boundary test")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9091

    # "size" mode: run size boundary test
    if len(sys.argv) > 3 and sys.argv[3] == "size":
        ok = run_size_test(host, port)
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
