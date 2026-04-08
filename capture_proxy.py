#!/usr/bin/env python3
"""TCP capture proxy: logs raw bytes from client, forwards to real backend, returns response."""
import socket
import struct
import sys
import time
import threading

LISTEN_PORT = 9092
DUMP_FILE = "/tmp/captured_request.bin"

def handle_client(client_sock, client_addr, backend_host, backend_port):
    print(f"[proxy] Connection from {client_addr}", flush=True)
    try:
        # Read framed Thrift: 4-byte length prefix + payload
        len_bytes = b''
        while len(len_bytes) < 4:
            chunk = client_sock.recv(4 - len(len_bytes))
            if not chunk:
                print("[proxy] Client closed before frame length", flush=True)
                return
            len_bytes += chunk

        frame_len = struct.unpack('>I', len_bytes)[0]
        print(f"[proxy] Frame length: {frame_len}", flush=True)

        payload = b''
        while len(payload) < frame_len:
            chunk = client_sock.recv(frame_len - len(payload))
            if not chunk:
                break
            payload += chunk

        full_frame = len_bytes + payload
        print(f"[proxy] Total captured: {len(full_frame)} bytes (4 + {len(payload)})", flush=True)
        print(f"[proxy] Hex dump (first 200 bytes): {full_frame[:200].hex()}", flush=True)

        # Parse Thrift method name from TBinaryProtocol
        if len(payload) >= 4:
            version = struct.unpack('>I', payload[:4])[0]
            if version & 0x80010000 == 0x80010000:  # version 1
                msg_type = version & 0xFF
                name_len = struct.unpack('>I', payload[4:8])[0]
                method_name = payload[8:8+name_len].decode('ascii', errors='replace')
                seq_id = struct.unpack('>I', payload[8+name_len:12+name_len])[0]
                print(f"[proxy] Method: {method_name}, msg_type: {msg_type}, seq_id: {seq_id}", flush=True)

        # Save to file
        with open(DUMP_FILE, 'wb') as f:
            f.write(full_frame)
        print(f"[proxy] Saved to {DUMP_FILE}", flush=True)

        # Forward to real backend
        print(f"[proxy] Forwarding to {backend_host}:{backend_port}...", flush=True)
        with socket.create_connection((backend_host, backend_port), timeout=10) as backend:
            backend.sendall(full_frame)

            # Read response
            resp_len_bytes = b''
            while len(resp_len_bytes) < 4:
                chunk = backend.recv(4 - len(resp_len_bytes))
                if not chunk:
                    break
                resp_len_bytes += chunk

            if len(resp_len_bytes) == 4:
                resp_frame_len = struct.unpack('>I', resp_len_bytes)[0]
                resp_payload = b''
                while len(resp_payload) < resp_frame_len:
                    chunk = backend.recv(resp_frame_len - len(resp_payload))
                    if not chunk:
                        break
                    resp_payload += chunk
                resp_full = resp_len_bytes + resp_payload
                print(f"[proxy] Response: {len(resp_full)} bytes", flush=True)
                print(f"[proxy] Response hex (first 100): {resp_full[:100].hex()}", flush=True)
                client_sock.sendall(resp_full)
            else:
                print("[proxy] No response from backend", flush=True)
    except Exception as e:
        print(f"[proxy] Error: {e}", flush=True)
    finally:
        client_sock.close()

def main():
    backend_host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    backend_port = int(sys.argv[2]) if len(sys.argv) > 2 else 9091

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', LISTEN_PORT))
    server.listen(5)
    print(f"[proxy] Listening on :{LISTEN_PORT}, forwarding to {backend_host}:{backend_port}", flush=True)

    while True:
        client_sock, addr = server.accept()
        t = threading.Thread(target=handle_client, args=(client_sock, addr, backend_host, backend_port))
        t.daemon = True
        t.start()

if __name__ == '__main__':
    main()
