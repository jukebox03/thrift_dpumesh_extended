#!/usr/bin/env python3
"""Simple TCP capture server: accepts one connection, dumps raw Thrift frame, sends exception back."""
import socket, struct, time, sys

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('0.0.0.0', 9091))
server.listen(5)
print(f"[capture] Listening on :9091", flush=True)

client_sock, addr = server.accept()
print(f"[capture] Connection from {addr}", flush=True)

# Read framed Thrift: 4-byte length + payload
len_bytes = b""
while len(len_bytes) < 4:
    chunk = client_sock.recv(4 - len(len_bytes))
    if not chunk:
        break
    len_bytes += chunk

frame_len = struct.unpack(">I", len_bytes)[0]
print(f"[capture] Frame length: {frame_len}", flush=True)

payload = b""
while len(payload) < frame_len:
    chunk = client_sock.recv(frame_len - len(payload))
    if not chunk:
        break
    payload += chunk

full_frame = len_bytes + payload
print(f"[capture] Total: {len(full_frame)} bytes", flush=True)
print(f"[capture] HEX: {full_frame.hex()}", flush=True)

# Parse method name
method = "unknown"
if len(payload) >= 8:
    version = struct.unpack(">I", payload[:4])[0]
    if version & 0x80010000 == 0x80010000:
        name_len = struct.unpack(">I", payload[4:8])[0]
        method = payload[8:8+name_len].decode("ascii", errors="replace")
        seq_id_offset = 8 + name_len
        if len(payload) >= seq_id_offset + 4:
            seq_id = struct.unpack(">I", payload[seq_id_offset:seq_id_offset+4])[0]
            print(f"[capture] Method: {method}, seq_id: {seq_id}", flush=True)

with open("/tmp/captured.hex", "w") as f:
    f.write(full_frame.hex())
print("[capture] Saved to /tmp/captured.hex", flush=True)

# Send Thrift exception back
client_sock.close()
server.close()
print("[capture] Done, sleeping...", flush=True)
time.sleep(600)
