// echo_tcp.go — TCP echo server (B안)
//
// Per connection: read N bytes → write the same N bytes back.
// We don't know N up-front; we just read whatever arrives and bounce it.
// bench_tcp drives a request/response pattern of fixed msg_size, so a
// simple "read up to msg_size, echo back" loop suffices.
//
// Listens on :9091 by default (matches sidecar upstream port).
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

const bufSize = 16 * 1024 // ≥ DPA_DMA_COPY_MAX (8K) for any single bench msg

func handleConn(c net.Conn) {
	defer c.Close()
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	buf := make([]byte, bufSize)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			if _, werr := c.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				// quietly drop
			}
			return
		}
	}
}

func main() {
	port := flag.Int("port", 9091, "echo TCP port")
	flag.Parse()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "[echo-tcp] LISTEN on :%d\n", *port)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConn(c)
	}
}
