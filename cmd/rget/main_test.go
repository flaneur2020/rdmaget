package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/flaneur2020/rget/rdma"
)

func TestGetCopiesFile(t *testing.T) {
	originalOpen := openRdmaDevice
	var devicesMu sync.Mutex
	var devices []*rdma.MemoryDevice
	openRdmaDevice = func(opts rdma.Options) (rdma.RdmaDevice, error) {
		device := rdma.NewMemoryDevice()
		devicesMu.Lock()
		devices = append(devices, device)
		devicesMu.Unlock()
		return device, nil
	}
	t.Cleanup(func() {
		openRdmaDevice = originalOpen
	})

	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	dst := filepath.Join(dir, "dst.bin")
	want := []byte("hello rdma chunk 0\nhello rdma chunk 1\nhello rdma chunk 2\n")
	if err := os.WriteFile(src, want, 0o644); err != nil {
		t.Fatal(err)
	}

	addr := freeTCPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runServe(ctx, []string{
			"-listen", addr,
			"-chunk-size", "8",
			"-chunk-buffers", "4",
		})
	}()

	waitForTCP(t, addr)

	if err := runGet(context.Background(), []string{
		"-addr", addr,
		"-chunk-size", "8",
		"-chunk-buffers", "4",
		"-o", dst,
		src,
	}); err != nil {
		t.Fatalf("runGet: %v", err)
	}

	cancel()
	if err := <-serverErr; err != nil && err != context.Canceled {
		t.Fatalf("runServe: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("output mismatch:\ngot  %q\nwant %q", got, want)
	}
	devicesMu.Lock()
	defer devicesMu.Unlock()
	if len(devices) != 2 {
		t.Fatalf("opened RDMA devices = %d, want 2", len(devices))
	}
	if max := devices[1].MaxActiveReads(); max < 2 {
		t.Fatalf("max concurrent RDMA reads = %d, want at least 2", max)
	}
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not start listening on %s: %v", addr, lastErr)
}
