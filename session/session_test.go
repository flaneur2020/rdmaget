package session

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/flaneur2020/rdmacp/protocol"
	"github.com/flaneur2020/rdmacp/rdma"
)

func TestClientServerCopiesFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	dst := filepath.Join(dir, "dst.bin")
	want := []byte("hello rdma chunk 0\nhello rdma chunk 1\nhello rdma chunk 2\n")
	if err := os.WriteFile(src, want, 0o644); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	dp := newMemoryPlane()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- ServeOne(ctx, listener, dp, 8)
	}()

	if err := RunClient(ctx, ClientConfig{
		Addr:      listener.Addr().String(),
		Path:      src,
		Output:    dst,
		ChunkSize: 8,
		DataPlane: dp,
	}); err != nil {
		t.Fatalf("RunClient: %v", err)
	}

	if err := <-serverErr; err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("output mismatch:\ngot  %q\nwant %q", got, want)
	}
}

type memoryPlane struct {
	nextID  atomic.Uint64
	regions sync.Map
}

type memoryConn struct {
	plane *memoryPlane
	local rdma.Endpoint
}

func newMemoryPlane() *memoryPlane {
	return &memoryPlane{}
}

func (p *memoryPlane) NewConn(ctx context.Context) (rdma.Conn, error) {
	id := p.nextID.Add(1)
	return &memoryConn{
		plane: p,
		local: rdma.Endpoint{
			Device: "memory",
			Port:   1,
			LID:    uint16(id),
			QPN:    uint32(id),
			PSN:    uint32(id + 100),
		},
	}, ctx.Err()
}

func (p *memoryPlane) Close() error {
	return nil
}

func (c *memoryConn) LocalEndpoint() rdma.Endpoint {
	return c.local
}

func (c *memoryConn) Connect(ctx context.Context, remote rdma.Endpoint) error {
	return ctx.Err()
}

func (c *memoryConn) RegisterRemoteRead(buf []byte) (rdma.MemoryRegion, func() error, error) {
	id := c.plane.nextID.Add(1)
	copyBuf := append([]byte(nil), buf...)
	c.plane.regions.Store(id, copyBuf)
	return protocol.RDMAMemoryRegion{
			Addr:   id,
			RKey:   uint32(id),
			Length: uint64(len(copyBuf)),
		}, func() error {
			c.plane.regions.Delete(id)
			return nil
		}, nil
}

func (c *memoryConn) Read(ctx context.Context, dst []byte, remote rdma.MemoryRegion) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value, ok := c.plane.regions.Load(remote.Addr)
	if !ok {
		return fmt.Errorf("memory rdma: region %d not found", remote.Addr)
	}
	src := value.([]byte)
	if uint32(remote.Addr) != remote.RKey {
		return fmt.Errorf("memory rdma: bad rkey")
	}
	if len(dst) > len(src) {
		return fmt.Errorf("memory rdma: read length %d exceeds region %d", len(dst), len(src))
	}
	copy(dst, src[:len(dst)])
	return nil
}

func (c *memoryConn) Close() error {
	return nil
}
