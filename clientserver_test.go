package rget_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flaneur2020/rget/client"
	"github.com/flaneur2020/rget/protocol"
	"github.com/flaneur2020/rget/rdma"
	"github.com/flaneur2020/rget/server"
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
		serverErr <- server.ServeOne(ctx, listener, dp, 8)
	}()

	if err := client.Run(ctx, client.Config{
		Addr:      listener.Addr().String(),
		Path:      src,
		Output:    dst,
		ChunkSize: 8,
		DataPlane: dp,
	}); err != nil {
		t.Fatalf("client.Run: %v", err)
	}

	if err := <-serverErr; err != nil {
		t.Fatalf("server.ServeOne: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("output mismatch:\ngot  %q\nwant %q", got, want)
	}
	if max := dp.maxActiveReads.Load(); max < 2 {
		t.Fatalf("max concurrent RDMA reads = %d, want at least 2", max)
	}
}

type memoryPlane struct {
	nextID         atomic.Uint64
	activeReads    atomic.Int64
	maxActiveReads atomic.Int64
	regions        sync.Map
}

type memoryConn struct {
	plane *memoryPlane
	local rdma.Endpoint
}

type memoryRemoteBuffer struct {
	plane  *memoryPlane
	id     uint64
	rkey   uint32
	buffer []byte
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

func (c *memoryConn) RegisterRemoteBuffer(size int) (rdma.RemoteBuffer, error) {
	id := c.plane.nextID.Add(1)
	buffer := make([]byte, size)
	c.plane.regions.Store(id, buffer)
	return &memoryRemoteBuffer{
		plane:  c.plane,
		id:     id,
		rkey:   uint32(id),
		buffer: buffer,
	}, nil
}

func (c *memoryConn) Read(ctx context.Context, dst []byte, remote rdma.MemoryRegion) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	active := c.plane.activeReads.Add(1)
	for {
		max := c.plane.maxActiveReads.Load()
		if active <= max || c.plane.maxActiveReads.CompareAndSwap(max, active) {
			break
		}
	}
	defer c.plane.activeReads.Add(-1)
	time.Sleep(5 * time.Millisecond)

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

func (b *memoryRemoteBuffer) Bytes() []byte {
	return b.buffer
}

func (b *memoryRemoteBuffer) Region() rdma.MemoryRegion {
	return protocol.RDMAMemoryRegion{
		Addr:   b.id,
		RKey:   b.rkey,
		Length: uint64(len(b.buffer)),
	}
}

func (b *memoryRemoteBuffer) Close() error {
	b.plane.regions.Delete(b.id)
	b.buffer = nil
	return nil
}
