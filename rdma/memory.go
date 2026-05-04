package rdma

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var memoryTransport struct {
	nextID  atomic.Uint64
	regions sync.Map
}

// MemoryDevice is an in-memory RdmaDevice implementation for tests and local simulations.
type MemoryDevice struct {
	mu             sync.Mutex
	endpoint       Addr
	connected      bool
	activeReads    atomic.Int64
	maxActiveReads atomic.Int64
}

type memoryConn struct {
	device *MemoryDevice
	local  Addr
}

type memoryRdmaBuffer struct {
	id     uint64
	rkey   uint32
	buffer []byte
}

// NewMemoryDevice returns an in-memory RDMA device that shares memory with other memory devices.
func NewMemoryDevice() *MemoryDevice {
	return &MemoryDevice{
		endpoint: newMemoryAddr(),
	}
}

func (d *MemoryDevice) Info() Addr {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connected {
		d.endpoint = newMemoryAddr()
		d.connected = false
	}
	return d.endpoint
}

func (d *MemoryDevice) Connect(ctx context.Context, remote Addr) (Conn, error) {
	d.mu.Lock()
	if d.connected {
		d.endpoint = newMemoryAddr()
	}
	local := d.endpoint
	d.connected = true
	d.mu.Unlock()
	return &memoryConn{
		device: d,
		local:  local,
	}, ctx.Err()
}

func newMemoryAddr() Addr {
	id := memoryTransport.nextID.Add(1)
	return Addr{
		Device: "memory",
		Port:   1,
		LID:    uint16(id),
		QPN:    uint32(id),
		PSN:    uint32(id + 100),
	}
}

func (d *MemoryDevice) Close() error {
	return nil
}

// MaxActiveReads returns the highest number of concurrent reads observed by this device.
func (d *MemoryDevice) MaxActiveReads() int64 {
	return d.maxActiveReads.Load()
}

func (c *memoryConn) LocalEndpoint() Addr {
	return c.local
}

func (c *memoryConn) RegisterRdmaBuffer(size int) (RdmaBuffer, error) {
	id := memoryTransport.nextID.Add(1)
	buffer := make([]byte, size)
	memoryTransport.regions.Store(id, buffer)
	return &memoryRdmaBuffer{
		id:     id,
		rkey:   uint32(id),
		buffer: buffer,
	}, nil
}

func (c *memoryConn) Read(ctx context.Context, dst RdmaBuffer, remote MemoryRegion) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dstBytes := dst.Bytes()
	if remote.Length == 0 {
		return nil
	}
	if remote.Length > uint64(len(dstBytes)) {
		return fmt.Errorf("memory rdma: read length %d exceeds local buffer %d", remote.Length, len(dstBytes))
	}
	active := c.device.activeReads.Add(1)
	for {
		max := c.device.maxActiveReads.Load()
		if active <= max || c.device.maxActiveReads.CompareAndSwap(max, active) {
			break
		}
	}
	defer c.device.activeReads.Add(-1)
	time.Sleep(5 * time.Millisecond)

	value, ok := memoryTransport.regions.Load(remote.Addr)
	if !ok {
		return fmt.Errorf("memory rdma: region %d not found", remote.Addr)
	}
	src := value.([]byte)
	if uint32(remote.Addr) != remote.RKey {
		return fmt.Errorf("memory rdma: bad rkey")
	}
	if remote.Length > uint64(len(src)) {
		return fmt.Errorf("memory rdma: read length %d exceeds region %d", remote.Length, len(src))
	}
	copy(dstBytes[:int(remote.Length)], src[:int(remote.Length)])
	return nil
}

func (c *memoryConn) Close() error {
	return nil
}

func (b *memoryRdmaBuffer) Bytes() []byte {
	return b.buffer
}

func (b *memoryRdmaBuffer) Region() MemoryRegion {
	return MemoryRegion{
		Addr:   b.id,
		RKey:   b.rkey,
		Length: uint64(len(b.buffer)),
	}
}

func (b *memoryRdmaBuffer) Close() error {
	memoryTransport.regions.Delete(b.id)
	b.buffer = nil
	return nil
}
