package rdma

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var memoryTransport struct {
	nextID   atomic.Uint64
	nextAddr atomic.Uint64
	regions  sync.Map
}

// MemoryDevice is an in-memory RdmaDevice implementation for tests and local simulations.
type MemoryDevice struct {
	mu             sync.Mutex
	endpoint       Endpoint
	connected      bool
	activeReads    atomic.Int64
	maxActiveReads atomic.Int64
}

type memoryConn struct {
	device *MemoryDevice
	local  Endpoint
}

type memoryRdmaBuffer struct {
	region *memoryRdmaRegion
	offset int
	size   int
	closed atomic.Bool
}

type memoryRdmaRegion struct {
	base   uint64
	rkey   uint32
	buffer []byte
	refs   atomic.Int64
}

// NewMemoryDevice returns an in-memory RDMA device that shares memory with other memory devices.
func NewMemoryDevice() *MemoryDevice {
	return &MemoryDevice{
		endpoint: newMemoryAddr(),
	}
}

func (d *MemoryDevice) Info() Endpoint {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connected {
		d.endpoint = newMemoryAddr()
		d.connected = false
	}
	return d.endpoint
}

func (d *MemoryDevice) Connect(ctx context.Context, remote Endpoint) (Conn, error) {
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

func newMemoryAddr() Endpoint {
	id := memoryTransport.nextID.Add(1)
	return Endpoint{
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

func (c *memoryConn) LocalEndpoint() Endpoint {
	return c.local
}

func (c *memoryConn) RegisterRdmaBuffers(size int, count int) ([]RdmaBuffer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("memory rdma: cannot register empty buffer")
	}
	if count <= 0 {
		return nil, fmt.Errorf("memory rdma: buffer count must be positive")
	}
	if count > math.MaxInt/size {
		return nil, fmt.Errorf("memory rdma: total buffer size exceeds max int")
	}

	total := size * count
	end := memoryTransport.nextAddr.Add(uint64(total) + 1)
	region := &memoryRdmaRegion{
		base:   end - uint64(total),
		rkey:   uint32(memoryTransport.nextID.Add(1)),
		buffer: make([]byte, total),
	}
	region.refs.Store(int64(count))
	memoryTransport.regions.Store(region.base, region)

	buffers := make([]RdmaBuffer, 0, count)
	for i := 0; i < count; i++ {
		buffers = append(buffers, &memoryRdmaBuffer{
			region: region,
			offset: i * size,
			size:   size,
		})
	}
	return buffers, nil
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

	region, offset, ok := findMemoryRegion(remote.Addr)
	if !ok {
		return fmt.Errorf("memory rdma: region %d not found", remote.Addr)
	}
	if region.rkey != remote.RKey {
		return fmt.Errorf("memory rdma: bad rkey")
	}
	if remote.Length > uint64(len(region.buffer))-offset {
		return fmt.Errorf("memory rdma: read length %d exceeds region %d", remote.Length, len(region.buffer))
	}
	start := int(offset)
	end := start + int(remote.Length)
	copy(dstBytes[:int(remote.Length)], region.buffer[start:end])
	return nil
}

func (c *memoryConn) Close() error {
	return nil
}

func findMemoryRegion(addr uint64) (*memoryRdmaRegion, uint64, bool) {
	var found *memoryRdmaRegion
	var offset uint64
	memoryTransport.regions.Range(func(_, value any) bool {
		region := value.(*memoryRdmaRegion)
		if addr < region.base {
			return true
		}
		currentOffset := addr - region.base
		if currentOffset >= uint64(len(region.buffer)) {
			return true
		}
		found = region
		offset = currentOffset
		return false
	})
	return found, offset, found != nil
}

func (b *memoryRdmaBuffer) Bytes() []byte {
	if b == nil || b.region == nil {
		return nil
	}
	return b.region.buffer[b.offset : b.offset+b.size]
}

func (b *memoryRdmaBuffer) Region() MemoryRegion {
	if b == nil || b.region == nil {
		return MemoryRegion{}
	}
	return MemoryRegion{
		Addr:   b.region.base + uint64(b.offset),
		RKey:   b.region.rkey,
		Length: uint64(b.size),
	}
}

func (b *memoryRdmaBuffer) Close() error {
	if b == nil || !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	if b.region != nil && b.region.refs.Add(-1) == 0 {
		memoryTransport.regions.Delete(b.region.base)
		b.region.buffer = nil
	}
	b.region = nil
	return nil
}
