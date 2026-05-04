package rdma

import (
	"context"
	"errors"

	"github.com/flaneur2020/rget/protocol"
)

var ErrUnsupported = errors.New("rdma: unsupported; rebuild on Linux with -tags rdma and libibverbs")

type Addr = protocol.RDMAEndpoint
type MemoryRegion = protocol.RDMAMemoryRegion

type RdmaDevice interface {
	Info() Addr
	Connect(ctx context.Context, remote Addr) (Conn, error)
	Close() error
}

type RdmaBuffer interface {
	Bytes() []byte
	Region() MemoryRegion
	Close() error
}

type Conn interface {
	LocalEndpoint() Addr
	RegisterRdmaBuffer(size int) (RdmaBuffer, error)
	Read(ctx context.Context, dst RdmaBuffer, remote MemoryRegion) error
	Close() error
}

type Options struct {
	Device   string
	Port     uint8
	GIDIndex int
}

func Open(opts Options) (RdmaDevice, error) {
	return open(opts)
}
