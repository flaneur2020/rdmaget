package rdma

import (
	"context"
	"errors"

	"github.com/flaneur2020/rdmacp/protocol"
)

var ErrUnsupported = errors.New("rdma: unsupported; rebuild on Linux with -tags rdma and libibverbs")

type Endpoint = protocol.RDMAEndpoint
type MemoryRegion = protocol.RDMAMemoryRegion

type Conn interface {
	LocalEndpoint() Endpoint
	Connect(ctx context.Context, remote Endpoint) error
	RegisterRemoteRead(buf []byte) (MemoryRegion, func() error, error)
	Read(ctx context.Context, dst []byte, remote MemoryRegion) error
	Close() error
}

type DataPlane interface {
	NewConn(ctx context.Context) (Conn, error)
	Close() error
}

type Options struct {
	Device   string
	Port     uint8
	GIDIndex int
}

func Open(opts Options) (DataPlane, error) {
	return open(opts)
}
