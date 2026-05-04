package rdma

import (
	"context"
	"errors"

	"github.com/flaneur2020/rget/protocol"
)

var ErrUnsupported = errors.New("rdma: unsupported; rebuild on Linux with -tags rdma and libibverbs")

type Endpoint = protocol.RDMAEndpoint
type MemoryRegion = protocol.RDMAMemoryRegion

type RemoteBuffer interface {
	Bytes() []byte
	Region() MemoryRegion
	Close() error
}

type Conn interface {
	LocalEndpoint() Endpoint
	Connect(ctx context.Context, remote Endpoint) error
	RegisterRemoteBuffer(size int) (RemoteBuffer, error)
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
