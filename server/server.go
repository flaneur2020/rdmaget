package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"

	"github.com/flaneur2020/rget/rdma"
	"github.com/flaneur2020/rget/transfer"
)

type Config struct {
	Addr         string
	ChunkSize    uint64
	ChunkBuffers int
	DataPlane    rdma.DataPlane
}

type server struct {
	cfg          Config
	chunkSize    uint64
	chunkBuffers int
}

func newServer(cfg Config) (*server, error) {
	if cfg.DataPlane == nil {
		return nil, errors.New("server: data plane is required")
	}
	chunkSize := cfg.ChunkSize
	if chunkSize == 0 {
		chunkSize = transfer.DefaultChunkSize
	}
	if chunkSize > math.MaxInt {
		return nil, fmt.Errorf("server: chunk size %d exceeds max int", chunkSize)
	}
	chunkBuffers := cfg.ChunkBuffers
	if chunkBuffers == 0 {
		chunkBuffers = transfer.DefaultChunkBuffers
	}
	if chunkBuffers < 0 {
		return nil, fmt.Errorf("server: chunk buffers must be positive")
	}
	return &server{
		cfg:          cfg,
		chunkSize:    chunkSize,
		chunkBuffers: chunkBuffers,
	}, nil
}

func Run(ctx context.Context, cfg Config) error {
	srv, err := newServer(cfg)
	if err != nil {
		return err
	}
	defer srv.close()
	return srv.run(ctx)
}

func (s *server) run(ctx context.Context) error {
	if s.cfg.Addr == "" {
		return errors.New("server: addr is required")
	}
	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.cfg.Addr, err)
	}
	defer listener.Close()

	errc := make(chan error, 1)
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					errc <- ctx.Err()
					return
				}
				errc <- fmt.Errorf("server: accept: %w", err)
				return
			}
			go func() {
				_ = s.serveConn(ctx, conn)
			}()
		}
	}()

	return <-errc
}

func ServeOne(ctx context.Context, listener net.Listener, dp rdma.DataPlane, chunkSize uint64) error {
	srv, err := newServer(Config{
		Addr:      listener.Addr().String(),
		ChunkSize: chunkSize,
		DataPlane: dp,
	})
	if err != nil {
		return err
	}
	defer srv.close()
	return srv.serveOne(ctx, listener)
}

func (s *server) serveOne(ctx context.Context, listener net.Listener) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	return s.serveConn(ctx, conn)
}

func (s *server) serveConn(ctx context.Context, conn net.Conn) error {
	session := NewSession(conn, s.cfg.DataPlane, s.chunkSize, s.chunkBuffers)
	return session.Run(ctx)
}

func (s *server) close() error {
	return nil
}
