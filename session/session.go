package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"

	"github.com/flaneur2020/rdmacp/protocol"
	"github.com/flaneur2020/rdmacp/rdma"
)

const DefaultChunkSize uint64 = 1 << 20

type ClientConfig struct {
	Addr      string
	Path      string
	Output    string
	ChunkSize uint64
	DataPlane rdma.DataPlane
}

type ServerConfig struct {
	Addr      string
	ChunkSize uint64
	DataPlane rdma.DataPlane
}

func RunClient(ctx context.Context, cfg ClientConfig) error {
	if cfg.Addr == "" {
		return errors.New("session: client addr is required")
	}
	if cfg.Path == "" {
		return errors.New("session: remote path is required")
	}
	if cfg.DataPlane == nil {
		return errors.New("session: data plane is required")
	}
	chunkSize := cfg.ChunkSize
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize > math.MaxInt {
		return fmt.Errorf("session: chunk size %d exceeds max int", chunkSize)
	}

	dpConn, err := cfg.DataPlane.NewConn(ctx)
	if err != nil {
		return err
	}
	defer dpConn.Close()

	ctrl, err := dialContext(ctx, cfg.Addr)
	if err != nil {
		return err
	}
	defer ctrl.Close()

	if err := protocol.EncodeFrame(ctrl, &protocol.NewSessionFrame{
		Path:           cfg.Path,
		ChunkSize:      chunkSize,
		ClientEndpoint: dpConn.LocalEndpoint(),
	}); err != nil {
		return err
	}

	var out io.WriteCloser
	if cfg.Output == "" || cfg.Output == "-" {
		out = nopWriteCloser{Writer: os.Stdout}
	} else {
		file, err := os.Create(cfg.Output)
		if err != nil {
			return fmt.Errorf("session: create output: %w", err)
		}
		out = file
	}
	defer out.Close()

	connected := false
	for {
		frame, err := protocol.DecodeFrame(ctrl)
		if err != nil {
			return err
		}

		switch msg := frame.(type) {
		case *protocol.ChunkBufferReadyFrame:
			if msg.Size == 0 && msg.Final {
				return nil
			}
			if !connected {
				if err := dpConn.Connect(ctx, msg.ServerEndpoint); err != nil {
					return err
				}
				connected = true
			}

			if msg.Size > math.MaxInt {
				return fmt.Errorf("session: chunk size %d exceeds max int", msg.Size)
			}
			chunk := make([]byte, int(msg.Size))
			remote := msg.Buffer
			remote.Length = msg.Size
			if err := dpConn.Read(ctx, chunk, remote); err != nil {
				return err
			}
			if _, err := out.Write(chunk); err != nil {
				return fmt.Errorf("session: write output: %w", err)
			}
			if err := protocol.EncodeFrame(ctrl, &protocol.ChunkBufferAckFrame{
				BufferIndex: msg.BufferIndex,
				ChunkID:     msg.ChunkID,
			}); err != nil {
				return err
			}
			if msg.Final {
				return nil
			}
		case *protocol.ErrorFrame:
			return fmt.Errorf("server error [%s]: %s", msg.ErrorKind, msg.Message)
		default:
			return fmt.Errorf("session: unexpected frame from server: %s", frame.Kind())
		}
	}
}

func RunServer(ctx context.Context, cfg ServerConfig) error {
	if cfg.Addr == "" {
		return errors.New("session: server addr is required")
	}
	if cfg.DataPlane == nil {
		return errors.New("session: data plane is required")
	}
	chunkSize := cfg.ChunkSize
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize > math.MaxInt {
		return fmt.Errorf("session: chunk size %d exceeds max int", chunkSize)
	}

	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("session: listen %s: %w", cfg.Addr, err)
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
				errc <- fmt.Errorf("session: accept: %w", err)
				return
			}
			go func() {
				_ = handleServerConn(ctx, conn, cfg.DataPlane, chunkSize)
			}()
		}
	}()

	return <-errc
}

func ServeOne(ctx context.Context, listener net.Listener, dp rdma.DataPlane, chunkSize uint64) error {
	if dp == nil {
		return errors.New("session: data plane is required")
	}
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize > math.MaxInt {
		return fmt.Errorf("session: chunk size %d exceeds max int", chunkSize)
	}
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	return handleServerConn(ctx, conn, dp, chunkSize)
}

func handleServerConn(ctx context.Context, ctrl net.Conn, dp rdma.DataPlane, chunkSize uint64) error {
	defer ctrl.Close()

	frame, err := protocol.DecodeFrame(ctrl)
	if err != nil {
		return err
	}
	newSession, ok := frame.(*protocol.NewSessionFrame)
	if !ok {
		_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
			ErrorKind: "bad_request",
			Message:   "first frame must be NEW_SESSION",
		})
		return fmt.Errorf("session: first frame was %s", frame.Kind())
	}
	if newSession.Path == "" {
		_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
			ErrorKind: "bad_request",
			Message:   "path is required",
		})
		return errors.New("session: empty path")
	}
	if newSession.ChunkSize > 0 {
		chunkSize = newSession.ChunkSize
	}

	file, err := os.Open(newSession.Path)
	if err != nil {
		_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
			ErrorKind: "open_failed",
			Message:   err.Error(),
		})
		return fmt.Errorf("session: open %q: %w", newSession.Path, err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
			ErrorKind: "stat_failed",
			Message:   err.Error(),
		})
		return fmt.Errorf("session: stat %q: %w", newSession.Path, err)
	}
	if stat.IsDir() {
		_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
			ErrorKind: "bad_request",
			Message:   "path is a directory",
		})
		return fmt.Errorf("session: path %q is a directory", newSession.Path)
	}

	dpConn, err := dp.NewConn(ctx)
	if err != nil {
		_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
			ErrorKind: "rdma_failed",
			Message:   err.Error(),
		})
		return err
	}
	defer dpConn.Close()

	if err := dpConn.Connect(ctx, newSession.ClientEndpoint); err != nil {
		_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
			ErrorKind: "rdma_connect_failed",
			Message:   err.Error(),
		})
		return err
	}

	buffer := make([]byte, int(chunkSize))
	var chunkID uint64
	totalSize := uint64(stat.Size())
	var offset uint64
	if totalSize == 0 {
		return protocol.EncodeFrame(ctrl, &protocol.ChunkBufferReadyFrame{
			BufferIndex:    0,
			ChunkID:        0,
			Offset:         0,
			Size:           0,
			Final:          true,
			ServerEndpoint: dpConn.LocalEndpoint(),
		})
	}

	for offset < totalSize {
		want := min(chunkSize, totalSize-offset)
		n, readErr := io.ReadFull(file, buffer[:int(want)])
		if readErr != nil {
			_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
				ErrorKind: "read_failed",
				Message:   readErr.Error(),
			})
			return fmt.Errorf("session: read file: %w", readErr)
		}
		if n == 0 {
			return protocol.EncodeFrame(ctrl, &protocol.ChunkBufferReadyFrame{
				BufferIndex:    0,
				ChunkID:        chunkID,
				Offset:         offset,
				Size:           0,
				Final:          true,
				ServerEndpoint: dpConn.LocalEndpoint(),
			})
		}

		chunk := buffer[:n]
		mr, deregister, err := dpConn.RegisterRemoteRead(chunk)
		if err != nil {
			_ = protocol.EncodeFrame(ctrl, &protocol.ErrorFrame{
				ErrorKind: "rdma_register_failed",
				Message:   err.Error(),
			})
			return err
		}

		ready := &protocol.ChunkBufferReadyFrame{
			BufferIndex:    0,
			ChunkID:        chunkID,
			Offset:         offset,
			Size:           uint64(n),
			Final:          offset+uint64(n) == totalSize,
			ServerEndpoint: dpConn.LocalEndpoint(),
			Buffer:         mr,
		}
		if err := protocol.EncodeFrame(ctrl, ready); err != nil {
			_ = deregister()
			return err
		}

		ackFrame, err := protocol.DecodeFrame(ctrl)
		if err != nil {
			_ = deregister()
			return err
		}
		ack, ok := ackFrame.(*protocol.ChunkBufferAckFrame)
		if !ok {
			_ = deregister()
			return fmt.Errorf("session: expected CHUNK_BUFFER_ACK, got %s", ackFrame.Kind())
		}
		if ack.BufferIndex != ready.BufferIndex || ack.ChunkID != ready.ChunkID {
			_ = deregister()
			return fmt.Errorf("session: bad ack buffer=%d chunk=%d, want buffer=%d chunk=%d",
				ack.BufferIndex, ack.ChunkID, ready.BufferIndex, ready.ChunkID)
		}
		if err := deregister(); err != nil {
			return err
		}
		offset += uint64(n)
		if ready.Final {
			return nil
		}
		chunkID++
	}
	return nil
}

func dialContext(ctx context.Context, addr string) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("session: dial %s: %w", addr, err)
	}
	return conn, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}
