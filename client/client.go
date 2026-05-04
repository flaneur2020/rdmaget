package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"

	"github.com/flaneur2020/rget/protocol"
	"github.com/flaneur2020/rget/rdma"
	"github.com/flaneur2020/rget/transfer"
)

type Config struct {
	Addr         string
	Path         string
	Output       string
	ChunkSize    uint64
	ChunkBuffers int
	DataPlane    rdma.DataPlane
}

type readResult struct {
	ready *protocol.ChunkBufferReadyFrame
	data  []byte
	err   error
}

func Run(ctx context.Context, cfg Config) error {
	if cfg.Addr == "" {
		return errors.New("client: addr is required")
	}
	if cfg.Path == "" {
		return errors.New("client: remote path is required")
	}
	if cfg.DataPlane == nil {
		return errors.New("client: data plane is required")
	}
	chunkSize := cfg.ChunkSize
	if chunkSize == 0 {
		chunkSize = transfer.DefaultChunkSize
	}
	if chunkSize > math.MaxInt {
		return fmt.Errorf("client: chunk size %d exceeds max int", chunkSize)
	}
	chunkBuffers := cfg.ChunkBuffers
	if chunkBuffers == 0 {
		chunkBuffers = transfer.DefaultChunkBuffers
	}
	if chunkBuffers < 0 {
		return fmt.Errorf("client: chunk buffers must be positive")
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
		ChunkBuffers:   uint32(chunkBuffers),
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
			return fmt.Errorf("client: create output: %w", err)
		}
		out = file
	}
	defer out.Close()

	return runPipeline(ctx, ctrl, dpConn, out, chunkBuffers)
}

func runPipeline(ctx context.Context, ctrl net.Conn, dpConn rdma.Conn, out io.Writer, chunkBuffers int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	readyc := make(chan *protocol.ChunkBufferReadyFrame)
	resultc := make(chan readResult, chunkBuffers)
	errc := make(chan error, 1)
	var ctrlMu sync.Mutex

	go readReadyFrames(ctrl, readyc, errc)

	var workers sync.WaitGroup
	connected := false
	nextWrite := uint64(0)
	activeReads := 0
	pending := make(map[uint64]readResult)

	for readyc != nil || len(pending) > 0 || activeReads > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errc:
			cancel()
			workers.Wait()
			return err
		case ready, ok := <-readyc:
			if !ok {
				readyc = nil
				continue
			}
			if ready.Size == 0 && ready.Final {
				cancel()
				workers.Wait()
				return nil
			}
			if !connected {
				if err := dpConn.Connect(ctx, ready.ServerEndpoint); err != nil {
					cancel()
					workers.Wait()
					return err
				}
				connected = true
			}
			workers.Add(1)
			activeReads++
			go func() {
				defer workers.Done()
				resultc <- readChunk(ctx, dpConn, ready)
			}()
		case result := <-resultc:
			activeReads--
			if result.err != nil {
				cancel()
				workers.Wait()
				return result.err
			}
			if err := sendAck(ctrl, &ctrlMu, result.ready); err != nil {
				cancel()
				workers.Wait()
				return err
			}
			pending[result.ready.ChunkID] = result
			for {
				next, ok := pending[nextWrite]
				if !ok {
					break
				}
				if _, err := out.Write(next.data); err != nil {
					cancel()
					workers.Wait()
					return fmt.Errorf("client: write output: %w", err)
				}
				delete(pending, nextWrite)
				if next.ready.Final {
					cancel()
					workers.Wait()
					return nil
				}
				nextWrite++
			}
		}
	}
	return nil
}

func readReadyFrames(ctrl net.Conn, readyc chan<- *protocol.ChunkBufferReadyFrame, errc chan<- error) {
	defer close(readyc)
	for {
		frame, err := protocol.DecodeFrame(ctrl)
		if err != nil {
			errc <- err
			return
		}
		switch msg := frame.(type) {
		case *protocol.ChunkBufferReadyFrame:
			readyc <- msg
			if msg.Size == 0 && msg.Final {
				return
			}
		case *protocol.ErrorFrame:
			errc <- fmt.Errorf("server error [%s]: %s", msg.ErrorKind, msg.Message)
			return
		default:
			errc <- fmt.Errorf("client: unexpected frame from server: %s", frame.Kind())
			return
		}
	}
}

func readChunk(ctx context.Context, dpConn rdma.Conn, ready *protocol.ChunkBufferReadyFrame) readResult {
	if ready.Size > math.MaxInt {
		return readResult{
			ready: ready,
			err:   fmt.Errorf("client: chunk size %d exceeds max int", ready.Size),
		}
	}
	chunk := make([]byte, int(ready.Size))
	remote := ready.Buffer
	remote.Length = ready.Size
	if err := dpConn.Read(ctx, chunk, remote); err != nil {
		return readResult{ready: ready, err: err}
	}
	return readResult{ready: ready, data: chunk}
}

func sendAck(ctrl net.Conn, mu *sync.Mutex, ready *protocol.ChunkBufferReadyFrame) error {
	mu.Lock()
	defer mu.Unlock()
	return protocol.EncodeFrame(ctrl, &protocol.ChunkBufferAckFrame{
		BufferIndex: ready.BufferIndex,
		ChunkID:     ready.ChunkID,
	})
}

func dialContext(ctx context.Context, addr string) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	return conn, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}
