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
	RdmaDevice   rdma.RdmaDevice
}

type readResult struct {
	ready  *protocol.ChunkBufferReadyFrame
	buffer rdma.RdmaBuffer
	err    error
}

func Run(ctx context.Context, cfg Config) error {
	if cfg.Addr == "" {
		return errors.New("client: addr is required")
	}
	if cfg.Path == "" {
		return errors.New("client: remote path is required")
	}
	if cfg.RdmaDevice == nil {
		return errors.New("client: RDMA device is required")
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

	ctrl, err := dialContext(ctx, cfg.Addr)
	if err != nil {
		return err
	}
	defer ctrl.Close()

	if err := protocol.EncodeFrame(ctrl, &protocol.NewSessionFrame{
		Path:           cfg.Path,
		ChunkSize:      chunkSize,
		ChunkBuffers:   uint32(chunkBuffers),
		ClientEndpoint: cfg.RdmaDevice.Info(),
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

	return runPipeline(ctx, ctrl, cfg.RdmaDevice, out, chunkSize, chunkBuffers)
}

func runPipeline(ctx context.Context, ctrl net.Conn, device rdma.RdmaDevice, out io.Writer, chunkSize uint64, chunkBuffers int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	readyc := make(chan *protocol.ChunkBufferReadyFrame)
	resultc := make(chan readResult, chunkBuffers)
	errc := make(chan error, 1)
	var ctrlMu sync.Mutex
	var dpConn rdma.Conn
	var readBuffers []rdma.RdmaBuffer
	var freeBuffers chan rdma.RdmaBuffer
	defer func() {
		for _, buf := range readBuffers {
			_ = buf.Close()
		}
		if dpConn != nil {
			_ = dpConn.Close()
		}
	}()

	go readReadyFrames(ctrl, readyc, errc)

	var workers sync.WaitGroup
	connected := false
	nextWrite := uint64(0)
	activeReads := 0
	var queuedReady []*protocol.ChunkBufferReadyFrame
	pending := make(map[uint64]readResult)
	startReads := func() {
		for len(queuedReady) > 0 && freeBuffers != nil {
			var buffer rdma.RdmaBuffer
			select {
			case buffer = <-freeBuffers:
			default:
				return
			}
			ready := queuedReady[0]
			queuedReady = queuedReady[1:]
			workers.Add(1)
			activeReads++
			go func() {
				defer workers.Done()
				resultc <- readChunk(ctx, dpConn, buffer, ready)
			}()
		}
	}

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
				conn, err := device.Connect(ctx, ready.ServerEndpoint)
				if err != nil {
					cancel()
					workers.Wait()
					return err
				}
				dpConn = conn
				connected = true
			}
			if ready.Size > chunkSize {
				cancel()
				workers.Wait()
				return fmt.Errorf("client: chunk size %d exceeds requested chunk buffer %d", ready.Size, chunkSize)
			}
			if freeBuffers == nil {
				var err error
				readBuffers, freeBuffers, err = registerReadBuffers(dpConn, chunkSize, chunkBuffers)
				if err != nil {
					cancel()
					workers.Wait()
					return err
				}
			}
			queuedReady = append(queuedReady, ready)
			startReads()
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
				if _, err := out.Write(next.buffer.Bytes()[:int(next.ready.Size)]); err != nil {
					cancel()
					workers.Wait()
					return fmt.Errorf("client: write output: %w", err)
				}
				freeBuffers <- next.buffer
				delete(pending, nextWrite)
				if next.ready.Final {
					cancel()
					workers.Wait()
					return nil
				}
				nextWrite++
			}
			startReads()
		}
	}
	return nil
}

func registerReadBuffers(dpConn rdma.Conn, chunkSize uint64, chunkBuffers int) ([]rdma.RdmaBuffer, chan rdma.RdmaBuffer, error) {
	if chunkSize > math.MaxInt {
		return nil, nil, fmt.Errorf("client: chunk size %d exceeds max int", chunkSize)
	}
	buffers := make([]rdma.RdmaBuffer, 0, chunkBuffers)
	free := make(chan rdma.RdmaBuffer, chunkBuffers)
	for i := 0; i < chunkBuffers; i++ {
		buffer, err := dpConn.RegisterRdmaBuffer(int(chunkSize))
		if err != nil {
			for _, buffer := range buffers {
				_ = buffer.Close()
			}
			return nil, nil, err
		}
		buffers = append(buffers, buffer)
		free <- buffer
	}
	return buffers, free, nil
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

func readChunk(ctx context.Context, dpConn rdma.Conn, buffer rdma.RdmaBuffer, ready *protocol.ChunkBufferReadyFrame) readResult {
	if ready.Size > math.MaxInt {
		return readResult{
			ready: ready,
			err:   fmt.Errorf("client: chunk size %d exceeds max int", ready.Size),
		}
	}
	if uint64(len(buffer.Bytes())) < ready.Size {
		return readResult{
			ready: ready,
			err:   fmt.Errorf("client: RDMA buffer size %d smaller than chunk %d", len(buffer.Bytes()), ready.Size),
		}
	}
	remote := ready.Buffer
	remote.Length = ready.Size
	if err := dpConn.Read(ctx, buffer, remote); err != nil {
		return readResult{ready: ready, err: err}
	}
	return readResult{ready: ready, buffer: buffer}
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
