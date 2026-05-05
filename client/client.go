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

// RGetJob owns the client-side state for one rget transfer.
type RGetJob struct {
	cfg          Config
	chunkSize    uint64
	chunkBuffers int
	device       rdma.RdmaDevice

	ctx    context.Context
	cancel context.CancelFunc
	ctrl   net.Conn
	out    io.WriteCloser

	ctrlMu      sync.Mutex
	dpConn      rdma.Conn
	readBuffers []rdma.RdmaBuffer
	freeBuffers chan rdma.RdmaBuffer

	readyc      chan *protocol.ChunkBufferReadyFrame
	resultc     chan readResult
	errc        chan error
	workers     sync.WaitGroup
	connected   bool
	nextWrite   uint64
	activeReads int
	queuedReady []*protocol.ChunkBufferReadyFrame
	pending     map[uint64]readResult
}

// NewRGetJob validates cfg and applies default transfer settings.
func NewRGetJob(cfg Config) (*RGetJob, error) {
	if cfg.Addr == "" {
		return nil, errors.New("client: addr is required")
	}
	if cfg.Path == "" {
		return nil, errors.New("client: remote path is required")
	}
	if cfg.RdmaDevice == nil {
		return nil, errors.New("client: RDMA device is required")
	}
	chunkSize := cfg.ChunkSize
	if chunkSize == 0 {
		chunkSize = transfer.DefaultChunkSize
	}
	if chunkSize > math.MaxInt {
		return nil, fmt.Errorf("client: chunk size %d exceeds max int", chunkSize)
	}
	chunkBuffers := cfg.ChunkBuffers
	if chunkBuffers == 0 {
		chunkBuffers = transfer.DefaultChunkBuffers
	}
	if chunkBuffers < 0 {
		return nil, fmt.Errorf("client: chunk buffers must be positive")
	}

	return &RGetJob{
		cfg:          cfg,
		chunkSize:    chunkSize,
		chunkBuffers: chunkBuffers,
		device:       cfg.RdmaDevice,
	}, nil
}

func Run(ctx context.Context, cfg Config) error {
	job, err := NewRGetJob(cfg)
	if err != nil {
		return err
	}
	return job.Run(ctx)
}

func (j *RGetJob) Run(ctx context.Context) error {
	if err := j.openControl(ctx); err != nil {
		return err
	}
	defer j.Close()

	if err := j.sendHandshake(ctx); err != nil {
		return err
	}
	if err := j.openOutput(); err != nil {
		return err
	}
	return j.runPipeline(ctx)
}

func (j *RGetJob) Close() error {
	var err error
	for _, buf := range j.readBuffers {
		if closeErr := buf.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	j.readBuffers = nil
	if j.dpConn != nil {
		if closeErr := j.dpConn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		j.dpConn = nil
	}
	if j.out != nil {
		if closeErr := j.out.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		j.out = nil
	}
	if j.ctrl != nil {
		if closeErr := j.ctrl.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		j.ctrl = nil
	}
	return err
}

func (j *RGetJob) openControl(ctx context.Context) error {
	ctrl, err := dialContext(ctx, j.cfg.Addr)
	if err != nil {
		return err
	}
	j.ctrl = ctrl
	return nil
}

func (j *RGetJob) sendHandshake(ctx context.Context) error {
	return protocol.EncodeFrame(ctx, j.ctrl, &protocol.HandshakeFrame{
		Path:           j.cfg.Path,
		ChunkSize:      j.chunkSize,
		ChunkBuffers:   uint32(j.chunkBuffers),
		ClientEndpoint: j.device.Info(),
	})
}

func (j *RGetJob) openOutput() error {
	if j.cfg.Output == "" || j.cfg.Output == "-" {
		j.out = nopWriteCloser{Writer: os.Stdout}
		return nil
	}
	file, err := os.Create(j.cfg.Output)
	if err != nil {
		return fmt.Errorf("client: create output: %w", err)
	}
	j.out = file
	return nil
}

func (j *RGetJob) runPipeline(ctx context.Context) error {
	j.ctx, j.cancel = context.WithCancel(ctx)
	defer j.cancel()

	j.readyc = make(chan *protocol.ChunkBufferReadyFrame)
	j.resultc = make(chan readResult, j.chunkBuffers)
	j.errc = make(chan error, 1)
	j.pending = make(map[uint64]readResult)

	go j.readReadyFrames()

	for j.readyc != nil || len(j.pending) > 0 || j.activeReads > 0 {
		select {
		case <-j.ctx.Done():
			return j.stop(j.ctx.Err())
		case err := <-j.errc:
			return j.stop(err)
		case ready, ok := <-j.readyc:
			if !ok {
				j.readyc = nil
				continue
			}
			if ready.Size == 0 && ready.Final {
				return j.finish()
			}
			if err := j.handleReady(ready); err != nil {
				return j.stop(err)
			}
		case result := <-j.resultc:
			done, err := j.handleResult(result)
			if err != nil {
				return j.stop(err)
			}
			if done {
				return j.finish()
			}
		}
	}
	return nil
}

func (j *RGetJob) handleReady(ready *protocol.ChunkBufferReadyFrame) error {
	if !j.connected {
		conn, err := j.device.Connect(j.ctx, ready.ServerEndpoint)
		if err != nil {
			return err
		}
		j.dpConn = conn
		j.connected = true
	}
	if ready.Size > j.chunkSize {
		return fmt.Errorf("client: chunk size %d exceeds requested chunk buffer %d", ready.Size, j.chunkSize)
	}
	if j.freeBuffers == nil {
		if err := j.registerReadBuffers(); err != nil {
			return err
		}
	}
	j.queuedReady = append(j.queuedReady, ready)
	j.startReads()
	return nil
}

func (j *RGetJob) handleResult(result readResult) (bool, error) {
	j.activeReads--
	if result.err != nil {
		return false, result.err
	}
	if err := j.sendAck(result.ready); err != nil {
		return false, err
	}
	j.pending[result.ready.ChunkID] = result
	for {
		next, ok := j.pending[j.nextWrite]
		if !ok {
			break
		}
		if _, err := j.out.Write(next.buffer.Bytes()[:int(next.ready.Size)]); err != nil {
			return false, fmt.Errorf("client: write output: %w", err)
		}
		j.freeBuffers <- next.buffer
		delete(j.pending, j.nextWrite)
		if next.ready.Final {
			return true, nil
		}
		j.nextWrite++
	}
	j.startReads()
	return false, nil
}

func (j *RGetJob) registerReadBuffers() error {
	buffers, err := j.dpConn.RegisterRdmaBuffers(int(j.chunkSize), j.chunkBuffers)
	if err != nil {
		return err
	}

	j.readBuffers = buffers
	j.freeBuffers = make(chan rdma.RdmaBuffer, j.chunkBuffers)
	for _, buffer := range j.readBuffers {
		j.freeBuffers <- buffer
	}
	return nil
}

func (j *RGetJob) startReads() {
	for len(j.queuedReady) > 0 && j.freeBuffers != nil {
		var buffer rdma.RdmaBuffer
		select {
		case buffer = <-j.freeBuffers:
		default:
			return
		}
		ready := j.queuedReady[0]
		j.queuedReady = j.queuedReady[1:]
		j.workers.Add(1)
		j.activeReads++
		go func() {
			defer j.workers.Done()
			j.resultc <- j.readChunk(buffer, ready)
		}()
	}
}

func (j *RGetJob) sendAck(ready *protocol.ChunkBufferReadyFrame) error {
	j.ctrlMu.Lock()
	defer j.ctrlMu.Unlock()
	return protocol.EncodeFrame(j.ctx, j.ctrl, &protocol.ChunkBufferAckFrame{
		BufferIndex: ready.BufferIndex,
		ChunkID:     ready.ChunkID,
	})
}

func (j *RGetJob) stop(err error) error {
	if j.cancel != nil {
		j.cancel()
	}
	j.workers.Wait()
	return err
}

func (j *RGetJob) finish() error {
	if j.cancel != nil {
		j.cancel()
	}
	j.workers.Wait()
	return nil
}

func (j *RGetJob) readReadyFrames() {
	defer close(j.readyc)
	for {
		frame, err := protocol.DecodeFrame(j.ctx, j.ctrl)
		if err != nil {
			j.errc <- err
			return
		}
		switch msg := frame.(type) {
		case *protocol.ChunkBufferReadyFrame:
			j.readyc <- msg
			if msg.Size == 0 && msg.Final {
				return
			}
		case *protocol.ErrorFrame:
			j.errc <- fmt.Errorf("server error [%s]: %s", msg.ErrorKind, msg.Message)
			return
		default:
			j.errc <- fmt.Errorf("client: unexpected frame from server: %s", frame.Kind())
			return
		}
	}
}

func (j *RGetJob) readChunk(buffer rdma.RdmaBuffer, ready *protocol.ChunkBufferReadyFrame) readResult {
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
	if err := j.dpConn.Read(j.ctx, buffer, remote); err != nil {
		return readResult{ready: ready, err: err}
	}
	return readResult{ready: ready, buffer: buffer}
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
