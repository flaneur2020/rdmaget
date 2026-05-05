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

type readyEvent struct {
	ready *protocol.ChunkBufferReadyFrame
	err   error
}

type slidingWindow struct {
	nextAckedChunkID uint64
	activeReads      int
	readyQueue       []*protocol.ChunkBufferReadyFrame
	acked            map[uint64]readResult
	buffers          []rdma.RdmaBuffer
	freeBuffers      chan rdma.RdmaBuffer
}

func newSlidingWindow() slidingWindow {
	return slidingWindow{
		acked: make(map[uint64]readResult),
	}
}

func (w *slidingWindow) addReady(ready *protocol.ChunkBufferReadyFrame) {
	w.readyQueue = append(w.readyQueue, ready)
}

func (w *slidingWindow) popReady() *protocol.ChunkBufferReadyFrame {
	ready := w.readyQueue[0]
	w.readyQueue = w.readyQueue[1:]
	return ready
}

func (w *slidingWindow) addAcked(result readResult) {
	w.acked[result.ready.ChunkID] = result
}

func (w *slidingWindow) doneReading() bool {
	return len(w.readyQueue) == 0 && len(w.acked) == 0 && w.activeReads == 0
}

func (w *slidingWindow) hasBuffers() bool {
	return w.freeBuffers != nil
}

func (w *slidingWindow) setBuffers(buffers []rdma.RdmaBuffer) {
	w.buffers = buffers
	w.freeBuffers = make(chan rdma.RdmaBuffer, len(buffers))
	for _, buffer := range buffers {
		w.freeBuffers <- buffer
	}
}

func (w *slidingWindow) releaseBuffer(buffer rdma.RdmaBuffer) {
	w.freeBuffers <- buffer
}

func (w *slidingWindow) finishRead() {
	w.activeReads--
}

func (w *slidingWindow) drainAcked(out io.Writer) (bool, error) {
	for {
		result, ok := w.acked[w.nextAckedChunkID]
		if !ok {
			return false, nil
		}
		delete(w.acked, w.nextAckedChunkID)
		w.nextAckedChunkID++
		if _, err := out.Write(result.buffer.Bytes()[:int(result.ready.Size)]); err != nil {
			return false, fmt.Errorf("client: write output: %w", err)
		}
		w.releaseBuffer(result.buffer)
		if result.ready.Final {
			return true, nil
		}
	}
}

func (w *slidingWindow) fill(workers *sync.WaitGroup, resultc chan<- readResult, read func(rdma.RdmaBuffer, *protocol.ChunkBufferReadyFrame) readResult) {
	for len(w.readyQueue) > 0 && w.freeBuffers != nil {
		var buffer rdma.RdmaBuffer
		select {
		case buffer = <-w.freeBuffers:
		default:
			return
		}
		ready := w.popReady()
		workers.Add(1)
		w.activeReads++
		go func() {
			defer workers.Done()
			resultc <- read(buffer, ready)
		}()
	}
}

func (w *slidingWindow) Close() error {
	var err error
	for _, buf := range w.buffers {
		if closeErr := buf.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	w.buffers = nil
	return err
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

	dpConn rdma.Conn

	readyc  chan readyEvent
	resultc chan readResult
	workers sync.WaitGroup
	window  slidingWindow
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
	if closeErr := j.window.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
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

	j.readyc = make(chan readyEvent)
	j.resultc = make(chan readResult, j.chunkBuffers)
	j.window = newSlidingWindow()

	go j.readReadyFrames()

	for j.readyc != nil || !j.window.doneReading() {
		select {
		case <-j.ctx.Done():
			return j.stop(j.ctx.Err())
		case event, ok := <-j.readyc:
			if !ok {
				j.readyc = nil
				continue
			}
			if event.err != nil {
				return j.stop(event.err)
			}
			ready := event.ready
			if ready.Size == 0 && ready.Final {
				return j.finish()
			}
			if err := j.onReady(ready); err != nil {
				return j.stop(err)
			}
		case result := <-j.resultc:
			done, err := j.onReadResult(result)
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

func (j *RGetJob) onReady(ready *protocol.ChunkBufferReadyFrame) error {
	if j.dpConn == nil {
		conn, err := j.device.Connect(j.ctx, ready.ServerEndpoint)
		if err != nil {
			return err
		}
		j.dpConn = conn
	}
	if ready.Size > j.chunkSize {
		return fmt.Errorf("client: chunk size %d exceeds requested chunk buffer %d", ready.Size, j.chunkSize)
	}
	if !j.window.hasBuffers() {
		if err := j.registerReadBuffers(); err != nil {
			return err
		}
	}
	j.window.addReady(ready)
	j.window.fill(&j.workers, j.resultc, j.readChunk)
	return nil
}

func (j *RGetJob) onReadResult(result readResult) (bool, error) {
	j.window.finishRead()
	if result.err != nil {
		return false, result.err
	}
	if err := j.sendAck(result.ready); err != nil {
		return false, err
	}
	j.window.addAcked(result)
	done, err := j.window.drainAcked(j.out)
	if err != nil {
		return false, err
	}
	j.window.fill(&j.workers, j.resultc, j.readChunk)
	return done, nil
}

func (j *RGetJob) registerReadBuffers() error {
	buffers, err := j.dpConn.RegisterRdmaBuffers(int(j.chunkSize), j.chunkBuffers)
	if err != nil {
		return err
	}

	j.window.setBuffers(buffers)
	return nil
}

func (j *RGetJob) sendAck(ready *protocol.ChunkBufferReadyFrame) error {
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
			j.emitReadyEvent(readyEvent{err: err})
			return
		}
		switch msg := frame.(type) {
		case *protocol.ChunkBufferReadyFrame:
			if !j.emitReadyEvent(readyEvent{ready: msg}) {
				return
			}
			if msg.Size == 0 && msg.Final {
				return
			}
		case *protocol.ErrorFrame:
			j.emitReadyEvent(readyEvent{err: fmt.Errorf("server error [%s]: %s", msg.ErrorKind, msg.Message)})
			return
		default:
			j.emitReadyEvent(readyEvent{err: fmt.Errorf("client: unexpected frame from server: %s", frame.Kind())})
			return
		}
	}
}

func (j *RGetJob) emitReadyEvent(event readyEvent) bool {
	select {
	case j.readyc <- event:
		return true
	case <-j.ctx.Done():
		return false
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
