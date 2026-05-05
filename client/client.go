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

type readResultEvent struct {
	ready  *protocol.ChunkBufferReadyFrame
	buffer rdma.RdmaBuffer
	err    error
}

type readTask struct {
	ready  *protocol.ChunkBufferReadyFrame
	buffer rdma.RdmaBuffer
}

type readyEvent struct {
	ready *protocol.ChunkBufferReadyFrame
	err   error
}

type slidingWindow struct {
	nextAckedChunkID uint64
	activeReads      int
	readyQueue       chan *protocol.ChunkBufferReadyFrame
	acked            map[uint64]readResultEvent
	buffers          []rdma.RdmaBuffer
	freeBuffers      chan rdma.RdmaBuffer
}

func newSlidingWindow(conn rdma.Conn, chunkSize uint64, chunkBuffers int) (*slidingWindow, error) {
	buffers, err := conn.RegisterRdmaBuffers(int(chunkSize), chunkBuffers)
	if err != nil {
		return nil, err
	}
	window := &slidingWindow{
		acked:       make(map[uint64]readResultEvent),
		readyQueue:  make(chan *protocol.ChunkBufferReadyFrame, len(buffers)),
		buffers:     buffers,
		freeBuffers: make(chan rdma.RdmaBuffer, len(buffers)),
	}
	for _, buffer := range buffers {
		window.freeBuffers <- buffer
	}
	return window, nil
}

func (w *slidingWindow) addReady(ready *protocol.ChunkBufferReadyFrame) bool {
	select {
	case w.readyQueue <- ready:
		return true
	default:
		return false
	}
}

func (w *slidingWindow) popReady() *protocol.ChunkBufferReadyFrame {
	return <-w.readyQueue
}

func (w *slidingWindow) addAcked(result readResultEvent) {
	w.acked[result.ready.ChunkID] = result
}

func (w *slidingWindow) doneReading() bool {
	return len(w.readyQueue) == 0 && len(w.acked) == 0 && w.activeReads == 0
}

func (w *slidingWindow) releaseBuffer(buffer rdma.RdmaBuffer) {
	w.freeBuffers <- buffer
}

func (w *slidingWindow) finishRead() {
	w.activeReads--
}

func (w *slidingWindow) popAcked() (readResultEvent, bool) {
	result, ok := w.acked[w.nextAckedChunkID]
	if !ok {
		return readResultEvent{}, false
	}
	delete(w.acked, w.nextAckedChunkID)
	w.nextAckedChunkID++
	return result, true
}

func (w *slidingWindow) nextRead() (readTask, bool) {
	if len(w.readyQueue) == 0 || len(w.freeBuffers) == 0 {
		return readTask{}, false
	}
	w.activeReads++
	return readTask{
		ready:  w.popReady(),
		buffer: <-w.freeBuffers,
	}, true
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

	readyc      chan readyEvent
	readResultc chan readResultEvent
	workers     sync.WaitGroup
	window      *slidingWindow
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
	return j.run(ctx)
}

func (j *RGetJob) Close() error {
	var err error
	if j.window != nil {
		if closeErr := j.window.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		j.window = nil
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

func (j *RGetJob) run(ctx context.Context) error {
	j.ctx, j.cancel = context.WithCancel(ctx)
	defer j.cancel()

	j.readyc = make(chan readyEvent)
	j.readResultc = make(chan readResultEvent, j.chunkBuffers)

	go j.readFrames()

	for j.readyc != nil || (j.window != nil && !j.window.doneReading()) {
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
			if err := j.initDataPlane(ready); err != nil {
				return j.stop(err)
			}
			if err := j.handleReady(ready); err != nil {
				return j.stop(err)
			}
		case result := <-j.readResultc:
			done, err := j.handleReadResult(result)
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

func (j *RGetJob) initDataPlane(ready *protocol.ChunkBufferReadyFrame) error {
	if j.window != nil {
		return nil
	}
	if j.dpConn == nil {
		conn, err := j.device.Connect(j.ctx, ready.ServerEndpoint)
		if err != nil {
			return err
		}
		j.dpConn = conn
	}
	window, err := newSlidingWindow(j.dpConn, j.chunkSize, j.chunkBuffers)
	if err != nil {
		return err
	}
	j.window = window
	return nil
}

func (j *RGetJob) handleReady(ready *protocol.ChunkBufferReadyFrame) error {
	if ready.Size > j.chunkSize {
		return fmt.Errorf("client: chunk size %d exceeds requested chunk buffer %d", ready.Size, j.chunkSize)
	}
	if !j.window.addReady(ready) {
		return fmt.Errorf("client: ready queue is full")
	}
	j.fillReads()
	return nil
}

func (j *RGetJob) handleReadResult(result readResultEvent) (bool, error) {
	j.window.finishRead()
	if result.err != nil {
		return false, result.err
	}
	if err := j.sendAck(result.ready); err != nil {
		return false, err
	}
	j.window.addAcked(result)
	done, err := j.writeAcked()
	if err != nil {
		return false, err
	}
	j.fillReads()
	return done, nil
}

func (j *RGetJob) fillReads() {
	for {
		task, ok := j.window.nextRead()
		if !ok {
			return
		}
		j.workers.Add(1)
		go func() {
			defer j.workers.Done()
			j.readResultc <- j.readChunk(task.buffer, task.ready)
		}()
	}
}

func (j *RGetJob) writeAcked() (bool, error) {
	for {
		result, ok := j.window.popAcked()
		if !ok {
			return false, nil
		}
		if _, err := j.out.Write(result.buffer.Bytes()[:int(result.ready.Size)]); err != nil {
			return false, fmt.Errorf("client: write output: %w", err)
		}
		j.window.releaseBuffer(result.buffer)
		if result.ready.Final {
			return true, nil
		}
	}
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

func (j *RGetJob) readFrames() {
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

func (j *RGetJob) readChunk(buffer rdma.RdmaBuffer, ready *protocol.ChunkBufferReadyFrame) readResultEvent {
	if ready.Size > math.MaxInt {
		return readResultEvent{
			ready: ready,
			err:   fmt.Errorf("client: chunk size %d exceeds max int", ready.Size),
		}
	}
	if uint64(len(buffer.Bytes())) < ready.Size {
		return readResultEvent{
			ready: ready,
			err:   fmt.Errorf("client: RDMA buffer size %d smaller than chunk %d", len(buffer.Bytes()), ready.Size),
		}
	}
	remote := ready.Buffer
	remote.Length = ready.Size
	if err := j.dpConn.Read(j.ctx, buffer, remote); err != nil {
		return readResultEvent{ready: ready, err: err}
	}
	return readResultEvent{ready: ready, buffer: buffer}
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
