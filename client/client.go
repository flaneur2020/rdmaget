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
	readyChunk *protocol.ChunkBufferReadyFrame
	buffer     rdma.RdmaBuffer
	err        error
}

type slidingWindow struct {
	nextAckedChunkID uint64
	activeReads      int
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
		buffers:     buffers,
		freeBuffers: make(chan rdma.RdmaBuffer, len(buffers)),
	}
	for _, buffer := range buffers {
		window.freeBuffers <- buffer
	}
	return window, nil
}

func (w *slidingWindow) trackAcked(result readResultEvent) {
	w.acked[result.readyChunk.ChunkID] = result
}

func (w *slidingWindow) doneReading() bool {
	return len(w.acked) == 0 && w.activeReads == 0
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

func (w *slidingWindow) acquireReadBuffer() (rdma.RdmaBuffer, bool) {
	if len(w.freeBuffers) == 0 {
		return nil, false
	}
	buffer := <-w.freeBuffers
	w.activeReads++
	return buffer, true
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

	framec            chan protocol.Frame
	readResultc       chan readResultEvent
	activeReadWorkers sync.WaitGroup
	window            *slidingWindow
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

	j.framec = make(chan protocol.Frame)
	j.readResultc = make(chan readResultEvent, j.chunkBuffers)

	go j.readFrames()

	for j.framec != nil || (j.window != nil && !j.window.doneReading()) {
		select {
		case <-j.ctx.Done():
			return j.stop(j.ctx.Err())
		case frame, ok := <-j.framec:
			if !ok {
				j.framec = nil
				continue
			}
			switch frame := frame.(type) {
			case *protocol.ChunkBufferReadyFrame:
				done, err := j.handleReadyChunk(frame)
				if err != nil {
					return j.stop(err)
				}
				if done {
					return j.finish()
				}
			case *protocol.ErrorFrame:
				return j.stop(fmt.Errorf("server error [%s]: %s", frame.ErrorKind, frame.Message))
			default:
				return j.stop(fmt.Errorf("client: unexpected frame from server: %s", frame.Kind()))
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

func (j *RGetJob) handleReadyChunk(readyChunk *protocol.ChunkBufferReadyFrame) (bool, error) {
	if readyChunk.Size == 0 && readyChunk.Final {
		return true, nil
	}
	if err := j.initDataPlane(readyChunk); err != nil {
		return false, err
	}
	if readyChunk.Size > j.chunkSize {
		return false, fmt.Errorf("client: chunk size %d exceeds requested chunk buffer %d", readyChunk.Size, j.chunkSize)
	}
	buffer, ok := j.window.acquireReadBuffer()
	if !ok {
		return false, fmt.Errorf("client: no free RDMA buffer for ready chunk %d", readyChunk.ChunkID)
	}
	j.activeReadWorkers.Add(1)
	go func() {
		defer j.activeReadWorkers.Done()
		j.readResultc <- j.readChunk(buffer, readyChunk)
	}()
	return false, nil
}

func (j *RGetJob) initDataPlane(readyChunk *protocol.ChunkBufferReadyFrame) error {
	if j.window != nil {
		return nil
	}
	if j.dpConn == nil {
		conn, err := j.device.Connect(j.ctx, readyChunk.ServerEndpoint)
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

func (j *RGetJob) handleReadResult(result readResultEvent) (bool, error) {
	j.window.finishRead()
	if result.err != nil {
		return false, result.err
	}
	if err := j.sendAck(result.readyChunk); err != nil {
		return false, err
	}
	j.window.trackAcked(result)

	for {
		result, ok := j.window.popAcked()
		if !ok {
			return false, nil
		}
		if _, err := j.out.Write(result.buffer.Bytes()[:int(result.readyChunk.Size)]); err != nil {
			return false, fmt.Errorf("client: write output: %w", err)
		}
		j.window.releaseBuffer(result.buffer)
		if result.readyChunk.Final {
			return true, nil
		}
	}
}

func (j *RGetJob) sendAck(readyChunk *protocol.ChunkBufferReadyFrame) error {
	return protocol.EncodeFrame(j.ctx, j.ctrl, &protocol.ChunkBufferAckFrame{
		BufferIndex: readyChunk.BufferIndex,
		ChunkID:     readyChunk.ChunkID,
	})
}

func (j *RGetJob) stop(err error) error {
	if j.cancel != nil {
		j.cancel()
	}
	j.activeReadWorkers.Wait()
	return err
}

func (j *RGetJob) finish() error {
	if j.cancel != nil {
		j.cancel()
	}
	j.activeReadWorkers.Wait()
	return nil
}

func (j *RGetJob) readFrames() {
	defer close(j.framec)
	for {
		frame, err := protocol.DecodeFrame(j.ctx, j.ctrl)
		if err != nil {
			if !j.emitFrame(&protocol.ErrorFrame{
				ErrorKind: "decode_frame",
				Message:   err.Error(),
			}) {
				return
			}
			return
		}
		if !j.emitFrame(frame) {
			return
		}
		if readyChunk, ok := frame.(*protocol.ChunkBufferReadyFrame); ok && readyChunk.Size == 0 && readyChunk.Final {
			return
		}
	}
}

func (j *RGetJob) emitFrame(frame protocol.Frame) bool {
	select {
	case j.framec <- frame:
		return true
	case <-j.ctx.Done():
		return false
	}
}

func (j *RGetJob) readChunk(buffer rdma.RdmaBuffer, readyChunk *protocol.ChunkBufferReadyFrame) readResultEvent {
	if readyChunk.Size > math.MaxInt {
		return readResultEvent{
			readyChunk: readyChunk,
			err:        fmt.Errorf("client: chunk size %d exceeds max int", readyChunk.Size),
		}
	}
	if uint64(len(buffer.Bytes())) < readyChunk.Size {
		return readResultEvent{
			readyChunk: readyChunk,
			err:        fmt.Errorf("client: RDMA buffer size %d smaller than chunk %d", len(buffer.Bytes()), readyChunk.Size),
		}
	}
	remote := readyChunk.Buffer
	remote.Length = readyChunk.Size
	if err := j.dpConn.Read(j.ctx, buffer, remote); err != nil {
		return readResultEvent{readyChunk: readyChunk, err: err}
	}
	return readResultEvent{readyChunk: readyChunk, buffer: buffer}
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
