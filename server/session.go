package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"

	"github.com/flaneur2020/rget/protocol"
	"github.com/flaneur2020/rget/rdma"
	"github.com/flaneur2020/rget/transfer"
)

type RgetSession struct {
	ctrl         net.Conn
	device       rdma.RdmaDevice
	dpConn       rdma.Conn
	file         *os.File
	request      *protocol.HandshakeFrame
	chunkBuffers []chunkBuffer
	chunkSize    uint64
	totalSize    uint64
	inflight     map[uint64]*chunkBuffer
}

type chunkBuffer struct {
	index   uint64
	data    []byte
	remote  rdma.RdmaBuffer
	chunkID uint64
	offset  uint64
	size    uint64
	final   bool
}

func NewSession(ctrl net.Conn, device rdma.RdmaDevice, chunkSize uint64, chunkBuffers int) *RgetSession {
	if chunkSize == 0 {
		chunkSize = transfer.DefaultChunkSize
	}
	if chunkBuffers == 0 {
		chunkBuffers = transfer.DefaultChunkBuffers
	}
	return &RgetSession{
		ctrl:         ctrl,
		device:       device,
		chunkSize:    chunkSize,
		chunkBuffers: makeChunkBuffers(chunkBuffers),
		inflight:     make(map[uint64]*chunkBuffer),
	}
}

func (s *RgetSession) Run(ctx context.Context) error {
	defer s.Close()

	if err := s.readHandshake(ctx); err != nil {
		return err
	}
	if err := s.openFile(ctx); err != nil {
		return err
	}
	if err := s.openRDMAConn(ctx); err != nil {
		return err
	}
	if err := s.registerChunkBuffers(ctx); err != nil {
		return err
	}
	return s.serveChunks(ctx)
}

func (s *RgetSession) Close() error {
	var err error
	for i := range s.chunkBuffers {
		if closeErr := s.chunkBuffers[i].Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if s.file != nil {
		if closeErr := s.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		s.file = nil
	}
	if s.dpConn != nil {
		if closeErr := s.dpConn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		s.dpConn = nil
	}
	if s.ctrl != nil {
		if closeErr := s.ctrl.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		s.ctrl = nil
	}
	return err
}

func (s *RgetSession) readHandshake(ctx context.Context) error {
	frame, err := protocol.DecodeFrame(ctx, s.ctrl)
	if err != nil {
		return err
	}
	handshake, ok := frame.(*protocol.HandshakeFrame)
	if !ok {
		_ = s.sendError(ctx, "bad_request", "first frame must be HANDSHAKE")
		return fmt.Errorf("server: first frame was %s", frame.Kind())
	}
	if handshake.Path == "" {
		_ = s.sendError(ctx, "bad_request", "path is required")
		return errors.New("server: empty path")
	}
	if handshake.ChunkSize > 0 {
		if handshake.ChunkSize > math.MaxInt {
			_ = s.sendError(ctx, "bad_request", "chunk size exceeds max int")
			return fmt.Errorf("server: chunk size %d exceeds max int", handshake.ChunkSize)
		}
		s.chunkSize = handshake.ChunkSize
	}
	if handshake.ChunkBuffers > 0 {
		s.chunkBuffers = makeChunkBuffers(int(handshake.ChunkBuffers))
	}
	s.request = handshake
	return nil
}

func (s *RgetSession) openFile(ctx context.Context) error {
	file, err := os.Open(s.request.Path)
	if err != nil {
		_ = s.sendError(ctx, "open_failed", err.Error())
		return fmt.Errorf("server: open %q: %w", s.request.Path, err)
	}
	s.file = file

	stat, err := s.file.Stat()
	if err != nil {
		_ = s.sendError(ctx, "stat_failed", err.Error())
		return fmt.Errorf("server: stat %q: %w", s.request.Path, err)
	}
	if stat.IsDir() {
		_ = s.sendError(ctx, "bad_request", "path is a directory")
		return fmt.Errorf("server: path %q is a directory", s.request.Path)
	}

	s.totalSize = uint64(stat.Size())
	return nil
}

func (s *RgetSession) openRDMAConn(ctx context.Context) error {
	dpConn, err := s.device.Connect(ctx, s.request.ClientEndpoint)
	if err != nil {
		_ = s.sendError(ctx, "rdma_connect_failed", err.Error())
		return err
	}
	s.dpConn = dpConn
	return nil
}

func (s *RgetSession) registerChunkBuffers(ctx context.Context) error {
	buffers, err := s.dpConn.RegisterRdmaBuffers(int(s.chunkSize), len(s.chunkBuffers))
	if err != nil {
		_ = s.sendError(ctx, "rdma_register_failed", err.Error())
		return err
	}
	for i, buffer := range buffers {
		s.chunkBuffers[i].remote = buffer
		s.chunkBuffers[i].data = buffer.Bytes()
	}
	return nil
}

func (s *RgetSession) serveChunks(ctx context.Context) error {
	if s.totalSize == 0 {
		return s.sendReady(ctx, nil, s.dpConn.LocalEndpoint())
	}

	if err := s.fillWindow(ctx); err != nil {
		return err
	}
	for len(s.inflight) > 0 {
		ack, err := s.readAck(ctx)
		if err != nil {
			return err
		}
		buf, ok := s.inflight[ack.ChunkID]
		if !ok {
			return fmt.Errorf("server: ack for unknown chunk %d", ack.ChunkID)
		}
		if ack.BufferIndex != buf.index {
			return fmt.Errorf("server: bad ack buffer=%d chunk=%d, want buffer=%d",
				ack.BufferIndex, ack.ChunkID, buf.index)
		}
		delete(s.inflight, ack.ChunkID)
		buf.resetChunk()
		if !s.eof() {
			if err := s.fillNextChunk(ctx, buf); err != nil {
				return err
			}
			if err := s.sendReady(ctx, buf, s.dpConn.LocalEndpoint()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *RgetSession) fillWindow(ctx context.Context) error {
	for i := range s.chunkBuffers {
		if s.eof() {
			return nil
		}
		buf := &s.chunkBuffers[i]
		if err := s.fillNextChunk(ctx, buf); err != nil {
			return err
		}
		if err := s.sendReady(ctx, buf, s.dpConn.LocalEndpoint()); err != nil {
			return err
		}
	}
	return nil
}

func (s *RgetSession) fillNextChunk(ctx context.Context, buf *chunkBuffer) error {
	offset, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		_ = s.sendError(ctx, "seek_failed", err.Error())
		return fmt.Errorf("server: current file offset: %w", err)
	}
	want := min(s.chunkSize, s.totalSize-uint64(offset))
	dst := buf.data[:int(want)]
	n, readErr := io.ReadFull(s.file, dst)
	if readErr != nil {
		_ = s.sendError(ctx, "read_failed", readErr.Error())
		return fmt.Errorf("server: read file: %w", readErr)
	}
	if n == 0 {
		return nil
	}

	buf.chunkID = uint64(offset) / s.chunkSize
	buf.offset = uint64(offset)
	buf.size = uint64(n)
	buf.final = uint64(offset)+uint64(n) == s.totalSize
	s.inflight[buf.chunkID] = buf
	return nil
}

func (s *RgetSession) readAck(ctx context.Context) (*protocol.ChunkBufferAckFrame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ackFrame, err := protocol.DecodeFrame(ctx, s.ctrl)
	if err != nil {
		return nil, err
	}
	ack, ok := ackFrame.(*protocol.ChunkBufferAckFrame)
	if !ok {
		return nil, fmt.Errorf("server: expected CHUNK_BUFFER_ACK, got %s", ackFrame.Kind())
	}
	return ack, nil
}

func (s *RgetSession) sendReady(ctx context.Context, buf *chunkBuffer, endpoint rdma.Endpoint) error {
	ready := &protocol.ChunkBufferReadyFrame{
		BufferIndex:    0,
		ChunkID:        0,
		Offset:         0,
		Size:           0,
		Final:          true,
		ServerEndpoint: endpoint,
	}
	if buf != nil {
		region := buf.remote.Region()
		region.Length = buf.size
		ready = &protocol.ChunkBufferReadyFrame{
			BufferIndex:    buf.index,
			ChunkID:        buf.chunkID,
			Offset:         buf.offset,
			Size:           buf.size,
			Final:          buf.final,
			ServerEndpoint: endpoint,
			Buffer:         region,
		}
	}
	return protocol.EncodeFrame(ctx, s.ctrl, ready)
}

func (s *RgetSession) sendError(ctx context.Context, kind, message string) error {
	return protocol.EncodeFrame(ctx, s.ctrl, &protocol.ErrorFrame{
		ErrorKind: kind,
		Message:   message,
	})
}

func (s *RgetSession) eof() bool {
	offset, err := s.file.Seek(0, io.SeekCurrent)
	return err == nil && uint64(offset) >= s.totalSize
}

func (b *chunkBuffer) resetChunk() {
	b.chunkID = 0
	b.offset = 0
	b.size = 0
	b.final = false
}

func (b *chunkBuffer) Close() error {
	b.resetChunk()
	if b.remote == nil {
		return nil
	}
	err := b.remote.Close()
	b.remote = nil
	b.data = nil
	return err
}

func makeChunkBuffers(count int) []chunkBuffer {
	if count <= 0 {
		count = transfer.DefaultChunkBuffers
	}
	buffers := make([]chunkBuffer, count)
	for i := range buffers {
		buffers[i].index = uint64(i)
	}
	return buffers
}
