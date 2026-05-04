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
	dp           rdma.DataPlane
	dpConn       rdma.Conn
	file         *os.File
	request      *protocol.NewSessionFrame
	chunkBuffers []chunkBuffer
	chunkSize    uint64
	totalSize    uint64
	inflight     map[uint64]*chunkBuffer
}

type chunkBuffer struct {
	index   uint64
	data    []byte
	remote  rdma.RemoteBuffer
	chunkID uint64
	offset  uint64
	size    uint64
	final   bool
}

func NewSession(ctrl net.Conn, dp rdma.DataPlane, chunkSize uint64, chunkBuffers int) *RgetSession {
	if chunkSize == 0 {
		chunkSize = transfer.DefaultChunkSize
	}
	if chunkBuffers == 0 {
		chunkBuffers = transfer.DefaultChunkBuffers
	}
	return &RgetSession{
		ctrl:         ctrl,
		dp:           dp,
		chunkSize:    chunkSize,
		chunkBuffers: makeChunkBuffers(chunkBuffers),
		inflight:     make(map[uint64]*chunkBuffer),
	}
}

func (s *RgetSession) Run(ctx context.Context) error {
	defer s.Close()

	if err := s.readNewSession(); err != nil {
		return err
	}
	if err := s.openFile(); err != nil {
		return err
	}
	if err := s.openRDMAConn(ctx); err != nil {
		return err
	}
	if err := s.registerChunkBuffers(); err != nil {
		return err
	}
	return s.serveChunks()
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

func (s *RgetSession) readNewSession() error {
	frame, err := protocol.DecodeFrame(s.ctrl)
	if err != nil {
		return err
	}
	newSession, ok := frame.(*protocol.NewSessionFrame)
	if !ok {
		_ = s.sendError("bad_request", "first frame must be NEW_SESSION")
		return fmt.Errorf("server: first frame was %s", frame.Kind())
	}
	if newSession.Path == "" {
		_ = s.sendError("bad_request", "path is required")
		return errors.New("server: empty path")
	}
	if newSession.ChunkSize > 0 {
		if newSession.ChunkSize > math.MaxInt {
			_ = s.sendError("bad_request", "chunk size exceeds max int")
			return fmt.Errorf("server: chunk size %d exceeds max int", newSession.ChunkSize)
		}
		s.chunkSize = newSession.ChunkSize
	}
	if newSession.ChunkBuffers > 0 {
		s.chunkBuffers = makeChunkBuffers(int(newSession.ChunkBuffers))
	}
	s.request = newSession
	return nil
}

func (s *RgetSession) openFile() error {
	file, err := os.Open(s.request.Path)
	if err != nil {
		_ = s.sendError("open_failed", err.Error())
		return fmt.Errorf("server: open %q: %w", s.request.Path, err)
	}
	s.file = file

	stat, err := s.file.Stat()
	if err != nil {
		_ = s.sendError("stat_failed", err.Error())
		return fmt.Errorf("server: stat %q: %w", s.request.Path, err)
	}
	if stat.IsDir() {
		_ = s.sendError("bad_request", "path is a directory")
		return fmt.Errorf("server: path %q is a directory", s.request.Path)
	}

	s.totalSize = uint64(stat.Size())
	return nil
}

func (s *RgetSession) openRDMAConn(ctx context.Context) error {
	dpConn, err := s.dp.NewConn(ctx)
	if err != nil {
		_ = s.sendError("rdma_failed", err.Error())
		return err
	}
	s.dpConn = dpConn

	if err := s.dpConn.Connect(ctx, s.request.ClientEndpoint); err != nil {
		_ = s.sendError("rdma_connect_failed", err.Error())
		return err
	}
	return nil
}

func (s *RgetSession) registerChunkBuffers() error {
	for i := range s.chunkBuffers {
		remote, err := s.dpConn.RegisterRemoteBuffer(int(s.chunkSize))
		if err != nil {
			_ = s.sendError("rdma_register_failed", err.Error())
			return err
		}
		s.chunkBuffers[i].remote = remote
		s.chunkBuffers[i].data = remote.Bytes()
	}
	return nil
}

func (s *RgetSession) serveChunks() error {
	if s.totalSize == 0 {
		return s.sendReady(emptyChunkReady(s.dpConn.LocalEndpoint()))
	}

	if err := s.fillWindow(); err != nil {
		return err
	}
	for len(s.inflight) > 0 {
		ack, err := s.readAck()
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
			if err := s.fillBuffer(buf); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *RgetSession) fillWindow() error {
	for i := range s.chunkBuffers {
		if s.eof() {
			return nil
		}
		if err := s.fillBuffer(&s.chunkBuffers[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *RgetSession) fillBuffer(buf *chunkBuffer) error {
	offset, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		_ = s.sendError("seek_failed", err.Error())
		return fmt.Errorf("server: current file offset: %w", err)
	}
	want := min(s.chunkSize, s.totalSize-uint64(offset))
	dst := buf.data[:int(want)]
	n, readErr := io.ReadFull(s.file, dst)
	if readErr != nil {
		_ = s.sendError("read_failed", readErr.Error())
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
	return s.sendReady(buf.readyFrame(s.dpConn.LocalEndpoint()))
}

func (s *RgetSession) readAck() (*protocol.ChunkBufferAckFrame, error) {
	ackFrame, err := protocol.DecodeFrame(s.ctrl)
	if err != nil {
		return nil, err
	}
	ack, ok := ackFrame.(*protocol.ChunkBufferAckFrame)
	if !ok {
		return nil, fmt.Errorf("server: expected CHUNK_BUFFER_ACK, got %s", ackFrame.Kind())
	}
	return ack, nil
}

func (s *RgetSession) sendReady(ready *protocol.ChunkBufferReadyFrame) error {
	return protocol.EncodeFrame(s.ctrl, ready)
}

func (s *RgetSession) sendError(kind, message string) error {
	return protocol.EncodeFrame(s.ctrl, &protocol.ErrorFrame{
		ErrorKind: kind,
		Message:   message,
	})
}

func (s *RgetSession) eof() bool {
	offset, err := s.file.Seek(0, io.SeekCurrent)
	return err == nil && uint64(offset) >= s.totalSize
}

func (b *chunkBuffer) readyFrame(endpoint rdma.Endpoint) *protocol.ChunkBufferReadyFrame {
	region := b.remote.Region()
	region.Length = b.size
	return &protocol.ChunkBufferReadyFrame{
		BufferIndex:    b.index,
		ChunkID:        b.chunkID,
		Offset:         b.offset,
		Size:           b.size,
		Final:          b.final,
		ServerEndpoint: endpoint,
		Buffer:         region,
	}
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

func emptyChunkReady(endpoint rdma.Endpoint) *protocol.ChunkBufferReadyFrame {
	return &protocol.ChunkBufferReadyFrame{
		BufferIndex:    0,
		ChunkID:        0,
		Offset:         0,
		Size:           0,
		Final:          true,
		ServerEndpoint: endpoint,
	}
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
