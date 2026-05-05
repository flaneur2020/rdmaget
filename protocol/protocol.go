package protocol

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type FrameKind uint32

const (
	wireMagic       uint32 = 0x52474554 // RGET
	wireVersion     uint16 = 1
	wireHeaderBytes        = 12

	MaxPayloadLen uint32 = 4 << 20

	// HANDSHAKE must be sent first when the TCP control plane is established.
	FrameKindHandshake FrameKind = 1
	// CHUNK_BUFFER_READY is sent by the server after a chunk buffer has been
	// populated and registered for remote RDMA reads.
	FrameKindChunkBufferReady FrameKind = 2
	// CHUNK_BUFFER_ACK is sent by the client after it has completed the RDMA READ
	// for the advertised chunk buffer.
	FrameKindChunkBufferAck FrameKind = 3
	FrameKindError          FrameKind = 4
)

type Frame interface {
	Kind() FrameKind
}

type FrameHeader struct {
	Kind       FrameKind
	PayloadLen uint32
}

type RDMAEndpoint struct {
	Device   string   `json:"device,omitempty"`
	Port     uint8    `json:"port"`
	LID      uint16   `json:"lid,omitempty"`
	QPN      uint32   `json:"qpn"`
	PSN      uint32   `json:"psn"`
	GID      [16]byte `json:"gid,omitempty"`
	GIDIndex int      `json:"gid_index,omitempty"`
}

func (e RDMAEndpoint) HasGID() bool {
	return e.GID != [16]byte{}
}

type RDMAMemoryRegion struct {
	Addr   uint64 `json:"addr"`
	RKey   uint32 `json:"rkey"`
	Length uint64 `json:"length"`
}

type HandshakeFrame struct {
	SessionID      string       `json:"session_id,omitempty"`
	Path           string       `json:"path"`
	ChunkSize      uint64       `json:"chunk_size,omitempty"`
	ChunkBuffers   uint32       `json:"chunk_buffers,omitempty"`
	ClientEndpoint RDMAEndpoint `json:"client_endpoint"`
}

func (f HandshakeFrame) Kind() FrameKind {
	return FrameKindHandshake
}

type ChunkBufferReadyFrame struct {
	BufferIndex    uint64           `json:"buffer_index"`
	ChunkID        uint64           `json:"chunk_id"`
	Offset         uint64           `json:"offset"`
	Size           uint64           `json:"size"`
	Final          bool             `json:"final"`
	ServerEndpoint RDMAEndpoint     `json:"server_endpoint"`
	Buffer         RDMAMemoryRegion `json:"buffer"`
}

func (f ChunkBufferReadyFrame) Kind() FrameKind {
	return FrameKindChunkBufferReady
}

type ChunkBufferAckFrame struct {
	BufferIndex uint64 `json:"buffer_index"`
	ChunkID     uint64 `json:"chunk_id"`
}

func (f ChunkBufferAckFrame) Kind() FrameKind {
	return FrameKindChunkBufferAck
}

type ErrorFrame struct {
	ErrorKind string `json:"error_kind"`
	Message   string `json:"message"`
}

func (f ErrorFrame) Kind() FrameKind {
	return FrameKindError
}

func EncodeFrame(ctx context.Context, w io.Writer, frame Frame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	errc := make(chan error, 1)
	go func() {
		errc <- encodeFrame(w, frame)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errc:
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		return nil
	}
}

func encodeFrame(w io.Writer, frame Frame) error {
	if frame == nil {
		return errors.New("protocol: cannot encode nil frame")
	}

	payload, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("protocol: marshal %s: %w", frame.Kind(), err)
	}
	if len(payload) > int(MaxPayloadLen) {
		return fmt.Errorf("protocol: payload too large: %d > %d", len(payload), MaxPayloadLen)
	}

	var header [wireHeaderBytes]byte
	binary.BigEndian.PutUint32(header[0:4], wireMagic)
	binary.BigEndian.PutUint16(header[4:6], wireVersion)
	binary.BigEndian.PutUint16(header[6:8], uint16(frame.Kind()))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))

	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("protocol: write header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("protocol: write payload: %w", err)
	}
	return nil
}

func DecodeFrame(ctx context.Context, r io.Reader) (Frame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		frame Frame
		err   error
	}
	resultc := make(chan result, 1)
	go func() {
		frame, err := decodeFrame(r)
		resultc <- result{frame: frame, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultc:
		if result.err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, result.err
		}
		return result.frame, nil
	}
}

func decodeFrame(r io.Reader) (Frame, error) {
	var header [wireHeaderBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("protocol: read header: %w", err)
	}

	if got := binary.BigEndian.Uint32(header[0:4]); got != wireMagic {
		return nil, fmt.Errorf("protocol: bad magic 0x%x", got)
	}
	if got := binary.BigEndian.Uint16(header[4:6]); got != wireVersion {
		return nil, fmt.Errorf("protocol: unsupported version %d", got)
	}

	kind := FrameKind(binary.BigEndian.Uint16(header[6:8]))
	payloadLen := binary.BigEndian.Uint32(header[8:12])
	if payloadLen > MaxPayloadLen {
		return nil, fmt.Errorf("protocol: payload too large: %d > %d", payloadLen, MaxPayloadLen)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("protocol: read payload: %w", err)
	}

	frame, err := decodePayload(kind, payload)
	if err != nil {
		return nil, err
	}
	return frame, nil
}

func DecodeHeader(r io.Reader) (FrameHeader, error) {
	var header [wireHeaderBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return FrameHeader{}, fmt.Errorf("protocol: read header: %w", err)
	}
	if got := binary.BigEndian.Uint32(header[0:4]); got != wireMagic {
		return FrameHeader{}, fmt.Errorf("protocol: bad magic 0x%x", got)
	}
	if got := binary.BigEndian.Uint16(header[4:6]); got != wireVersion {
		return FrameHeader{}, fmt.Errorf("protocol: unsupported version %d", got)
	}
	payloadLen := binary.BigEndian.Uint32(header[8:12])
	if payloadLen > MaxPayloadLen {
		return FrameHeader{}, fmt.Errorf("protocol: payload too large: %d > %d", payloadLen, MaxPayloadLen)
	}
	return FrameHeader{
		Kind:       FrameKind(binary.BigEndian.Uint16(header[6:8])),
		PayloadLen: payloadLen,
	}, nil
}

func (k FrameKind) String() string {
	switch k {
	case FrameKindHandshake:
		return "HANDSHAKE"
	case FrameKindChunkBufferReady:
		return "CHUNK_BUFFER_READY"
	case FrameKindChunkBufferAck:
		return "CHUNK_BUFFER_ACK"
	case FrameKindError:
		return "ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint32(k))
	}
}

func decodePayload(kind FrameKind, payload []byte) (Frame, error) {
	switch kind {
	case FrameKindHandshake:
		var frame HandshakeFrame
		if err := json.Unmarshal(payload, &frame); err != nil {
			return nil, fmt.Errorf("protocol: decode %s: %w", kind, err)
		}
		return &frame, nil
	case FrameKindChunkBufferReady:
		var frame ChunkBufferReadyFrame
		if err := json.Unmarshal(payload, &frame); err != nil {
			return nil, fmt.Errorf("protocol: decode %s: %w", kind, err)
		}
		return &frame, nil
	case FrameKindChunkBufferAck:
		var frame ChunkBufferAckFrame
		if err := json.Unmarshal(payload, &frame); err != nil {
			return nil, fmt.Errorf("protocol: decode %s: %w", kind, err)
		}
		return &frame, nil
	case FrameKindError:
		var frame ErrorFrame
		if err := json.Unmarshal(payload, &frame); err != nil {
			return nil, fmt.Errorf("protocol: decode %s: %w", kind, err)
		}
		return &frame, nil
	default:
		return nil, fmt.Errorf("protocol: unknown frame kind %d", uint32(kind))
	}
}
