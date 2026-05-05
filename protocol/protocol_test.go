package protocol

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	frames := []Frame{
		&NewSessionFrame{
			SessionID: "s1",
			Path:      "/tmp/file",
			ChunkSize: 4096,
			ClientEndpoint: RDMAEndpoint{
				Device: "rxe0",
				Port:   1,
				LID:    1,
				QPN:    17,
				PSN:    99,
			},
		},
		&ChunkBufferReadyFrame{
			BufferIndex: 2,
			ChunkID:     3,
			Offset:      4096,
			Size:        1024,
			Final:       true,
			ServerEndpoint: RDMAEndpoint{
				Device: "rxe0",
				Port:   1,
				QPN:    18,
				PSN:    100,
			},
			Buffer: RDMAMemoryRegion{
				Addr:   0x1000,
				RKey:   42,
				Length: 1024,
			},
		},
		&ChunkBufferAckFrame{BufferIndex: 2, ChunkID: 3},
		&ErrorFrame{ErrorKind: "bad_request", Message: "missing path"},
	}

	var wire bytes.Buffer
	for _, frame := range frames {
		if err := EncodeFrame(context.Background(), &wire, frame); err != nil {
			t.Fatalf("EncodeFrame(%s): %v", frame.Kind(), err)
		}
	}

	for _, want := range frames {
		got, err := DecodeFrame(context.Background(), &wire)
		if err != nil {
			t.Fatalf("DecodeFrame(%s): %v", want.Kind(), err)
		}
		if got.Kind() != want.Kind() {
			t.Fatalf("kind mismatch: got %s want %s", got.Kind(), want.Kind())
		}
	}

	_, err := DecodeFrame(context.Background(), &wire)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("DecodeFrame EOF = %v, want io.EOF", err)
	}
}

func TestDecodeFrameRejectsBadMagic(t *testing.T) {
	wire := bytes.NewBuffer([]byte{
		0, 0, 0, 0,
		0, 1,
		0, byte(FrameKindNewSession),
		0, 0, 0, 0,
	})

	_, err := DecodeFrame(context.Background(), wire)
	if err == nil {
		t.Fatal("DecodeFrame succeeded with bad magic")
	}
}
