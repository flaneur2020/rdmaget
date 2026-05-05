package rdma

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryRegisterRdmaBuffersReturnsChunkViews(t *testing.T) {
	device := NewMemoryDevice()
	conn, err := device.Connect(context.Background(), device.Info())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	buffers, err := conn.RegisterRdmaBuffers(8, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer closeRdmaBuffers(t, buffers)

	if len(buffers) != 3 {
		t.Fatalf("buffers = %d, want 3", len(buffers))
	}

	base := buffers[0].Region()
	for i, buffer := range buffers {
		region := buffer.Region()
		if len(buffer.Bytes()) != 8 {
			t.Fatalf("buffer %d len = %d, want 8", i, len(buffer.Bytes()))
		}
		if region.Length != 8 {
			t.Fatalf("buffer %d region length = %d, want 8", i, region.Length)
		}
		if region.RKey != base.RKey {
			t.Fatalf("buffer %d rkey = %d, want %d", i, region.RKey, base.RKey)
		}
		wantAddr := base.Addr + uint64(i*8)
		if region.Addr != wantAddr {
			t.Fatalf("buffer %d addr = %d, want %d", i, region.Addr, wantAddr)
		}
	}

	copy(buffers[1].Bytes(), []byte("chunk-01"))

	readBuffers, err := conn.RegisterRdmaBuffers(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeRdmaBuffers(t, readBuffers)

	remote := buffers[1].Region()
	if err := conn.Read(context.Background(), readBuffers[0], remote); err != nil {
		t.Fatal(err)
	}
	if got, want := readBuffers[0].Bytes(), []byte("chunk-01"); !bytes.Equal(got, want) {
		t.Fatalf("read buffer = %q, want %q", got, want)
	}
}

func closeRdmaBuffers(t *testing.T, buffers []RdmaBuffer) {
	t.Helper()
	for _, buffer := range buffers {
		if err := buffer.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
