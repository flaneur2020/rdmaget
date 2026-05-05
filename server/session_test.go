package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestReadAckReturnsContextDeadline(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	session := NewSession(serverConn, nil, 0, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := session.readAck(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("readAck err = %v, want context deadline exceeded", err)
	}
}
