package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/flaneur2020/rdmacp/rdma"
	"github.com/flaneur2020/rdmacp/session"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage(os.Stderr)
		return errors.New("missing command")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "serve":
		return runServe(ctx, args[1:])
	case "get":
		return runGet(ctx, args[1:])
	case "help", "-h", "--help":
		usage(os.Stdout)
		return nil
	default:
		usage(os.Stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("listen", ":7471", "TCP control-plane listen address")
	chunkSize := fs.Uint64("chunk-size", session.DefaultChunkSize, "chunk buffer size in bytes")
	device := fs.String("rdma-device", "", "RDMA verbs device name, empty selects the first device")
	port := fs.Uint("rdma-port", 1, "RDMA port number")
	gidIndex := fs.Int("gid-index", 0, "RDMA GID index")
	if err := fs.Parse(args); err != nil {
		return err
	}

	dp, err := rdma.Open(rdma.Options{
		Device:   *device,
		Port:     uint8(*port),
		GIDIndex: *gidIndex,
	})
	if err != nil {
		return err
	}
	defer dp.Close()

	return session.RunServer(ctx, session.ServerConfig{
		Addr:      *addr,
		ChunkSize: *chunkSize,
		DataPlane: dp,
	})
}

func runGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", "127.0.0.1:7471", "server TCP control-plane address")
	output := fs.String("o", "-", "output path, or - for stdout")
	chunkSize := fs.Uint64("chunk-size", session.DefaultChunkSize, "requested chunk buffer size in bytes")
	device := fs.String("rdma-device", "", "RDMA verbs device name, empty selects the first device")
	port := fs.Uint("rdma-port", 1, "RDMA port number")
	gidIndex := fs.Int("gid-index", 0, "RDMA GID index")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("get requires exactly one remote path")
	}

	dp, err := rdma.Open(rdma.Options{
		Device:   *device,
		Port:     uint8(*port),
		GIDIndex: *gidIndex,
	})
	if err != nil {
		return err
	}
	defer dp.Close()

	return session.RunClient(ctx, session.ClientConfig{
		Addr:      *addr,
		Path:      fs.Arg(0),
		Output:    *output,
		ChunkSize: *chunkSize,
		DataPlane: dp,
	})
}

func usage(out *os.File) {
	fmt.Fprintln(out, `usage:
  rdmacp serve [-listen :7471] [-chunk-size 1048576] [-rdma-device rxe0] [-rdma-port 1] [-gid-index 0]
  rdmacp get [-addr 127.0.0.1:7471] [-o output] [-chunk-size 1048576] remote-path

Build real RDMA support on Linux with:
  go build -tags rdma ./cmd/rdmacp`)
}
