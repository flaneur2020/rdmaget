# rget

`rget` copies a file with a TCP control plane and an RDMA data plane.

Control flow:

1. Client opens the TCP control connection and sends `NEW_SESSION`.
2. Server opens the requested file, registers a fixed list of equal-width chunk buffers for remote read, fills available buffers, and sends `CHUNK_BUFFER_READY`.
3. Client performs an RDMA READ from the advertised chunk buffer.
4. Client sends `CHUNK_BUFFER_ACK` over the TCP control plane.
5. Server reuses the acknowledged chunk buffer and repeats until the final chunk.

## Build

Default builds compile everywhere, but real RDMA returns an unsupported error:

```sh
go test ./...
go build ./cmd/rget
```

Build real verbs support on Linux with `libibverbs` headers installed:

```sh
CGO_ENABLED=1 go test -tags rdma ./...
CGO_ENABLED=1 go build -tags rdma ./cmd/rget
```

## Run

On the server:

```sh
./rget serve -listen :7471 -chunk-buffers 4 -rdma-device rxe0 -rdma-port 1 -gid-index 0
```

On the client:

```sh
./rget get -addr 127.0.0.1:7471 -chunk-buffers 4 -o copy.bin /path/on/server/file.bin
```

For same-host soft-RDMA smoke testing, both commands can run in the same Linux VM.

## Lima soft-RDMA setup

The VM kernel must include `rdma_rxe`. In Ubuntu:

```sh
sudo apt-get install -y golang-go build-essential pkg-config libibverbs-dev rdma-core ibverbs-utils iproute2 kmod
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev eth0
rdma link show
ibv_devinfo
```

Then build and run:

```sh
CGO_ENABLED=1 go build -tags rdma -o rget ./cmd/rget
dd if=/dev/urandom of=/tmp/source.bin bs=1M count=8
./rget serve -listen 127.0.0.1:7471 -rdma-device rxe0 &
./rget get -addr 127.0.0.1:7471 -rdma-device rxe0 -o /tmp/copy.bin /tmp/source.bin
cmp /tmp/source.bin /tmp/copy.bin
```

The current `lima default` VM used during development was Ubuntu 24.10 with kernel `6.11.0-9-generic`; it did not ship `rdma_rxe`, so real RDMA execution could not be completed there. Linux compilation with `-tags rdma` did pass after installing `libibverbs-dev`.
