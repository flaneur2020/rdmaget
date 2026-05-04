//go:build linux && cgo && rdma

package rdma

/*
#cgo LDFLAGS: -libverbs
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <string.h>

static int rget_port_attr(struct ibv_context *ctx, uint8_t port, struct ibv_port_attr *attr) {
	return ibv_query_port(ctx, port, attr);
}

static union ibv_gid rget_zero_gid(void) {
	union ibv_gid gid;
	memset(&gid, 0, sizeof(gid));
	return gid;
}

static int rget_modify_qp_init(struct ibv_qp *qp, uint8_t port) {
	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.pkey_index = 0;
	attr.port_num = port;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	return ibv_modify_qp(qp, &attr,
		IBV_QP_STATE |
		IBV_QP_PKEY_INDEX |
		IBV_QP_PORT |
		IBV_QP_ACCESS_FLAGS);
}

static int rget_modify_qp_rtr(struct ibv_qp *qp, uint32_t dest_qpn, uint32_t rq_psn, uint16_t dlid, uint8_t port, uint8_t sgid_index, const unsigned char *dgid, int use_gid) {
	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = IBV_MTU_1024;
	attr.dest_qp_num = dest_qpn;
	attr.rq_psn = rq_psn;
	attr.max_dest_rd_atomic = 16;
	attr.min_rnr_timer = 12;
	attr.ah_attr.is_global = use_gid ? 1 : 0;
	attr.ah_attr.dlid = dlid;
	attr.ah_attr.sl = 0;
	attr.ah_attr.src_path_bits = 0;
	attr.ah_attr.port_num = port;
	if (use_gid) {
		memcpy(attr.ah_attr.grh.dgid.raw, dgid, 16);
		attr.ah_attr.grh.flow_label = 0;
		attr.ah_attr.grh.sgid_index = sgid_index;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.traffic_class = 0;
	}
	return ibv_modify_qp(qp, &attr,
		IBV_QP_STATE |
		IBV_QP_AV |
		IBV_QP_PATH_MTU |
		IBV_QP_DEST_QPN |
		IBV_QP_RQ_PSN |
		IBV_QP_MAX_DEST_RD_ATOMIC |
		IBV_QP_MIN_RNR_TIMER);
}

static int rget_modify_qp_rts(struct ibv_qp *qp, uint32_t sq_psn) {
	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 14;
	attr.retry_cnt = 7;
	attr.rnr_retry = 7;
	attr.sq_psn = sq_psn;
	attr.max_rd_atomic = 16;
	return ibv_modify_qp(qp, &attr,
		IBV_QP_STATE |
		IBV_QP_TIMEOUT |
		IBV_QP_RETRY_CNT |
		IBV_QP_RNR_RETRY |
		IBV_QP_SQ_PSN |
		IBV_QP_MAX_QP_RD_ATOMIC);
}

static struct ibv_qp *rget_create_qp(struct ibv_pd *pd, struct ibv_cq *cq) {
	struct ibv_qp_init_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.send_cq = cq;
	attr.recv_cq = cq;
	attr.qp_type = IBV_QPT_RC;
	attr.cap.max_send_wr = 128;
	attr.cap.max_recv_wr = 1;
	attr.cap.max_send_sge = 1;
	attr.cap.max_recv_sge = 1;
	return ibv_create_qp(pd, &attr);
}

static int rget_post_rdma_read(struct ibv_qp *qp, void *dst, uint32_t lkey, uint32_t len, uint64_t remote_addr, uint32_t rkey) {
	struct ibv_sge sge;
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)dst;
	sge.length = len;
	sge.lkey = lkey;

	struct ibv_send_wr wr;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = 1;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_RDMA_READ;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.wr.rdma.remote_addr = remote_addr;
	wr.wr.rdma.rkey = rkey;

	struct ibv_send_wr *bad_wr = NULL;
	return ibv_post_send(qp, &wr, &bad_wr);
}
*/
import "C"

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"unsafe"
)

type verbsPlane struct {
	ctx      *C.struct_ibv_context
	pd       *C.struct_ibv_pd
	device   string
	port     uint8
	gidIndex int
}

type verbsConn struct {
	plane    *verbsPlane
	cq       *C.struct_ibv_cq
	qp       *C.struct_ibv_qp
	endpoint Endpoint
	closed   bool
}

type verbsRemoteBuffer struct {
	ptr    unsafe.Pointer
	bytes  []byte
	mr     *C.struct_ibv_mr
	closed bool
}

func open(opts Options) (DataPlane, error) {
	port := opts.Port
	if port == 0 {
		port = 1
	}

	var count C.int
	list := C.ibv_get_device_list(&count)
	if list == nil {
		return nil, fmt.Errorf("rdma: ibv_get_device_list failed")
	}
	defer C.ibv_free_device_list(list)
	if count == 0 {
		return nil, fmt.Errorf("rdma: no verbs devices found")
	}

	devices := unsafe.Slice(list, int(count))
	var selected *C.struct_ibv_device
	var selectedName string
	for _, dev := range devices {
		name := C.GoString(C.ibv_get_device_name(dev))
		if opts.Device == "" || opts.Device == name {
			selected = dev
			selectedName = name
			break
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("rdma: device %q not found", opts.Device)
	}

	ctx := C.ibv_open_device(selected)
	if ctx == nil {
		return nil, fmt.Errorf("rdma: open device %s failed", selectedName)
	}

	pd := C.ibv_alloc_pd(ctx)
	if pd == nil {
		C.ibv_close_device(ctx)
		return nil, fmt.Errorf("rdma: alloc pd failed")
	}

	return &verbsPlane{
		ctx:      ctx,
		pd:       pd,
		device:   selectedName,
		port:     port,
		gidIndex: opts.GIDIndex,
	}, nil
}

func (p *verbsPlane) NewConn(ctx context.Context) (Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cq := C.ibv_create_cq(p.ctx, 128, nil, nil, 0)
	if cq == nil {
		return nil, fmt.Errorf("rdma: create cq failed")
	}

	qp := C.rget_create_qp(p.pd, cq)
	if qp == nil {
		C.ibv_destroy_cq(cq)
		return nil, fmt.Errorf("rdma: create qp failed")
	}

	if rc := C.rget_modify_qp_init(qp, C.uint8_t(p.port)); rc != 0 {
		C.ibv_destroy_qp(qp)
		C.ibv_destroy_cq(cq)
		return nil, fmt.Errorf("rdma: modify qp INIT failed: %d", int(rc))
	}

	var portAttr C.struct_ibv_port_attr
	if rc := C.rget_port_attr(p.ctx, C.uint8_t(p.port), &portAttr); rc != 0 {
		C.ibv_destroy_qp(qp)
		C.ibv_destroy_cq(cq)
		return nil, fmt.Errorf("rdma: query port failed: %d", int(rc))
	}

	var gid C.union_ibv_gid = C.rget_zero_gid()
	if p.gidIndex >= 0 {
		if rc := C.ibv_query_gid(p.ctx, C.uint8_t(p.port), C.int(p.gidIndex), &gid); rc != 0 {
			C.ibv_destroy_qp(qp)
			C.ibv_destroy_cq(cq)
			return nil, fmt.Errorf("rdma: query gid index %d failed: %d", p.gidIndex, int(rc))
		}
	}

	psn, err := randomPSN()
	if err != nil {
		C.ibv_destroy_qp(qp)
		C.ibv_destroy_cq(cq)
		return nil, err
	}

	conn := &verbsConn{
		plane: p,
		cq:    cq,
		qp:    qp,
		endpoint: Endpoint{
			Device:   p.device,
			Port:     p.port,
			LID:      uint16(portAttr.lid),
			QPN:      uint32(qp.qp_num),
			PSN:      psn,
			GIDIndex: p.gidIndex,
		},
	}
	copy(conn.endpoint.GID[:], C.GoBytes(unsafe.Pointer(&gid), 16))
	return conn, nil
}

func (p *verbsPlane) Close() error {
	if p == nil {
		return nil
	}
	var err error
	if p.pd != nil {
		if rc := C.ibv_dealloc_pd(p.pd); rc != 0 {
			err = fmt.Errorf("rdma: dealloc pd failed: %d", int(rc))
		}
		p.pd = nil
	}
	if p.ctx != nil {
		if rc := C.ibv_close_device(p.ctx); rc != 0 && err == nil {
			err = fmt.Errorf("rdma: close device failed: %d", int(rc))
		}
		p.ctx = nil
	}
	return err
}

func (c *verbsConn) LocalEndpoint() Endpoint {
	return c.endpoint
}

func (c *verbsConn) Connect(ctx context.Context, remote Endpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	useGID := 0
	if remote.HasGID() {
		useGID = 1
	}
	if rc := C.rget_modify_qp_rtr(
		c.qp,
		C.uint32_t(remote.QPN),
		C.uint32_t(remote.PSN),
		C.uint16_t(remote.LID),
		C.uint8_t(c.plane.port),
		C.uint8_t(c.plane.gidIndex),
		(*C.uchar)(unsafe.Pointer(&remote.GID[0])),
		C.int(useGID),
	); rc != 0 {
		return fmt.Errorf("rdma: modify qp RTR failed: %d", int(rc))
	}
	if rc := C.rget_modify_qp_rts(c.qp, C.uint32_t(c.endpoint.PSN)); rc != 0 {
		return fmt.Errorf("rdma: modify qp RTS failed: %d", int(rc))
	}
	return nil
}

func (c *verbsConn) RegisterRemoteBuffer(size int) (RemoteBuffer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("rdma: cannot register empty buffer")
	}
	cbuf := C.malloc(C.size_t(size))
	if cbuf == nil {
		return nil, fmt.Errorf("rdma: allocate remote buffer failed")
	}

	mr := C.ibv_reg_mr(
		c.plane.pd,
		cbuf,
		C.size_t(size),
		C.IBV_ACCESS_LOCAL_WRITE|C.IBV_ACCESS_REMOTE_READ,
	)
	if mr == nil {
		C.free(cbuf)
		return nil, fmt.Errorf("rdma: register mr failed")
	}
	return &verbsRemoteBuffer{
		ptr:   cbuf,
		bytes: unsafe.Slice((*byte)(cbuf), size),
		mr:    mr,
	}, nil
}

func (b *verbsRemoteBuffer) Bytes() []byte {
	return b.bytes
}

func (b *verbsRemoteBuffer) Region() MemoryRegion {
	if b == nil || b.mr == nil {
		return MemoryRegion{}
	}
	return MemoryRegion{
		Addr:   uint64(uintptr(b.ptr)),
		RKey:   uint32(b.mr.rkey),
		Length: uint64(len(b.bytes)),
	}
}

func (b *verbsRemoteBuffer) Close() error {
	if b == nil || b.closed {
		return nil
	}
	b.closed = true
	var err error
	if b.mr != nil {
		if rc := C.ibv_dereg_mr(b.mr); rc != 0 {
			err = fmt.Errorf("rdma: dereg mr failed: %d", int(rc))
		}
		b.mr = nil
	}
	if b.ptr != nil {
		C.free(b.ptr)
		b.ptr = nil
		b.bytes = nil
	}
	return err
}

func (c *verbsConn) Read(ctx context.Context, dst []byte, remote MemoryRegion) error {
	if len(dst) == 0 {
		return nil
	}
	if uint64(len(dst)) > remote.Length {
		return fmt.Errorf("rdma: read length %d exceeds remote region %d", len(dst), remote.Length)
	}
	cbuf := C.malloc(C.size_t(len(dst)))
	if cbuf == nil {
		return fmt.Errorf("rdma: allocate local read buffer failed")
	}
	defer C.free(cbuf)

	mr := C.ibv_reg_mr(
		c.plane.pd,
		cbuf,
		C.size_t(len(dst)),
		C.IBV_ACCESS_LOCAL_WRITE,
	)
	if mr == nil {
		return fmt.Errorf("rdma: register local read buffer failed")
	}
	defer C.ibv_dereg_mr(mr)

	if rc := C.rget_post_rdma_read(
		c.qp,
		cbuf,
		C.uint32_t(mr.lkey),
		C.uint32_t(len(dst)),
		C.uint64_t(remote.Addr),
		C.uint32_t(remote.RKey),
	); rc != 0 {
		return fmt.Errorf("rdma: post read failed: %d", int(rc))
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var wc C.struct_ibv_wc
		n := C.ibv_poll_cq(c.cq, 1, &wc)
		if n < 0 {
			return fmt.Errorf("rdma: poll cq failed: %d", int(n))
		}
		if n == 0 {
			continue
		}
		if wc.status != C.IBV_WC_SUCCESS {
			return fmt.Errorf("rdma: read completion failed: status=%d", int(wc.status))
		}
		C.memcpy(unsafe.Pointer(&dst[0]), cbuf, C.size_t(len(dst)))
		return nil
	}
}

func (c *verbsConn) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	var err error
	if c.qp != nil {
		if rc := C.ibv_destroy_qp(c.qp); rc != 0 {
			err = fmt.Errorf("rdma: destroy qp failed: %d", int(rc))
		}
		c.qp = nil
	}
	if c.cq != nil {
		if rc := C.ibv_destroy_cq(c.cq); rc != 0 && err == nil {
			err = fmt.Errorf("rdma: destroy cq failed: %d", int(rc))
		}
		c.cq = nil
	}
	return err
}

func randomPSN() (uint32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("rdma: random psn: %w", err)
	}
	return binary.BigEndian.Uint32(b[:]) & 0xffffff, nil
}
