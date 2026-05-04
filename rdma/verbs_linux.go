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

static int rget_post_rdma_read_wr_id(struct ibv_qp *qp, uint64_t wr_id, void *dst, uint32_t lkey, uint32_t len, uint64_t remote_addr, uint32_t rkey) {
	struct ibv_sge sge;
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)dst;
	sge.length = len;
	sge.lkey = lkey;

	struct ibv_send_wr wr;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = wr_id;
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
	"sync"
	"unsafe"
)

type verbsDevice struct {
	mu       sync.Mutex
	ctx      *C.struct_ibv_context
	pd       *C.struct_ibv_pd
	device   string
	port     uint8
	gidIndex int
	conn     *verbsConn
	conns    []*verbsConn
	connErr  error
	closed   bool
}

type verbsConn struct {
	device    *verbsDevice
	mu        sync.Mutex
	nextWRID  uint64
	done      map[uint64]C.struct_ibv_wc
	cq        *C.struct_ibv_cq
	qp        *C.struct_ibv_qp
	endpoint  Addr
	connected bool
	closed    bool
}

type verbsRdmaBuffer struct {
	ptr    unsafe.Pointer
	bytes  []byte
	mr     *C.struct_ibv_mr
	closed bool
}

func open(opts Options) (RdmaDevice, error) {
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

	device := &verbsDevice{
		ctx:      ctx,
		pd:       pd,
		device:   selectedName,
		port:     port,
		gidIndex: opts.GIDIndex,
	}
	conn, err := device.newConn()
	if err != nil {
		_ = device.Close()
		return nil, err
	}
	device.conn = conn
	device.conns = append(device.conns, conn)
	return device, nil
}

func (d *verbsDevice) Info() Addr {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return Addr{
			Device:   d.device,
			Port:     d.port,
			GIDIndex: d.gidIndex,
		}
	}
	if d.conn == nil || d.conn.closed || d.conn.connected {
		conn, err := d.newConn()
		if err != nil {
			d.connErr = err
			return Addr{
				Device:   d.device,
				Port:     d.port,
				GIDIndex: d.gidIndex,
			}
		}
		d.conn = conn
		d.conns = append(d.conns, conn)
		d.connErr = nil
	}
	return d.conn.endpoint
}

func (d *verbsDevice) Connect(ctx context.Context, remote Addr) (Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, fmt.Errorf("rdma: device is closed")
	}
	if d.connErr != nil {
		err := d.connErr
		d.connErr = nil
		return nil, err
	}
	conn := d.conn
	if conn == nil || conn.closed || conn.connected {
		var err error
		conn, err = d.newConn()
		if err != nil {
			return nil, err
		}
		d.conn = conn
		d.conns = append(d.conns, conn)
	}

	useGID := 0
	if remote.HasGID() {
		useGID = 1
	}
	if rc := C.rget_modify_qp_rtr(
		conn.qp,
		C.uint32_t(remote.QPN),
		C.uint32_t(remote.PSN),
		C.uint16_t(remote.LID),
		C.uint8_t(conn.device.port),
		C.uint8_t(conn.device.gidIndex),
		(*C.uchar)(unsafe.Pointer(&remote.GID[0])),
		C.int(useGID),
	); rc != 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("rdma: modify qp RTR failed: %d", int(rc))
	}
	if rc := C.rget_modify_qp_rts(conn.qp, C.uint32_t(conn.endpoint.PSN)); rc != 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("rdma: modify qp RTS failed: %d", int(rc))
	}
	conn.connected = true
	return conn, nil
}

func (d *verbsDevice) newConn() (*verbsConn, error) {
	cq := C.ibv_create_cq(d.ctx, 128, nil, nil, 0)
	if cq == nil {
		return nil, fmt.Errorf("rdma: create cq failed")
	}

	qp := C.rget_create_qp(d.pd, cq)
	if qp == nil {
		C.ibv_destroy_cq(cq)
		return nil, fmt.Errorf("rdma: create qp failed")
	}

	if rc := C.rget_modify_qp_init(qp, C.uint8_t(d.port)); rc != 0 {
		C.ibv_destroy_qp(qp)
		C.ibv_destroy_cq(cq)
		return nil, fmt.Errorf("rdma: modify qp INIT failed: %d", int(rc))
	}

	var portAttr C.struct_ibv_port_attr
	if rc := C.rget_port_attr(d.ctx, C.uint8_t(d.port), &portAttr); rc != 0 {
		C.ibv_destroy_qp(qp)
		C.ibv_destroy_cq(cq)
		return nil, fmt.Errorf("rdma: query port failed: %d", int(rc))
	}

	var gid C.union_ibv_gid = C.rget_zero_gid()
	if d.gidIndex >= 0 {
		if rc := C.ibv_query_gid(d.ctx, C.uint8_t(d.port), C.int(d.gidIndex), &gid); rc != 0 {
			C.ibv_destroy_qp(qp)
			C.ibv_destroy_cq(cq)
			return nil, fmt.Errorf("rdma: query gid index %d failed: %d", d.gidIndex, int(rc))
		}
	}

	psn, err := randomPSN()
	if err != nil {
		C.ibv_destroy_qp(qp)
		C.ibv_destroy_cq(cq)
		return nil, err
	}

	conn := &verbsConn{
		device: d,
		done:   make(map[uint64]C.struct_ibv_wc),
		cq:     cq,
		qp:     qp,
		endpoint: Addr{
			Device:   d.device,
			Port:     d.port,
			LID:      uint16(portAttr.lid),
			QPN:      uint32(qp.qp_num),
			PSN:      psn,
			GIDIndex: d.gidIndex,
		},
	}
	copy(conn.endpoint.GID[:], C.GoBytes(unsafe.Pointer(&gid), 16))
	return conn, nil
}

func (d *verbsDevice) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	var err error
	for _, conn := range d.conns {
		if closeErr := conn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	d.conn = nil
	d.conns = nil
	if d.pd != nil {
		if rc := C.ibv_dealloc_pd(d.pd); rc != 0 && err == nil {
			err = fmt.Errorf("rdma: dealloc pd failed: %d", int(rc))
		}
		d.pd = nil
	}
	if d.ctx != nil {
		if rc := C.ibv_close_device(d.ctx); rc != 0 && err == nil {
			err = fmt.Errorf("rdma: close device failed: %d", int(rc))
		}
		d.ctx = nil
	}
	return err
}

func (c *verbsConn) LocalEndpoint() Addr {
	return c.endpoint
}

func (c *verbsConn) RegisterRdmaBuffer(size int) (RdmaBuffer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("rdma: cannot register empty buffer")
	}
	cbuf := C.malloc(C.size_t(size))
	if cbuf == nil {
		return nil, fmt.Errorf("rdma: allocate remote buffer failed")
	}

	mr := C.ibv_reg_mr(
		c.device.pd,
		cbuf,
		C.size_t(size),
		C.IBV_ACCESS_LOCAL_WRITE|C.IBV_ACCESS_REMOTE_READ,
	)
	if mr == nil {
		C.free(cbuf)
		return nil, fmt.Errorf("rdma: register mr failed")
	}
	return &verbsRdmaBuffer{
		ptr:   cbuf,
		bytes: unsafe.Slice((*byte)(cbuf), size),
		mr:    mr,
	}, nil
}

func (b *verbsRdmaBuffer) Bytes() []byte {
	return b.bytes
}

func (b *verbsRdmaBuffer) Region() MemoryRegion {
	if b == nil || b.mr == nil {
		return MemoryRegion{}
	}
	return MemoryRegion{
		Addr:   uint64(uintptr(b.ptr)),
		RKey:   uint32(b.mr.rkey),
		Length: uint64(len(b.bytes)),
	}
}

func (b *verbsRdmaBuffer) Close() error {
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

func (c *verbsConn) Read(ctx context.Context, dst RdmaBuffer, remote MemoryRegion) error {
	buffer, ok := dst.(*verbsRdmaBuffer)
	if !ok {
		return fmt.Errorf("rdma: read destination was not registered by this connection")
	}
	if buffer.closed || buffer.mr == nil {
		return fmt.Errorf("rdma: read destination buffer is closed")
	}
	dstBytes := dst.Bytes()
	if remote.Length == 0 {
		return nil
	}
	if remote.Length > uint64(len(dstBytes)) {
		return fmt.Errorf("rdma: read length %d exceeds local buffer %d", remote.Length, len(dstBytes))
	}

	wrID := c.nextSendWRID()
	if rc := C.rget_post_rdma_read_wr_id(
		c.qp,
		C.uint64_t(wrID),
		buffer.ptr,
		C.uint32_t(buffer.mr.lkey),
		C.uint32_t(remote.Length),
		C.uint64_t(remote.Addr),
		C.uint32_t(remote.RKey),
	); rc != 0 {
		return fmt.Errorf("rdma: post read failed: %d", int(rc))
	}

	if err := c.waitCompletion(ctx, wrID); err != nil {
		return err
	}
	return nil
}

func (c *verbsConn) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeLocked()
}

func (c *verbsConn) closeLocked() error {
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

func (c *verbsConn) nextSendWRID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextWRID++
	return c.nextWRID
}

func (c *verbsConn) waitCompletion(ctx context.Context, wrID uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		c.mu.Lock()
		if wc, ok := c.done[wrID]; ok {
			delete(c.done, wrID)
			c.mu.Unlock()
			return checkCompletion(wc)
		}
		var wc C.struct_ibv_wc
		n := C.ibv_poll_cq(c.cq, 1, &wc)
		if n < 0 {
			c.mu.Unlock()
			return fmt.Errorf("rdma: poll cq failed: %d", int(n))
		}
		if n == 0 {
			c.mu.Unlock()
			continue
		}
		got := uint64(wc.wr_id)
		if got == wrID {
			c.mu.Unlock()
			return checkCompletion(wc)
		}
		c.done[got] = wc
		c.mu.Unlock()
	}
}

func checkCompletion(wc C.struct_ibv_wc) error {
	if wc.status != C.IBV_WC_SUCCESS {
		return fmt.Errorf("rdma: read completion failed: status=%d", int(wc.status))
	}
	return nil
}

func randomPSN() (uint32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("rdma: random psn: %w", err)
	}
	return binary.BigEndian.Uint32(b[:]) & 0xffffff, nil
}
