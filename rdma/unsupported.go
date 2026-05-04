//go:build !linux || !cgo || !rdma

package rdma

func open(opts Options) (RdmaDevice, error) {
	return nil, ErrUnsupported
}
