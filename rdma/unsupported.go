//go:build !linux || !cgo || !rdma

package rdma

func open(opts Options) (DataPlane, error) {
	return nil, ErrUnsupported
}
