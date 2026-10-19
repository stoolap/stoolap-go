//go:build !cgo && (linux || darwin)

package fakecgo

type (
	size_t         uintptr
	sigset_t       [128]byte
	pthread_attr_t [64]byte
	pthread_t      int
	pthread_key_t  uint64
)

type sighow int32

type G struct {
	stacklo uintptr
	stackhi uintptr
}

type ThreadStart struct {
	g   *G
	tls *uintptr
	fn  uintptr
}
