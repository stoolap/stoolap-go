//go:build !cgo && linux

package fakecgo

type (
	pthread_cond_t  [48]byte
	pthread_mutex_t [48]byte
)

var (
	PTHREAD_COND_INITIALIZER  = pthread_cond_t{}
	PTHREAD_MUTEX_INITIALIZER = pthread_mutex_t{}
)

const (
	SIG_BLOCK   sighow = 0
	SIG_UNBLOCK sighow = 1
	SIG_SETMASK sighow = 2
)
