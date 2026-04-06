//go:build linux && amd64 && !cgo

#include "textflag.h"

TEXT _pthread_attr_getstacksize(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_attr_getstacksize(SB)

TEXT _pthread_attr_destroy(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_attr_destroy(SB)
