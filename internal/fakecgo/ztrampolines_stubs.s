//go:build linux && amd64 && !cgo

#include "textflag.h"

TEXT _malloc(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_malloc(SB)

TEXT _free(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_free(SB)

TEXT _setenv(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_setenv(SB)

TEXT _unsetenv(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_unsetenv(SB)

TEXT _sigfillset(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_sigfillset(SB)

TEXT _nanosleep(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_nanosleep(SB)

TEXT _abort(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_abort(SB)

TEXT _pthread_attr_init(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_attr_init(SB)

TEXT _pthread_create(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_create(SB)

TEXT _pthread_detach(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_detach(SB)

TEXT _pthread_sigmask(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_sigmask(SB)

TEXT _pthread_mutex_lock(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_mutex_lock(SB)

TEXT _pthread_mutex_unlock(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_mutex_unlock(SB)

TEXT _pthread_cond_broadcast(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_cond_broadcast(SB)

TEXT _pthread_setspecific(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_setspecific(SB)
