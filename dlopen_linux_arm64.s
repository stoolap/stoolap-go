//go:build linux && arm64 && !cgo

#include "textflag.h"

// Addressable stubs that jump to dynamically imported libdl symbols.

TEXT libdl_dlopen(SB),NOSPLIT|NOFRAME,$0-0
	JMP stoolap_dlopen_sym(SB)

TEXT libdl_dlsym(SB),NOSPLIT|NOFRAME,$0-0
	JMP stoolap_dlsym_sym(SB)

TEXT libdl_dlerror(SB),NOSPLIT|NOFRAME,$0-0
	JMP stoolap_dlerror_sym(SB)
