/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//go:build !cgo && (linux || darwin)

#include "textflag.h"
#include "go_asm.h"
#include "abi_arm64.h"

// Each x_cgo_*_trampoline is called by the Go runtime through the
// matching _cgo_* hook with the C calling convention and forwards to the
// Go implementation stored in the *_call variables.

TEXT x_cgo_init_trampoline(SB), NOSPLIT, $0-0
	MOVD ·x_cgo_init_call(SB), R9
	MOVD (R9), R9
	CALL R9
	RET

TEXT x_cgo_thread_start_trampoline(SB), NOSPLIT, $0-0
	MOVD ·x_cgo_thread_start_call(SB), R9
	MOVD (R9), R9
	CALL R9
	RET

TEXT x_cgo_setenv_trampoline(SB), NOSPLIT, $0-0
	MOVD ·x_cgo_setenv_call(SB), R9
	MOVD (R9), R9
	CALL R9
	RET

TEXT x_cgo_unsetenv_trampoline(SB), NOSPLIT, $0-0
	MOVD ·x_cgo_unsetenv_call(SB), R9
	MOVD (R9), R9
	CALL R9
	RET

TEXT x_cgo_notify_runtime_init_done_trampoline(SB), NOSPLIT, $0-0
	CALL ·x_cgo_notify_runtime_init_done(SB)
	RET

TEXT x_cgo_bindm_trampoline(SB), NOSPLIT, $0
	CALL ·x_cgo_bindm(SB)
	RET

TEXT ·setg_trampoline(SB), NOSPLIT, $0-16
	MOVD G+8(FP), R0
	MOVD setg+0(FP), R9
	CALL R9
	RET

// threadentry_trampoline is the pthread start routine for threads the
// runtime asks fakecgo to create.
TEXT threadentry_trampoline(SB), NOSPLIT, $0-0
	SUB  $(8*24), RSP
	STP  (R0, R1), (8*1)(RSP)
	MOVD R3, (8*3)(RSP)

	SAVE_R19_TO_R28(8*4)
	SAVE_F8_TO_F15(8*14)

	MOVD ·threadentry_call(SB), R9
	MOVD (R9), R9
	CALL R9
	MOVD $0, R0
	RESTORE_R19_TO_R28(8*4)
	RESTORE_F8_TO_F15(8*14)
	ADD $(8*24), RSP
	RET

TEXT ·call5(SB), NOSPLIT, $0-0
	MOVD fn+0(FP), R9
	MOVD a1+8(FP), R0
	MOVD a2+16(FP), R1
	MOVD a3+24(FP), R2
	MOVD a4+32(FP), R3
	MOVD a5+40(FP), R4
	CALL R9
	MOVD R0, ret+48(FP)
	RET
