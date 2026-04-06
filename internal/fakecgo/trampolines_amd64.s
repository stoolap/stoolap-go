//go:build linux && amd64 && !cgo

#include "textflag.h"
#include "go_asm.h"
#include "abi_amd64.h"

TEXT x_cgo_init_trampoline(SB), NOSPLIT, $16
	MOVQ DI, AX
	MOVQ SI, BX
	MOVQ ·x_cgo_init_call(SB), R11
	MOVQ (R11), R11
	CALL R11
	RET

TEXT x_cgo_thread_start_trampoline(SB), NOSPLIT, $8
	MOVQ DI, AX
	MOVQ ·x_cgo_thread_start_call(SB), R11
	MOVQ (R11), R11
	CALL R11
	RET

TEXT x_cgo_setenv_trampoline(SB), NOSPLIT, $8
	MOVQ DI, AX
	MOVQ ·x_cgo_setenv_call(SB), R11
	MOVQ (R11), R11
	CALL R11
	RET

TEXT x_cgo_unsetenv_trampoline(SB), NOSPLIT, $8
	MOVQ DI, AX
	MOVQ ·x_cgo_unsetenv_call(SB), R11
	MOVQ (R11), R11
	CALL R11
	RET

TEXT x_cgo_notify_runtime_init_done_trampoline(SB), NOSPLIT, $0
	JMP ·x_cgo_notify_runtime_init_done(SB)

TEXT x_cgo_bindm_trampoline(SB), NOSPLIT, $0
	JMP ·x_cgo_bindm(SB)

TEXT ·setg_trampoline(SB), NOSPLIT, $0-16
	MOVQ G+8(FP), DI
	MOVQ setg+0(FP), R11
	XORL AX, AX
	CALL R11
	RET

TEXT threadentry_trampoline(SB), NOSPLIT, $0
	PUSH_REGS_HOST_TO_ABI0()

	PXOR X15, X15

	MOVQ DI, AX
	MOVQ ·threadentry_call(SB), R11
	MOVQ (R11), R11
	CALL R11

	POP_REGS_HOST_TO_ABI0()
	RET

TEXT ·call5(SB), NOSPLIT, $0-56
	MOVQ fn+0(FP), R11
	MOVQ a1+8(FP), DI
	MOVQ a2+16(FP), SI
	MOVQ a3+24(FP), DX
	MOVQ a4+32(FP), CX
	MOVQ a5+40(FP), R8

	XORL AX, AX

	PUSHQ BP
	MOVQ SP, BP
	SUBQ $16, SP
	ANDQ $-16, SP

	CALL R11

	MOVQ BP, SP
	POPQ BP

	MOVQ AX, ret+48(FP)
	RET
