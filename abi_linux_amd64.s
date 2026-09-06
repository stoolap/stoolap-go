//go:build linux && amd64

#include "textflag.h"

// Same trampolines as abi_amd64.s but compiled only for linux/amd64.
// The Go-side dispatch uses runtime.cgocall instead of asmcgocall
// to preserve FS (TLS) on the g0 stack.

GLOBL ·abiCallABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallABI0(SB)/8, $abiCallTrampoline(SB)

TEXT abiCallTrampoline(SB), NOSPLIT|NOFRAME, $0
	SUBQ $40, SP
	MOVQ BX, 8(SP)
	MOVQ R12, 16(SP)
	MOVQ DI, R12

	MOVQ 0(R12), R10
	MOVQ 8(R12), DI
	MOVQ 16(R12), SI
	MOVQ 24(R12), DX
	MOVQ 32(R12), CX
	MOVQ 40(R12), R8
	MOVQ 48(R12), R9

	XORL AX, AX
	CALL R10

	MOVQ AX, 56(R12)
	MOVQ 16(SP), R12
	MOVQ 8(SP), BX
	ADDQ $40, SP
	RET

GLOBL ·abiCallPtrLenABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallPtrLenABI0(SB)/8, $abiCallPtrLenTrampoline(SB)

TEXT abiCallPtrLenTrampoline(SB), NOSPLIT|NOFRAME, $0
	SUBQ $40, SP
	MOVQ BX, 8(SP)
	MOVQ R12, 16(SP)
	MOVQ DI, R12

	MOVQ 0(R12), R10
	MOVQ 8(R12), DI
	MOVQ 16(R12), SI
	LEAQ 24(R12), DX

	XORL AX, AX
	CALL R10

	MOVQ AX, 32(R12)
	MOVQ 16(SP), R12
	MOVQ 8(SP), BX
	ADDQ $40, SP
	RET

GLOBL ·abiCallFloatABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallFloatABI0(SB)/8, $abiCallFloatTrampoline(SB)

TEXT abiCallFloatTrampoline(SB), NOSPLIT|NOFRAME, $0
	SUBQ $40, SP
	MOVQ BX, 8(SP)
	MOVQ R12, 16(SP)
	MOVQ DI, R12

	MOVQ 0(R12), R10
	MOVQ 8(R12), DI
	MOVQ 16(R12), SI

	XORL AX, AX
	CALL R10

	MOVSD X0, 24(R12)
	MOVQ 16(SP), R12
	MOVQ 8(SP), BX
	ADDQ $40, SP
	RET
