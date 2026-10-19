//go:build windows && amd64

#include "textflag.h"

// abiCallTrampoline for the Windows x64 ABI, entered via runtime.cgocall on g0.
// CX (RCX) points at abiCallFrame.
// Windows x64: args in RCX, RDX, R8, R9, then the stack from RSP+32; return in
// RAX. The callee owns the 32-byte shadow space at RSP+0..31 and may write to
// it, so our saved registers live above the stack arguments.
//
// Frame layout (abiCallFrame): fn(0), a1(8), a2(16), a3(24), a4(32), a5(40),
// a6(48), a7(56), r1(64).

GLOBL ·abiCallABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallABI0(SB)/8, $abiCallTrampoline(SB)

TEXT abiCallTrampoline(SB), NOSPLIT, $80
	MOVQ BX, 56(SP)        // save callee-saved BX above the argument area
	MOVQ R12, 64(SP)       // save callee-saved R12
	MOVQ CX, R12           // frame ptr (Windows: CX)
	MOVQ 0(R12), AX        // fn
	MOVQ 8(R12), CX        // a1 -> RCX
	MOVQ 16(R12), DX       // a2 -> RDX
	MOVQ 24(R12), R8       // a3 -> R8
	MOVQ 32(R12), R9       // a4 -> R9
	MOVQ 40(R12), BX
	MOVQ BX, 32(SP)        // a5: first stack argument after the shadow space
	MOVQ 48(R12), BX
	MOVQ BX, 40(SP)        // a6
	MOVQ 56(R12), BX
	MOVQ BX, 48(SP)        // a7
	CALL AX
	MOVQ AX, 64(R12)       // r1 = return value
	MOVQ 64(SP), R12
	MOVQ 56(SP), BX
	RET
