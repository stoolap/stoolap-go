//go:build windows && amd64

#include "textflag.h"

// abiCallTrampoline for Windows x64 ABI.
// CX (RCX) points at abiCallFrame from runtime.asmcgocall.
// Windows x64: args in RCX, RDX, R8, R9; return in RAX; float in XMM0.
// 32-byte shadow space required before CALL.

GLOBL ·abiCallABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallABI0(SB)/8, $abiCallTrampoline(SB)

TEXT abiCallTrampoline(SB), NOSPLIT, $64
	MOVQ BX, 8(SP)
	MOVQ R12, 16(SP)
	MOVQ CX, R12           // frame ptr from asmcgocall (Windows: CX)

	MOVQ 0(R12), AX        // fn
	MOVQ 8(R12), CX        // a1 -> RCX
	MOVQ 16(R12), DX       // a2 -> RDX
	MOVQ 24(R12), R8       // a3 -> R8
	MOVQ 32(R12), R9       // a4 -> R9
	// a5 goes on stack (5th arg)
	MOVQ 40(R12), BX
	MOVQ BX, 32(SP)        // shadow space + 5th arg at RSP+32

	CALL AX

	MOVQ AX, 48(R12)       // r1 = return value
	MOVQ 16(SP), R12
	MOVQ 8(SP), BX
	RET

// abiCallPtrLenTrampoline for Windows x64 ABI.
// Frame layout (abiCallPtrLenFrame): fn(0), a1(8), a2(16), outLen(24), r1(32).
GLOBL ·abiCallPtrLenABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallPtrLenABI0(SB)/8, $abiCallPtrLenTrampoline(SB)

TEXT abiCallPtrLenTrampoline(SB), NOSPLIT, $64
	MOVQ BX, 8(SP)
	MOVQ R12, 16(SP)
	MOVQ CX, R12           // frame ptr (Windows: CX)

	MOVQ 0(R12), AX        // fn
	MOVQ 8(R12), CX        // a1 -> RCX
	MOVQ 16(R12), DX       // a2 -> RDX
	LEAQ 24(R12), R8       // &outLen -> R8 (arg3)

	CALL AX

	MOVQ AX, 32(R12)       // r1 = char pointer
	MOVQ 16(SP), R12
	MOVQ 8(SP), BX
	RET

// abiCallFloatTrampoline captures float64 from XMM0 (Windows x64).
// Frame layout (abiCallFloatFrame): fn(0), a1(8), a2(16), r1(24).
GLOBL ·abiCallFloatABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallFloatABI0(SB)/8, $abiCallFloatTrampoline(SB)

TEXT abiCallFloatTrampoline(SB), NOSPLIT, $64
	MOVQ BX, 8(SP)
	MOVQ R12, 16(SP)
	MOVQ CX, R12           // frame ptr (Windows: CX)

	MOVQ 0(R12), AX        // fn
	MOVQ 8(R12), CX        // a1 -> RCX
	MOVQ 16(R12), DX       // a2 -> RDX

	CALL AX

	MOVSD X0, 24(R12)      // float64 from XMM0
	MOVQ 16(SP), R12
	MOVQ 8(SP), BX
	RET
