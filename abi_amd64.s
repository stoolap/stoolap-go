//go:build darwin && amd64

#include "textflag.h"

// abiCallTrampoline is entered via runtime.asmcgocall on g0.
// DI (RDI) points at abiCallFrame; we marshal up to 6 integer/pointer args
// into the System V AMD64 ABI registers, call the foreign function, and store
// the integer return value back into frame.r1.
//
// System V AMD64 ABI: args in RDI, RSI, RDX, RCX, R8, R9; return in RAX.
// Callee-saved: RBX, RBP, R12-R15.

GLOBL ·abiCallABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallABI0(SB)/8, $abiCallTrampoline(SB)

// Entered from runtime.asmcgocall via a JMP-based landing pad, so SP arrives
// with the return address already on the stack (8 mod 16). Adjust manually
// instead of relying on frame sizing so the outbound foreign CALL is made with
// 16-byte aligned SP on linux/amd64.
TEXT abiCallTrampoline(SB), NOSPLIT|NOFRAME, $0
	SUBQ $40, SP
	MOVQ BX, 8(SP)         // save callee-saved BX
	MOVQ R12, 16(SP)       // save callee-saved R12
	MOVQ DI, R12           // save frame ptr in callee-saved R12

	MOVQ 0(R12), R10       // fn
	MOVQ 8(R12), DI        // a1 -> RDI
	MOVQ 16(R12), SI       // a2 -> RSI
	MOVQ 24(R12), DX       // a3 -> RDX
	MOVQ 32(R12), CX       // a4 -> RCX
	MOVQ 40(R12), R8       // a5 -> R8

	XORL AX, AX            // no vector registers used
	CALL R10

	MOVQ AX, 56(R12)       // r1 = return value
	MOVQ 16(SP), R12       // restore R12
	MOVQ 8(SP), BX         // restore BX
	ADDQ $40, SP
	RET

// abiCallPtrLenTrampoline: specialized for fn(a1, a2, &out_len).
// Frame layout (abiCallPtrLenFrame): fn(0), a1(8), a2(16), outLen(24), r1(32).
// Passes &frame.outLen (R12+24) as arg3 (RDX) instead of a value.
GLOBL ·abiCallPtrLenABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallPtrLenABI0(SB)/8, $abiCallPtrLenTrampoline(SB)

TEXT abiCallPtrLenTrampoline(SB), NOSPLIT|NOFRAME, $0
	SUBQ $40, SP
	MOVQ BX, 8(SP)         // save callee-saved BX
	MOVQ R12, 16(SP)       // save callee-saved R12
	MOVQ DI, R12           // save frame ptr

	MOVQ 0(R12), R10       // fn
	MOVQ 8(R12), DI        // a1 -> RDI
	MOVQ 16(R12), SI       // a2 -> RSI
	LEAQ 24(R12), DX       // &outLen -> RDX (arg3)

	XORL AX, AX            // no vector registers used
	CALL R10

	MOVQ AX, 32(R12)       // r1 = char pointer
	MOVQ 16(SP), R12       // restore R12
	MOVQ 8(SP), BX         // restore BX
	ADDQ $40, SP
	RET

// abiCallFloatTrampoline captures a float64 return value from XMM0 (X0).
// Frame layout (abiCallFloatFrame): fn(0), a1(8), a2(16), r1(24).
GLOBL ·abiCallFloatABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallFloatABI0(SB)/8, $abiCallFloatTrampoline(SB)

TEXT abiCallFloatTrampoline(SB), NOSPLIT|NOFRAME, $0
	SUBQ $40, SP
	MOVQ BX, 8(SP)         // save callee-saved BX
	MOVQ R12, 16(SP)       // save callee-saved R12
	MOVQ DI, R12           // save frame ptr

	MOVQ 0(R12), R10       // fn
	MOVQ 8(R12), DI        // a1 -> RDI
	MOVQ 16(R12), SI       // a2 -> RSI

	XORL AX, AX            // no vector registers used
	CALL R10

	MOVSD X0, 24(R12)      // store float64 from XMM0
	MOVQ 16(SP), R12       // restore R12
	MOVQ 8(SP), BX         // restore BX
	ADDQ $40, SP
	RET
