//go:build darwin && amd64

#include "textflag.h"

// abiCallTrampoline is entered via runtime.cgocall on g0.
// DI (RDI) points at abiCallFrame; we marshal up to 7 integer/pointer args
// into the System V AMD64 ABI registers (the 7th goes on the stack), call the
// foreign function, and store the integer return value back into frame.r1.
//
// System V AMD64 ABI: args in RDI, RSI, RDX, RCX, R8, R9, then the stack;
// return in RAX. Callee-saved: RBX, RBP, R12-R15.
//
// Frame layout (abiCallFrame): fn(0), a1(8), a2(16), a3(24), a4(32), a5(40),
// a6(48), a7(56), r1(64).

GLOBL ·abiCallABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallABI0(SB)/8, $abiCallTrampoline(SB)

// SP arrives with the return address already on the stack (8 mod 16). Adjust
// manually instead of relying on frame sizing so the outbound foreign CALL is
// made with 16-byte aligned SP.
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
	MOVQ 48(R12), R9       // a6 -> R9
	MOVQ 56(R12), AX
	MOVQ AX, 0(SP)         // a7 -> first stack argument
	XORL AX, AX            // no vector registers used
	CALL R10
	MOVQ AX, 64(R12)       // r1 = return value
	MOVQ 16(SP), R12       // restore R12
	MOVQ 8(SP), BX         // restore BX
	ADDQ $40, SP
	RET
