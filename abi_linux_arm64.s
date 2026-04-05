//go:build linux && arm64

#include "textflag.h"

// abiCallTrampoline is entered via runtime.asmcgocall on g0.
// R0 points at abiCallFrame; we marshal up to 5 integer/pointer args into the
// platform ABI registers, call the foreign function, and store the integer
// return value back into frame.r1.

GLOBL ·abiCallABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallABI0(SB)/8, $abiCallTrampoline(SB)

TEXT abiCallTrampoline(SB), NOSPLIT, $16
	STP  (R19, R20), 16(RSP)
	MOVD R0, R19
	MOVD RSP, R20
	SUB  $16, RSP

	MOVD 0(R19), R12
	MOVD 8(R19), R0
	MOVD 16(R19), R1
	MOVD 24(R19), R2
	MOVD 32(R19), R3
	MOVD 40(R19), R4
	BL   (R12)

	MOVD R20, RSP
	MOVD R0, 48(R19)
	LDP  16(RSP), (R19, R20)
	RET

// abiCallPtrLenTrampoline: specialized for fn(a1, a2, &out_len).
// Frame layout (abiCallPtrLenFrame): fn(0), a1(8), a2(16), outLen(24), r1(32).
// Passes &frame.outLen (R19+24) as arg3 instead of a value.
GLOBL ·abiCallPtrLenABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallPtrLenABI0(SB)/8, $abiCallPtrLenTrampoline(SB)

TEXT abiCallPtrLenTrampoline(SB), NOSPLIT, $16
	STP  (R19, R20), 16(RSP)
	MOVD R0, R19
	MOVD RSP, R20
	SUB  $16, RSP

	MOVD 0(R19), R12       // fn
	MOVD 8(R19), R0        // rows ptr
	MOVD 16(R19), R1       // column index
	ADD  $24, R19, R2      // &frame.outLen (address of outLen field)
	BL   (R12)

	MOVD R20, RSP
	MOVD R0, 32(R19)       // r1 = char pointer
	LDP  16(RSP), (R19, R20)
	RET

// abiCallFloatTrampoline captures a float64 return value from F0.
// Frame layout (abiCallFloatFrame): fn(0), a1(8), a2(16), r1(24).
GLOBL ·abiCallFloatABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallFloatABI0(SB)/8, $abiCallFloatTrampoline(SB)

TEXT abiCallFloatTrampoline(SB), NOSPLIT, $16
	STP  (R19, R20), 16(RSP)
	MOVD R0, R19
	MOVD RSP, R20
	SUB  $16, RSP

	MOVD 0(R19), R12
	MOVD 8(R19), R0
	MOVD 16(R19), R1
	BL   (R12)

	MOVD R20, RSP
	FMOVD F0, 24(R19)
	LDP   16(RSP), (R19, R20)
	RET
