//go:build darwin && arm64

#include "textflag.h"

// abiCallTrampoline is entered via runtime.cgocall on g0.
// R0 points at abiCallFrame; we marshal up to 7 integer/pointer args into the
// AAPCS64 registers R0-R6, call the foreign function, and store the integer
// return value back into frame.r1.
//
// Frame layout (abiCallFrame): fn(0), a1(8), a2(16), a3(24), a4(32), a5(40),
// a6(48), a7(56), r1(64).

GLOBL ·abiCallABI0(SB), NOPTR|RODATA, $8
DATA ·abiCallABI0(SB)/8, $abiCallTrampoline(SB)

TEXT abiCallTrampoline(SB), NOSPLIT, $16
	STP  (R19, R20), 16(RSP)
	MOVD R0, R19
	MOVD RSP, R20
	// The Go prologue saved the caller's FP at -8(RSP). Drop below it so the
	// callee's frame cannot overwrite that slot; R20 restores RSP afterwards.
	SUB  $16, RSP
	MOVD 0(R19), R12
	MOVD 8(R19), R0
	MOVD 16(R19), R1
	MOVD 24(R19), R2
	MOVD 32(R19), R3
	MOVD 40(R19), R4
	MOVD 48(R19), R5
	MOVD 56(R19), R6
	BL   (R12)
	MOVD R20, RSP
	MOVD R0, 64(R19)
	LDP  16(RSP), (R19, R20)
	RET
