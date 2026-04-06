// Copyright 2021 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifdef GOOS_windows

#define REGS_HOST_TO_ABI0_STACK (28*8 + 8)

#define PUSH_REGS_HOST_TO_ABI0()	\
	PUSHFQ			\
	CLD			\
	ADJSP	$(REGS_HOST_TO_ABI0_STACK - 8)	\
	MOVQ	DI, (0*0)(SP)	\
	MOVQ	SI, (1*8)(SP)	\
	MOVQ	BP, (2*8)(SP)	\
	MOVQ	BX, (3*8)(SP)	\
	MOVQ	R12, (4*8)(SP)	\
	MOVQ	R13, (5*8)(SP)	\
	MOVQ	R14, (6*8)(SP)	\
	MOVQ	R15, (7*8)(SP)	\
	MOVUPS	X6, (8*8)(SP)	\
	MOVUPS	X7, (10*8)(SP)	\
	MOVUPS	X8, (12*8)(SP)	\
	MOVUPS	X9, (14*8)(SP)	\
	MOVUPS	X10, (16*8)(SP)	\
	MOVUPS	X11, (18*8)(SP)	\
	MOVUPS	X12, (20*8)(SP)	\
	MOVUPS	X13, (22*8)(SP)	\
	MOVUPS	X14, (24*8)(SP)	\
	MOVUPS	X15, (26*8)(SP)

#define POP_REGS_HOST_TO_ABI0()	\
	MOVQ	(0*0)(SP), DI	\
	MOVQ	(1*8)(SP), SI	\
	MOVQ	(2*8)(SP), BP	\
	MOVQ	(3*8)(SP), BX	\
	MOVQ	(4*8)(SP), R12	\
	MOVQ	(5*8)(SP), R13	\
	MOVQ	(6*8)(SP), R14	\
	MOVQ	(7*8)(SP), R15	\
	MOVUPS	(8*8)(SP), X6	\
	MOVUPS	(10*8)(SP), X7	\
	MOVUPS	(12*8)(SP), X8	\
	MOVUPS	(14*8)(SP), X9	\
	MOVUPS	(16*8)(SP), X10	\
	MOVUPS	(18*8)(SP), X11	\
	MOVUPS	(20*8)(SP), X12	\
	MOVUPS	(22*8)(SP), X13	\
	MOVUPS	(24*8)(SP), X14	\
	MOVUPS	(26*8)(SP), X15	\
	ADJSP	$-(REGS_HOST_TO_ABI0_STACK - 8)	\
	POPFQ

#else

#define REGS_HOST_TO_ABI0_STACK (6*8)

#define PUSH_REGS_HOST_TO_ABI0()	\
	ADJSP	$(REGS_HOST_TO_ABI0_STACK)	\
	MOVQ	BP, (5*8)(SP)	\
	LEAQ	(5*8)(SP), BP	\
	MOVQ	BX, (0*8)(SP)	\
	MOVQ	R12, (1*8)(SP)	\
	MOVQ	R13, (2*8)(SP)	\
	MOVQ	R14, (3*8)(SP)	\
	MOVQ	R15, (4*8)(SP)

#define POP_REGS_HOST_TO_ABI0()	\
	MOVQ	(0*8)(SP), BX	\
	MOVQ	(1*8)(SP), R12	\
	MOVQ	(2*8)(SP), R13	\
	MOVQ	(3*8)(SP), R14	\
	MOVQ	(4*8)(SP), R15	\
	MOVQ	(5*8)(SP), BP	\
	ADJSP	$-(REGS_HOST_TO_ABI0_STACK)

#endif
