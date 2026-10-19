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
#include "abi_arm64.h"

// crosscall2 is entered from foreign code; it saves the callee-saved
// registers and hands fn, arg and ctxt to runtime.cgocallback.
TEXT crosscall2(SB), NOSPLIT|NOFRAME, $0
	SUB  $(8*24), RSP
	STP  (R0, R1), (8*1)(RSP)
	MOVD R3, (8*3)(RSP)

	SAVE_R19_TO_R28(8*4)
	SAVE_F8_TO_F15(8*14)
	STP (R29, R30), (8*22)(RSP)

	BL runtime·cgocallback(SB)

	RESTORE_R19_TO_R28(8*4)
	RESTORE_F8_TO_F15(8*14)
	LDP (8*22)(RSP), (R29, R30)

	ADD $(8*24), RSP
	RET
