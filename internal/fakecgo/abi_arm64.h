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

// Macros for saving and restoring the callee-saved registers of the AAPCS64
// ABI around a call into Go from a foreign thread.

#define SAVE_R19_TO_R28(offset) \
	STP	(R19, R20), ((offset)+0*8)(RSP) \
	STP	(R21, R22), ((offset)+2*8)(RSP) \
	STP	(R23, R24), ((offset)+4*8)(RSP) \
	STP	(R25, R26), ((offset)+6*8)(RSP) \
	STP	(R27, g), ((offset)+8*8)(RSP)
#define RESTORE_R19_TO_R28(offset) \
	LDP	((offset)+0*8)(RSP), (R19, R20) \
	LDP	((offset)+2*8)(RSP), (R21, R22) \
	LDP	((offset)+4*8)(RSP), (R23, R24) \
	LDP	((offset)+6*8)(RSP), (R25, R26) \
	LDP	((offset)+8*8)(RSP), (R27, g) /* R28 */
#define SAVE_F8_TO_F15(offset) \
	FSTPD	(F8, F9), ((offset)+0*8)(RSP) \
	FSTPD	(F10, F11), ((offset)+2*8)(RSP) \
	FSTPD	(F12, F13), ((offset)+4*8)(RSP) \
	FSTPD	(F14, F15), ((offset)+6*8)(RSP)
#define RESTORE_F8_TO_F15(offset) \
	FLDPD	((offset)+0*8)(RSP), (F8, F9) \
	FLDPD	((offset)+2*8)(RSP), (F10, F11) \
	FLDPD	((offset)+4*8)(RSP), (F12, F13) \
	FLDPD	((offset)+6*8)(RSP), (F14, F15)
