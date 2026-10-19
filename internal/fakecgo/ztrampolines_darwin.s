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

//go:build !cgo && darwin

#include "textflag.h"

TEXT _pthread_self(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_self(SB)

TEXT _pthread_get_stacksize_np(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_get_stacksize_np(SB)

TEXT _pthread_attr_setstacksize(SB), NOSPLIT|NOFRAME, $0-0
	JMP stoolap_fakecgo_pthread_attr_setstacksize(SB)
