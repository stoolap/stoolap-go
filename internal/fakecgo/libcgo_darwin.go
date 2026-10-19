//go:build !cgo && darwin

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

package fakecgo

type (
	pthread_mutex_t struct {
		sig    int64
		opaque [56]byte
	}
	pthread_cond_t struct {
		sig    int64
		opaque [40]byte
	}
)

var (
	PTHREAD_COND_INITIALIZER  = pthread_cond_t{sig: 0x3CB0B1BB}
	PTHREAD_MUTEX_INITIALIZER = pthread_mutex_t{sig: 0x32AAABA7}
)

const (
	SIG_BLOCK   sighow = 1
	SIG_UNBLOCK sighow = 2
	SIG_SETMASK sighow = 3
)
