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

// Package stoolaplib carries the prebuilt libstoolap.so for linux/amd64.
// Importing it makes the Go tool download this module, and Dir tells the
// driver where the library file lives.
package stoolaplib

import (
	"path/filepath"
	"runtime"
)

// Dir is the directory holding libstoolap.so. It is empty when the build
// used -trimpath, in which case the driver searches the module cache instead.
var Dir string

func init() {
	if _, file, _, ok := runtime.Caller(0); ok && filepath.IsAbs(file) {
		Dir = filepath.Dir(file)
	}
}
