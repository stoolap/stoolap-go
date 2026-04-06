//go:build linux && amd64 && !cgo

package stoolap

// Import the minimal fake-cgo runtime glue so Go keeps pthread TLS intact on
// linux/amd64 no-cgo builds. Without this, glibc's dlopen/malloc path runs with
// Go TLS in %fs and crashes before the library is even loaded.
import _ "github.com/stoolap/stoolap-go/internal/fakecgo"
