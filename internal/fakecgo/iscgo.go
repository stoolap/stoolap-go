//go:build !cgo && (linux || darwin)

package fakecgo

import _ "unsafe"

// Tell the runtime that cgo-style initialization is present, even though the
// hooks are implemented entirely in Go.
//
//go:linkname _iscgo runtime.iscgo
var _iscgo bool = true
