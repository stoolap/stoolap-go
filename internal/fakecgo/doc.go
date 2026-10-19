//go:build !cgo && (linux || darwin)

// Package fakecgo installs the minimal runtime/cgo hooks needed for linux/amd64
// no-cgo builds to keep the C TLS model intact.
package fakecgo
