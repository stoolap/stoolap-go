module github.com/stoolap/stoolap-go/example/wasm_benchmark

go 1.24.0

require (
	github.com/ncruces/go-sqlite3 v0.32.0
	github.com/stoolap/stoolap-go/wasm v0.0.0
	github.com/stoolap/stoolap-go/wasm2go v0.0.0
)

require (
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/tetratelabs/wazero v1.11.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
)

replace (
	github.com/stoolap/stoolap-go/wasm => ../../wasm
	github.com/stoolap/stoolap-go/wasm2go => ../../wasm2go
)
