module github.com/stoolap/stoolap-go/example/wasm_benchmark

go 1.26.0

require (
	github.com/ncruces/go-sqlite3 v0.35.6
	github.com/stoolap/stoolap-go/wasm v0.0.0
)

require (
	github.com/ncruces/go-sqlite3-wasm/v6 v6.3.35304 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/tetratelabs/wazero v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/stoolap/stoolap-go/wasm => ../../wasm
