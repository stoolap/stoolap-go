module github.com/stoolap/stoolap-go/example/benchmark

go 1.24

require (
	github.com/mattn/go-sqlite3 v1.14.34
	github.com/stoolap/stoolap-go v0.3.5
)

require (
	github.com/stoolap/stoolap-go/lib/darwin_arm64 v0.3.5 // indirect
	github.com/stoolap/stoolap-go/lib/linux_amd64 v0.3.5 // indirect
	github.com/stoolap/stoolap-go/lib/windows_amd64 v0.3.5 // indirect
)

replace (
	github.com/stoolap/stoolap-go => ../..
	github.com/stoolap/stoolap-go/lib/darwin_arm64 => ../../lib/darwin_arm64
	github.com/stoolap/stoolap-go/lib/linux_amd64 => ../../lib/linux_amd64
	github.com/stoolap/stoolap-go/lib/windows_amd64 => ../../lib/windows_amd64
)
