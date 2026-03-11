module github.com/stoolap/stoolap-go

go 1.24

require (
	github.com/stoolap/stoolap-go/lib/darwin_arm64 v0.3.5
	github.com/stoolap/stoolap-go/lib/linux_amd64 v0.3.5
	github.com/stoolap/stoolap-go/lib/windows_amd64 v0.3.5
)

replace (
	github.com/stoolap/stoolap-go/lib/darwin_arm64 => ./lib/darwin_arm64
	github.com/stoolap/stoolap-go/lib/linux_amd64 => ./lib/linux_amd64
	github.com/stoolap/stoolap-go/lib/windows_amd64 => ./lib/windows_amd64
)
