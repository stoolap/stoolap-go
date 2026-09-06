module github.com/stoolap/stoolap-go

go 1.24

require (
	github.com/stoolap/stoolap-go/lib/darwin_arm64 v0.4.2
	github.com/stoolap/stoolap-go/lib/linux_amd64 v0.4.2
	github.com/stoolap/stoolap-go/lib/windows_amd64 v0.4.2
)

replace (
	github.com/stoolap/stoolap-go/lib/darwin_arm64 => ./lib/darwin_arm64
	github.com/stoolap/stoolap-go/lib/linux_amd64 => ./lib/linux_amd64
	github.com/stoolap/stoolap-go/lib/windows_amd64 => ./lib/windows_amd64
)

// v0.4.1 on proxy.golang.org predates the fix for locating the bundled
// shared library, so clean installs fail. Use v0.4.2 or later.
retract v0.4.1
