module github.com/stoolap/stoolap-go/cmd/stoolap

go 1.23.0

require (
	github.com/chzyer/readline v1.5.1
	github.com/jedib0t/go-pretty/v6 v6.6.7
	github.com/spf13/cobra v1.9.1
	github.com/stoolap/stoolap-go v0.1.2
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)

replace github.com/stoolap/stoolap-go => ../..
