---
title: Installation Guide
category: Getting Started
order: 1
---

# Installation Guide

This guide walks you through the process of installing Stoolap on different platforms and environments.

## Prerequisites

- Go 1.23 or later
- Git (for installation from source)
- Basic familiarity with command line tools

## Installation Methods

### Method 1: Using Go Get (Recommended)

The easiest way to install Stoolap is via Go's package management:

```bash
go get github.com/stoolap/stoolap
```

This command downloads the source code, compiles it, and installs the binary into your `$GOPATH/bin` directory.

### Method 2: Building from Source

If you need the latest features or want to make modifications:

```bash
# Clone the repository
git clone https://github.com/stoolap/stoolap.git

# Navigate to the directory
cd stoolap

# Build the project
go build -o stoolap ./cmd/stoolap

# Optional: Install the binary to $GOPATH/bin
go install ./cmd/stoolap
```

## Platform-Specific Instructions

### macOS

On macOS, you can use either the Go method or build from source as described above.

```bash
# Make the binary executable if needed
chmod +x ./stoolap

# Optionally move to a directory in your PATH
sudo mv ./stoolap /usr/local/bin/
```

### Linux

For Linux users, after downloading or building the binary:

```bash
# Make the binary executable
chmod +x ./stoolap

# Optionally move to a directory in your PATH
sudo mv ./stoolap /usr/local/bin/
```

### Windows

On Windows:

1. Build from source as described above
2. Place the executable in a suitable location, such as `C:\Program Files\Stoolap`
3. Add the directory to your PATH through System Properties > Advanced > Environment Variables

## Using Stoolap as a Library

To use Stoolap in your Go application, you can import it directly:

```bash
go get github.com/stoolap/stoolap
```

Then use it in your code:

```go
import "github.com/stoolap/stoolap"

func main() {
    db, err := stoolap.Open("memory://")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Use the database...
}
```

See the [API Reference](api-reference) for complete documentation of the Stoolap API.

## Verifying Installation

To verify that Stoolap CLI was installed correctly:

```bash
stoolap --version
```

This should display the version number of your Stoolap installation.

## Next Steps

After installing Stoolap, you can:

- Follow the [Quick Start Tutorial](quickstart) to create your first database using the CLI
- Learn about [Connection Strings](connection-strings) to configure your database
- Check the [API Reference](api-reference) for using Stoolap in your Go applications
- Check the [SQL Commands](sql-commands) reference for working with data

## Troubleshooting

If you encounter issues during installation:

- Ensure your Go version is 1.18 or higher with `go version`
- Check your `$GOPATH` is correctly set
- For permission issues on Linux/macOS, use `sudo` as needed

If problems persist, please [open an issue](https://github.com/stoolap/stoolap/issues) on GitHub with details about your environment and the error you're experiencing.
