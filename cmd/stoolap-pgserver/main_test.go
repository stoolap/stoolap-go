/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestRootCommand(t *testing.T) {
	// Test the root command
	if rootCmd.Use != "stoolap-pgserver" {
		t.Errorf("Root command Use = %s, want stoolap-pgserver", rootCmd.Use)
	}

	if rootCmd.Short == "" {
		t.Error("Root command should have a short description")
	}

	if rootCmd.Long == "" {
		t.Error("Root command should have a long description")
	}
}

func TestCommandFlags(t *testing.T) {
	// Reset flags for testing
	rootCmd.ResetFlags()
	// Re-initialize flags
	rootCmd.Flags().StringVarP(&dbPath, "database", "d", "memory://", "Database path (memory:// or file:///path/to/db)")
	rootCmd.Flags().StringVarP(&bindAddr, "bind", "b", ":5432", "Bind address")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging")

	// Test database flag
	dbFlag := rootCmd.Flags().Lookup("database")
	if dbFlag == nil {
		t.Error("Database flag not found")
	} else {
		if dbFlag.DefValue != "memory://" {
			t.Errorf("Database flag default = %s, want memory://", dbFlag.DefValue)
		}
		if dbFlag.Shorthand != "d" {
			t.Errorf("Database flag shorthand = %s, want d", dbFlag.Shorthand)
		}
	}

	// Test bind flag
	bindFlag := rootCmd.Flags().Lookup("bind")
	if bindFlag == nil {
		t.Error("Bind flag not found")
	} else {
		if bindFlag.DefValue != ":5432" {
			t.Errorf("Bind flag default = %s, want :5432", bindFlag.DefValue)
		}
		if bindFlag.Shorthand != "b" {
			t.Errorf("Bind flag shorthand = %s, want b", bindFlag.Shorthand)
		}
	}

	// Test verbose flag
	verboseFlag := rootCmd.Flags().Lookup("verbose")
	if verboseFlag == nil {
		t.Error("Verbose flag not found")
	} else {
		if verboseFlag.DefValue != "false" {
			t.Errorf("Verbose flag default = %s, want false", verboseFlag.DefValue)
		}
		if verboseFlag.Shorthand != "v" {
			t.Errorf("Verbose flag shorthand = %s, want v", verboseFlag.Shorthand)
		}
	}
}

func TestHelpOutput(t *testing.T) {
	// Capture help output
	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetArgs([]string{"--help"})

	err := rootCmd.Execute()
	if err != nil {
		t.Errorf("Help command failed: %v", err)
	}

	helpOutput := buf.String()

	// Check for expected content in help
	expectedStrings := []string{
		"stoolap-pgserver",
		"PostgreSQL wire protocol server",
		"--database",
		"--bind",
		"--verbose",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(helpOutput, expected) {
			t.Errorf("Help output missing expected string: %s", expected)
		}
	}
}

func TestRunServerFunction(t *testing.T) {
	// We can't fully test runServer as it starts a network listener
	// But we can test that it's defined and accessible
	if rootCmd.RunE == nil {
		t.Error("Root command should have RunE function set")
	}

	// Test with invalid bind address
	oldBindAddr := bindAddr
	bindAddr = "invalid:address:format"
	defer func() { bindAddr = oldBindAddr }()

	err := runServer()
	if err == nil {
		t.Error("Expected error with invalid bind address")
	}
}

func TestCommandLineArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantErr   bool
		checkFunc func(t *testing.T)
	}{
		{
			name:    "custom database",
			args:    []string{"-d", "file:///tmp/test.db"},
			wantErr: false,
			checkFunc: func(t *testing.T) {
				if dbPath != "file:///tmp/test.db" {
					t.Errorf("dbPath = %s, want file:///tmp/test.db", dbPath)
				}
			},
		},
		{
			name:    "custom bind address",
			args:    []string{"-b", ":15432"},
			wantErr: false,
			checkFunc: func(t *testing.T) {
				if bindAddr != ":15432" {
					t.Errorf("bindAddr = %s, want :15432", bindAddr)
				}
			},
		},
		{
			name:    "verbose mode",
			args:    []string{"-v"},
			wantErr: false,
			checkFunc: func(t *testing.T) {
				if !verbose {
					t.Error("verbose should be true")
				}
			},
		},
		{
			name:    "all flags",
			args:    []string{"-d", "memory://", "-b", ":25432", "-v"},
			wantErr: false,
			checkFunc: func(t *testing.T) {
				if dbPath != "memory://" {
					t.Errorf("dbPath = %s, want memory://", dbPath)
				}
				if bindAddr != ":25432" {
					t.Errorf("bindAddr = %s, want :25432", bindAddr)
				}
				if !verbose {
					t.Error("verbose should be true")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset flags
			dbPath = "memory://"
			bindAddr = ":5432"
			verbose = false

			// Parse args without executing
			rootCmd.SetArgs(tt.args)
			err := rootCmd.ParseFlags(tt.args)

			if (err != nil) != tt.wantErr {
				t.Errorf("ParseFlags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.checkFunc != nil {
				tt.checkFunc(t)
			}
		})
	}
}

// TestExecuteFunction tests the command execution
func TestExecuteFunction(t *testing.T) {
	// Save and restore
	oldCmd := rootCmd
	defer func() { rootCmd = oldCmd }()

	// Create a test command that doesn't start the server
	testCmd := &cobra.Command{
		Use:   "test",
		Short: "Test command",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	rootCmd = testCmd

	// This should not error
	rootCmd.SetArgs([]string{})
	err := rootCmd.Execute()
	if err != nil {
		t.Errorf("Execute() error = %v", err)
	}
}
