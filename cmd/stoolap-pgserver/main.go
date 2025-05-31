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
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	dbPath   string
	bindAddr string
	verbose  bool
)

var rootCmd = &cobra.Command{
	Use:   "stoolap-pgserver",
	Short: "PostgreSQL wire protocol server for Stoolap",
	Long: `Stoolap PostgreSQL wire protocol server provides PostgreSQL compatibility,
allowing any PostgreSQL client or driver to connect to a Stoolap database.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServer()
	},
}

func init() {
	rootCmd.Flags().StringVarP(&dbPath, "database", "d", "memory://", "Database path (memory:// or file:///path/to/db)")
	rootCmd.Flags().StringVarP(&bindAddr, "bind", "b", ":5432", "Bind address")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
