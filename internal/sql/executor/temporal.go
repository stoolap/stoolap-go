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
package executor

import (
	"fmt"

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// TemporalContext holds AS OF information for temporal queries
type TemporalContext struct {
	Type  string // "TRANSACTION" or "TIMESTAMP"
	Value any    // int64 for TRANSACTION, time.Time for TIMESTAMP
}

// extractTemporalContext extracts AS OF information from a table source
func extractTemporalContext(tableSource *parser.SimpleTableSource) (*TemporalContext, error) {
	if tableSource.AsOf == nil {
		return nil, nil // No temporal context
	}

	// Extract the value based on type
	switch tableSource.AsOf.Type {
	case "TRANSACTION":
		// For AS OF TRANSACTION, expect an integer literal
		switch v := tableSource.AsOf.Value.(type) {
		case *parser.IntegerLiteral:
			return &TemporalContext{
				Type:  "TRANSACTION",
				Value: v.Value,
			}, nil
		default:
			return nil, fmt.Errorf("AS OF TRANSACTION requires an integer value, got %T", v)
		}

	case "TIMESTAMP":
		// For AS OF TIMESTAMP, expect a string literal that can be parsed as timestamp
		switch v := tableSource.AsOf.Value.(type) {
		case *parser.StringLiteral:
			// Use the existing ParseTimestamp function from converters.go
			ts, err := storage.ParseTimestamp(v.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp in AS OF clause: %w", err)
			}
			// Return the parsed time.Time object
			return &TemporalContext{
				Type:  "TIMESTAMP",
				Value: ts,
			}, nil
		default:
			return nil, fmt.Errorf("AS OF TIMESTAMP requires a string value, got %T", v)
		}

	default:
		return nil, fmt.Errorf("unsupported AS OF type: %s", tableSource.AsOf.Type)
	}
}
