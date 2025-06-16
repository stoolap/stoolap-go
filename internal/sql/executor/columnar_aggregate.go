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

	"github.com/stoolap/stoolap/internal/storage"
)

// OptimizedAggregateOnColumnar performs aggregation using columnar operations
func OptimizedAggregateOnColumnar(cr *ColumnarResult, functions []*SqlFunction) (storage.Result, error) {
	// For simple aggregations without GROUP BY, we can use columnar operations
	resultValues := make([]interface{}, len(functions))

	for i, fn := range functions {
		switch fn.Name {
		case "COUNT":
			if fn.Column == "*" {
				resultValues[i] = cr.CountColumnar()
			} else if fn.IsDistinct {
				count, err := cr.DistinctCountColumnar(fn.Column)
				if err != nil {
					return nil, err
				}
				resultValues[i] = count
			} else {
				count, err := cr.CountNonNull(fn.Column)
				if err != nil {
					return nil, err
				}
				resultValues[i] = count
			}
		case "MIN":
			min, err := cr.MinColumnar(fn.Column)
			if err != nil {
				return nil, err
			}
			resultValues[i] = min.AsInterface()
		case "MAX":
			max, err := cr.MaxColumnar(fn.Column)
			if err != nil {
				return nil, err
			}
			resultValues[i] = max.AsInterface()
		case "SUM":
			sum, err := cr.SumColumnar(fn.Column)
			if err != nil {
				return nil, err
			}
			resultValues[i] = sum.AsInterface()
		case "AVG":
			avg, err := cr.AvgColumnar(fn.Column)
			if err != nil {
				return nil, err
			}
			resultValues[i] = avg.AsInterface()
		default:
			return nil, fmt.Errorf("unsupported aggregate function for columnar: %s", fn.Name)
		}
	}

	// Create result columns
	resultColumns := make([]string, len(functions))
	for i, fn := range functions {
		resultColumns[i] = fn.GetColumnName()
	}

	// Return a single-row result
	return &ExecResult{
		columns:  resultColumns,
		rows:     [][]interface{}{resultValues},
		isMemory: true,
	}, nil
}
