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
package driver

import (
	"database/sql/driver"

	"github.com/stoolap/stoolap-go/internal/cs"
)

// execResult implements driver.Result.
type execResult int64

func (r execResult) LastInsertId() (int64, error) { return 0, nil }
func (r execResult) RowsAffected() (int64, error) { return int64(r), nil }

// acquireValues acquires a pooled ValueBuf, fills it from args, and returns it.
// Caller must call vb.Release() after the values have been consumed.
func acquireValues(args []driver.NamedValue) (*cs.ValueBuf, error) {
	vb := cs.AcquireValueBuf(len(args))
	for i, arg := range args {
		v, err := cs.GoValueToFFI(arg.Value)
		if err != nil {
			vb.Release()
			return nil, err
		}
		vb.Values[i] = v
	}
	return vb, nil
}

// valuesToNamed converts old-style driver.Value to driver.NamedValue.
func valuesToNamed(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return named
}
