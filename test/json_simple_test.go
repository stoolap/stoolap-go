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
package test

import (
	"encoding/json"
	"testing"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// TestJSONRegexMatching tests the regex patterns for JSON
func TestJSONRegexMatching(t *testing.T) {
	validJSONs := []string{
		`{"name":"John","age":30}`,
		`[1,2,3,4]`,
		`{"user":{"name":"John","age":30}}`,
		`[{"name":"John"},{"name":"Jane"}]`,
		`{"users":[{"name":"John"},{"name":"Jane"}]}`,
		`[]`,
		`{}`,
		`{"":""}`,
		`{"escaped":"quote"}`,
	}

	invalidJSONs := []string{
		`{name:"John"}`,        // Missing quotes around property name
		`{"name":"John"`,       // Missing closing brace
		`{"name":"John",}`,     // Trailing comma
		`{"name":John}`,        // Missing quotes around string value
		`{name}`,               // Invalid format
		`[1,2,3,}`,             // Mismatched brackets
		`{]`,                   // Mismatched brackets
		`{name:"John",age:30}`, // Missing quotes around property names
	}

	for _, jsonStr := range validJSONs {
		var result interface{}
		err := json.Unmarshal([]byte(jsonStr), &result)
		if err != nil {
			t.Errorf("Valid JSON failed to parse: %s, Error: %v", jsonStr, err)
		}
	}

	for _, jsonStr := range invalidJSONs {
		var result interface{}
		err := json.Unmarshal([]byte(jsonStr), &result)
		if err == nil {
			t.Errorf("Invalid JSON unexpectedly parsed: %s", jsonStr)
		}
	}
}

// TestJSONStorageValue tests converting between JSON and storage values
func TestJSONStorageValue(t *testing.T) {
	// Test with map (object)
	jsonMap := map[string]interface{}{
		"name": "John",
		"age":  30,
		"address": map[string]interface{}{
			"city": "New York",
			"zip":  "10001",
		},
	}

	// Test with array
	jsonArray := []interface{}{1, 2, 3, 4}

	// Convert map to storage value and back
	storageMap := storage.ConvertGoValueToStorageValue(jsonMap)
	goMap := storage.ConvertStorageValueToGoValue(storageMap, storage.JSON)

	// Check that the map is returned as is
	mapResult, ok := goMap.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map but got %T", goMap)
	}

	// Check a few values
	if name, ok := mapResult["name"]; !ok || name != "John" {
		t.Errorf("Name mismatch in map result: got %v", name)
	}

	if age, ok := mapResult["age"]; !ok || age != 30 {
		t.Errorf("Age mismatch in map result: got %v", age)
	}

	// Convert array to storage value and back
	storageArray := storage.ConvertGoValueToStorageValue(jsonArray)
	goArray := storage.ConvertStorageValueToGoValue(storageArray, storage.JSON)

	// Check that the array is returned as is
	arrayResult, ok := goArray.([]interface{})
	if !ok {
		t.Fatalf("Expected array but got %T", goArray)
	}

	// Check array length
	if len(arrayResult) != 4 {
		t.Errorf("Expected array of length 4 but got %d", len(arrayResult))
	}

	// Test with JSON string
	jsonStr := `{"name":"John","age":30}`
	goValue := storage.ConvertStorageValueToGoValue(jsonStr, storage.JSON)

	// Check that the JSON string is returned as is
	if goValue != jsonStr {
		t.Errorf("Expected JSON string to be returned as is, got %v", goValue)
	}
}
