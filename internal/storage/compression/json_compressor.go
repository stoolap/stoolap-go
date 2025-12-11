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
package compression

import (
	"encoding/json"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// JSONCompressor is a specialized compressor for JSON data
type JSONCompressor struct {
	// No fields needed for this simple compressor
}

// NewJSONCompressor creates a new JSON compressor
func NewJSONCompressor() *JSONCompressor {
	return &JSONCompressor{}
}

// Compress implements the legacy Compressor interface
func (c *JSONCompressor) Compress(data []byte) ([]byte, error) {
	// Simple implementation that just tags the data with the JSON compressor type
	result := make([]byte, len(data)+1)
	result[0] = byte(JSONCompression)
	copy(result[1:], data)
	return result, nil
}

// Decompress implements the legacy Compressor interface
func (c *JSONCompressor) Decompress(data []byte) ([]byte, error) {
	// Skip the compressor type byte
	if len(data) == 0 {
		return nil, ErrInvalidCompressedData
	}

	return data[1:], nil
}

// Type returns the type of the compressor
func (c *JSONCompressor) Type() CompressionType {
	return JSONCompression
}

// CompressBinary implements the BinaryCompressor interface for JSON data
func (c *JSONCompressor) CompressBinary(data []interface{}) ([]byte, error) {
	// First convert to JSON strings
	jsonData := make([]byte, 0, 1024)

	// Add compressor type as first byte
	jsonData = append(jsonData, byte(JSONCompression))

	// Then encode each object
	for _, item := range data {
		encoded, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}

		// Append length and then data
		length := uint32(len(encoded))
		lenBytes := make([]byte, 4)

		// Store length as big-endian
		lenBytes[0] = byte(length >> 24)
		lenBytes[1] = byte(length >> 16)
		lenBytes[2] = byte(length >> 8)
		lenBytes[3] = byte(length)

		jsonData = append(jsonData, lenBytes...)
		jsonData = append(jsonData, encoded...)
	}

	return jsonData, nil
}

// DecompressBinary implements the BinaryCompressor interface for JSON data
func (c *JSONCompressor) DecompressBinary(data []byte, valueType storage.DataType) ([]interface{}, error) {
	// Skip the compressor type byte
	if len(data) < 1 {
		return nil, ErrInvalidCompressedData
	}

	// Verify the compressor type
	if data[0] != byte(JSONCompression) {
		return nil, ErrInvalidCompressor
	}

	// Parse the length-prefixed JSON objects
	result := make([]interface{}, 0)
	pos := 1

	for pos < len(data) {
		// Need at least 4 bytes for length
		if pos+4 > len(data) {
			break
		}

		// Read length
		length := uint32(data[pos])<<24 | uint32(data[pos+1])<<16 | uint32(data[pos+2])<<8 | uint32(data[pos+3])
		pos += 4

		// Ensure we have enough data
		if pos+int(length) > len(data) {
			return nil, ErrInvalidCompressedData
		}

		// Extract the JSON data
		jsonData := data[pos : pos+int(length)]
		pos += int(length)

		// For storage.JSON type, we just store the string representation
		if valueType == storage.JSON {
			result = append(result, string(jsonData))
		} else {
			// Otherwise, parse into an interface{}
			var parsed interface{}
			if err := json.Unmarshal(jsonData, &parsed); err != nil {
				return nil, err
			}
			result = append(result, parsed)
		}
	}

	return result, nil
}

// Ensure type registry is updated
func init() {
	// Register the JSON compressor factory
	RegisterCompressor(JSONCompression, func() Compressor {
		return NewJSONCompressor()
	})
}
