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
	"errors"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// Common errors
var (
	ErrInvalidCompressedData = errors.New("invalid compressed data")
	ErrInvalidCompressor     = errors.New("invalid compressor type")
)

// Compressor is the interface that all compression algorithms must implement
type Compressor interface {
	// Compress compresses the given data
	Compress(data []byte) ([]byte, error)

	// Decompress decompresses the given data
	Decompress(data []byte) ([]byte, error)

	// Type returns the compression type
	Type() CompressionType
}

// BinaryCompressor is a newer interface for binary serialization
type BinaryCompressor interface {
	// Compress compresses the given data
	Compress(data []interface{}) ([]byte, error)

	// Decompress decompresses the given data
	Decompress(data []byte, valueType storage.DataType) ([]interface{}, error)

	// Type returns the compression type
	Type() byte
}

// Constants for compressor types
const (
	CompressorTypeNone       byte = 0
	CompressorTypeDictionary byte = 1
	CompressorTypeRunLength  byte = 2
	CompressorTypeDelta      byte = 3
	CompressorTypeBitPack    byte = 4
	CompressorTypeTime       byte = 5
	CompressorTypeJSON       byte = 6
)

// CompressionType identifies different compression algorithms (legacy)
type CompressionType uint8

const (
	// None means no compression
	None CompressionType = iota

	// Dictionary for dictionary encoding (for text/string data)
	Dictionary

	// RunLength for run-length encoding (for repetitive data)
	RunLength

	// Delta for delta encoding (for numeric sequences)
	Delta

	// BitPack for bitmap encoding (for boolean data)
	BitPack

	// TimeCompression for specialized time/date compression
	TimeCompression

	// JSONCompression for JSON data compression
	JSONCompression
)

// These constants are used for serialization
const (
	CompressionHeaderSize = 5 // 1 byte for type + 4 bytes for data size
)

// compressorFactories maps compressor types to factory functions
var compressorFactories = make(map[CompressionType]func() Compressor)

// RegisterCompressor registers a compressor factory for a specific type
func RegisterCompressor(compType CompressionType, factory func() Compressor) {
	compressorFactories[compType] = factory
}

// CreateCompressor creates a compressor of the specified type
// For Delta compression, you need to specify if the data is floating point with isFloat
func CreateCompressor(compType CompressionType, isFloat ...bool) Compressor {
	// Use the factory if available
	if factory, ok := compressorFactories[compType]; ok {
		return factory()
	}

	// Fall back to legacy creation logic
	switch compType {
	case Dictionary:
		return NewDictionaryCompressor()
	case RunLength:
		return NewRunLengthCompressor()
	case Delta:
		if len(isFloat) > 0 {
			return NewDeltaCompressor(isFloat[0])
		}
		// Default to integer delta encoding if not specified
		return NewDeltaCompressor(false)
	case BitPack:
		return NewBitPackCompressor()
	case TimeCompression:
		// Default to DeltaEncoding format for time compression which is good for timeseries data
		return NewTimeCompressor(DeltaEncoding)
	case JSONCompression:
		return NewJSONCompressor()
	default:
		return nil
	}
}
