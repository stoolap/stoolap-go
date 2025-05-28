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
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
)

func TestDeltaEncoder(t *testing.T) {
	t.Run("EmptyData", func(t *testing.T) {
		encoder := NewDeltaEncoder(false)

		// Test encoding empty data
		encoded, err := encoder.Encode([]byte{})
		if err != nil {
			t.Fatalf("Failed to encode empty data: %v", err)
		}
		if len(encoded) != 0 {
			t.Errorf("Expected empty result, got %d bytes", len(encoded))
		}

		// Test decoding empty data
		decoded, err := encoder.Decode([]byte{})
		if err != nil {
			t.Fatalf("Failed to decode empty data: %v", err)
		}
		if len(decoded) != 0 {
			t.Errorf("Expected empty result, got %d bytes", len(decoded))
		}
	})

	t.Run("Int32Sequential", func(t *testing.T) {
		testDeltaInt32(t, 1000, 1) // Sequential numbers
	})

	t.Run("Int32SmallDeltas", func(t *testing.T) {
		testDeltaInt32(t, 1000, 5) // Small deltas
	})

	t.Run("Int32RandomValues", func(t *testing.T) {
		testDeltaInt32(t, 1000, 1000) // Random values with potentially large deltas
	})

	t.Run("Int64Sequential", func(t *testing.T) {
		testDeltaInt64(t, 1000, 1) // Sequential numbers
	})

	t.Run("Int64SmallDeltas", func(t *testing.T) {
		testDeltaInt64(t, 1000, 50) // Small deltas
	})

	t.Run("Int64LargeDeltas", func(t *testing.T) {
		testDeltaInt64(t, 1000, math.MaxInt32) // Large deltas
	})

	t.Run("Float32Sequential", func(t *testing.T) {
		testDeltaFloat32(t, 1000, 0.1) // Sequential with small increment
	})

	t.Run("Float32SmallDeltas", func(t *testing.T) {
		testDeltaFloat32(t, 1000, 0.001) // Very small deltas
	})

	t.Run("Float32RandomValues", func(t *testing.T) {
		testDeltaFloat32(t, 1000, 100.0) // Random with potentially large deltas
	})

	t.Run("Float64Sequential", func(t *testing.T) {
		testDeltaFloat64(t, 1000, 0.1) // Sequential with small increment
	})

	t.Run("Float64SmallDeltas", func(t *testing.T) {
		testDeltaFloat64(t, 1000, 0.0001) // Very small deltas
	})

	t.Run("Float64RandomValues", func(t *testing.T) {
		testDeltaFloat64(t, 1000, 1000.0) // Random with potentially large deltas
	})
}

func testDeltaInt32(t *testing.T, count int, maxDelta int32) {
	// Generate test data
	original := make([]byte, count*4)
	buf := bytes.NewBuffer(original[:0])

	value := int32(0)
	for i := 0; i < count; i++ {
		delta := int32(rand.Intn(int(maxDelta) + 1))
		value += delta
		binary.Write(buf, binary.LittleEndian, value)
	}

	// Create encoder and test
	encoder := NewDeltaEncoder(false)

	// Encode
	encoded, err := encoder.Encode(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to encode data: %v", err)
	}

	// Display compression stats
	originalSize := len(buf.Bytes())
	encodedSize := len(encoded)
	compressionRatio := float64(encodedSize) / float64(originalSize)
	t.Logf("Int32 test (max delta %d): Original size: %d bytes, Encoded size: %d bytes, Ratio: %.2f",
		maxDelta, originalSize, encodedSize, compressionRatio)

	// Decode
	decoded, err := encoder.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}

	// Verify
	if len(decoded) != originalSize {
		t.Errorf("Decoded size mismatch: expected %d, got %d", originalSize, len(decoded))
	}

	// Compare values
	for i := 0; i < count; i++ {
		offset := i * 4
		originalValue := int32(0)
		decodedValue := int32(0)

		originalReader := bytes.NewReader(buf.Bytes()[offset : offset+4])
		decodedReader := bytes.NewReader(decoded[offset : offset+4])

		binary.Read(originalReader, binary.LittleEndian, &originalValue)
		binary.Read(decodedReader, binary.LittleEndian, &decodedValue)

		if originalValue != decodedValue {
			t.Errorf("Value mismatch at index %d: expected %d, got %d",
				i, originalValue, decodedValue)
			break
		}
	}
}

func testDeltaInt64(t *testing.T, count int, maxDelta int64) {
	// Generate test data
	original := make([]byte, count*8)
	buf := bytes.NewBuffer(original[:0])

	value := int64(0)
	for i := 0; i < count; i++ {
		delta := rand.Int63n(maxDelta + 1)
		value += delta
		binary.Write(buf, binary.LittleEndian, value)
	}

	// Create encoder and test
	encoder := NewDeltaEncoder(false)

	// Encode
	encoded, err := encoder.Encode(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to encode data: %v", err)
	}

	// Display compression stats
	originalSize := len(buf.Bytes())
	encodedSize := len(encoded)
	compressionRatio := float64(encodedSize) / float64(originalSize)
	t.Logf("Int64 test (max delta %d): Original size: %d bytes, Encoded size: %d bytes, Ratio: %.2f",
		maxDelta, originalSize, encodedSize, compressionRatio)

	// Decode
	decoded, err := encoder.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}

	// Verify
	if len(decoded) != originalSize {
		t.Errorf("Decoded size mismatch: expected %d, got %d", originalSize, len(decoded))
	}

	// Compare values
	for i := 0; i < count; i++ {
		offset := i * 8
		originalValue := int64(0)
		decodedValue := int64(0)

		originalReader := bytes.NewReader(buf.Bytes()[offset : offset+8])
		decodedReader := bytes.NewReader(decoded[offset : offset+8])

		binary.Read(originalReader, binary.LittleEndian, &originalValue)
		binary.Read(decodedReader, binary.LittleEndian, &decodedValue)

		if originalValue != decodedValue {
			t.Errorf("Value mismatch at index %d: expected %d, got %d",
				i, originalValue, decodedValue)
			break
		}
	}
}

func testDeltaFloat32(t *testing.T, count int, maxDelta float32) {
	// Generate test data
	original := make([]byte, count*4)
	buf := bytes.NewBuffer(original[:0])

	value := float32(0)
	for i := 0; i < count; i++ {
		delta := float32(rand.Float32() * maxDelta)
		value += delta
		binary.Write(buf, binary.LittleEndian, value)
	}

	// Create encoder and test
	encoder := NewDeltaEncoder(true)

	// Encode
	encoded, err := encoder.Encode(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to encode data: %v", err)
	}

	// Display compression stats
	originalSize := len(buf.Bytes())
	encodedSize := len(encoded)
	compressionRatio := float64(encodedSize) / float64(originalSize)
	t.Logf("Float32 test (max delta %.4f): Original size: %d bytes, Encoded size: %d bytes, Ratio: %.2f",
		maxDelta, originalSize, encodedSize, compressionRatio)

	// Decode
	decoded, err := encoder.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}

	// Verify
	if len(decoded) != originalSize {
		t.Errorf("Decoded size mismatch: expected %d, got %d", originalSize, len(decoded))
	}

	// Compare values
	for i := 0; i < count; i++ {
		offset := i * 4
		originalValue := float32(0)
		decodedValue := float32(0)

		originalReader := bytes.NewReader(buf.Bytes()[offset : offset+4])
		decodedReader := bytes.NewReader(decoded[offset : offset+4])

		binary.Read(originalReader, binary.LittleEndian, &originalValue)
		binary.Read(decodedReader, binary.LittleEndian, &decodedValue)

		// For floating point, allow small epsilon difference due to precision
		// Float32 has limited precision and the delta encoder uses float64 internally,
		// which can introduce additional precision loss during conversions
		epsilon := float64(2e-4)
		if math.Abs(float64(originalValue-decodedValue)) > epsilon {
			t.Errorf("Value mismatch at index %d: expected %f, got %f (diff: %e > %e)",
				i, originalValue, decodedValue, math.Abs(float64(originalValue-decodedValue)), epsilon)
			break
		}
	}
}

func testDeltaFloat64(t *testing.T, count int, maxDelta float64) {
	// Generate test data
	original := make([]byte, count*8)
	buf := bytes.NewBuffer(original[:0])

	value := float64(0)
	for i := 0; i < count; i++ {
		delta := rand.Float64() * maxDelta
		value += delta
		binary.Write(buf, binary.LittleEndian, value)
	}

	// Create encoder and test
	encoder := NewDeltaEncoder(true)

	// Encode
	encoded, err := encoder.Encode(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to encode data: %v", err)
	}

	// Display compression stats
	originalSize := len(buf.Bytes())
	encodedSize := len(encoded)
	compressionRatio := float64(encodedSize) / float64(originalSize)
	t.Logf("Float64 test (max delta %.4f): Original size: %d bytes, Encoded size: %d bytes, Ratio: %.2f",
		maxDelta, originalSize, encodedSize, compressionRatio)

	// Decode
	decoded, err := encoder.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}

	// Verify
	if len(decoded) != originalSize {
		t.Errorf("Decoded size mismatch: expected %d, got %d", originalSize, len(decoded))
	}

	// Compare values
	for i := 0; i < count; i++ {
		offset := i * 8
		originalValue := float64(0)
		decodedValue := float64(0)

		originalReader := bytes.NewReader(buf.Bytes()[offset : offset+8])
		decodedReader := bytes.NewReader(decoded[offset : offset+8])

		binary.Read(originalReader, binary.LittleEndian, &originalValue)
		binary.Read(decodedReader, binary.LittleEndian, &decodedValue)

		// For floating point, allow small epsilon difference due to precision
		if math.Abs(originalValue-decodedValue) > 1e-12 {
			t.Errorf("Value mismatch at index %d: expected %f, got %f",
				i, originalValue, decodedValue)
			break
		}
	}
}

func TestDeltaCompressor(t *testing.T) {
	t.Run("IntCompressor", func(t *testing.T) {
		compressor := NewDeltaCompressor(false)

		// Generate sequential integer data
		count := 1000
		data := make([]byte, count*4)
		buf := bytes.NewBuffer(data[:0])

		for i := 0; i < count; i++ {
			binary.Write(buf, binary.LittleEndian, int32(i))
		}

		// Compress
		compressed, err := compressor.Compress(buf.Bytes())
		if err != nil {
			t.Fatalf("Failed to compress data: %v", err)
		}

		// Display compression stats
		originalSize := len(buf.Bytes())
		compressedSize := len(compressed)
		compressionRatio := float64(compressedSize) / float64(originalSize)
		t.Logf("Int32 sequential: Original size: %d bytes, Compressed size: %d bytes, Ratio: %.2f",
			originalSize, compressedSize, compressionRatio)

		// Decompress
		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Failed to decompress data: %v", err)
		}

		// Verify
		if len(decompressed) != originalSize {
			t.Errorf("Decompressed size mismatch: expected %d, got %d", originalSize, len(decompressed))
		}

		if !bytes.Equal(buf.Bytes(), decompressed) {
			t.Errorf("Decompressed data does not match original")
		}
	})

	t.Run("FloatCompressor", func(t *testing.T) {
		compressor := NewDeltaCompressor(true)

		// Generate sequential float data with small increments
		count := 1000
		data := make([]byte, count*8)
		buf := bytes.NewBuffer(data[:0])

		for i := 0; i < count; i++ {
			binary.Write(buf, binary.LittleEndian, float64(i)*0.1)
		}

		// Compress
		compressed, err := compressor.Compress(buf.Bytes())
		if err != nil {
			t.Fatalf("Failed to compress data: %v", err)
		}

		// Display compression stats
		originalSize := len(buf.Bytes())
		compressedSize := len(compressed)
		compressionRatio := float64(compressedSize) / float64(originalSize)
		t.Logf("Float64 sequential: Original size: %d bytes, Compressed size: %d bytes, Ratio: %.2f",
			originalSize, compressedSize, compressionRatio)

		// Decompress
		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Failed to decompress data: %v", err)
		}

		// Verify
		if len(decompressed) != originalSize {
			t.Errorf("Decompressed size mismatch: expected %d, got %d", originalSize, len(decompressed))
		}

		if !bytes.Equal(buf.Bytes(), decompressed) {
			t.Errorf("Decompressed data does not match original")
		}
	})

	t.Run("EmptyCompressor", func(t *testing.T) {
		compressor := NewDeltaCompressor(false)

		// Test with empty data
		compressed, err := compressor.Compress([]byte{})
		if err != nil {
			t.Fatalf("Failed to compress empty data: %v", err)
		}

		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Failed to decompress empty data: %v", err)
		}

		if len(decompressed) != 0 {
			t.Errorf("Expected empty result, got %d bytes", len(decompressed))
		}
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		// Create invalid compressed data with wrong type
		invalidData := make([]byte, CompressionHeaderSize)
		invalidData[0] = byte(RunLength) // Wrong type

		compressor := NewDeltaCompressor(false)
		_, err := compressor.Decompress(invalidData)
		if err == nil {
			t.Errorf("Expected error for type mismatch, got nil")
		}
	})
}

func BenchmarkDeltaCompression(b *testing.B) {
	b.Run("Int32Sequential", func(b *testing.B) {
		benchmarkDeltaCompression(b, 10000, 4, false, 1)
	})

	b.Run("Int32RandomSmall", func(b *testing.B) {
		benchmarkDeltaCompression(b, 10000, 4, false, 10)
	})

	b.Run("Int32RandomLarge", func(b *testing.B) {
		benchmarkDeltaCompression(b, 10000, 4, false, 1000)
	})

	b.Run("Int64Sequential", func(b *testing.B) {
		benchmarkDeltaCompression(b, 10000, 8, false, 1)
	})

	b.Run("Float32Sequential", func(b *testing.B) {
		benchmarkDeltaCompression(b, 10000, 4, true, 0.1)
	})

	b.Run("Float64Sequential", func(b *testing.B) {
		benchmarkDeltaCompression(b, 10000, 8, true, 0.1)
	})

	b.Run("Float64Random", func(b *testing.B) {
		benchmarkDeltaCompression(b, 10000, 8, true, 100.0)
	})
}

func benchmarkDeltaCompression(b *testing.B, itemCount int, itemSize int, isFloat bool, deltaScale float64) {
	// Generate test data
	dataSize := itemCount * itemSize
	data := make([]byte, dataSize)
	buf := bytes.NewBuffer(data[:0])

	if isFloat {
		if itemSize == 4 {
			// Generate float32 data
			value := float32(0)
			for i := 0; i < itemCount; i++ {
				delta := float32(rand.Float32() * float32(deltaScale))
				value += delta
				binary.Write(buf, binary.LittleEndian, value)
			}
		} else {
			// Generate float64 data
			value := float64(0)
			for i := 0; i < itemCount; i++ {
				delta := rand.Float64() * deltaScale
				value += delta
				binary.Write(buf, binary.LittleEndian, value)
			}
		}
	} else {
		if itemSize == 4 {
			// Generate int32 data
			value := int32(0)
			for i := 0; i < itemCount; i++ {
				delta := int32(rand.Intn(int(deltaScale)) + 1)
				value += delta
				binary.Write(buf, binary.LittleEndian, value)
			}
		} else {
			// Generate int64 data
			value := int64(0)
			for i := 0; i < itemCount; i++ {
				delta := int64(rand.Intn(int(deltaScale)) + 1)
				value += delta
				binary.Write(buf, binary.LittleEndian, value)
			}
		}
	}

	data = buf.Bytes()

	// Create compressor
	compressor := NewDeltaCompressor(isFloat)

	// Reset timer before benchmarking
	b.ResetTimer()

	// Benchmark compression
	for i := 0; i < b.N; i++ {
		compressed, err := compressor.Compress(data)
		if err != nil {
			b.Fatalf("Compression failed: %v", err)
		}

		// Decompress to ensure correctness and benchmark decompression too
		_, err = compressor.Decompress(compressed)
		if err != nil {
			b.Fatalf("Decompression failed: %v", err)
		}
	}
}
