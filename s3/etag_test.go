package s3

import (
	"bytes"
	"io"
	"testing"
)

func TestETagCalculator(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{
			name:     "hello",
			data:     []byte("hello"),
			expected: "5d41402abc4b2a76b9719d911017c592",
		},
		{
			name:     "hello world",
			data:     []byte("hello world"),
			expected: "5eb63bbbe01eeed093cb22bb8f5acdc3",
		},
		{
			name:     "1MB zeros",
			data:     make([]byte, 1<<20),
			expected: "b6d81b360a5672d80c27430f39153e2c",
		},
		{
			// Multipart: 17MB of zeros (just over the 16MB part size)
			// s3PartSize is 16777216 bytes (1<<24 = 16MB)
			// Part 1: 16777216 bytes of zeros -> MD5: 2c2ceccb5ec5574f791d45b63c940cff
			// Part 2: 1048576 bytes of zeros  -> MD5: b6d81b360a5672d80c27430f39153e2c
			// Combined MD5 of concatenated hashes: d7f0e9878e982a33237d7f6f946e7951-2
			name:     "17MB zeros multipart",
			data:     make([]byte, 17<<20),
			expected: "d7f0e9878e982a33237d7f6f946e7951-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := CreateETagCalculator()
			_, err := io.Copy(calc.Writer, bytes.NewReader(tt.data))
			if err != nil {
				t.Fatal(err)
			}
			calc.Writer.Close()
			result := <-calc.Result
			if result.ETag != tt.expected {
				t.Errorf("ETag = %q, want %q", result.ETag, tt.expected)
			}
			if result.Size != int64(len(tt.data)) {
				t.Errorf("Size = %d, want %d", result.Size, len(tt.data))
			}
		})
	}
}
