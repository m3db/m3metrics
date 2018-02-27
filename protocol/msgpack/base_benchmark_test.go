// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package msgpack

import (
	"bytes"
	"math/rand"
	"testing"
)

var (
	smallFloat64s  []float64
	mediumFloat64s []float64
	largeFloat64s  []float64
)

func BenchmarkEncodeFloat64NativeSmall(b *testing.B) {
	benchEncode(b, smallFloat64s, nonPackedEncoding)
}

func BenchmarkEncodeFloat64NativeMedium(b *testing.B) {
	benchEncode(b, mediumFloat64s, nonPackedEncoding)
}

func BenchmarkEncodeFloat64NativeLarge(b *testing.B) {
	benchEncode(b, largeFloat64s, nonPackedEncoding)
}

func BenchmarkEncodeFloat64PackedSmall(b *testing.B) {
	benchEncode(b, smallFloat64s, packedEncoding)
}

func BenchmarkEncodeFloat64PackedMedium(b *testing.B) {
	benchEncode(b, mediumFloat64s, packedEncoding)
}

func BenchmarkEncodeFloat64PackedLarge(b *testing.B) {
	benchEncode(b, largeFloat64s, packedEncoding)
}

func BenchmarkDecodeFloat64NativeSmall(b *testing.B) {
	benchDecode(b, smallFloat64s, nonPackedEncoding)
}

func BenchmarkDecodeFloat64NativeMedium(b *testing.B) {
	benchDecode(b, mediumFloat64s, nonPackedEncoding)
}

func BenchmarkDecodeFloat64NativeLarge(b *testing.B) {
	benchDecode(b, largeFloat64s, nonPackedEncoding)
}

func BenchmarkDecodeFloat64PackedSmall(b *testing.B) {
	benchDecode(b, smallFloat64s, packedEncoding)
}

func BenchmarkDecodeFloat64PackedMedium(b *testing.B) {
	benchDecode(b, mediumFloat64s, packedEncoding)
}

func BenchmarkDecodeFloat64PackedLarge(b *testing.B) {
	benchDecode(b, largeFloat64s, packedEncoding)
}

func benchEncode(b *testing.B, values []float64, encodingType encodingType) {
	buffer := NewBufferedEncoder()
	encoder := newBaseEncoder(buffer)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		encoder.encodeFloat64Slice(values, encodingType)
	}
}

func benchDecode(b *testing.B, values []float64, encodingType encodingType) {
	encoded := encodedBytes(values, encodingType)
	iterator := newBaseIterator(nil, nil)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reader := bytes.NewBuffer(encoded)
		iterator.reset(reader)
		iterator.decodeFloat64Slice(encodingType)
	}
}

func encodedBytes(values []float64, encodingType encodingType) []byte {
	buffer := NewBufferedEncoder()
	encoder := newBaseEncoder(buffer)
	encoder.encodeFloat64Slice(values, encodingType)
	return buffer.Bytes()
}

func init() {
	smallFloat64s = make([]float64, 16)
	for i := 0; i < len(smallFloat64s); i++ {
		smallFloat64s[i] = rand.Float64() * 100
	}

	mediumFloat64s = make([]float64, 1120)
	for i := 0; i < len(mediumFloat64s); i++ {
		mediumFloat64s[i] = rand.Float64() * 100
	}

	largeFloat64s = make([]float64, 65536)
	for i := 0; i < len(largeFloat64s); i++ {
		largeFloat64s[i] = rand.Float64() * 100
	}
}
