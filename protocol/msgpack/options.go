// Copyright (c) 2016 Uber Technologies, Inc.
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
	"github.com/m3db/m3metrics/policy"
	xpool "github.com/m3db/m3x/pool"
)

const (
	// Whether the iterator should ignore higher-than-supported version
	// by default for unaggregated iterator.
	defaultUnaggregatedIgnoreHigherVersion = false

	// Default reader buffer size for the unaggregated iterator.
	defaultUnaggregatedReaderBufferSize = 1440

	// Whether a float slice is considered a "large" slice and therefore
	// resort to the pool for allocating that slice.
	defaultLargeFloatsSize = 1024

	// Whether the iterator should ignore higher-than-supported version
	// by default for aggregated iterator.
	defaultAggregatedIgnoreHigherVersion = false

	// Default reader buffer size for the aggregated iterator.
	defaultAggregatedReaderBufferSize = 1440
)

type baseEncoderOptions struct {
	enabled    bool
	compressor policy.Compressor
}

// NewBaseEncoderOptions creates a new set of base encoder options
func NewBaseEncoderOptions() BaseEncoderOptions {
	return baseEncoderOptions{
		compressor: policy.NewNoopCompressor(),
	}
}

func (o baseEncoderOptions) SetPolicyCompressionEnabled(enabled bool) BaseEncoderOptions {
	opts := o
	opts.enabled = enabled
	return opts
}

func (o baseEncoderOptions) PolicyCompressionEnabled() bool {
	return o.enabled
}

func (o baseEncoderOptions) SetPolicyCompressor(compressor policy.Compressor) BaseEncoderOptions {
	opts := o
	opts.compressor = compressor
	return opts
}

func (o baseEncoderOptions) PolicyCompressor() policy.Compressor {
	return o.compressor
}

type baseIteratorOptions struct {
	enabled      bool
	decompressor policy.Decompressor
}

// NewBaseIteratorOptions creates a new set of base iterator options
func NewBaseIteratorOptions() BaseIteratorOptions {
	return baseIteratorOptions{
		decompressor: policy.NewNoopDecompressor(),
	}
}

func (o baseIteratorOptions) SetPolicyDecompressionEnabled(enabled bool) BaseIteratorOptions {
	opts := o
	opts.enabled = enabled
	return opts
}

func (o baseIteratorOptions) PolicyDecompressionEnabled() bool {
	return o.enabled
}

func (o baseIteratorOptions) SetPolicyDecompressor(decompressor policy.Decompressor) BaseIteratorOptions {
	opts := o
	opts.decompressor = decompressor
	return opts
}

func (o baseIteratorOptions) PolicyDecompressor() policy.Decompressor {
	return o.decompressor
}

type unaggregatedIteratorOptions struct {
	ignoreHigherVersion bool
	readerBufferSize    int
	largeFloatsSize     int
	largeFloatsPool     xpool.FloatsPool
	iteratorPool        UnaggregatedIteratorPool
	baseIteratorOpts    BaseIteratorOptions
}

// NewUnaggregatedIteratorOptions creates a new set of unaggregated iterator options.
func NewUnaggregatedIteratorOptions() UnaggregatedIteratorOptions {
	largeFloatsPool := xpool.NewFloatsPool(nil, nil)
	largeFloatsPool.Init()

	return &unaggregatedIteratorOptions{
		ignoreHigherVersion: defaultUnaggregatedIgnoreHigherVersion,
		readerBufferSize:    defaultUnaggregatedReaderBufferSize,
		largeFloatsSize:     defaultLargeFloatsSize,
		largeFloatsPool:     largeFloatsPool,
		baseIteratorOpts:    NewBaseIteratorOptions(),
	}
}

func (o *unaggregatedIteratorOptions) SetIgnoreHigherVersion(value bool) UnaggregatedIteratorOptions {
	opts := *o
	opts.ignoreHigherVersion = value
	return &opts
}

func (o *unaggregatedIteratorOptions) IgnoreHigherVersion() bool {
	return o.ignoreHigherVersion
}

func (o *unaggregatedIteratorOptions) SetReaderBufferSize(value int) UnaggregatedIteratorOptions {
	opts := *o
	opts.readerBufferSize = value
	return &opts
}

func (o *unaggregatedIteratorOptions) ReaderBufferSize() int {
	return o.readerBufferSize
}

func (o *unaggregatedIteratorOptions) SetLargeFloatsSize(value int) UnaggregatedIteratorOptions {
	opts := *o
	opts.largeFloatsSize = value
	return &opts
}

func (o *unaggregatedIteratorOptions) LargeFloatsSize() int {
	return o.largeFloatsSize
}

func (o *unaggregatedIteratorOptions) SetLargeFloatsPool(value xpool.FloatsPool) UnaggregatedIteratorOptions {
	opts := *o
	opts.largeFloatsPool = value
	return &opts
}

func (o *unaggregatedIteratorOptions) LargeFloatsPool() xpool.FloatsPool {
	return o.largeFloatsPool
}

func (o *unaggregatedIteratorOptions) SetIteratorPool(value UnaggregatedIteratorPool) UnaggregatedIteratorOptions {
	opts := *o
	opts.iteratorPool = value
	return &opts
}

func (o *unaggregatedIteratorOptions) IteratorPool() UnaggregatedIteratorPool {
	return o.iteratorPool
}

func (o *unaggregatedIteratorOptions) SetBaseIteratorOptions(itOpts BaseIteratorOptions) UnaggregatedIteratorOptions {
	opts := o
	opts.baseIteratorOpts = itOpts
	return opts
}

func (o *unaggregatedIteratorOptions) BaseIteratorOptions() BaseIteratorOptions {
	return o.baseIteratorOpts
}

type aggregatedIteratorOptions struct {
	ignoreHigherVersion bool
	readerBufferSize    int
	iteratorPool        AggregatedIteratorPool
	baseIteratorOpts    BaseIteratorOptions
}

// NewAggregatedIteratorOptions creates a new set of aggregated iterator options.
func NewAggregatedIteratorOptions() AggregatedIteratorOptions {
	return &aggregatedIteratorOptions{
		ignoreHigherVersion: defaultAggregatedIgnoreHigherVersion,
		readerBufferSize:    defaultAggregatedReaderBufferSize,
		baseIteratorOpts:    NewBaseIteratorOptions(),
	}
}

func (o *aggregatedIteratorOptions) SetIgnoreHigherVersion(value bool) AggregatedIteratorOptions {
	opts := *o
	opts.ignoreHigherVersion = value
	return &opts
}

func (o *aggregatedIteratorOptions) IgnoreHigherVersion() bool {
	return o.ignoreHigherVersion
}

func (o *aggregatedIteratorOptions) SetReaderBufferSize(value int) AggregatedIteratorOptions {
	opts := *o
	opts.readerBufferSize = value
	return &opts
}

func (o *aggregatedIteratorOptions) ReaderBufferSize() int {
	return o.readerBufferSize
}

func (o *aggregatedIteratorOptions) SetIteratorPool(value AggregatedIteratorPool) AggregatedIteratorOptions {
	opts := *o
	opts.iteratorPool = value
	return &opts
}

func (o *aggregatedIteratorOptions) IteratorPool() AggregatedIteratorPool {
	return o.iteratorPool
}

func (o *aggregatedIteratorOptions) SetBaseIteratorOptions(itOpts BaseIteratorOptions) AggregatedIteratorOptions {
	opts := o
	opts.baseIteratorOpts = itOpts
	return opts
}

func (o *aggregatedIteratorOptions) BaseIteratorOptions() BaseIteratorOptions {
	return o.baseIteratorOpts
}
