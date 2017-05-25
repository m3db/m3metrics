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

package policy

import (
	"fmt"

	"github.com/cw9/bitset"
)

// AggregationIDCompressor can compress AggregationTypes into an AggregationID.
type AggregationIDCompressor interface {
	Compress(aggTypes AggregationTypes) (AggregationID, error)
}

// AggregationIDDecompressor can decompress AggregationID.
type AggregationIDDecompressor interface {
	// Decompress decompresses aggregation types,
	// returns error if any invalid aggregation type is encountered.
	Decompress(pool AggregationTypesPool, compressed AggregationID) (AggregationTypes, error)
}

type aggregationIDCompressor struct {
	bs *bitset.BitSet
}

// NewAggregationTypeCompressor returns a new AggregationTypeCompressor.
func NewAggregationTypeCompressor() AggregationIDCompressor {
	// NB(cw): If we start to support more than 64 types, the library will
	// expand the underlying word list itself.
	return &aggregationIDCompressor{
		bs: bitset.New(totalAggregationTypes),
	}
}

func (c *aggregationIDCompressor) Compress(aggTypes AggregationTypes) (AggregationID, error) {
	c.bs.ClearAll()
	for _, aggType := range aggTypes {
		if !aggType.IsValid() {
			return DefaultAggregationID, fmt.Errorf("could not compress invalid AggregationType %v", aggType)
		}
		c.bs.Set(uint(aggType))
	}

	codes := c.bs.Bytes()
	var id AggregationID
	// NB(cw) it's guaranteed that len(id) == len(codes) == AggregationIDLen, we need to copy
	// the words in bitset out because the bitset contains a slice internally
	for i := 0; i < AggregationIDLen; i++ {
		id[i] = codes[i]
	}
	return id, nil
}

type aggregationIDDecompressor struct {
	bs  *bitset.BitSet
	buf []uint64
}

// NewAggregationTypeDecompressor returns a new AggregationTypeDecompressor.
func NewAggregationTypeDecompressor() AggregationIDDecompressor {
	bs := bitset.New(totalAggregationTypes)
	return &aggregationIDDecompressor{
		bs:  bs,
		buf: bs.Bytes(),
	}
}

func (c *aggregationIDDecompressor) Decompress(pool AggregationTypesPool, id AggregationID) (AggregationTypes, error) {
	// NB(cw) it's guaranteed that len(c.buf) == len(id) == AggregationIDLen, we need to copy
	// the words from id into a slice to be used in bitset
	for i := range id {
		c.buf[i] = id[i]
	}

	c.bs.Reset(c.buf)

	var res AggregationTypes
	if pool == nil {
		res = make(AggregationTypes, 0, totalAggregationTypes)
	} else {
		res = pool.Get()
	}

	for i, e := c.bs.NextSet(0); e; i, e = c.bs.NextSet(i + 1) {
		aggType := AggregationType(i)
		if !aggType.IsValid() {
			return DefaultAggregationTypes, fmt.Errorf("invalid AggregationType: %s", aggType.String())
		}

		res = append(res, aggType)
	}

	return res, nil
}
