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
	"testing"

	"github.com/m3db/m3x/pool"
	"github.com/stretchr/testify/require"
)

func TestAggregationTypeIsValid(t *testing.T) {
	require.True(t, P9999.IsValid())
	require.False(t, AggregationType(int(P9999)+1).IsValid())
}

func TestAggregationTypesIsDefault(t *testing.T) {
	require.True(t, DefaultAggregationTypes.IsDefault())

	require.False(t, AggregationTypes{Upper}.IsDefault())
}

func TestParseParseAggregationTypes(t *testing.T) {
	inputs := []struct {
		str      string
		expected AggregationTypes
	}{
		{
			str:      "Lower",
			expected: AggregationTypes{Lower},
		},
		{
			str:      "Lower,Upper",
			expected: AggregationTypes{Lower, Upper},
		},
	}
	for _, input := range inputs {
		res, err := ParseAggregationTypes(input.str)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
}

func TestQuantiles(t *testing.T) {
	res := AggregationTypes{Median, P95, P99}.PooledQuantiles(nil)
	require.Equal(t, []float64{0.5, 0.95, 0.99}, res)

	p := pool.NewFloatsPool(
		[]pool.Bucket{
			pool.Bucket{Capacity: 10, Count: 1},
		},
		nil,
	)
	p.Init()
	res = AggregationTypes{Median, P95, P99}.PooledQuantiles(p)
	require.Equal(t, []float64{0.5, 0.95, 0.99}, res)

	p.Put(res)

	res2 := AggregationTypes{P90, P95, P99}.PooledQuantiles(p)
	require.Equal(t, []float64{0.9, 0.95, 0.99}, res2)
	require.Equal(t, res, res2)
	p.Put(res2)

	res3 := AggregationTypes{Count}.PooledQuantiles(p)
	require.Nil(t, res3)

	res4 := AggregationTypes{P10, P20, P30, P40, P50, Median, P60, P70, P80, P90, P95, P99, P999, P9999}.PooledQuantiles(p)
	require.Equal(t, []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 0.9999}, res4)
}

func TestCompressedAggregationTypesIsDefault(t *testing.T) {
	var id AggregationID
	require.True(t, id.IsDefault())

	id[0] = 8
	require.False(t, id.IsDefault())

	id[0] = 0
	require.True(t, id.IsDefault())
}

func TestCompressedAggregationTypesMerge(t *testing.T) {
	testcases := []struct {
		input  AggregationID
		other  AggregationID
		result AggregationID
		merged bool
	}{
		{DefaultAggregationID, DefaultAggregationID, DefaultAggregationID, false},
		{mustCompress(Mean), DefaultAggregationID, mustCompress(Mean), false},
		{DefaultAggregationID, mustCompress(Mean), mustCompress(Mean), true},
		{mustCompress(Lower), mustCompress(Upper), mustCompress(Lower, Upper), true},
		{mustCompress(Lower), mustCompress(Lower, Upper), mustCompress(Lower, Upper), true},
		{mustCompress(Lower, Upper), mustCompress(Lower), mustCompress(Lower, Upper), false},
	}

	for _, test := range testcases {
		res, merged := test.input.Merge(test.other)
		require.Equal(t, test.result, res)
		require.Equal(t, test.merged, merged)
	}
}

func mustCompress(aggTypes ...AggregationType) AggregationID {
	res, err := NewAggregationIDCompressor().Compress(aggTypes)
	if err != nil {
		panic(err.Error())
	}
	return res
}
