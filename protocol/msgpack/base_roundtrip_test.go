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

/*
func TestAggregationTypesRoundTrip(t *testing.T) {
	inputs := []policy.AggregationID{
		policy.DefaultAggregationID,
		policy.AggregationID{5},
		policy.AggregationID{100},
		policy.AggregationID{12345},
	}

	for _, input := range inputs {
		enc := newBaseEncoder(NewBufferedEncoder()).(*baseEncoder)
		it := newBaseIterator(enc.bufEncoder.Buffer(), 16).(*baseIterator)

		enc.encodeCompressedAggregationTypes(input)
		r := it.decodeCompressedAggregationTypes()
		require.Equal(t, input, r)
	}
}

func TestUnaggregatedPolicyRoundTrip(t *testing.T) {
	inputs := []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), policy.AggregationID{8}),
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.AggregationID{100}),
	}

	for _, input := range inputs {
		enc := newBaseEncoder(NewBufferedEncoder()).(*baseEncoder)
		enc.encodePolicy(input)

		it := newBaseIterator(enc.bufEncoder.Buffer(), 16).(*baseIterator)
		r := it.decodePolicy()
		require.Equal(t, input, r)
	}
}
*/
