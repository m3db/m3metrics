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
func TestUnaggregatedIteratorDecodeBatchTimerNoAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder().(*unaggregatedEncoder)
	bt := unaggregated.BatchTimer{
		ID:     []byte("foo"),
		Values: []float64{1.0, 2.0, 3.0, 4.0},
	}
	enc.encodeBatchTimer(bt)
	require.NoError(t, enc.err())

	// Allocate a large enough buffer to avoid triggering an allocation.
	it := testUnaggregatedIterator(enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.timerValues = make([]float64, 1000)
	it.decodeBatchTimer()

	require.NoError(t, it.Err())
	mu := it.Metric()
	require.Equal(t, unaggregated.BatchTimerType, mu.Type)
	require.Equal(t, id.RawID("foo"), mu.ID)
	require.Equal(t, bt.Values, mu.BatchTimerVal)
	require.Equal(t, cap(it.timerValues), cap(mu.BatchTimerVal))
	require.False(t, mu.OwnsID)
	require.Nil(t, mu.TimerValPool)
}

func TestUnaggregatedIteratorDecodeBatchTimerWithAllocNonPoolAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder().(*unaggregatedEncoder)
	bt := unaggregated.BatchTimer{
		ID:     []byte("foo"),
		Values: []float64{1.0, 2.0, 3.0, 4.0},
	}
	enc.encodeBatchTimer(bt)
	require.NoError(t, enc.err())

	// Allocate a large enough buffer to avoid triggering an allocation.
	it := testUnaggregatedIterator(enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.decodeBatchTimer()

	require.NoError(t, it.Err())
	mu := it.Metric()
	require.Equal(t, unaggregated.BatchTimerType, mu.Type)
	require.Equal(t, id.RawID("foo"), mu.ID)
	require.Equal(t, bt.Values, mu.BatchTimerVal)
	require.Equal(t, cap(it.timerValues), cap(mu.BatchTimerVal))
	require.False(t, mu.OwnsID)
	require.Nil(t, mu.TimerValPool)
}

func TestUnaggregatedIteratorDecodeBatchTimerWithAllocPoolAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder().(*unaggregatedEncoder)
	bt := unaggregated.BatchTimer{
		ID:     []byte("foo"),
		Values: []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0},
	}
	enc.encodeBatchTimer(bt)
	require.NoError(t, enc.err())

	// Allocate a large enough buffer to avoid triggering an allocation.
	it := testUnaggregatedIterator(enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.timerValues = nil
	it.largeFloatsSize = 2
	it.decodeBatchTimer()

	require.NoError(t, it.Err())
	mu := it.Metric()
	require.Equal(t, unaggregated.BatchTimerType, mu.Type)
	require.Equal(t, id.RawID("foo"), mu.ID)
	require.Equal(t, bt.Values, mu.BatchTimerVal)
	require.True(t, cap(mu.BatchTimerVal) >= len(bt.Values))
	require.Nil(t, it.timerValues)
	require.False(t, mu.OwnsID)
	require.NotNil(t, mu.TimerValPool)
	require.Equal(t, it.largeFloatsPool, mu.TimerValPool)
}
*/
