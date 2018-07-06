// Copyright (c) 2018 Uber Technologies, Inc.
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

package transformation

import (
	"math"
	"time"
)

const (
	nanosPerSecond = time.Second / time.Nanosecond
)

// perSecond computes the derivative between consecutive datapoints, taking into
// account the time interval between the values.
// * It skips NaN values.
// * It assumes the timestamps are monotonically increasing, and values are non-decreasing.
//   If either of the two conditions is not met, an empty datapoint is returned.
func perSecond(prev, curr Datapoint) Datapoint {
	if prev.TimeNanos >= curr.TimeNanos || math.IsNaN(prev.Value) || math.IsNaN(curr.Value) {
		return emptyDatapoint
	}
	diff := curr.Value - prev.Value
	if diff < 0 {
		return emptyDatapoint
	}
	rate := diff * float64(nanosPerSecond) / float64(curr.TimeNanos-prev.TimeNanos)
	return Datapoint{TimeNanos: curr.TimeNanos, Value: rate}
}
