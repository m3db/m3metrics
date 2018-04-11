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

package protobuf

import (
	"testing"

	"github.com/m3db/m3metrics/generated/proto/metricpb"

	"github.com/stretchr/testify/require"
)

var (
	testCounterBeforeResetProto = metricpb.Counter{
		Id:    []byte("testCounter"),
		Value: 1234,
	}
	testCounterAfterResetProto = metricpb.Counter{
		Id:    []byte{},
		Value: 0,
	}
	testBatchTimerBeforeResetProto = metricpb.BatchTimer{
		Id:     []byte("testBatchTimer"),
		Values: []float64{13.45, 98.23},
	}
	testBatchTimerAfterResetProto = metricpb.BatchTimer{
		Id:     []byte{},
		Values: []float64{},
	}
	testGaugeBeforeResetProto = metricpb.Gauge{
		Id:    []byte("testGauge"),
		Value: 3.48,
	}
	testGaugeAfterResetProto = metricpb.Gauge{
		Id:    []byte{},
		Value: 0.0,
	}
	testMetadatasBeforeResetProto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{
			{
				CutoverNanos: 1234,
			},
			{
				CutoverNanos: 5678,
			},
		},
	}
	testMetadatasAfterResetProto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{},
	}
)

func TestResetMetricWithMetadatasProtoNilProto(t *testing.T) {
	require.NotPanics(t, func() { resetMetricWithMetadatasProto(nil) })
}

func TestResetMetricWithMetadatasProtoOnlyCounter(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter:   testCounterBeforeResetProto,
			Metadatas: testMetadatasBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter:   testCounterAfterResetProto,
			Metadatas: testMetadatasAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.CounterWithMetadatas.Counter.Id) > 0)
	require.True(t, cap(input.CounterWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestResetMetricWithMetadatasProtoOnlyBatchTimer(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		BatchTimerWithMetadatas: &metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimerBeforeResetProto,
			Metadatas:  testMetadatasBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		BatchTimerWithMetadatas: &metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimerAfterResetProto,
			Metadatas:  testMetadatasAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.BatchTimerWithMetadatas.BatchTimer.Id) > 0)
	require.True(t, cap(input.BatchTimerWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestResetMetricWithMetadatasProtoOnlyGauge(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		GaugeWithMetadatas: &metricpb.GaugeWithMetadatas{
			Gauge:     testGaugeBeforeResetProto,
			Metadatas: testMetadatasBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		GaugeWithMetadatas: &metricpb.GaugeWithMetadatas{
			Gauge:     testGaugeAfterResetProto,
			Metadatas: testMetadatasAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.GaugeWithMetadatas.Gauge.Id) > 0)
	require.True(t, cap(input.GaugeWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestResetMetricWithMetadatasProtoAll(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter:   testCounterBeforeResetProto,
			Metadatas: testMetadatasBeforeResetProto,
		},
		BatchTimerWithMetadatas: &metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimerBeforeResetProto,
			Metadatas:  testMetadatasBeforeResetProto,
		},
		GaugeWithMetadatas: &metricpb.GaugeWithMetadatas{
			Gauge:     testGaugeBeforeResetProto,
			Metadatas: testMetadatasBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter:   testCounterAfterResetProto,
			Metadatas: testMetadatasAfterResetProto,
		},
		BatchTimerWithMetadatas: &metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimerAfterResetProto,
			Metadatas:  testMetadatasAfterResetProto,
		},
		GaugeWithMetadatas: &metricpb.GaugeWithMetadatas{
			Gauge:     testGaugeAfterResetProto,
			Metadatas: testMetadatasAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.CounterWithMetadatas.Counter.Id) > 0)
	require.True(t, cap(input.CounterWithMetadatas.Metadatas.Metadatas) > 0)
	require.True(t, cap(input.BatchTimerWithMetadatas.BatchTimer.Id) > 0)
	require.True(t, cap(input.BatchTimerWithMetadatas.Metadatas.Metadatas) > 0)
	require.True(t, cap(input.GaugeWithMetadatas.Gauge.Id) > 0)
	require.True(t, cap(input.GaugeWithMetadatas.Metadatas.Metadatas) > 0)
}
