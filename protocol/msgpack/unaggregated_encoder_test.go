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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	errTestVarint   = errors.New("test varint error")
	errTestFloat64  = errors.New("test float64 error")
	errTestBytes    = errors.New("test bytes error")
	errTestArrayLen = errors.New("test array len error")
)

func TestUnaggregatedEncodeCounterWithDefaultPolicies(t *testing.T) {
	policies := testDefaultStagedPolicies
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncode(t, encoder, testCounter, policies))
	expected := expectedResultsForUnaggregatedMetricWithPolicies(t, testCounter, policies)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeBatchTimerWithDefaultPolicies(t *testing.T) {
	policies := testDefaultStagedPolicies
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncode(t, encoder, testBatchTimer, policies))
	expected := expectedResultsForUnaggregatedMetricWithPolicies(t, testBatchTimer, policies)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeGaugeWithDefaultPolicies(t *testing.T) {
	policies := testDefaultStagedPolicies
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncode(t, encoder, testGauge, policies))
	expected := expectedResultsForUnaggregatedMetricWithPolicies(t, testGauge, policies)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeAllTypesWithDefaultPolicies(t *testing.T) {
	var expected []interface{}
	encoder, results := testCapturingUnaggregatedEncoder(t)
	for _, input := range testInputWithAllTypesAndDefaultPolicies {
		require.NoError(t, testUnaggregatedEncode(t, encoder, input.metric, input.policiesList))
		expected = append(expected, expectedResultsForUnaggregatedMetricWithPolicies(t, input.metric, input.policiesList)...)
	}

	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeAllTypesWithSingleCustomPolicies(t *testing.T) {
	var expected []interface{}
	encoder, results := testCapturingUnaggregatedEncoder(t)
	for _, input := range testInputWithAllTypesAndSingleCustomPolicies {
		require.NoError(t, testUnaggregatedEncode(t, encoder, input.metric, input.policiesList))
		expected = append(expected, expectedResultsForUnaggregatedMetricWithPolicies(t, input.metric, input.policiesList)...)
	}

	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeVarintError(t *testing.T) {
	counter := testCounter
	policies := testDefaultStagedPolicies

	// Intentionally return an error when encoding varint.
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeVarintFn = func(value int64) {
		baseEncoder.encodeErr = errTestVarint
	}

	// Assert the error is expected.
	require.Equal(t, errTestVarint, testUnaggregatedEncode(t, encoder, counter, policies))

	// Assert re-encoding doesn't change the error.
	require.Equal(t, errTestVarint, testUnaggregatedEncode(t, encoder, counter, policies))
}

func TestUnaggregatedEncodeFloat64Error(t *testing.T) {
	gauge := testGauge
	policies := testDefaultStagedPolicies

	// Intentionally return an error when encoding float64.
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeFloat64Fn = func(value float64) {
		baseEncoder.encodeErr = errTestFloat64
	}

	// Assert the error is expected.
	require.Equal(t, errTestFloat64, testUnaggregatedEncode(t, encoder, gauge, policies))

	// Assert re-encoding doesn't change the error.
	require.Equal(t, errTestFloat64, testUnaggregatedEncode(t, encoder, gauge, policies))
}

func TestUnaggregatedEncodeBytesError(t *testing.T) {
	timer := testBatchTimer
	policies := testDefaultStagedPolicies

	// Intentionally return an error when encoding array length.
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeBytesFn = func(value []byte) {
		baseEncoder.encodeErr = errTestBytes
	}

	// Assert the error is expected.
	require.Equal(t, errTestBytes, testUnaggregatedEncode(t, encoder, timer, policies))

	// Assert re-encoding doesn't change the error.
	require.Equal(t, errTestBytes, testUnaggregatedEncode(t, encoder, timer, policies))
}

func TestUnaggregatedEncodeArrayLenError(t *testing.T) {
	gauge := testGauge
	policies := policy.PoliciesList{
		policy.NewStagedPolicies(
			time.Now().UnixNano(),
			false,
			[]policy.Policy{
				policy.NewPolicy(time.Second, xtime.Second, time.Hour),
			},
		),
	}

	// Intentionally return an error when encoding array length.
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeArrayLenFn = func(value int) {
		baseEncoder.encodeErr = errTestArrayLen
	}

	// Assert the error is expected.
	require.Equal(t, errTestArrayLen, testUnaggregatedEncode(t, encoder, gauge, policies))

	// Assert re-encoding doesn't change the error.
	require.Equal(t, errTestArrayLen, testUnaggregatedEncode(t, encoder, gauge, policies))
}

func TestUnaggregatedEncoderReset(t *testing.T) {
	metric := testCounter
	policies := testDefaultStagedPolicies

	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeErr = errTestVarint
	require.Equal(t, errTestVarint, testUnaggregatedEncode(t, encoder, metric, policies))

	encoder.Reset(NewBufferedEncoder())
	require.NoError(t, testUnaggregatedEncode(t, encoder, metric, policies))
}

func testCapturingUnaggregatedEncoder(t *testing.T) (UnaggregatedEncoder, *[]interface{}) {
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	result := testCapturingBaseEncoder(encoder.encoderBase)
	return encoder, result
}

func expectedResultsForPolicy(t *testing.T, p policy.Policy) []interface{} {
	results := []interface{}{numFieldsForType(policyType)}

	resolutionValue, err := policy.ValueFromResolution(p.Resolution())
	if err == nil {
		results = append(results, []interface{}{
			numFieldsForType(knownResolutionType),
			int64(knownResolutionType),
			int64(resolutionValue),
		}...)
	} else {
		results = append(results, []interface{}{
			numFieldsForType(unknownResolutionType),
			int64(unknownResolutionType),
			int64(p.Resolution().Window),
			int64(p.Resolution().Precision),
		}...)
	}

	retentionValue, err := policy.ValueFromRetention(p.Retention())
	if err == nil {
		results = append(results, []interface{}{
			numFieldsForType(knownRetentionType),
			int64(knownRetentionType),
			int64(retentionValue),
		}...)
	} else {
		results = append(results, []interface{}{
			numFieldsForType(unknownRetentionType),
			int64(unknownRetentionType),
			int64(p.Retention()),
		}...)
	}

	return results
}

func expectedResultsForStagedPolicies(t *testing.T, sp policy.StagedPolicies) []interface{} {
	policies := sp.Policies()
	results := []interface{}{
		numFieldsForType(stagedPoliciesType),
		sp.CutoverNs,
		sp.Tombstoned,
		len(policies),
	}
	for _, p := range policies {
		results = append(results, expectedResultsForPolicy(t, p)...)
	}
	return results
}

func expectedResultsForPoliciesList(t *testing.T, pl policy.PoliciesList) []interface{} {
	if pl.IsDefault() {
		return []interface{}{
			numFieldsForType(defaultPoliciesListType),
			int64(defaultPoliciesListType),
		}
	}
	results := []interface{}{
		numFieldsForType(customPoliciesListType),
		int64(customPoliciesListType),
		len(pl),
	}
	for _, sp := range pl {
		results = append(results, expectedResultsForStagedPolicies(t, sp)...)
	}
	return results
}

func expectedResultsForUnaggregatedMetricWithPolicies(
	t *testing.T,
	m unaggregated.MetricUnion,
	pl policy.PoliciesList,
) []interface{} {
	results := []interface{}{
		int64(unaggregatedVersion),
		numFieldsForType(rootObjectType),
	}

	switch m.Type {
	case unaggregated.CounterType:
		results = append(results, []interface{}{
			int64(counterWithPoliciesListType),
			numFieldsForType(counterWithPoliciesListType),
			numFieldsForType(counterType),
			[]byte(m.ID),
			m.CounterVal,
		}...)
	case unaggregated.BatchTimerType:
		results = append(results, []interface{}{
			int64(batchTimerWithPoliciesListType),
			numFieldsForType(batchTimerWithPoliciesListType),
			numFieldsForType(batchTimerType),
			[]byte(m.ID),
			len(m.BatchTimerVal),
		}...)
		for _, v := range m.BatchTimerVal {
			results = append(results, v)
		}
	case unaggregated.GaugeType:
		results = append(results, []interface{}{
			int64(gaugeWithPoliciesListType),
			numFieldsForType(gaugeWithPoliciesListType),
			numFieldsForType(gaugeType),
			[]byte(m.ID),
			m.GaugeVal,
		}...)
	default:
		require.Fail(t, fmt.Sprintf("unrecognized metric type %v", m.Type))
	}

	plRes := expectedResultsForPoliciesList(t, pl)
	results = append(results, plRes...)

	return results
}
