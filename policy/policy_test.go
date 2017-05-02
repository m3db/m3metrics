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

package policy

import (
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoliciesByResolutionAsc(t *testing.T) {
	inputs := []Policy{
		NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
		NewPolicy(10*time.Second, xtime.Second, 2*time.Hour),
		NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
		NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		NewPolicy(time.Minute, xtime.Minute, time.Hour),
		NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewPolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
	}
	expected := []Policy{inputs[2], inputs[0], inputs[1], inputs[5], inputs[4], inputs[3], inputs[6]}
	sort.Sort(ByResolutionAsc(inputs))
	require.Equal(t, expected, inputs)
}

func TestDefaultVersionedPolicies(t *testing.T) {
	var (
		version = 2
		cutover = time.Now()
	)
	vp := DefaultVersionedPolicies(version, cutover)
	require.Equal(t, version, vp.Version)
	require.Equal(t, cutover, vp.Cutover)
	require.True(t, vp.IsDefault())
	require.Equal(t, defaultPolicies, vp.Policies())
}

func TestCustomVersionedPolicies(t *testing.T) {
	var (
		version  = 2
		cutover  = time.Now()
		policies = []Policy{
			NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
			NewPolicy(10*time.Second, xtime.Second, 2*time.Hour),
		}
	)
	vp := CustomVersionedPolicies(version, cutover, policies)
	require.Equal(t, version, vp.Version)
	require.Equal(t, cutover, vp.Cutover)
	require.False(t, vp.IsDefault())
	require.Equal(t, policies, vp.Policies())
}

func TestNewPolicyFromSchema(t *testing.T) {
	tests := []struct {
		policy         *schema.Policy
		expectedPolicy Policy
		expectedErr    bool
	}{
		// Test case for nil policy.
		{
			policy:      nil,
			expectedErr: true,
		},
		// Test case for nil retention.
		{
			policy: &schema.Policy{
				Resolution: &schema.Resolution{WindowSize: 100, Precision: 10},
				Retention:  nil,
			},
			expectedErr: true,
		},
		// Test case for nil resolution.
		{
			policy: &schema.Policy{
				Resolution: nil,
				Retention:  &schema.Retention{Period: 20},
			},
			expectedErr: true,
		},
		// Test case for invalid precision.
		{
			policy: &schema.Policy{
				Resolution: &schema.Resolution{WindowSize: 100, Precision: 42},
				Retention:  &schema.Retention{Period: 20},
			},
			expectedErr: true,
		},
		// Test case for a valid policy.
		{
			policy: &schema.Policy{
				Resolution: &schema.Resolution{WindowSize: 100, Precision: int64(time.Second)},
				Retention:  &schema.Retention{Period: 20},
			},
			expectedPolicy: Policy{
				resolution: Resolution{Window: 100, Precision: xtime.Second},
				retention:  20,
			},
			expectedErr: false,
		},
	}

	for _, test := range tests {
		actualPolicy, actualErr := NewPolicyFromSchema(test.policy)
		if test.expectedErr {
			assert.Error(t, actualErr)
			continue
		}

		assert.NoError(t, actualErr)
		assert.Equal(t, test.expectedPolicy, actualPolicy)
	}
}

func TestNewPoliciesFromSchema(t *testing.T) {
	tests := []struct {
		policies         []*schema.Policy
		expectedPolicies []Policy
		expectedErr      bool
	}{
		// Test case for invalid precision.
		{
			policies: []*schema.Policy{
				&schema.Policy{
					Resolution: &schema.Resolution{WindowSize: 100, Precision: 42},
					Retention:  &schema.Retention{Period: 20},
				},
			},
			expectedErr: true,
		},
		// Test case for valid policies.
		{
			policies: []*schema.Policy{
				&schema.Policy{
					Resolution: &schema.Resolution{WindowSize: 100, Precision: int64(time.Second)},
					Retention:  &schema.Retention{Period: 20},
				},
			},
			expectedPolicies: []Policy{
				Policy{
					resolution: Resolution{Window: 100, Precision: xtime.Second},
					retention:  20,
				},
			},
			expectedErr: false,
		},
	}

	for _, test := range tests {
		actualPolicies, actualErr := NewPoliciesFromSchema(test.policies)
		if test.expectedErr {
			assert.Error(t, actualErr)
			continue
		}

		assert.NoError(t, actualErr)
		assert.Equal(t, test.expectedPolicies, actualPolicies)
	}
}
