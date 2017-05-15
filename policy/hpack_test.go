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
	"time"

	"github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

func TestBitflag(t *testing.T) {
	bf := NewBitflag().Set(0).
		Set(1).
		Set(4)

	var expected uint64 = 19
	require.Equal(t, expected, uint64(bf))

	require.Panics(t, func() {
		bf.Set(64)
	})
}

func TestBitflagIterator(t *testing.T) {
	bf := NewBitflag().Set(0).
		Set(1).
		Set(4).
		Set(11).
		Set(37)
	iter := bf.Iterator()

	tests := []struct {
		expectedNext  bool
		expectedIndex int
	}{
		{true, 0},
		{true, 1},
		{true, 4},
		{true, 11},
		{true, 37},
		{false, -1},
	}

	for _, test := range tests {
		next := iter.Next()
		require.Equal(t, test.expectedNext, next)
		if next {
			require.Equal(t, test.expectedIndex, iter.Index())
		}
	}
}

func TestEncoder(t *testing.T) {
	var (
		enc          = NewEncoder()
		firstPolicy  = NewPolicy(time.Second, xtime.Second, time.Hour)
		secondPolicy = NewPolicy(10*time.Second, xtime.Second, 24*time.Hour)
		thirdPolicy  = NewPolicy(time.Minute, xtime.Minute, 24*7*time.Hour)
		buffer       = make([]Policy, 0)
	)

	tests := []struct {
		policies, expectedPolicies []Policy
		expectedBitflag            Bitflag
	}{
		{
			// New policies should be returned in the slice of policies.
			policies:         []Policy{firstPolicy, secondPolicy},
			expectedPolicies: []Policy{firstPolicy, secondPolicy},
			expectedBitflag:  NewBitflag(),
		},
		{
			// Since these policies have been seen already they should
			// now be encoded in the bitflag.
			policies:         []Policy{firstPolicy, secondPolicy},
			expectedPolicies: []Policy{},
			expectedBitflag:  NewBitflag().Set(0).Set(1),
		},
		{
			// The old policies should still be encoded in the bitflag but
			// the new policy should be returned in the slice of policies.
			policies:         []Policy{firstPolicy, secondPolicy, thirdPolicy},
			expectedPolicies: []Policy{thirdPolicy},
			expectedBitflag:  NewBitflag().Set(0).Set(1),
		},
		{
			// All policies have been seen at least once before and so they
			// should all be encoded in the bitflag.
			policies:         []Policy{firstPolicy, secondPolicy, thirdPolicy},
			expectedPolicies: []Policy{},
			expectedBitflag:  NewBitflag().Set(0).Set(1).Set(2),
		},
		{
			// Removed policies should not be included in the returned bitflag.
			policies:         []Policy{firstPolicy, thirdPolicy},
			expectedPolicies: []Policy{},
			expectedBitflag:  NewBitflag().Set(0).Set(2),
		},
	}

	for _, test := range tests {
		buffer = buffer[:0]
		actualBitflag, actualPolicies := enc.Encode(test.policies, buffer)
		require.Equal(t, test.expectedBitflag, actualBitflag)
		require.Equal(t, test.expectedPolicies, actualPolicies)
	}
}

func TestDecoder(t *testing.T) {
	var (
		dec          = NewDecoder()
		firstPolicy  = NewPolicy(time.Second, xtime.Second, time.Hour)
		secondPolicy = NewPolicy(10*time.Second, xtime.Second, 24*time.Hour)
		thirdPolicy  = NewPolicy(time.Minute, xtime.Minute, 24*7*time.Hour)
	)

	tests := []struct {
		policies, expectedPolicies []Policy
		bitflag, expectedBitflag   Bitflag
	}{
		{
			// New policies should be added to returned bitflag.
			policies:         []Policy{firstPolicy, secondPolicy},
			expectedPolicies: []Policy{firstPolicy, secondPolicy},
			bitflag:          NewBitflag(),
			expectedBitflag:  NewBitflag().Set(0).Set(1),
		},
		{
			// Policies that were seen previously should be decoded from
			// the bitflag.
			policies:         []Policy{},
			expectedPolicies: []Policy{firstPolicy, secondPolicy},
			bitflag:          NewBitflag().Set(0).Set(1),
			expectedBitflag:  NewBitflag().Set(0).Set(1),
		},
		{
			// New policies should be returned in the bitflag in addition
			// to any old policies.
			policies:         []Policy{thirdPolicy},
			expectedPolicies: []Policy{thirdPolicy, firstPolicy, secondPolicy},
			bitflag:          NewBitflag().Set(0).Set(1),
			expectedBitflag:  NewBitflag().Set(0).Set(1).Set(2),
		},
		{
			// All policies should now be decoded from the bitflag.
			policies:         []Policy{},
			expectedPolicies: []Policy{firstPolicy, secondPolicy, thirdPolicy},
			bitflag:          NewBitflag().Set(0).Set(1).Set(2),
			expectedBitflag:  NewBitflag().Set(0).Set(1).Set(2),
		},
		{
			// Removed policies should not be returned.
			policies:         []Policy{},
			expectedPolicies: []Policy{firstPolicy, thirdPolicy},
			bitflag:          NewBitflag().Set(0).Set(2),
			expectedBitflag:  NewBitflag().Set(0).Set(2),
		},
	}

	for _, test := range tests {
		actualBitflag, actualPolicies, err := dec.Decode(test.policies, test.bitflag)
		require.NoError(t, err)
		require.Equal(t, test.expectedBitflag, actualBitflag)
		require.Equal(t, test.expectedPolicies, actualPolicies)
	}
}

func TestDecodeError(t *testing.T) {
	var (
		dec      = NewDecoder()
		policies = make([]Policy, 0)
		bitflag  = NewBitflag().Set(0)
	)

	// Decoder should return an error when encountering a bitflag it has
	// never seen before.
	_, _, err := dec.Decode(policies, bitflag)
	require.Error(t, err)
}

func TestRoundTrip(t *testing.T) {
	var (
		enc          = NewEncoder()
		dec          = NewDecoder()
		firstPolicy  = NewPolicy(time.Second, xtime.Second, time.Hour)
		secondPolicy = NewPolicy(10*time.Second, xtime.Second, 24*time.Hour)
		thirdPolicy  = NewPolicy(time.Minute, xtime.Minute, 24*7*time.Hour)
		buffer       = make([]Policy, 0)
	)

	tests := []struct {
		policies, expectedPolicies []Policy
		expectedBitflag            Bitflag
	}{
		{
			policies:         []Policy{firstPolicy, secondPolicy},
			expectedPolicies: []Policy{firstPolicy, secondPolicy},
			expectedBitflag:  NewBitflag().Set(0).Set(1),
		},
		{
			// Add a new policy.
			policies:         []Policy{firstPolicy, secondPolicy, thirdPolicy},
			expectedPolicies: []Policy{thirdPolicy, firstPolicy, secondPolicy},
			expectedBitflag:  NewBitflag().Set(0).Set(1).Set(2),
		},
		{
			// Remove a policy.
			policies:         []Policy{firstPolicy, thirdPolicy},
			expectedPolicies: []Policy{firstPolicy, thirdPolicy},
			expectedBitflag:  NewBitflag().Set(0).Set(2),
		},
	}

	for _, test := range tests {
		buffer = buffer[:0]
		bitflag, policies := enc.Encode(test.policies, buffer)
		actualBitflag, actualPolicies, err := dec.Decode(policies, bitflag)
		require.NoError(t, err)
		require.Equal(t, test.expectedBitflag, actualBitflag)
		require.Equal(t, test.expectedPolicies, actualPolicies)
	}
}
