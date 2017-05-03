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

package rules

import (
	"testing"
	"time"

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestMatchResult(t *testing.T) {
	var (
		cutoverNs  = int64(12345)
		expireAtNs = int64(67890)
		mappings   = policy.PoliciesList{
			policy.NewStagedPolicies(
				cutoverNs,
				false,
				[]policy.Policy{
					policy.NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			),
		}
		rollups = []RollupResult{
			{
				ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
				PoliciesList: policy.PoliciesList{
					policy.NewStagedPolicies(
						cutoverNs,
						false,
						[]policy.Policy{
							policy.NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
							policy.NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
							policy.NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
						},
					),
				},
			},
			{
				ID:           b("rName2|rtagName1=rtagValue1"),
				PoliciesList: policy.PoliciesList{policy.NewStagedPolicies(cutoverNs, false, nil)},
			},
		}
	)

	res := NewMatchResult(expireAtNs, mappings, rollups)
	require.False(t, res.HasExpired(time.Unix(0, 0)))
	require.True(t, res.HasExpired(time.Unix(0, 100000)))

	require.Equal(t, mappings, res.MappingsAt(time.Unix(0, 0)))

	var (
		expectedRollupIDs = [][]byte{
			b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
			b("rName2|rtagName1=rtagValue1"),
		}
		expectedRollupPolicies = policy.PoliciesList{
			rollups[0].PoliciesList[0],
			rollups[1].PoliciesList[0],
		}
		rollupIDs      [][]byte
		rollupPolicies policy.PoliciesList
	)
	require.Equal(t, 2, res.NumRollups())
	for i := 0; i < 2; i++ {
		rollup := res.RollupsAt(i, time.Unix(0, 0))
		rollupIDs = append(rollupIDs, rollup.ID)
		rollupPolicies = append(rollupPolicies, rollup.PoliciesList...)
	}
	require.Equal(t, expectedRollupIDs, rollupIDs)
	require.Equal(t, expectedRollupPolicies, rollupPolicies)
}
