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
	"bytes"
	"sort"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
)

// RollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be grouped and rolled up across the provided set of tags, named
// with the provided name, and aggregated and retained under the provided policies
type RollupTarget struct {
	Name     []byte          // name of the rollup metric
	Tags     [][]byte        // a set of sorted tags rollups are performed on
	Policies []policy.Policy // defines how the rollup metric is aggregated and retained
}

func newRollupTarget(target *schema.RollupTarget) (RollupTarget, error) {
	policies, err := policy.NewPoliciesFromSchema(target.Policies)
	if err != nil {
		return emptyRollupTarget, err
	}

	tags := make([]string, len(target.Tags))
	copy(tags, target.Tags)
	sort.Strings(tags)

	return RollupTarget{
		Name:     []byte(target.Name),
		Tags:     bytesArrayFromStringArray(tags),
		Policies: policies,
	}, nil
}

// sameTransform determines whether two targets have the same transformation
func (t RollupTarget) sameTransform(other RollupTarget) bool {
	if !bytes.Equal(t.Name, other.Name) {
		return false
	}
	if len(t.Tags) != len(other.Tags) {
		return false
	}
	for i := 0; i < len(t.Tags); i++ {
		if !bytes.Equal(t.Tags[i], other.Tags[i]) {
			return false
		}
	}
	return true
}

// clone clones a rollup target
func (t RollupTarget) clone() RollupTarget {
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return RollupTarget{
		Name:     t.Name,
		Tags:     bytesArrayCopy(t.Tags),
		Policies: policies,
	}
}
