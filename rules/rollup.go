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
	"errors"
	"sort"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
)

var (
	emptyRollupTarget rollupTarget

	errNilRollupTargetSchema      = errors.New("nil rollup target schema")
	errNilRollupRuleSchema        = errors.New("nil rollup rule schema")
	errNilRollupRuleChangesSchema = errors.New("nil rollup rule changes schema")
)

// rollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be grouped and rolled up across the provided set of tags, named
// with the provided name, and aggregated and retained under the provided policies.
type rollupTarget struct {
	Name     []byte
	Tags     [][]byte
	Policies []policy.Policy
}

func newRollupTarget(target *schema.RollupTarget) (rollupTarget, error) {
	if target == nil {
		return emptyRollupTarget, errNilRollupTargetSchema
	}
	policies, err := policy.NewPoliciesFromSchema(target.Policies)
	if err != nil {
		return emptyRollupTarget, err
	}
	tags := make([]string, len(target.Tags))
	copy(tags, target.Tags)
	sort.Strings(tags)
	return rollupTarget{
		Name:     []byte(target.Name),
		Tags:     bytesArrayFromStringArray(tags),
		Policies: policies,
	}, nil
}

// sameTransform determines whether two targets have the same transformation.
func (t *rollupTarget) sameTransform(other rollupTarget) bool {
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

// clone clones a rollup target.
func (t *rollupTarget) clone() rollupTarget {
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return rollupTarget{
		Name:     t.Name,
		Tags:     bytesArrayCopy(t.Tags),
		Policies: policies,
	}
}

// rollupRule defines a rule such that if a metric matches the provided filters,
// it is rolled up using the provided list of rollup targets.
type rollupRule struct {
	name       string
	tombstoned bool
	cutoverNs  int64
	filter     filters.Filter
	targets    []rollupTarget
}

func newRollupRule(
	r *schema.RollupRule,
	iterfn filters.NewSortedTagIteratorFn,
) (*rollupRule, error) {
	if r == nil {
		return nil, errNilRollupRuleSchema
	}
	targets := make([]rollupTarget, 0, len(r.Targets))
	for _, t := range r.Targets {
		target, err := newRollupTarget(t)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	filter, err := filters.NewTagsFilter(r.TagFilters, iterfn, filters.Conjunction)
	if err != nil {
		return nil, err
	}
	return &rollupRule{
		name:       r.Name,
		tombstoned: r.Tombstoned,
		cutoverNs:  r.CutoverTime,
		filter:     filter,
		targets:    targets,
	}, nil
}

// rollupRuleChanges stores rollup rule changes.
type rollupRuleChanges struct {
	uuid    string
	changes []*rollupRule
}

func newRollupRuleChanges(
	mc *schema.RollupRuleChanges,
	iterfn filters.NewSortedTagIteratorFn,
) (*rollupRuleChanges, error) {
	if mc == nil {
		return nil, errNilRollupRuleChangesSchema
	}
	changes := make([]*rollupRule, 0, len(mc.Changes))
	for i := 0; i < len(mc.Changes); i++ {
		mr, err := newRollupRule(mc.Changes[i], iterfn)
		if err != nil {
			return nil, err
		}
		changes = append(changes, mr)
	}
	return &rollupRuleChanges{
		uuid:    mc.Uuid,
		changes: changes,
	}, nil
}

// ActiveRule returns the latest rule whose cutover time is earlier than or
// equal to t, or nil if not found.
func (rc *rollupRuleChanges) ActiveRule(t time.Time) *rollupRule {
	idx := rc.activeIndex(t)
	if idx < 0 {
		return nil
	}
	return rc.changes[idx]
}

// ActiveRules returns the rule that's in effect at time t and all future
// rules after time t.
func (rc *rollupRuleChanges) ActiveRules(t time.Time) *rollupRuleChanges {
	idx := rc.activeIndex(t)
	if idx < 0 {
		return rc
	}
	return &rollupRuleChanges{uuid: rc.uuid, changes: rc.changes[idx:]}
}

func (rc *rollupRuleChanges) activeIndex(t time.Time) int {
	target := t.UnixNano()
	idx := 0
	for idx < len(rc.changes) && rc.changes[idx].cutoverNs <= target {
		idx++
	}
	idx--
	return idx
}

func bytesArrayFromStringArray(values []string) [][]byte {
	result := make([][]byte, len(values))
	for i, str := range values {
		result[i] = []byte(str)
	}
	return result
}

func bytesArrayCopy(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i, b := range values {
		result[i] = make([]byte, len(b))
		copy(result[i], b)
	}
	return result
}
