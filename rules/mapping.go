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
	"errors"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
)

var (
	errNilMappingRuleSchema        = errors.New("nil mapping rule schema")
	errNilMappingRuleChangesSchema = errors.New("nil mapping rule changes schema")
)

// mappingRule defines a rule such that if a metric matches the provided filters,
// it is aggregated and retained under the provided set of policies.
type mappingRule struct {
	name       string
	tombstoned bool
	cutoverNs  int64
	filter     filters.Filter
	policies   []policy.Policy
}

func newMappingRule(
	r *schema.MappingRule,
	iterfn filters.NewSortedTagIteratorFn,
) (*mappingRule, error) {
	if r == nil {
		return nil, errNilMappingRuleSchema
	}
	policies, err := policy.NewPoliciesFromSchema(r.Policies)
	if err != nil {
		return nil, err
	}
	filter, err := filters.NewTagsFilter(r.TagFilters, iterfn, filters.Conjunction)
	if err != nil {
		return nil, err
	}
	return &mappingRule{
		name:       r.Name,
		tombstoned: r.Tombstoned,
		cutoverNs:  r.CutoverTime,
		filter:     filter,
		policies:   policies,
	}, nil
}

// mappingRuleChanges stores mapping rule changes.
type mappingRuleChanges struct {
	uuid    string
	changes []*mappingRule
}

func newMappingRuleChanges(
	mc *schema.MappingRuleChanges,
	iterfn filters.NewSortedTagIteratorFn,
) (*mappingRuleChanges, error) {
	if mc == nil {
		return nil, errNilMappingRuleChangesSchema
	}
	changes := make([]*mappingRule, 0, len(mc.Changes))
	for i := 0; i < len(mc.Changes); i++ {
		mr, err := newMappingRule(mc.Changes[i], iterfn)
		if err != nil {
			return nil, err
		}
		changes = append(changes, mr)
	}
	return &mappingRuleChanges{
		uuid:    mc.Uuid,
		changes: changes,
	}, nil
}

// ActiveRule returns the latest rule whose cutover time is earlier than or
// equal to t, or nil if not found.
func (mc *mappingRuleChanges) ActiveRule(t time.Time) *mappingRule {
	idx := mc.activeIndex(t)
	if idx < 0 {
		return nil
	}
	return mc.changes[idx]
}

// ActiveRules returns the rule that's in effect at time t and all future
// rules after time t.
func (mc *mappingRuleChanges) ActiveRules(t time.Time) *mappingRuleChanges {
	idx := mc.activeIndex(t)
	// If there are no rules that are currently in effect, it means either all
	// rules are in the future, or there are no rules.
	if idx < 0 {
		return mc
	}
	return &mappingRuleChanges{uuid: mc.uuid, changes: mc.changes[idx:]}
}

func (mc *mappingRuleChanges) activeIndex(t time.Time) int {
	target := t.UnixNano()
	idx := 0
	for idx < len(mc.changes) && mc.changes[idx].cutoverNs <= target {
		idx++
	}
	idx--
	return idx
}
