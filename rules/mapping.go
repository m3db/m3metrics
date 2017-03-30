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
	name          string
	tombstoned    bool
	cutoverTimeNs int64
	filter        filters.Filter
	policies      []policy.Policy // defines how the metrics should be aggregated and retained
}

func newMappingRule(
	r *schema.MappingRule,
	iterfn filters.NewSortedTagIteratorFn,
) (*mappingRule, error) {
	policies, err := policy.NewPoliciesFromSchema(r.Policies)
	if err != nil {
		return nil, err
	}
	filter, err := filters.NewTagsFilter(r.TagFilters, iterfn, filters.Conjunction)
	if err != nil {
		return nil, err
	}
	return &mappingRule{
		name:          r.Name,
		tombstoned:    r.Tombstoned,
		cutoverTimeNs: r.CutoverTime,
		filter:        filter,
		policies:      policies,
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
