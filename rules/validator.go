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
	"fmt"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
)

// Validator validates a ruleset.
type Validator interface {
	// Validate validates a ruleset.
	Validate(rs RuleSet) error
}

type validator struct {
	opts ValidatorOptions
}

// NewValidator creates a new validator.
func NewValidator(opts ValidatorOptions) Validator {
	return &validator{opts: opts}
}

func (v *validator) Validate(rs RuleSet) error {
	// Only the latest (a.k.a. the first) view needs to be validated
	// because that is the view that may be invalid due to latest update.
	latest, err := rs.Latest()
	if err != nil {
		return NewValidationError(fmt.Sprintf("could not get the latest ruleset snapshot: %v", err))
	}
	if err := v.validateMappingRules(latest.MappingRules); err != nil {
		return NewValidationError(fmt.Sprintf("could not validate mapping rules: %v", err))
	}
	if err := v.validateRollupRules(latest.RollupRules); err != nil {
		return NewValidationError(fmt.Sprintf("could not validate rollup rules: %v", err))
	}
	return nil
}

func (v *validator) validateMappingRules(rules map[string]*MappingRuleView) error {
	namesSeen := make(map[string]struct{}, len(rules))
	for _, view := range rules {
		// Validate that no rules with the same name exist.
		if _, exists := namesSeen[view.Name]; exists {
			return NewRuleConflictError(fmt.Sprintf("mapping rule %s already exists", view.Name))
		}
		namesSeen[view.Name] = struct{}{}

		// Validate that the filter is valid.
		if err := v.validateFilters(view.Filters); err != nil {
			return err
		}

		// Validate that the policies are valid.
		t := v.opts.MetricTypeFn()(view.Filters)
		for _, p := range view.Policies {
			if err := v.validatePolicy(t, p); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *validator) validateRollupRules(rules map[string]*RollupRuleView) error {
	namesSeen := make(map[string]struct{}, len(rules))
	for _, view := range rules {
		// Validate that no rules with the same name exist.
		if _, exists := namesSeen[view.Name]; exists {
			return NewRuleConflictError(fmt.Sprintf("rollup rule %s already exists", view.Name))
		}
		namesSeen[view.Name] = struct{}{}

		// Validate that the filter is valid.
		if err := v.validateFilters(view.Filters); err != nil {
			return err
		}

		// Validate that the policies are valid.
		t := v.opts.MetricTypeFn()(view.Filters)
		for _, target := range view.Targets {
			for _, p := range target.Policies {
				if err := v.validatePolicy(t, p); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (v *validator) validateFilters(f map[string]string) error {
	for _, filter := range f {
		// Validating the filter expression by actually constructing the filter.
		if _, err := filters.NewFilter([]byte(filter)); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validatePolicy(t metric.Type, p policy.Policy) error {
	// Validate storage policy.
	if !v.opts.IsAllowedStoragePolicyFor(t, p.StoragePolicy) {
		return fmt.Errorf("storage policy %v is not allowed for metric type %v", p.StoragePolicy, t)
	}

	// Validate aggregation function.
	if isDefaultAggFn := p.AggregationID.IsDefault(); isDefaultAggFn {
		return nil
	}
	aggTypes, err := p.AggregationID.AggregationTypes()
	if err != nil {
		return err
	}
	for _, aggType := range aggTypes {
		if !v.opts.IsAllowedCustomAggregationTypeFor(t, aggType) {
			return fmt.Errorf("aggregation type %v is not allowed for metric type %v", aggType, t)
		}
	}

	return nil
}
