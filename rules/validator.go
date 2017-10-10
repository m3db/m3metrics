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
	"fmt"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
)

var (
	errEmptyMappingRuleViewList = errors.New("empty mapping rule view list")
	errEmptyRollupRuleViewList  = errors.New("empty rollup rule view list")
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
	latest, err := rs.Latest()

	// Validate mapping rules.
	mappingRules, err := rs.MappingRules()
	if err != nil {
		return err
	}
	if err := v.validateMappingRules(mappingRules); err != nil {
		return err
	}

	// Validate rollup rules.
	rollupRules, err := rs.RollupRules()
	if err != nil {
		return err
	}
	if err := v.validateRollupRules(rollupRules); err != nil {
		return err
	}

	return nil
}

func (v *validator) validateMappingRules(rules MappingRules) error {
	namesSeen := make(map[string]struct{}, len(rules))
	for _, views := range rules {
		if len(views) == 0 {
			return errEmptyMappingRuleViewList
		}
		// Only the latest (a.k.a. the first) view needs to be validated
		// because that is the view that may be invalid due to latest update.
		view := views[0]
		if view.Tombstoned {
			continue
		}

		// Validate that no rules with the same name exist.
		if _, exists := namesSeen[view.Name]; exists {
			msg := fmt.Sprintf("mapping rule %s already exists", view.Name)
			return RuleConflictError{msg: msg, ConflictRuleUUID: view.ID}
		}
		namesSeen[view.Name] = struct{}{}

		// Validate that the filter is valid.
		if err := v.validateFilters(view.Filters); err != nil {
			return err
		}

		// Validate that the policies are valid.
		t := v.opts.MetricTypeFn()(view.Filters)
		for _, p := range view.Policies {
			if err := v.validatePolicy(p, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *validator) validateRollupRules(rules RollupRules) error {
	namesSeen := make(map[string]struct{}, len(rules))
	for _, views := range rules {
		if len(views) == 0 {
			return errEmptyRollupRuleViewList
		}
		// Only the latest (a.k.a. the first) view needs to be validated
		// because that is the view that may be invalid due to latest update.
		view := views[0]
		if view.Tombstoned {
			continue
		}

		// Validate that no rules with the same name exist.
		if _, exists := namesSeen[view.Name]; exists {
			msg := fmt.Sprintf("rollup rule %s already exists", view.Name)
			return RuleConflictError{msg: msg, ConflictRuleUUID: view.ID}
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
				if err := v.validatePolicy(p, t); err != nil {
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

func (v *validator) validatePolicy(p policy.Policy, t metric.Type) error {
	// Validate policy resolution.
	resolution := p.Resolution().Window
	if minResolution := v.opts.MinPolicyResolutionFor(t); resolution < minResolution {
		return fmt.Errorf("policy resolution %v is smaller than the minimum resolution supported (%v) for metric type %v", resolution, minResolution, t)
	}
	if maxResolution := v.opts.MaxPolicyResolutionFor(t); resolution > maxResolution {
		return fmt.Errorf("policy resolution %v is larger than the maximum resolution supported (%v) for metric type %v", resolution, maxResolution, t)
	}

	// Validate aggregation function.
	if isDefaultAggFn := p.AggregationID.IsDefault(); isDefaultAggFn {
		return nil
	}
	if customAggFnEnabled := v.opts.CustomAggregationFunctionEnabledFor(t); !customAggFnEnabled {
		return fmt.Errorf("policy %v contains unsupported custom aggregation function %v for metric type %v", p, p.AggregationID, t)
	}

	return nil
}
