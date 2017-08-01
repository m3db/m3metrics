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

package handlers

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/pborman/uuid"
)

var (
	errMultipleMatches = errors.New("more than one match found")
	errRuleSetExists   = errors.New("ruleset already exists")
	errNoPolicies      = errors.New("no policies provided")
	errNoTargets       = errors.New("no targets provided")
	errNoFilters       = errors.New("no filters provided")
)

//RollupTarget is a representation of rules.RollupTarget where the policies are
//still in strings format.
type RollupTarget struct {
	Name     string   `json:"name"`
	Tags     []string `json:"tags"`
	Policies []string `json:"policies"`
}

// TODO(dgromov): Return aggregate errors instead of the first one.
func parsePolicies(policies []string) ([]policy.Policy, error) {
	if len(policies) == 0 {
		return nil, errNoPolicies
	}

	result := make([]policy.Policy, len(policies))
	for i, p := range policies {
		parsed, err := policy.ParsePolicy(p)
		if err != nil {
			return nil, fmt.Errorf("Cannot parse: %s. %v", p, err)
		}
		result[i] = parsed
	}
	return result, nil
}

func parseRollupTargets(rts []RollupTarget) ([]rules.RollupTarget, error) {
	if len(rts) == 0 {
		return nil, errNoTargets
	}

	result := make([]rules.RollupTarget, len(rts))
	for i, rt := range rts {
		if len(rt.Name) == 0 {
			return nil, fmt.Errorf("Cannot parse targets. %v", errNoNameGiven)
		}

		if len(rt.Tags) == 0 {
			return nil, fmt.Errorf("Cannot parse target %s. Must provide tags", rt.Name)
		}

		parsedPolicies, err := parsePolicies(rt.Policies)
		if err != nil {
			return nil, err
		}

		result[i] = rules.NewRollupTargetFromFields(rt.Name, rt.Tags, parsedPolicies)
	}
	return result, nil
}

// AddMappingRule ...
func (h *Handler) AddMappingRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	ruleName string,
	filters map[string]string,
	policies []string,
) error {
	if len(ruleName) == 0 {
		return errNoNameGiven
	}

	if len(filters) == 0 {
		return errNoFilters
	}

	parsedPolicies, err := parsePolicies(policies)
	if err != nil {
		return err
	}

	// TODO(dgromov): Handle resurected rule
	if err := rs.AddMappingRule(ruleName, filters, parsedPolicies, h.opts.PropagationDelay); err != nil {
		return fmt.Errorf("Failed to add Mapping Rule. %v", err)
	}

	return nil
}

// UpdateMappingRule ...
func (h *Handler) UpdateMappingRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	originalRuleName string,
	newRuleName string,
	filters map[string]string,
	policies []string,
) error {
	parsedPolicies, err := parsePolicies(policies)
	if err != nil {
		return err
	}
	if err := rs.UpdateMappingRule(originalRuleName, newRuleName, filters, parsedPolicies, h.opts.PropagationDelay); err != nil {
		return err
	}

	return nil
}

// DeleteMappingRule ...
func (h *Handler) DeleteMappingRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	ruleName string,
) error {
	if err := rs.DeleteMappingRule(ruleName, h.opts.PropagationDelay); err != nil {
		return err
	}

	return nil
}

// AddRollupRule ...
func (h *Handler) AddRollupRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	ruleName string,
	filters map[string]string,
	rollupTargets []RollupTarget,
) error {
	if len(ruleName) == 0 {
		return errNoNameGiven
	}
	if len(filters) == 0 {
		return errNoFilters
	}

	parsedPolicies, err := parseRollupTargets(rollupTargets)
	if err != nil {
		return err
	}

	if err := rs.AddRollupRule(ruleName, filters, parsedPolicies, h.opts.PropagationDelay); err != nil {
		return fmt.Errorf("Failed to add Rollup Rule. %v", err)
	}

	return nil
}

// UpdateRollupRule ...
func (h *Handler) UpdateRollupRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	originalRuleName string,
	newRuleName string,
	filters map[string]string,
	rollupTargets []RollupTarget,
) error {
	parsedPolicies, err := parseRollupTargets(rollupTargets)
	if err != nil {
		return err
	}
	if err := rs.UpdateRollupRule(originalRuleName, newRuleName, filters, parsedPolicies, h.opts.PropagationDelay); err != nil {
		return err
	}
	return nil
}

// DeleteRollupRule ...
func (h *Handler) DeleteRollupRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	ruleName string,
) error {
	if err := rs.DeleteRollupRule(ruleName, h.opts.PropagationDelay); err != nil {
		return err
	}
	return nil
}

// RuleSet returns the version and the persisted ruleset data in kv store.
func (h Handler) RuleSet(nsName string) (rules.RuleSet, error) {
	ruleSetKey := h.RuleSetKey(nsName)
	value, err := h.store.Get(ruleSetKey)

	if err != nil {
		return nil, err
	}
	version := value.Version()
	var ruleSet schema.RuleSet
	if err := value.Unmarshal(&ruleSet); err != nil {
		return nil, fmt.Errorf("Could not fetch RuleSet %s: %v", nsName, err.Error())
	}
	rs, err := rules.NewRuleSetFromSchema(version, &ruleSet, rules.NewOptions())
	if err != nil {
		return nil, fmt.Errorf("Could not fetch RuleSet %s: %v", nsName, err.Error())
	}
	return rs, err
}

// ValidateRuleSet validates that a Ruleset is active.
func (h Handler) ValidateRuleSet(rs rules.RuleSet) error {
	if rs.Tombstoned() {
		rsKey := h.RuleSetKey(string(rs.Namespace()))
		return fmt.Errorf("ruleset %s is tombstoned", rsKey)
	}
	return nil
}

// Add a blank ruleset
func (h Handler) initRuleSet(nsName string) (rules.RuleSet, error) {
	existing, err := h.RuleSet(nsName)
	if err != nil {
		if err != kv.ErrNotFound {
			return nil, err
		}
	}
	now := time.Now().UnixNano()

	if err == kv.ErrNotFound {
		rs, err := rules.NewRuleSetFromSchema(0, &schema.RuleSet{
			Uuid:          uuid.New(),
			Namespace:     nsName,
			CreatedAt:     now,
			LastUpdatedAt: now,
			Tombstoned:    false,
			CutoverTime:   now,
		}, rules.NewOptions())

		return rs, err
	}
	if err := h.ValidateRuleSet(existing); err == nil {
		return nil, errRuleSetExists
	}

	err = existing.Revive(h.opts.PropagationDelay)
	if err != nil {
		return nil, err
	}

	return existing, nil
}
