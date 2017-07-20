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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
)

var (
	errMultipleMatches    = errors.New("more than one match found")
	errInvalidCutoverTime = errors.New("cutover time is in the past")
)

//RollupTarget is a representation of rules.RollupTarget where the policies are
//still in strings format.
type RollupTarget struct {
	Name     string
	Tags     []string
	Policies []string
}

// NewRollupTarget ...
func NewRollupTarget(name string, tags []string, policies []string) RollupTarget {
	return RollupTarget{Name: name, Tags: tags, Policies: policies}
}

// TODO(dgromov): Return aggregate errors instead of the first one.
func parsePolicies(policies []string) ([]policy.Policy, error) {
	result := make([]policy.Policy, len(policies))
	for i, p := range policies {
		parsed, err := policy.ParsePolicy(p)
		if err != nil {
			return nil, err
		}
		result[i] = parsed
	}
	return result, nil
}

func parseRollupTargets(rts []RollupTarget) ([]rules.RollupTarget, error) {
	result := make([]rules.RollupTarget, len(rts))
	for i, rt := range rts {
		parsedPolicies, err := parsePolicies(rt.Policies)
		if err != nil {
			return nil, err
		}

		result[i] = rules.RollupTarget{
			Name:     rt.Name,
			Tags:     rt.Tags,
			Policies: parsedPolicies,
		}
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
	parsedPolicies, err := parsePolicies(policies)
	if err != nil {
		return err
	}

	if err := rs.AppendMappingRule(ruleName, filters, parsedPolicies, h.opts.PropagationDelay); err != nil {
		return err
	}

	if err := h.persistRuleSet(rs, nss); err != nil {
		return err
	}
	return nil
}

// UpdateMappingRule ...
func (h *Handler) UpdateMappingRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	ruleID string,
	ruleName string,
	filters map[string]string,
	policies []string,
) error {
	parsedPolicies, err := parsePolicies(policies)
	if err != nil {
		return err
	}
	if err := rs.UpdateMappingRule(ruleID, ruleName, filters, parsedPolicies, h.opts.PropagationDelay); err != nil {
		return err
	}
	if err := h.persistRuleSet(rs, nss); err != nil {
		return err
	}
	return nil
}

// DeleteMappingRule ...
func (h *Handler) DeleteMappingRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	ruleID string,
) error {
	if err := rs.DeleteMappingRule(ruleID, h.opts.PropagationDelay); err != nil {
		return err
	}
	if err := h.persistRuleSet(rs, nss); err != nil {
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
	parsedPolicies, err := parseRollupTargets(rollupTargets)
	if err != nil {
		return err
	}

	if err := rs.AppendRollupRule(ruleName, filters, parsedPolicies, h.opts.PropagationDelay); err != nil {
		return err
	}

	if err := h.persistRuleSet(rs, nss); err != nil {
		return err
	}
	return nil
}

// UpdateRollupRule ...
func (h *Handler) UpdateRollupRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	ruleID string,
	ruleName string,
	filters map[string]string,
	rollupTargets []RollupTarget,
) error {
	parsedPolicies, err := parseRollupTargets(rollupTargets)
	if err != nil {
		return err
	}
	if err := rs.UpdateRollupRule(ruleID, ruleName, filters, parsedPolicies, h.opts.PropagationDelay); err != nil {
		return err
	}
	if err := h.persistRuleSet(rs, nss); err != nil {
		return err
	}
	return nil
}

// DeleteRollupRule ...
func (h *Handler) DeleteRollupRule(
	rs rules.RuleSet,
	nss *rules.Namespaces,
	ruleID string,
) error {
	if err := rs.DeleteRollupRule(ruleID, h.opts.PropagationDelay); err != nil {
		return err
	}
	if err := h.persistRuleSet(rs, nss); err != nil {
		return err
	}
	return nil
}

func (h *Handler) persistRuleSet(
	rs rules.RuleSet,
	nss *rules.Namespaces,
) error {
	ruleSetVersion := rs.Version()
	ruleSetKey := h.RuleSetKey(string(rs.Namespace()))
	namespacesKey := h.opts.NamespacesKey

	ns, err := nss.Namespace(string(rs.Namespace()))
	if err != nil {
		return err
	}
	ns.Update(ruleSetVersion + 1)

	rsSchema, err := rs.Schema()
	if err != nil {
		return err
	}
	nssSchema, err := nss.Schema()
	if err != nil {
		return err
	}

	namespacesCond := kv.NewCondition().
		SetKey(ruleSetKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(nss.Version)

	ruleSetCond := kv.NewCondition().
		SetKey(namespacesKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(ruleSetVersion)

	conditions := []kv.Condition{
		namespacesCond,
		ruleSetCond,
	}

	ops := []kv.Op{
		kv.NewSetOp(ruleSetKey, rsSchema),
		kv.NewSetOp(namespacesKey, nssSchema),
	}

	if _, err := h.store.Commit(conditions, ops); err != nil {
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
		return nil, err
	}
	rs, err := rules.NewRuleSet(version, &ruleSet, rules.NewOptions())
	if err != nil {
		return nil, err
	}
	return rs, err
}

// ValidateRuleSet validates that a Ruleset is active.
func (h Handler) ValidateRuleSet(rs rules.RuleSet, ruleSetKey string) error {
	if rs.Tombstoned() {
		return fmt.Errorf("ruleset %s is tombstoned", ruleSetKey)
	}
	return nil
}
