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

// AddMappingRule ...
func (h *Handler) AddMappingRule(
	rs rules.RuleSet,
	ruleSetKey string,
	nss *rules.Namespaces,
	namespacesKey string,
	ruleName string,
	filters map[string]string,
	policies []policy.Policy,
) error {
	if err := rs.AppendMappingRule(ruleName, filters, policies, h.opts.PropagationDelay); err != nil {
		return err
	}

	if err := h.persistRuleSet(rs, ruleSetKey, nss, namespacesKey); err != nil {
		return err
	}
	return nil
}

// UpdateMappingRule ...
func (h *Handler) UpdateMappingRule(
	rs rules.RuleSet,
	ruleSetKey string,
	nss *rules.Namespaces,
	namespacesKey string,
	ruleID string,
	ruleName string,
	filters map[string]string,
	policies []policy.Policy) error {

	if err := rs.UpdateMappingRule(ruleID, ruleName, filters, policies, h.opts.PropagationDelay); err != nil {
		return nil
	}

	if err := h.persistRuleSet(rs, ruleSetKey, nss, namespacesKey); err != nil {
		return err
	}

	return nil

}

// func (r *RuleSetHandler) DeleteMappingRule(n newMappingRule) {
// 	v, err := r.rs.Tombstone(n.UUID)
// 	// -> Returns error if rs
// }

func (h *Handler) persistRuleSet(
	rs rules.RuleSet,
	ruleSetKey string,
	nss *rules.Namespaces,
	namespacesKey string,
) error {
	ruleSetVersion := rs.Version()
	// Not sure about this plus 1 here
	ns, err := nss.Namespace(string(rs.Namespace()))
	if err != nil {
		return err
	}
	ns.Update(ruleSetVersion + 1)

	namespacesCond := kv.NewCondition().
		SetKey(ruleSetKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(nss)

	ruleSetCond := kv.NewCondition().
		SetKey(namespacesKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(ruleSetVersion)

	conditions := []kv.Condition{
		namespacesCond,
		ruleSetCond,
	}

	rsSchema, err := rs.Schema()
	if err != nil {
		return err
	}
	nssSchema, err := nss.Schema()
	if err != nil {
		return err
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
func (h Handler) RuleSet(ruleSetKey string) (rules.RuleSet, error) {
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

// ValidateRuleSet validates that a valid RuleSet exists in that keyspace.
func (h Handler) ValidateRuleSet(rs rules.RuleSet, ruleSetKey string) error {
	if rs.Tombstoned() {
		return fmt.Errorf("ruleset %s is tombstoned", ruleSetKey)
	}
	return nil
}
