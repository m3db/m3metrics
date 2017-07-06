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
	"fmt"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
)

// RuleByID fetches a rule by uuid from a given ruleset
func RuleByID(ruleSet *schema.RuleSet, uuid string) (*schema.MappingRule, *schema.RollupRule, error) {
	var (
		mappingRule *schema.MappingRule
		rollupRule  *schema.RollupRule
	)
	for _, mr := range ruleSet.MappingRules {
		if mr.Uuid != uuid {
			continue
		}
		if mappingRule == nil {
			mappingRule = mr
		} else {
			return nil, nil, errMultipleMatches
		}
	}

	for _, rr := range ruleSet.RollupRules {
		if rr.Uuid != uuid {
			continue
		}
		if rollupRule == nil {
			rollupRule = rr
		} else {
			return nil, nil, errMultipleMatches
		}
	}

	if mappingRule != nil && rollupRule != nil {
		return nil, nil, errMultipleMatches
	}

	if mappingRule == nil && rollupRule == nil {
		return nil, nil, kv.ErrNotFound
	}
	return mappingRule, rollupRule, nil
}

// Rule returns the rule with a given name, or an error if there are mutliple matches
func Rule(ruleSet *schema.RuleSet, ruleName string) (*schema.MappingRule, *schema.RollupRule, error) {
	var (
		mappingRule *schema.MappingRule
		rollupRule  *schema.RollupRule
	)
	for _, mr := range ruleSet.MappingRules {
		if len(mr.Snapshots) == 0 {
			continue
		}

		latestSnapshot := mr.Snapshots[len(mr.Snapshots)-1]
		name := latestSnapshot.Name
		if name != ruleName || latestSnapshot.Tombstoned {
			continue
		}
		if mappingRule == nil {
			mappingRule = mr
		} else {
			return nil, nil, errMultipleMatches
		}
	}
	for _, rr := range ruleSet.RollupRules {
		if len(rr.Snapshots) == 0 {
			continue
		}
		latestSnapshot := rr.Snapshots[len(rr.Snapshots)-1]
		name := latestSnapshot.Name
		if name != ruleName || latestSnapshot.Tombstoned {
			continue
		}
		if rollupRule == nil {
			rollupRule = rr
		} else {
			return nil, nil, errMultipleMatches
		}
	}
	if mappingRule != nil && rollupRule != nil {
		return nil, nil, errMultipleMatches
	}

	if mappingRule == nil && rollupRule == nil {
		return nil, nil, kv.ErrNotFound
	}
	return mappingRule, rollupRule, nil
}

// RuleSet returns the version and the persisted ruleset data in kv store.
func RuleSet(store kv.Store, ruleSetKey string) (int, *schema.RuleSet, error) {
	value, err := store.Get(ruleSetKey)
	if err != nil {
		return 0, nil, err
	}
	version := value.Version()
	var ruleSet schema.RuleSet
	if err := value.Unmarshal(&ruleSet); err != nil {
		return 0, nil, err
	}

	return version, &ruleSet, nil
}

// RuleSetKey returns the ruleset key given the namespace name.
func RuleSetKey(keyFmt string, namespace string) string {
	return fmt.Sprintf(keyFmt, namespace)
}

// ValidateRuleSet validates that a valid RuleSet exists in that keyspace.
func ValidateRuleSet(store kv.Store, ruleSetKey string) (int, *schema.RuleSet, error) {
	ruleSetVersion, ruleSet, err := RuleSet(store, ruleSetKey)
	if err != nil {
		return 0, nil, fmt.Errorf("could not read ruleSet data for key %s: %v", ruleSetKey, err)
	}
	if ruleSet.Tombstoned {
		return 0, nil, fmt.Errorf("ruleset %s is tombstoned", ruleSetKey)
	}
	return ruleSetVersion, ruleSet, nil
}

// UpdateRules updates an existing RuleSet by appending a diffRuleSet to it.
func UpdateRules(store kv.TxnStore,
	ruleSet, diffRuleSet *schema.RuleSet,
	ruleSetKey string, ruleSetVersion int,
	namespacesKey string, namespacesVersion int,
	propDelay time.Duration,
) error {
	// There is nothing to do if the diff is nil
	if diffRuleSet == nil {
		return nil
	}

	if ruleSet == nil {
		ruleSet = diffRuleSet
	}

	err := appendToRuleSet(ruleSet, diffRuleSet)
	if err != nil {
		return err
	}

	nowNs := time.Now().UnixNano()
	ruleSet.LastUpdatedAt = nowNs
	ruleSet.CutoverTime = NewCutoverNs(nowNs, ruleSet.CutoverTime, propDelay)

	// Perform a transaction and only update if the namespaces version
	// and ruleSet version were unchanged.
	namespacesCond := kv.NewCondition().
		SetKey(namespacesKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(namespacesVersion)
	ruleSetCond := kv.NewCondition().
		SetKey(ruleSetKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(ruleSetVersion)
	conditions := []kv.Condition{
		namespacesCond,
		ruleSetCond,
	}
	ops := []kv.Op{
		kv.NewSetOp(ruleSetKey, ruleSet),
	}

	if _, err := store.Commit(conditions, ops); err != nil {
		return fmt.Errorf("unable to update kv store: %v", err)
	}

	return nil
}

// AppendToRuleSet appends a ruleset diff onto the end of a ruleset
func appendToRuleSet(orig, diff *schema.RuleSet) error {
	for _, m := range diff.MappingRules {
		mr, rr, err := RuleByID(orig, m.Uuid)
		if err != nil && err != kv.ErrNotFound {
			return err
		}

		if rr != nil {
			return fmt.Errorf("rule with ID: %s is a rollup rule. Cannot make a mapping rule", rr.Uuid)
		}

		// A rule with no snapshots should not be added.
		if m.Snapshots == nil {
			continue
		}

		ls := len(m.Snapshots)
		if ls == 0 {
			continue
		}

		lastSnap := m.Snapshots[ls-1]

		if err == kv.ErrNotFound {
			newMappingRule := &schema.MappingRule{
				Uuid:      m.Uuid,
				Snapshots: []*schema.MappingRuleSnapshot{m.Snapshots[ls-1]},
			}
			orig.MappingRules = append(orig.MappingRules, newMappingRule)
		} else {
			mr.Snapshots = append(mr.Snapshots, lastSnap)
		}
	}

	for _, r := range diff.RollupRules {
		mr, rr, err := RuleByID(orig, r.Uuid)
		if err != nil && err != kv.ErrNotFound {
			return err
		}

		if mr != nil {
			return fmt.Errorf("rule with ID: %s is a mapping rule. Cannot make a rollup rule", mr.Uuid)
		}

		// A rule with no snapshots should not be added.
		if r.Snapshots == nil {
			continue
		}

		ls := len(r.Snapshots)
		if ls == 0 {
			continue
		}
		lastSnap := r.Snapshots[ls-1]

		if err == kv.ErrNotFound {
			newRollupRule := &schema.RollupRule{
				Uuid:      r.Uuid,
				Snapshots: []*schema.RollupRuleSnapshot{r.Snapshots[ls-1]},
			}
			orig.RollupRules = append(orig.RollupRules, newRollupRule)
			continue
		}

		rr.Snapshots = append(rr.Snapshots, lastSnap)
	}
	return nil
}
