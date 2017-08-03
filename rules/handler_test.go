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
// THE SOFTWARE

package rules

import (
	"testing"
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestNamespaceAdd(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	nssHandler := nss.Handler()

	err = nssHandler.AddNamespace("bar")
	require.NoError(t, err)

	finalNamespaces := nssHandler.Namespaces()
	ns, err := finalNamespaces.Namespace("bar")
	require.NoError(t, err)
	require.False(t, ns.Tombstoned())
}

func TestNamespaceAddDup(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	nssHandler := nss.Handler()

	err = nssHandler.AddNamespace("foo")
	require.Error(t, err)
}

func TestNamespaceRevive(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	ns, err := nss.Namespace("foo")
	require.NoError(t, err)

	nssHandler := nss.Handler()
	err = nssHandler.DeleteNamespace("foo", 4)
	require.NoError(t, err)

	ns, err = nss.Namespace("foo")
	require.True(t, ns.Tombstoned())

	err = nssHandler.AddNamespace("foo")
	require.NoError(t, err)

	ns, err = nssHandler.Namespaces().Namespace("foo")
	require.NoError(t, err)
	require.Equal(t, ns.snapshots[len(ns.snapshots)-1].forRuleSetVersion, 5)
	require.Equal(t, len(ns.snapshots), 3)
}

func TestNamespaceDelete(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	ns, err := nss.Namespace("foo")
	require.NoError(t, err)

	nssHandler := nss.Handler()
	err = nssHandler.DeleteNamespace("foo", 4)
	require.NoError(t, err)
	ns, err = nssHandler.Namespaces().Namespace("foo")
	require.NoError(t, err)
	require.True(t, ns.Tombstoned())
	require.Equal(t, ns.snapshots[len(ns.snapshots)-1].forRuleSetVersion, 5)
}

func TestNamespaceDeleteMissing(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
				},
			},
		},
	}
	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	nssHandler := nss.Handler()

	err = nssHandler.DeleteNamespace("bar", 4)
	require.Error(t, err)
}

func TestNamespaceDeleteTombstoned(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	nssHandler := nss.Handler()

	err = nssHandler.DeleteNamespace("foo", 4)
	require.Error(t, err)
}

func TestValidateNamespace(t *testing.T) {
	nss, err := NewNamespaces(1, testNamespaces)
	require.NoError(t, err)
	nssHandler := nss.Handler()

	err = nssHandler.ValidateNamespace("fooNs")
	require.NoError(t, err)
}

func TestValidateNamespaceTombstoned(t *testing.T) {
	nss, err := NewNamespaces(1, testNamespaces)
	require.NoError(t, err)
	nssHandler := nss.Handler()
	err = nssHandler.ValidateNamespace("barNs")
	require.Error(t, err)
}

func TestValidateNamespaceNoSnaps(t *testing.T) {
	nss, err := NewNamespaces(1, badNamespaces)
	require.NoError(t, err)
	nssHandler := nss.Handler()
	err = nssHandler.ValidateNamespace("barNs")
	require.Error(t, err)
}
func TestAddMappingRule(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(HandlerOptions{propagationDelay: 10})

	_, err = rs.(*ruleSet).getMappingRuleByName("foo")
	require.Error(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	cfg := MappingRuleConfig{Name: "foo", Filters: newFilters, Policies: p}
	err = rsHandler.AddMappingRule(cfg)
	require.NoError(t, err)

	_, err = rsHandler.RuleSet().(*ruleSet).getMappingRuleByName("foo")
	require.NoError(t, err)
}

func TestAddMappingRuleDup(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}
	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))

	m, err := rs.(*ruleSet).getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	cfg := MappingRuleConfig{Name: "mappingRule5.snapshot1", Filters: newFilters, Policies: p}

	err = rsHandler.AddMappingRule(cfg)
	require.Error(t, err)

}

func TestAddMappingRuleRevive(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))

	m, err := rs.(*ruleSet).getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	err = rsHandler.DeleteMappingRule("mappingRule5.snapshot1")
	require.NoError(t, err)

	newFilters := map[string]string{"test": "bar"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	cfg := MappingRuleConfig{Name: "mappingRule5.snapshot1", Filters: newFilters, Policies: p}
	err = rsHandler.AddMappingRule(cfg)
	require.NoError(t, err)

	mr, err := rs.(*ruleSet).getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[len(mr.snapshots)-1].rawFilters, newFilters)
}

func TestUpdateMappingRule(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))
	_, err = rs.(*ruleSet).getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	cfg := MappingRuleConfig{Name: "foo", Filters: newFilters, Policies: p}
	updateCfg := MappingRuleUpdate{Name: "mappingRule5.snapshot1", Config: cfg}

	err = rsHandler.UpdateMappingRule(updateCfg)
	require.NoError(t, err)

	_, err = rsHandler.RuleSet().(*ruleSet).getMappingRuleByName("foo")
	require.NoError(t, err)
}

func TestDeleteMappingRule(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))

	m, err := rs.(*ruleSet).getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	err = rsHandler.DeleteMappingRule("mappingRule5.snapshot1")
	require.NoError(t, err)

	m, err = rsHandler.RuleSet().(*ruleSet).getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.True(t, m.Tombstoned())
}

func TestAddRollupRule(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))

	_, err = rs.(*ruleSet).getRollupRuleByName("foo")
	require.Error(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}

	newTargets := []RollupTarget{
		RollupTarget{
			Name:     []byte("blah"),
			Tags:     [][]byte{[]byte("a")},
			Policies: p,
		},
	}
	rrc := RollupRuleConfig{Name: "foo", Filters: newFilters, Targets: newTargets}
	err = rsHandler.AddRollupRule(rrc)
	require.NoError(t, err)

	_, err = rsHandler.RuleSet().(*ruleSet).getRollupRuleByName("foo")
	require.NoError(t, err)
}

func TestAddRollupRuleDup(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}
	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))

	m, err := rs.(*ruleSet).getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}

	newTargets := []RollupTarget{
		RollupTarget{
			Name:     []byte("blah"),
			Tags:     [][]byte{[]byte("a")},
			Policies: p,
		},
	}
	newFilters := map[string]string{"test": "bar"}
	rrc := RollupRuleConfig{Name: "rollupRule5.snapshot1", Filters: newFilters, Targets: newTargets}
	err = rsHandler.AddRollupRule(rrc)
	require.Error(t, err)
}

func TestReviveRollupRule(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))

	rr, err := rs.(*ruleSet).getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	snapshot := rr.snapshots[len(rr.snapshots)-1]

	err = rsHandler.DeleteRollupRule("rollupRule5.snapshot1")
	require.NoError(t, err)

	rr, err = rsHandler.RuleSet().(*ruleSet).getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	require.True(t, rr.Tombstoned())

	rrc := RollupRuleConfig{Name: "rollupRule5.snapshot1", Filters: snapshot.rawFilters, Targets: snapshot.targets}
	err = rsHandler.AddRollupRule(rrc)
	require.NoError(t, err)

	rr, err = rs.(*ruleSet).getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	require.Equal(t, rr.snapshots[len(rr.snapshots)-1].rawFilters, snapshot.rawFilters)
}

func TestUpdateRollupRule(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))

	_, err = rs.(*ruleSet).getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	newTargets := []RollupTarget{
		RollupTarget{
			Name:     []byte("blah"),
			Tags:     [][]byte{[]byte("a")},
			Policies: p,
		},
	}

	cfg := RollupRuleConfig{Name: "foo", Filters: newFilters, Targets: newTargets}
	updateCfg := RollupRuleUpdate{Name: "rollupRule5.snapshot1", Config: cfg}
	err = rsHandler.UpdateRollupRule(updateCfg)
	require.NoError(t, err)

	_, err = rsHandler.RuleSet().(*ruleSet).getRollupRuleByName("foo")
	require.NoError(t, err)
}

func TestDeleteRollupRule(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	require.NoError(t, err)
	rsHandler := rs.Handler(NewHandlerOptions(10))

	r, err := rsHandler.RuleSet().(*ruleSet).getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, r)

	err = rsHandler.DeleteRollupRule("rollupRule5.snapshot1")
	require.NoError(t, err)

	r, err = rsHandler.RuleSet().(*ruleSet).getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	require.True(t, r.Tombstoned())
}

func TestDeleteRuleset(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	rsHandler := rs.Handler(NewHandlerOptions(10))
	require.NoError(t, err)

	err = rsHandler.Delete()
	require.NoError(t, err)

	require.True(t, rsHandler.RuleSet().Tombstoned())
	for _, m := range rsHandler.RuleSet().(*ruleSet).mappingRules {
		require.True(t, m.Tombstoned())
	}

	for _, r := range rsHandler.RuleSet().(*ruleSet).rollupRules {
		require.True(t, r.Tombstoned())
	}
}

func TestReviveRuleSet(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	rsHandler := rs.Handler(NewHandlerOptions(10))
	require.NoError(t, err)

	err = rsHandler.Delete()
	require.NoError(t, err)

	err = rsHandler.Revive()
	require.NoError(t, err)

	require.False(t, rs.Tombstoned())
	for _, m := range rs.(*ruleSet).mappingRules {
		require.True(t, m.Tombstoned())
	}

	for _, r := range rs.(*ruleSet).rollupRules {
		require.True(t, r.Tombstoned())
	}
}
func TestValidateRuleSet(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	rsHandler := rs.Handler(NewHandlerOptions(10))
	require.NoError(t, err)

	err = rsHandler.Validate()
	require.NoError(t, err)
}

func TestValidateRuleSetTombstoned(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    true,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := NewRuleSetFromSchema(version, expectedRs, opts)
	rsHandler := rs.Handler(NewHandlerOptions(10))
	require.NoError(t, err)

	err = rsHandler.Validate()
	require.Error(t, err)
}
