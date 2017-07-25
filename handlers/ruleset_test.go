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
	"testing"
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testRuleSetKey = fmt.Sprintf(testRuleSetKeyFmt, testNamespace)
	testRuleSet    = &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "fooNs",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules: []*schema.MappingRule{
			&schema.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.MappingRuleSnapshot{
					&schema.MappingRuleSnapshot{
						Name:        "foo",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Policies: []*schema.Policy{
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &schema.Retention{
										Period: int64(24 * time.Hour),
									},
								},
								AggregationTypes: []schema.AggregationType{
									schema.AggregationType_P999,
								},
							},
						},
					},
					&schema.MappingRuleSnapshot{
						Name:        "foo",
						Tombstoned:  false,
						CutoverTime: 67890,
						TagFilters: map[string]string{
							"tag3": "value3",
							"tag4": "value4",
						},
						Policies: []*schema.Policy{
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &schema.Retention{
										Period: int64(24 * time.Hour),
									},
								},
							},
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(5 * time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &schema.Retention{
										Period: int64(48 * time.Hour),
									},
								},
							},
						},
					},
				},
			},
			&schema.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.MappingRuleSnapshot{
					&schema.MappingRuleSnapshot{
						Name:        "dup",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Policies: []*schema.Policy{
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &schema.Retention{
										Period: int64(24 * time.Hour),
									},
								},
								AggregationTypes: []schema.AggregationType{
									schema.AggregationType_P999,
								},
							},
						},
					},
				},
			},
		},
		RollupRules: []*schema.RollupRule{
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{
						Name:        "foo2",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(10 * time.Second),
												Precision:  int64(time.Second),
											},
											Retention: &schema.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
								},
							},
						},
					},
					&schema.RollupRuleSnapshot{
						Name:        "bar",
						Tombstoned:  true,
						CutoverTime: 67890,
						TagFilters: map[string]string{
							"tag3": "value3",
							"tag4": "value4",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &schema.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(5 * time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &schema.Retention{
												Period: int64(48 * time.Hour),
											},
										},
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_MEAN,
										},
									},
								},
							},
						},
					},
				},
			},
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{
						Name:        "foo",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(10 * time.Second),
												Precision:  int64(time.Second),
											},
											Retention: &schema.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
								},
							},
						},
					},
					&schema.RollupRuleSnapshot{
						Name:        "baz",
						Tombstoned:  false,
						CutoverTime: 67890,
						TagFilters: map[string]string{
							"tag3": "value3",
							"tag4": "value4",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &schema.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(5 * time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &schema.Retention{
												Period: int64(48 * time.Hour),
											},
										},
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_MEAN,
										},
									},
								},
							},
						},
					},
				},
			},
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{
						Name:        "dup",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(10 * time.Second),
												Precision:  int64(time.Second),
											},
											Retention: &schema.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	testTombstonedRuleSet = &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "deadNs",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    true,
		CutoverTime:   34923,
	}
)

func TestRuleSet(t *testing.T) {
	h := testHandler()
	h.store.Set(testRuleSetKey, testRuleSet)
	s, err := h.RuleSet(testNamespace)
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestRuleSetError(t *testing.T) {
	h := testHandler()
	h.store.Set(testRuleSetKey, &schema.Namespace{Name: "x"})
	s, err := h.RuleSet("blah")
	require.Error(t, err)
	require.Nil(t, s)
}

func TestValidateRuleSet(t *testing.T) {
	h := testHandler()
	h.store.Set(testRuleSetKey, testRuleSet)
	s, err := h.RuleSet(testNamespace)
	require.NoError(t, err)

	err = h.ValidateRuleSet(s)
	require.NoError(t, err)
}

func TestValidateRuleSetTombstoned(t *testing.T) {
	h := testHandler()
	h.store.Set(testRuleSetKey, testTombstonedRuleSet)
	s, err := h.RuleSet(testNamespace)
	require.NoError(t, err)

	err = h.ValidateRuleSet(s)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tombstoned")
}

func TestAddMappingRule(t *testing.T) {
	h, nss, _ := bootstrap()
	h.store.Set(testRuleSetKey, testRuleSet)
	rs, err := h.RuleSet(testNamespace)

	newName := "newRule"
	newFilters := map[string]string{
		"tag1": "value",
		"tag2": "value",
	}
	newPolicies := []string{"10s@1s:1h0m0s"}
	err = h.AddMappingRule(rs, nss, newName, newFilters, newPolicies)
	require.NoError(t, err)

	err = h.Persist(rs, nss, false)
	require.NoError(t, err)

	rs, err = h.RuleSet(testNamespace)
	require.NoError(t, err)

	err = h.AddMappingRule(rs, nss, newName, newFilters, newPolicies)
	require.Error(t, err)
}

func TestAddMappingRuleInvalid(t *testing.T) {
	h, nss, _ := bootstrap()
	h.store.Set(testRuleSetKey, testRuleSet)
	rs, err := h.RuleSet(testNamespace)
	newFilters := map[string]string{
		"tag1": "value",
		"tag2": "value",
	}
	newPolicies := []string{"10s@1s:1h0m0s"}

	err = h.AddMappingRule(rs, nss, "", newFilters, newPolicies)
	require.Error(t, err)

	err = h.AddMappingRule(rs, nss, "test", map[string]string{}, newPolicies)
	require.Error(t, err)

	err = h.AddMappingRule(rs, nss, "", newFilters, []string{})
	require.Error(t, err)
}

func TestAddRollupRule(t *testing.T) {
	h, nss, _ := bootstrap()
	h.store.Set(testRuleSetKey, testRuleSet)
	rs, err := h.RuleSet(testNamespace)

	newName := "newRule"
	newFilters := map[string]string{
		"tag1": "value",
		"tag2": "value",
	}
	newPolicies := []string{"10s@1s:1h0m0s"}
	newTargets := []RollupTarget{
		RollupTarget{
			Name:     "blah",
			Tags:     []string{"a", "b"},
			Policies: newPolicies,
		},
	}

	err = h.AddRollupRule(rs, nss, newName, newFilters, newTargets)
	require.NoError(t, err)

	err = h.Persist(rs, nss, false)
	require.NoError(t, err)

	rs, err = h.RuleSet(testNamespace)
	require.NoError(t, err)

	err = h.AddRollupRule(rs, nss, newName, newFilters, newTargets)
	require.Error(t, err)
}

func TestAddRollupRuleInvalid(t *testing.T) {
	h, nss, _ := bootstrap()
	h.store.Set(testRuleSetKey, testRuleSet)
	rs, err := h.RuleSet(testNamespace)
	newFilters := map[string]string{
		"tag1": "value",
		"tag2": "value",
	}
	newPolicies := []string{"10s@1s:1h0m0s"}
	newTargets := []RollupTarget{
		RollupTarget{
			Name:     "blah",
			Tags:     []string{"a", "b"},
			Policies: newPolicies,
		},
	}

	err = h.AddRollupRule(rs, nss, "", newFilters, newTargets)
	require.Error(t, err)

	err = h.AddRollupRule(rs, nss, "test", map[string]string{}, newTargets)
	require.Error(t, err)

	err = h.AddRollupRule(rs, nss, "", newFilters, []RollupTarget{})
	require.Error(t, err)
}

func TestParsePolicies(t *testing.T) {
	policyStrings := []string{
		"10s@1s:1h0m0s",
		"1m0s@1m:12h0m0s",
	}

	policies := []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), policy.DefaultAggregationID),
	}

	res, err := parsePolicies(policyStrings)
	require.NoError(t, err)
	require.Equal(t, res, policies)
}

func TestParsePoliciesError(t *testing.T) {
	emptyPolicies := []string{}
	_, err := parsePolicies(emptyPolicies)
	require.Error(t, err)
	require.Equal(t, err, errNoPolicies)

	invalidPolicies := []string{
		"blah",
	}
	_, err = parsePolicies(invalidPolicies)
	require.Error(t, err)
}

func TestParseTarget(t *testing.T) {
	inTargets := []RollupTarget{
		RollupTarget{
			Name:     "t",
			Tags:     []string{"a", "b"},
			Policies: []string{"10s@1s:1h0m0s"},
		},
		RollupTarget{
			Name:     "t2",
			Tags:     []string{"a", "b"},
			Policies: []string{"10s@1s:1h0m0s"},
		},
	}

	outTargets := []rules.RollupTarget{
		rules.RollupTarget{
			Name: []byte("t"),
			Tags: [][]byte{[]byte("a"), []byte("b")},
			Policies: []policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID),
			},
		},
		rules.RollupTarget{
			Name: []byte("t2"),
			Tags: [][]byte{[]byte("a"), []byte("b")},
			Policies: []policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID),
			},
		},
	}

	res, err := parseRollupTargets(inTargets)
	require.NoError(t, err)
	require.Equal(t, res, outTargets)
}

func TestParseTargetErrors(t *testing.T) {
	emptyTargets := []RollupTarget{}
	_, err := parseRollupTargets(emptyTargets)
	require.Error(t, err)
	require.Equal(t, err, errNoTargets)

	invalidTargetNoName := []RollupTarget{
		RollupTarget{
			Name:     "",
			Tags:     []string{"a", "b"},
			Policies: []string{"10s@1s:1h0m0s"},
		},
	}
	_, err = parseRollupTargets(invalidTargetNoName)
	require.Error(t, err)

	invalidTargetNoTags := []RollupTarget{
		RollupTarget{
			Name:     "t",
			Tags:     []string{},
			Policies: []string{"10s@1s:1h0m0s"},
		},
	}

	_, err = parseRollupTargets(invalidTargetNoTags)
	require.Error(t, err)

	invalidTargetBadPolicy := []RollupTarget{
		RollupTarget{
			Name:     "t",
			Tags:     []string{"a", "b"},
			Policies: []string{"blah"},
		},
	}

	_, err = parseRollupTargets(invalidTargetBadPolicy)
	require.Error(t, err)
}

// github.com/m3db/m3metrics/handlers/ruleset.go Handler.DeleteMappingRule
// github.com/m3db/m3metrics/handlers/ruleset.go Handler.DeleteRollupRule
// github.com/m3db/m3metrics/handlers/ruleset.go Handler.UpdateMappingRule
// github.com/m3db/m3metrics/handlers/ruleset.go Handler.UpdateRollupRule
