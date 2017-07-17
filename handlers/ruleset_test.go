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
	"testing"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/generated/proto/schema"

	"github.com/stretchr/testify/require"
)

const (
	testKeyFmt    = "rules/%s"
	testNamespace = "ns"
)

func testRuleSet() *schema.RuleSet {
	return &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    true,
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
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c69",
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
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c70",
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
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_P999,
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
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_P999,
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
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c71",
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
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_P999,
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
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_MEAN,
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
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c72",
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
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_P999,
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
}

func TestRuleSetKey(t *testing.T) {
	expected := "rules/ns"
	actual := RuleSetKey(testKeyFmt, testNamespace)
	require.Equal(t, expected, actual)
}

func TestRuleSet(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, testRuleSet())
	_, s, err := RuleSet(store, rulesSetKey)
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestRuleSetError(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, &schema.Namespace{Name: "x"})
	_, s, err := RuleSet(store, "blah")
	require.Error(t, err)
	require.Nil(t, s)
}

func TestValidateRuleSetTombstoned(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, testRuleSet())
	_, _, err := ValidateRuleSet(store, rulesSetKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tombstoned")
}

func TestValidateRuleSetInvalid(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, nil)
	_, _, err := ValidateRuleSet(store, rulesSetKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not read")
}

func TestRule(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	rs := testRuleSet()
	store.Set(rulesSetKey, rs)
	_, r, err := RuleSet(store, rulesSetKey)
	require.NoError(t, err)

	m, s, err := Rule(r, "foo")
	require.Nil(t, s)
	require.NoError(t, err)
	require.Equal(t, m, rs.MappingRules[0])

	m, s, err = Rule(r, "baz")
	require.Nil(t, m)
	require.NoError(t, err)
	require.Equal(t, s, rs.RollupRules[1])
}

func TestRuleDup(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, testRuleSet())
	_, r, err := RuleSet(store, rulesSetKey)
	require.NoError(t, err)

	m, s, err := Rule(r, "dup")
	require.Error(t, err)
	require.Equal(t, errMultipleMatches, err)
	require.Nil(t, m)
	require.Nil(t, s)
}

func TestAppendToRuleSet(t *testing.T) {
	diffRuleSet := &schema.RuleSet{
		Uuid:      "ruleset",
		Namespace: "namespace",
		MappingRules: []*schema.MappingRule{
			&schema.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.MappingRuleSnapshot{
					&schema.MappingRuleSnapshot{Name: "foo", Tombstoned: false},
				},
			},
		},
		RollupRules: []*schema.RollupRule{
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c72",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{Name: "foo5", Tombstoned: false},
					&schema.RollupRuleSnapshot{Name: "foo5", Tombstoned: true},
				},
			},
		},
	}
	rs := testRuleSet()
	m0, _, err := RuleByID(rs, "12669817-13ae-40e6-ba2f-33087b262c68")
	mStartLen := len(m0.Snapshots)
	_, r0, err := RuleByID(rs, "12669817-13ae-40e6-ba2f-33087b262c72")
	rStartLen := len(r0.Snapshots)

	err = appendToRuleSet(rs, diffRuleSet)

	m1, r1, err := RuleByID(rs, "12669817-13ae-40e6-ba2f-33087b262c68")
	require.NoError(t, err)
	require.Nil(t, r1)
	snapLen := len(m1.Snapshots)
	s1 := m1.Snapshots[snapLen-1]

	m2, r2, err := RuleByID(diffRuleSet, "12669817-13ae-40e6-ba2f-33087b262c68")
	require.NoError(t, err)
	require.Nil(t, r2)
	snapLen2 := len(m2.Snapshots)
	s2 := m2.Snapshots[snapLen2-1]

	require.Equal(t, s1, s2)
	require.Equal(t, mStartLen+1, snapLen)

	m1, r1, err = RuleByID(rs, "12669817-13ae-40e6-ba2f-33087b262c72")
	require.NoError(t, err)
	require.Nil(t, m1)
	require.NotNil(t, r1)
	snapLen = len(r1.Snapshots)
	s3 := r1.Snapshots[snapLen-1]

	m2, r2, err = RuleByID(diffRuleSet, "12669817-13ae-40e6-ba2f-33087b262c72")
	require.NoError(t, err)
	require.Nil(t, m2)
	require.NotNil(t, r2)
	snapLen2 = len(r2.Snapshots)
	s4 := r2.Snapshots[snapLen2-1]

	require.Equal(t, s3, s4)
	require.Equal(t, rStartLen+1, snapLen)
}

func TestAppendToRuleSetEmpty(t *testing.T) {
	diffRuleSet := &schema.RuleSet{
		Uuid: "ruleset",
		MappingRules: []*schema.MappingRule{
			&schema.MappingRule{},
		},
	}
	rs := testRuleSet()
	err := appendToRuleSet(rs, diffRuleSet)
	require.NoError(t, err)
	require.Equal(t, rs, testRuleSet())
}

func TestAppendNotFound(t *testing.T) {
	diffRuleSet := &schema.RuleSet{
		Uuid: "ruleset",
		MappingRules: []*schema.MappingRule{
			&schema.MappingRule{
				Uuid: "newOne",
				Snapshots: []*schema.MappingRuleSnapshot{
					&schema.MappingRuleSnapshot{Name: "foo", Tombstoned: false},
				},
			},
		},
		RollupRules: []*schema.RollupRule{
			&schema.RollupRule{
				Uuid: "another",
			},
		},
	}

	rs := testRuleSet()
	m1, r1, err1 := RuleByID(rs, "newOne")
	m2, r2, err2 := RuleByID(rs, "another")
	require.Error(t, err1)
	require.EqualError(t, err1, kv.ErrNotFound.Error())
	require.EqualError(t, err2, kv.ErrNotFound.Error())
	require.Nil(t, m1)
	require.Nil(t, m2)
	require.Nil(t, r1)
	require.Nil(t, r2)

	err := appendToRuleSet(rs, diffRuleSet)
	require.NoError(t, err)
	m1, r1, err1 = RuleByID(rs, "newOne")
	m2, r2, err2 = RuleByID(rs, "another")
	require.NoError(t, err1)
	require.EqualError(t, err2, kv.ErrNotFound.Error())
	require.NotNil(t, m1)
	require.Nil(t, m2)
	require.Nil(t, r1)
	require.Nil(t, r2)
}

func TestAppendMissmatch(t *testing.T) {
	diffRuleSet := &schema.RuleSet{
		Uuid: "foo1",
		RollupRules: []*schema.RollupRule{
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
			},
		},
	}

	rs := testRuleSet()
	err := appendToRuleSet(rs, diffRuleSet)
	require.Error(t, err)

	diffRuleSet = &schema.RuleSet{
		Uuid: "foo1",
		MappingRules: []*schema.MappingRule{
			&schema.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c72",
			},
		},
	}

	rs = testRuleSet()
	err = appendToRuleSet(rs, diffRuleSet)
	require.Error(t, err)
}

func TestUpdateRules(t *testing.T) {
	diffRuleSet := &schema.RuleSet{
		Uuid:      "ruleset",
		Namespace: "namespace",
		MappingRules: []*schema.MappingRule{
			&schema.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.MappingRuleSnapshot{
					&schema.MappingRuleSnapshot{Name: "goal", Tombstoned: false},
				},
			},
		},
		RollupRules: []*schema.RollupRule{
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c72",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{Name: "foo5", Tombstoned: false},
					&schema.RollupRuleSnapshot{Name: "foo5", Tombstoned: true},
				},
			},
		},
	}

	store := mem.NewStore()
	rs := testRuleSet()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(testNamespaceKey, testNamespaces)
	store.Set(rulesSetKey, rs)
	err := CreateNamespace(store, testNamespaceKey, testNamespace, rulesSetKey, 1)
	require.NoError(t, err)

	nsv, _, err := Namespaces(store, testNamespaceKey)
	rsv, rs, err := RuleSet(store, rulesSetKey)
	require.NoError(t, err)

	err = UpdateRules(store, rs, diffRuleSet, rulesSetKey, rsv, testNamespaceKey, nsv, 1)
	require.NoError(t, err)

	rsv2, rs, err := RuleSet(store, rulesSetKey)
	require.Equal(t, rsv+1, rsv2)
	mr, rr, err := Rule(rs, "goal")
	require.NoError(t, err)
	require.Nil(t, rr)

	totalLen := len(mr.Snapshots)
	// totalLen := len(diffRuleSet.MappingRules[0].Snapshots)
	require.Equal(t, diffRuleSet.MappingRules[0].Snapshots[0], mr.Snapshots[totalLen-1])

}
