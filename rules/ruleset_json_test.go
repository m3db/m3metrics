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
	"encoding/json"
	"testing"

	"github.com/m3db/m3metrics/policy"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestRuleSetSnapshot(t *testing.T) {
	mappingRules := []MappingRuleJSON{
		*testMappingRuleJSON("mr1_id", "mr1"),
		*testMappingRuleJSON("mr2_id", "mr2"),
	}
	rollupRules := []RollupRuleJSON{
		*testRollupRuleJSON("rr1_id", "rr1", []RollupTargetJSON{*testRollupTargetJSON("target1")}),
		*testRollupRuleJSON("rr2_id", "rr2", []RollupTargetJSON{*testRollupTargetJSON("target2")}),
	}
	fixture := testRuleSetJSON("rs_ns", mappingRules, rollupRules)
	expected := &RuleSetSnapshot{
		Namespace:    "rs_ns",
		Version:      1,
		CutoverNanos: 0,
		MappingRules: map[string]*MappingRuleView{
			"mr1_id": {
				ID:       "mr1_id",
				Name:     "mr1",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
			"mr2_id": {
				ID:       "mr2_id",
				Name:     "mr2",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
		},
		RollupRules: map[string]*RollupRuleView{
			"rr1_id": {
				ID:     "rr1_id",
				Name:   "rr1",
				Filter: "filter",
				Targets: []RollupTargetView{
					{
						Name:     "target1",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
			"rr2_id": {
				ID:     "rr2_id",
				Name:   "rr2",
				Filter: "filter",
				Targets: []RollupTargetView{
					{
						Name:     "target2",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
		},
	}
	actual, err := fixture.RuleSetSnapshot(GenerateID)
	require.NoError(t, err)
	require.EqualValues(t, expected, actual)
}

func TestRuleSetSnapshotGenerateMissingID(t *testing.T) {
	mappingRules := []MappingRuleJSON{
		*testMappingRuleJSON("", "mr"),
		*testMappingRuleJSON("", "mr"),
	}
	rollupRules := []RollupRuleJSON{
		*testRollupRuleJSON("", "rr", []RollupTargetJSON{*testRollupTargetJSON("target")}),
		*testRollupRuleJSON("", "rr", []RollupTargetJSON{*testRollupTargetJSON("target")}),
	}
	fixture := testRuleSetJSON("namespace", mappingRules, rollupRules)

	actual, err := fixture.RuleSetSnapshot(GenerateID)
	require.NoError(t, err)
	mrIDs := []string{}
	rrIDs := []string{}

	// Test that generated IDs are UUIDs and add them to their respective lists for further testing.
	for id := range actual.MappingRules {
		require.NotNil(t, uuid.Parse(id))
		mrIDs = append(mrIDs, id)
	}
	for id := range actual.RollupRules {
		require.NotNil(t, uuid.Parse(id))
		rrIDs = append(rrIDs, id)
	}

	expected := &RuleSetSnapshot{
		Namespace:    "namespace",
		Version:      1,
		CutoverNanos: 0,
		MappingRules: map[string]*MappingRuleView{
			mrIDs[0]: {
				ID:       mrIDs[0],
				Name:     "mr",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
			mrIDs[1]: {
				ID:       mrIDs[1],
				Name:     "mr",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
		},
		RollupRules: map[string]*RollupRuleView{
			rrIDs[0]: {
				ID:     rrIDs[0],
				Name:   "rr",
				Filter: "filter",
				Targets: []RollupTargetView{
					{
						Name:     "target",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
			rrIDs[1]: {
				ID:     rrIDs[1],
				Name:   "rr",
				Filter: "filter",
				Targets: []RollupTargetView{
					{
						Name:     "target",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
		},
	}
	require.EqualValues(t, expected, actual)
}

func TestRuleSetSnapshotFailMissingMappingRuleID(t *testing.T) {
	mappingRules := []MappingRuleJSON{
		*testMappingRuleJSON("", "mr"),
		*testMappingRuleJSON("id1", "mr"),
	}
	rollupRules := []RollupRuleJSON{
		*testRollupRuleJSON("id2", "rr", []RollupTargetJSON{*testRollupTargetJSON("target")}),
		*testRollupRuleJSON("id3", "rr", []RollupTargetJSON{*testRollupTargetJSON("target")}),
	}
	fixture := testRuleSetJSON("namespace", mappingRules, rollupRules)

	_, err := fixture.RuleSetSnapshot(DontGenerateID)
	require.Error(t, err)
}

func TestRuleSetSnapshotFailMissingRollupRuleID(t *testing.T) {
	mappingRules := []MappingRuleJSON{
		*testMappingRuleJSON("id1", "mr"),
		*testMappingRuleJSON("id2", "mr"),
	}
	rollupRules := []RollupRuleJSON{
		*testRollupRuleJSON("id3", "rr", []RollupTargetJSON{*testRollupTargetJSON("target")}),
		*testRollupRuleJSON("", "rr", []RollupTargetJSON{*testRollupTargetJSON("target")}),
	}
	fixture := testRuleSetJSON("namespace", mappingRules, rollupRules)

	_, err := fixture.RuleSetSnapshot(DontGenerateID)
	require.Error(t, err)
}

func TestRuleSetsSort(t *testing.T) {
	rulesetJSON := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"policies":[
						"10s:2d|Count,P99,P9999",
						"1m:40d|Count,P99,P9999",
						"1m:40d|Count,P9999"
					]
				}
			],
			"rollupRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"targets":[
						{
							"name": "rollup_target_1",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"10s:2d|Count,P99,P9999",
								"1m:40d|Count,P99,P9999",
								"1m:40d|Count,P9999"
							]
						},
						{
							"name": "rollup_target_2",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"1m:40d|Count,P99,P9999",
								"1m:40d|Count,P9999",
								"10s:2d|Count,P99,P9999"
							]
						}
					]
				}
			]
		}
	`
	var rs1 RuleSetJSON
	err := json.Unmarshal([]byte(rulesetJSON), &rs1)
	require.NoError(t, err)
	var rs2 RuleSetJSON
	err = json.Unmarshal([]byte(rulesetJSON), &rs2)
	require.NoError(t, err)

	rulesets := RuleSets(map[string]*RuleSetJSON{"rs1": &rs1, "rs2": &rs2})

	expectedJSON := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"policies":[
						"10s:2d|Count,P99,P9999",
						"1m:40d|Count,P9999",
						"1m:40d|Count,P99,P9999"
					]
				}
			],
			"rollupRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"targets":[
						{
							"name": "rollup_target_1",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"10s:2d|Count,P99,P9999",
								"1m:40d|Count,P9999",
								"1m:40d|Count,P99,P9999"
							]
						},
						{
							"name": "rollup_target_2",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"10s:2d|Count,P99,P9999",
								"1m:40d|Count,P9999",
								"1m:40d|Count,P99,P9999"
							]
						}
					]
				}
			]
		}
	`

	var expected RuleSetJSON
	err = json.Unmarshal([]byte(expectedJSON), &expected)
	require.NoError(t, err)

	rulesets.Sort()

	require.Equal(t, expected, *rulesets["rs1"])
	require.Equal(t, expected, *rulesets["rs2"])
}

func TestRuleSetSort(t *testing.T) {
	rulesetJSON := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"policies":[
						"10s:2d|Count,P99,P9999",
						"1m:40d|Count,P99,P9999",
						"1m:40d|Count,P9999"
					]
				}
			],
			"rollupRules":[
				{
					"name":"sample_rollup_rule_1",
					"filter":"filter_1",
					"targets":[
						{
							"name": "rollup_target_1",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"10s:2d|Count,P99,P9999",
								"1m:40d|Count,P99,P9999",
								"1m:40d|Count,P9999"
							]
						},
						{
							"name": "rollup_target_2",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"1m:40d|Count,P99,P9999",
								"1m:40d|Count,P9999",
								"10s:2d|Count,P99,P9999"
							]
						}
					]
				}
			]
		}
	`

	expectedRuleSetJSON := `
	{
		"id": "namespace",
		"mappingRules":[
			{
				"name":"sample_mapping_rule_1",
				"filter":"filter_1",
				"policies":[
					"10s:2d|Count,P99,P9999",
					"1m:40d|Count,P9999",
					"1m:40d|Count,P99,P9999"
				]
			}
		],
		"rollupRules":[
			{
				"name":"sample_rollup_rule_1",
				"filter":"filter_1",
				"targets":[
					{
						"name": "rollup_target_1",
						"tags": [
							"tag1","tag2","tag3"
						],
						"policies":[
							"10s:2d|Count,P99,P9999",
							"1m:40d|Count,P9999",
							"1m:40d|Count,P99,P9999"
						]
					},
					{
						"name": "rollup_target_2",
						"tags": [
							"tag1","tag2","tag3"
						],
						"policies":[
							"10s:2d|Count,P99,P9999",
							"1m:40d|Count,P9999",
							"1m:40d|Count,P99,P9999"
						]
					}
				]
			}
		]
	}
`
	var rs RuleSetJSON
	err := json.Unmarshal([]byte(rulesetJSON), &rs)
	require.NoError(t, err)
	var expected RuleSetJSON
	err = json.Unmarshal([]byte(expectedRuleSetJSON), &expected)
	require.NoError(t, err)

	rs.Sort()
	require.Equal(t, expected, rs)
}

func TestRuleSetSortByRollupRuleNameAsc(t *testing.T) {
	rulesetJSON := `
		{
			"id": "namespace",
			"rollupRules":[
				{
					"name":"sample_rollup_rule_2",
					"filter":"filter_1",
					"targets":[]
				},
				{
					"name":"sample_rollup_rule_1",
					"filter":"filter_2",
					"targets":[]
				}
			]
		}
	`

	expectedRuleSetJSON := `
		{
			"id": "namespace",
			"rollupRules":[
				{
					"name":"sample_rollup_rule_1",
					"filter":"filter_2",
					"targets":[]
				},
				{
					"name":"sample_rollup_rule_2",
					"filter":"filter_1",
					"targets":[]
				}
			]
		}
	`
	var rs RuleSetJSON
	err := json.Unmarshal([]byte(rulesetJSON), &rs)
	require.NoError(t, err)
	var expected RuleSetJSON
	err = json.Unmarshal([]byte(expectedRuleSetJSON), &expected)
	require.NoError(t, err)

	rs.Sort()
	require.Equal(t, expected, rs)
}

func TestRuleSetSortByMappingNameAsc(t *testing.T) {
	rulesetJSON := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_2",
					"filter":"filter_1",
					"policies":[]
				},
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_2",
					"policies":[]
				}
			]
		}
	`

	expectedRuleSetJSON := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_2",
					"policies":[]
				},
				{
					"name":"sample_mapping_rule_2",
					"filter":"filter_1",
					"policies":[]
				}
			]
		}
	`
	var rs RuleSetJSON
	err := json.Unmarshal([]byte(rulesetJSON), &rs)
	require.NoError(t, err)
	var expected RuleSetJSON
	err = json.Unmarshal([]byte(expectedRuleSetJSON), &expected)
	require.NoError(t, err)

	rs.Sort()
	require.Equal(t, expected, rs)
}

func testRuleSetJSON(namespace string, mappingRules []MappingRuleJSON,
	rollupRules []RollupRuleJSON) *RuleSetJSON {
	return &RuleSetJSON{
		Namespace:     namespace,
		Version:       1,
		CutoverMillis: 0,
		MappingRules:  mappingRules,
		RollupRules:   rollupRules,
	}
}
