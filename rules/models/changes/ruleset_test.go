package changes

import (
	"testing"

	"github.com/m3db/m3metrics/rules/models"

	"github.com/stretchr/testify/require"
)

func TestRuleSetChangeSetsSort(t *testing.T) {
	expected := RuleSetsChanges{
		RuleSetChanges{
			Namespace: "service1",
			MappingRuleChanges: []MappingRuleChange{
				{
					Op: AddOp,
					RuleData: &models.MappingRule{
						Name: "Add1",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.MappingRule{
						Name: "Add2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("mrID2"),
					RuleData: &models.MappingRule{
						Name: "change1",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("mrID3"),
					RuleData: &models.MappingRule{
						Name: "change2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("mrID1"),
					RuleData: &models.MappingRule{
						Name: "change3",
					},
				},
			},
			RollupRuleChanges: []RollupRuleChange{
				{
					Op: AddOp,
					RuleData: &models.RollupRule{
						Name: "Add1",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.RollupRule{
						Name: "Add2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("rrID2"),
					RuleData: &models.RollupRule{
						Name: "change1",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("rrID3"),
					RuleData: &models.RollupRule{
						Name: "change2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("rrID1"),
					RuleData: &models.RollupRule{
						Name: "change3",
					},
				},
			},
		},
		RuleSetChanges{
			Namespace: "service2",
		},
		RuleSetChanges{
			Namespace: "service3",
		},
	}

	ruleSets.Sort()
	require.Equal(t, expected[0].RollupRuleChanges, ruleSets[0].RollupRuleChanges)
	require.Equal(t, expected[0].MappingRuleChanges, ruleSets[0].MappingRuleChanges)
}

var (
	ruleSets = RuleSetsChanges{
		RuleSetChanges{
			Namespace: "service3",
		},
		RuleSetChanges{
			Namespace: "service2",
		},
		RuleSetChanges{
			Namespace: "service1",
			MappingRuleChanges: []MappingRuleChange{
				{
					Op:     ChangeOp,
					RuleID: p("mrID1"),
					RuleData: &models.MappingRule{
						Name: "change3",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.MappingRule{
						Name: "Add2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("mrID2"),
					RuleData: &models.MappingRule{
						Name: "change1",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("mrID3"),
					RuleData: &models.MappingRule{
						Name: "change2",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.MappingRule{
						Name: "Add1",
					},
				},
			},
			RollupRuleChanges: []RollupRuleChange{
				{
					Op:     ChangeOp,
					RuleID: p("rrID1"),
					RuleData: &models.RollupRule{
						Name: "change3",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.RollupRule{
						Name: "Add2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("rrID2"),
					RuleData: &models.RollupRule{
						Name: "change1",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: p("rrID3"),
					RuleData: &models.RollupRule{
						Name: "change2",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.RollupRule{
						Name: "Add1",
					},
				},
			},
		},
	}
)

func p(s string) *string {
	return &s
}
