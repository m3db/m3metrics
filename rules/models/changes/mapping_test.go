package changes

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3metrics/rules/models"
)

func TestSortMappingRuleChanges(t *testing.T) {
	ruleChanges := []MappingRuleChange{
		{
			Op:     RemoveOp,
			RuleID: "rrID5",
		},
		{
			Op:     RemoveOp,
			RuleID: "rrID4",
		},
		{
			Op:     ChangeOp,
			RuleID: "rrID1",
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
			RuleID: "rrID2",
			RuleData: &models.MappingRule{
				Name: "change1",
			},
		},
		{
			Op:     RemoveOp,
			RuleID: "rrID5",
		},
		{
			Op:     RemoveOp,
			RuleID: "rrID4",
		},
		{
			Op:     ChangeOp,
			RuleID: "rrID3",
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
		{
			Op:     ChangeOp,
			RuleID: "rrID2",
			RuleData: &models.MappingRule{
				Name: "change1",
			},
		},
	}
	expected := []MappingRuleChange{
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
			Op:     RemoveOp,
			RuleID: "rrID4",
		},
		{
			Op:     RemoveOp,
			RuleID: "rrID4",
		},
		{
			Op:     RemoveOp,
			RuleID: "rrID5",
		},
		{
			Op:     RemoveOp,
			RuleID: "rrID5",
		},
		{
			Op:     ChangeOp,
			RuleID: "rrID2",
			RuleData: &models.MappingRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: "rrID2",
			RuleData: &models.MappingRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: "rrID3",
			RuleData: &models.MappingRule{
				Name: "change2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: "rrID1",
			RuleData: &models.MappingRule{
				Name: "change3",
			},
		},
	}

	sort.Sort(mappingRuleChangesByOpNameAsc(ruleChanges))
	require.Equal(t, expected, ruleChanges)
}
