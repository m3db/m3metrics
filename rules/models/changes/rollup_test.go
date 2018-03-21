package changes

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3metrics/rules/models"
)

func TestSortRollupRuleChanges(t *testing.T) {
	ruleChanges := []RollupRuleChange{
		{
			Op:     DeleteOp,
			RuleID: p("rrID5"),
		},
		{
			Op:     DeleteOp,
			RuleID: p("rrID4"),
		},
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
			Op:     DeleteOp,
			RuleID: p("rrID5"),
		},
		{
			Op:     DeleteOp,
			RuleID: p("rrID4"),
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
		{
			Op:     ChangeOp,
			RuleID: p("rrID2"),
			RuleData: &models.RollupRule{
				Name: "change1",
			},
		},
	}
	expected := []RollupRuleChange{
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
		{
			Op:     DeleteOp,
			RuleID: p("rrID4"),
		},
		{
			Op:     DeleteOp,
			RuleID: p("rrID4"),
		},
		{
			Op:     DeleteOp,
			RuleID: p("rrID5"),
		},
		{
			Op:     DeleteOp,
			RuleID: p("rrID5"),
		},
	}

	sort.Sort(rollupRuleChangesByOpAscNameAscIDAsc(ruleChanges))
	require.Equal(t, expected, ruleChanges)
}
