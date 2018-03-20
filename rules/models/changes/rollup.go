package changes

import "github.com/m3db/m3metrics/rules/models"

// RollupRuleChange is a rollup rule diff.
type RollupRuleChange struct {
	Op       UpdateOp           `json:"updateOp"`
	RuleID   string             `json:"ruleID,omitempty"`
	RuleData *models.RollupRule `json:"to,omitempty"`
}

type rollupRuleChangesByOpNameAsc []RollupRuleChange

func (a rollupRuleChangesByOpNameAsc) Len() int      { return len(a) }
func (a rollupRuleChangesByOpNameAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a rollupRuleChangesByOpNameAsc) Less(i, j int) bool {
	if a[i].Op < a[j].Op {
		return true
	}
	if a[i].Op > a[j].Op {
		return false
	}

	return compareRollupRuleChanges(a[i], a[j]) < 0
}

func compareRollupRuleChanges(a, b RollupRuleChange) int {
	// Remove operations maybe not have RuleData. Only compare IDs.
	if a.Op == 2 {
		return compareRemoveRollupRuleChanges(a, b)
	}
	return compareRollupRules(a.RuleData, b.RuleData)
}

// compare compares two rollup rules by id, returning
// * -1 if a < b
// * 1 if a > b
// * 0 if a == b
func compareRemoveRollupRuleChanges(a, b RollupRuleChange) int {
	if a.RuleID < b.RuleID {
		return -1
	}
	if a.RuleID > b.RuleID {
		return 1
	}
	return 0
}

// compare compares two rollup rules by name, returning
// * -1 if a < b
// * 1 if a > b
// * 0 if a == b
func compareRollupRules(a, b *models.RollupRule) int {
	if a.Name < b.Name {
		return -1
	}
	if a.Name > b.Name {
		return 1
	}
	return 0
}
