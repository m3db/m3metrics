package changes

import "github.com/m3db/m3metrics/rules/models"

// MappingRuleChange is a mapping rule diff.
type MappingRuleChange struct {
	Op       UpdateOp            `json:"updateOp"`
	RuleID   string              `json:"ruleID,omitempty"`
	RuleData *models.MappingRule `json:"ruleData,omitempty"`
}

type mappingRuleChangesByOpNameAsc []MappingRuleChange

func (a mappingRuleChangesByOpNameAsc) Len() int      { return len(a) }
func (a mappingRuleChangesByOpNameAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a mappingRuleChangesByOpNameAsc) Less(i, j int) bool {
	if a[i].Op < a[j].Op {
		return true
	}
	if a[i].Op > a[j].Op {
		return false
	}
	return compareMappingRuleChanges(a[i], a[j]) < 0
}

func compareMappingRuleChanges(a, b MappingRuleChange) int {
	// Remove operations maybe not have RuleData. Only compare IDs.
	if a.Op == 2 {
		return compareRemoveMappingRuleChanges(a, b)
	}
	return compareMappingRules(a.RuleData, b.RuleData)
}

// compare compares two mapping rules by id, returning
// * -1 if a < b
// * 1 if a > b
// * 0 if a == b
func compareRemoveMappingRuleChanges(a, b MappingRuleChange) int {
	if a.RuleID < b.RuleID {
		return -1
	}
	if a.RuleID > b.RuleID {
		return 1
	}
	return 0
}

// compare compares two mapping rules by name, returning
// * -1 if a < b
// * 1 if a > b
// * 0 if a == b
func compareMappingRules(a, b *models.MappingRule) int {
	if a.Name < b.Name {
		return -1
	}
	if a.Name > b.Name {
		return 1
	}
	return 0
}
