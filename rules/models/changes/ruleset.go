package changes

import (
	"sort"
)

// RuleSetChanges is a ruleset diff.
type RuleSetChanges struct {
	Namespace          string              `json:"namespace"`
	MappingRuleChanges []MappingRuleChange `json:"mappingRuleChanges"`
	RollupRuleChanges  []RollupRuleChange  `json:"rollupRuleChanges"`
}

// Sort sorts the ruleset diff by op and rule names.
func (d *RuleSetChanges) Sort() {
	sort.Sort(mappingRuleChangesByOpAscNameAscIDAsc(d.MappingRuleChanges))
	sort.Sort(rollupRuleChangesByOpAscNameAscIDAsc(d.RollupRuleChanges))
}

// RuleSetsChanges is a list of ruleset diffs.
type RuleSetsChanges []RuleSetChanges

// Sort sorts the ruleset diffs by op and namespace in ascending order.
func (d RuleSetsChanges) Sort() {
	for i := range d {
		d[i].Sort()
	}
	sort.Sort(ruleSetChangesNamespaceAsc(d))
}

type ruleSetChangesNamespaceAsc []RuleSetChanges

func (a ruleSetChangesNamespaceAsc) Len() int      { return len(a) }
func (a ruleSetChangesNamespaceAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ruleSetChangesNamespaceAsc) Less(i, j int) bool {
	return a[i].Namespace < a[j].Namespace
}
