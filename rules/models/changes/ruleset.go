package changes

import (
	"encoding/json"
	"fmt"
	"sort"
)

// UpdateOp is an update operation.
type UpdateOp int

// A list of supported update operations.
const (
	UnknownOp UpdateOp = iota
	AddOp
	RemoveOp
	ChangeOp
)

// ParseUpdateOp parses an input string into an update op.
func ParseUpdateOp(str string) (UpdateOp, error) {
	switch str {
	case "add":
		return AddOp, nil
	case "remove":
		return RemoveOp, nil
	case "change":
		return ChangeOp, nil
	default:
		return UnknownOp, fmt.Errorf("unknown update op %s", str)
	}
}

func (op UpdateOp) String() string {
	switch op {
	case AddOp:
		return "add"
	case RemoveOp:
		return "remove"
	case ChangeOp:
		return "change"
	default:
		return "unknown"
	}
}

// MarshalJSON marshals update op as JSON.
func (op UpdateOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(op.String())
}

// UnmarshalJSON unmarshals data into update op.
func (op *UpdateOp) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	res, err := ParseUpdateOp(str)
	if err != nil {
		return err
	}
	*op = res
	return nil
}

// RuleSetChangeSet is a ruleset diff.
type RuleSetChangeSet struct {
	Namespace          string              `json:"namespace"`
	MappingRuleChanges []MappingRuleChange `json:"mappingRuleChanges"`
	RollupRuleChanges  []RollupRuleChange  `json:"rollupRuleChanges"`
}

// Sort sorts the ruleset diff by op and rule names.
func (d *RuleSetChangeSet) Sort() {
	sort.Sort(mappingRuleChangesByOpNameAsc(d.MappingRuleChanges))
	sort.Sort(rollupRuleChangesByOpNameAsc(d.RollupRuleChanges))
}

// RuleSetChangeSets is a list of ruleset diffs.
type RuleSetChangeSets []RuleSetChangeSet

// Sort sorts the ruleset diffs by op and namespace in ascending order.
func (d RuleSetChangeSets) Sort() {
	for i := range d {
		d[i].Sort()
	}
	sort.Sort(ruleSetChangesNamespaceAsc(d))
}

type ruleSetChangesNamespaceAsc []RuleSetChangeSet

func (a ruleSetChangesNamespaceAsc) Len() int      { return len(a) }
func (a ruleSetChangesNamespaceAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ruleSetChangesNamespaceAsc) Less(i, j int) bool {
	return a[i].Namespace < a[j].Namespace
}
