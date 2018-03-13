// Copyright (c) 2018 Uber Technologies, Inc.
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
	"sort"

	"github.com/m3db/m3metrics/policy"
)

// MappingRuleJSON is a common json serializable mapping rule.
type MappingRuleJSON struct {
	ID                  string          `json:"id,omitempty"`
	Name                string          `json:"name" validate:"required"`
	CutoverMillis       int64           `json:"cutoverMillis,omitempty"`
	Filter              string          `json:"filter" validate:"required"`
	Policies            []policy.Policy `json:"policies" validate:"required"`
	LastUpdatedBy       string          `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64           `json:"lastUpdatedAtMillis"`
}

// NewMappingRuleJSON takes a MappingRuleView and returns the equivalent MappingRuleJSON.
func NewMappingRuleJSON(mrv *MappingRuleView) MappingRuleJSON {
	return MappingRuleJSON{
		ID:                  mrv.ID,
		Name:                mrv.Name,
		Filter:              mrv.Filter,
		Policies:            mrv.Policies,
		CutoverMillis:       mrv.CutoverNanos / nanosPerMilli,
		LastUpdatedBy:       mrv.LastUpdatedBy,
		LastUpdatedAtMillis: mrv.LastUpdatedAtNanos / nanosPerMilli,
	}
}

// ToMappingRuleView returns a ToMappingRuleView type.
func (m MappingRuleJSON) ToMappingRuleView() *MappingRuleView {
	return &MappingRuleView{
		ID:       m.ID,
		Name:     m.Name,
		Filter:   m.Filter,
		Policies: m.Policies,
	}
}

// Equals determines whether two mapping rules are equal.
func (m *MappingRuleJSON) Equals(other *MappingRuleJSON) bool {
	if m == nil && other == nil {
		return true
	}
	if m == nil || other == nil {
		return false
	}
	return m.ID == other.ID &&
		m.Name == other.Name &&
		m.Filter == other.Filter &&
		policy.Policies(m.Policies).Equals(policy.Policies(other.Policies))
}

// Sort sorts the policies inside the mapping rule.
func (m *MappingRuleJSON) Sort() {
	sort.Sort(policy.ByResolutionAscRetentionDesc(m.Policies))
}

type mappingRuleJSONsByNameAsc []MappingRuleJSON

func (a mappingRuleJSONsByNameAsc) Len() int           { return len(a) }
func (a mappingRuleJSONsByNameAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a mappingRuleJSONsByNameAsc) Less(i, j int) bool { return a[i].Name < a[j].Name }
