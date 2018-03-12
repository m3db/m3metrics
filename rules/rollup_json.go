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
	"sort"

	"github.com/m3db/m3metrics/policy"
)

// RollupTargetJSON is a common json serializable rollup target. Implements Sort interface.
type RollupTargetJSON struct {
	Name     string          `json:"name" validate:"required"`
	Tags     []string        `json:"tags" validate:"required"`
	Policies []policy.Policy `json:"policies" validate:"required"`
}

// RollupRuleJSON is a common json serializable rollup rule. Implements Sort interface.
type RollupRuleJSON struct {
	ID                  string             `json:"id,omitempty"`
	Name                string             `json:"name" validate:"required"`
	Filter              string             `json:"filter" validate:"required"`
	Targets             []RollupTargetJSON `json:"targets" validate:"required,dive,required"`
	CutoverMillis       int64              `json:"cutoverMillis,omitempty"`
	LastUpdatedBy       string             `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64              `json:"lastUpdatedAtMillis"`
}

// NewRollupTargetJSON takes a RollupTargetView and returns the equivalent RollupTargetJSON.
func NewRollupTargetJSON(t RollupTargetView) RollupTargetJSON {
	return RollupTargetJSON(t)
}

// NewRollupRuleJSON takes a RollupRuleView and returns the equivalent RollupRuleJSON.
func NewRollupRuleJSON(rrv *RollupRuleView) RollupRuleJSON {
	targets := make([]RollupTargetJSON, len(rrv.Targets))
	for i, t := range rrv.Targets {
		targets[i] = NewRollupTargetJSON(t)
	}
	return RollupRuleJSON{
		ID:                  rrv.ID,
		Name:                rrv.Name,
		Filter:              rrv.Filter,
		Targets:             targets,
		CutoverMillis:       rrv.CutoverNanos / nanosPerMilli,
		LastUpdatedBy:       rrv.LastUpdatedBy,
		LastUpdatedAtMillis: rrv.LastUpdatedAtNanos / nanosPerMilli,
	}
}

// RollupTargetView returns the equivalent RollupTargetView.
func (t RollupTargetJSON) RollupTargetView() RollupTargetView {
	return RollupTargetView(t)
}

// RollupRuleView returns the equivalent RollupRuleView.
func (r RollupRuleJSON) RollupRuleView() *RollupRuleView {
	targets := make([]RollupTargetView, len(r.Targets))
	for i, t := range r.Targets {
		targets[i] = t.RollupTargetView()
	}

	return &RollupRuleView{
		ID:      r.ID,
		Name:    r.Name,
		Filter:  r.Filter,
		Targets: targets,
	}
}

type rollupTargets []RollupTargetJSON

func (t rollupTargets) Equals(other rollupTargets) bool {
	if len(t) != len(other) {
		return false
	}
	for i := 0; i < len(t); i++ {
		if !t[i].Equals(&other[i]) {
			return false
		}
	}
	return true
}

type rollupTargetsByNameTagsAsc []RollupTargetJSON

func (a rollupTargetsByNameTagsAsc) Len() int      { return len(a) }
func (a rollupTargetsByNameTagsAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a rollupTargetsByNameTagsAsc) Less(i, j int) bool {
	if a[i].Name < a[j].Name {
		return true
	}
	if a[i].Name > a[j].Name {
		return false
	}
	tIdx := 0
	for tIdx < len(a[i].Tags) && tIdx < len(a[j].Tags) {
		ti := a[i].Tags[tIdx]
		tj := a[j].Tags[tIdx]
		if ti < tj {
			return true
		}
		if ti > tj {
			return false
		}
		tIdx++
	}
	return len(a[i].Tags) < len(a[j].Tags)
}

func (t rollupTargetsJSON) Equals(other rollupTargetsJSON) bool {
	if len(t) != len(other) {
		return false
	}
	for i := 0; i < len(t); i++ {
		if !t[i].Equals(&other[i]) {
			return false
		}
	}
	return true
}

type rollupTargetsJSON []RollupTargetJSON

// Equals determines whether two rollup targets are equal.
func (t *RollupTargetJSON) Equals(other *RollupTargetJSON) bool {
	if t == nil && other == nil {
		return true
	}
	if t == nil || other == nil {
		return false
	}
	if t.Name != other.Name {
		return false
	}
	if len(t.Tags) != len(other.Tags) {
		return false
	}
	for i := 0; i < len(t.Tags); i++ {
		if t.Tags[i] != other.Tags[i] {
			return false
		}
	}
	return policies(t.Policies).Equals(policies(other.Policies))
}

type rollupRulesByNameAsc []RollupRuleJSON

func (a rollupRulesByNameAsc) Len() int           { return len(a) }
func (a rollupRulesByNameAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a rollupRulesByNameAsc) Less(i, j int) bool { return a[i].Name < a[j].Name }

// Equals determines whether two rollup rules are equal.
func (r *RollupRuleJSON) Equals(other *RollupRuleJSON) bool {
	if r == nil && other == nil {
		return true
	}
	if r == nil || other == nil {
		return false
	}
	return r.Name == other.Name &&
		r.Filter == other.Filter &&
		rollupTargetsJSON(r.Targets).Equals(other.Targets)
}

// Sort sorts the rollup targets inside the rollup rule.
func (r *RollupRuleJSON) Sort() {
	for i := range r.Targets {
		r.Targets[i].Sort()
	}
	sort.Sort(rollupTargetsByNameTagsAsc(r.Targets))
}

// Sort sorts the policies inside the rollup target.
func (t *RollupTargetJSON) Sort() {
	sort.Strings(t.Tags)
	sort.Sort(policy.ByResolutionAscRetentionDesc(t.Policies))
}
