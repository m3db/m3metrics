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

package models

import (
	"sort"

	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/policy"
)

// RollupTarget is a common json serializable rollup target.
type RollupTarget struct {
	Pipeline        op.Pipeline            `json:"pipeline" validate:"required"`
	StoragePolicies policy.StoragePolicies `json:"storagePolicies" validate:"required"`
}

// NewRollupTarget takes a RollupTargetView and returns the equivalent RollupTarget.
func NewRollupTarget(t RollupTargetView) RollupTarget {
	return RollupTarget(t)
}

// ToRollupTargetView returns the equivalent ToRollupTargetView.
func (t RollupTarget) ToRollupTargetView() RollupTargetView {
	return RollupTargetView(t)
}

// Sort sorts the storage policies inside the rollup target.
func (t *RollupTarget) Sort() {
	sort.Sort(policy.ByResolutionAscRetentionDesc(t.StoragePolicies))
}

// Equal determines whether two rollup targets are equal.
func (t *RollupTarget) Equal(other *RollupTarget) bool {
	if t == nil && other == nil {
		return true
	}
	if t == nil || other == nil {
		return false
	}
	return t.Pipeline.Equal(other.Pipeline) && t.StoragePolicies.Equal(other.StoragePolicies)
}

// RollupTargetView is a human friendly representation of a rollup rule target at a given point in time.
type RollupTargetView struct {
	Pipeline        op.Pipeline
	StoragePolicies policy.StoragePolicies
}

/*
// IsCompatibleWith returns whether two rollup target views are compatible.
func (rtv *RollupTargetView) IsCompatibleWith(other RollupTargetView) bool {
	rollup
}
*/

// RollupRule is a common json serializable rollup rule. Implements Sort interface.
type RollupRule struct {
	ID                  string         `json:"id,omitempty"`
	Name                string         `json:"name" validate:"required"`
	Filter              string         `json:"filter" validate:"required"`
	Targets             []RollupTarget `json:"targets" validate:"required,dive,required"`
	CutoverMillis       int64          `json:"cutoverMillis,omitempty"`
	LastUpdatedBy       string         `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64          `json:"lastUpdatedAtMillis"`
}

// NewRollupRule takes a RollupRuleView and returns the equivalent RollupRule.
func NewRollupRule(rrv *RollupRuleView) RollupRule {
	targets := make([]RollupTarget, len(rrv.Targets))
	for i, t := range rrv.Targets {
		targets[i] = NewRollupTarget(t)
	}
	return RollupRule{
		ID:                  rrv.ID,
		Name:                rrv.Name,
		Filter:              rrv.Filter,
		Targets:             targets,
		CutoverMillis:       rrv.CutoverNanos / nanosPerMilli,
		LastUpdatedBy:       rrv.LastUpdatedBy,
		LastUpdatedAtMillis: rrv.LastUpdatedAtNanos / nanosPerMilli,
	}
}

// ToRollupRuleView returns the equivalent ToRollupRuleView.
func (r RollupRule) ToRollupRuleView() *RollupRuleView {
	targets := make([]RollupTargetView, len(r.Targets))
	for i, t := range r.Targets {
		targets[i] = t.ToRollupTargetView()
	}

	return &RollupRuleView{
		ID:      r.ID,
		Name:    r.Name,
		Filter:  r.Filter,
		Targets: targets,
	}
}

// Equals determines whether two rollup rules are equal.
func (r *RollupRule) Equals(other *RollupRule) bool {
	if r == nil && other == nil {
		return true
	}
	if r == nil || other == nil {
		return false
	}
	return r.Name == other.Name &&
		r.Filter == other.Filter &&
		rollupTargets(r.Targets).Equals(other.Targets)
}

/*
// Sort sorts the rollup targets inside the rollup rule.
func (r *RollupRule) Sort() {
	for i := range r.Targets {
		r.Targets[i].Sort()
	}
	sort.Sort(rollupTargetsByNameTagsAsc(r.Targets))
}
*/

// RollupRuleView is a human friendly representation of a rollup rule at a given point in time.
type RollupRuleView struct {
	ID                 string
	Name               string
	Tombstoned         bool
	CutoverNanos       int64
	Filter             string
	Targets            []RollupTargetView
	LastUpdatedBy      string
	LastUpdatedAtNanos int64
}

// RollupRuleViews belonging to a ruleset indexed by uuid.
// Each value contains the entire snapshot history of the rule.
type RollupRuleViews map[string][]*RollupRuleView

/*
type rollupTargetsByNameTagsAsc []RollupTarget

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
*/

type rollupTargets []RollupTarget

func (t rollupTargets) Equals(other rollupTargets) bool {
	if len(t) != len(other) {
		return false
	}
	for i := 0; i < len(t); i++ {
		if !t[i].Equal(&other[i]) {
			return false
		}
	}
	return true
}

type rollupRulesByNameAsc []RollupRule

func (a rollupRulesByNameAsc) Len() int           { return len(a) }
func (a rollupRulesByNameAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a rollupRulesByNameAsc) Less(i, j int) bool { return a[i].Name < a[j].Name }
