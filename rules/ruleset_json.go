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
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3metrics/policy"
	"github.com/pborman/uuid"
)

const (
	nanosPerMilli = int64(time.Millisecond / time.Nanosecond)
)

// IDGenType describes the scheme for generating IDs in rule snapshots from RuleSetJSON structs
// which dont have a ID.
type IDGenType int

const (
	// GenerateID Generates a UUID v4.
	GenerateID IDGenType = iota
	// DontGenerateID does not generate ID for the rule snapshot.
	DontGenerateID
)

// RuleSetJSON is a common json serializable rule set. Implements Sort interface.
type RuleSetJSON struct {
	Namespace     string            `json:"id"`
	Version       int               `json:"version"`
	CutoverMillis int64             `json:"cutoverMillis"`
	MappingRules  []MappingRuleJSON `json:"mappingRules"`
	RollupRules   []RollupRuleJSON  `json:"rollupRules"`
}

// NewRuleSetJSON takes a RuleSetSnapshot and returns the equivalent RuleSetJSON.
func NewRuleSetJSON(latest *RuleSetSnapshot) RuleSetJSON {
	var mrJSON []MappingRuleJSON
	for _, m := range latest.MappingRules {
		mrJSON = append(mrJSON, NewMappingRuleJSON(m))
	}
	var rrJSON []RollupRuleJSON
	for _, r := range latest.RollupRules {
		rrJSON = append(rrJSON, NewRollupRuleJSON(r))
	}
	return RuleSetJSON{
		Namespace:     latest.Namespace,
		Version:       latest.Version,
		CutoverMillis: latest.CutoverNanos / nanosPerMilli,
		MappingRules:  mrJSON,
		RollupRules:   rrJSON,
	}
}

// RuleSetSnapshot create a RuleSetSnapshot from a RuleSetJSON. If the RuleSetJSON has no IDs
// for any of its mapping rules or rollup rules, it generates missing IDs and sets as a string UUID
// string so they can be stored in a mapping (id -> rule).
func (r RuleSetJSON) RuleSetSnapshot(IDGenType IDGenType) (*RuleSetSnapshot, error) {
	mappingRules := make(map[string]*MappingRuleView, len(r.MappingRules))
	for _, mr := range r.MappingRules {
		id := mr.ID
		if id == "" {
			if IDGenType == DontGenerateID {
				return nil, fmt.Errorf("can't convert RuleSetJSON to ruleSetSnapshot, no mapping rule id for %v", mr)
			}
			id = uuid.New()
			mr.ID = id
		}
		mappingRules[id] = mr.MappingRuleView()
	}

	rollupRules := make(map[string]*RollupRuleView, len(r.RollupRules))
	for _, rr := range r.RollupRules {
		id := rr.ID
		if id == "" {
			if IDGenType == DontGenerateID {
				return nil, fmt.Errorf("can't convert RuleSetJSON to ruleSetSnapshot, no rollup rule id for %v", rr)
			}
			id = uuid.New()
			rr.ID = id
		}
		rollupRules[id] = rr.RollupRuleView()
	}

	return &RuleSetSnapshot{
		Namespace:    r.Namespace,
		Version:      r.Version,
		MappingRules: mappingRules,
		RollupRules:  rollupRules,
	}, nil
}

// Sort sorts the rules in the ruleset.
func (r *RuleSetJSON) Sort() {
	for i := range r.MappingRules {
		r.MappingRules[i].Sort()
	}
	sort.Sort(mappingRulesByNameAsc(r.MappingRules))

	for i := range r.RollupRules {
		r.RollupRules[i].Sort()
	}
	sort.Sort(rollupRulesByNameAsc(r.RollupRules))
}

// RuleSets is a collection of rulesets.
type RuleSets map[string]*RuleSetJSON

// Sort sorts each ruleset based on it's own sort method.
func (rss RuleSets) Sort() {
	for _, rs := range rss {
		rs.Sort()
	}
}

type policies []policy.Policy

func (p policies) Equals(other policies) bool {
	if len(p) != len(other) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] != other[i] {
			return false
		}
	}
	return true
}
