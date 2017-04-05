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
	"bytes"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
)

var (
	errNilRuleSetSchema = errors.New("nil rule set schema")

	// timeMax is the maximum time value for which time.Before() and
	// time.After() still work.
	timeMax = time.Unix(1<<63-62135596801, 999999999).UnixNano()
)

// TagPair contains a tag name and a tag value.
type TagPair struct {
	Name  []byte
	Value []byte
}

// Matcher matches metrics against rules to determine applicable policies.
type Matcher interface {
	// Match returns applicable policies given a metric id for a given time.
	Match(id []byte, t time.Time) MatchResult
}

type activeRuleSet struct {
	version            int
	iterFn             filters.NewSortedTagIteratorFn
	newIDFn            NewIDFn
	mappingRuleChanges []*mappingRuleChanges
	rollupRuleChanges  []*rollupRuleChanges
	cutoverTimesAsc    []int64
}

func newActiveRuleSet(
	version int,
	iterFn filters.NewSortedTagIteratorFn,
	newIDFn NewIDFn,
	mappingRuleChanges []*mappingRuleChanges,
	rollupRuleChanges []*rollupRuleChanges,
) *activeRuleSet {
	uniqueCutoverTimes := make(map[int64]struct{})
	for _, ruleChanges := range mappingRuleChanges {
		for _, rule := range ruleChanges.changes {
			uniqueCutoverTimes[rule.cutoverNs] = struct{}{}
		}
	}
	for _, ruleChanges := range rollupRuleChanges {
		for _, rule := range ruleChanges.changes {
			uniqueCutoverTimes[rule.cutoverNs] = struct{}{}
		}
	}

	cutoverTimeAsc := make([]int64, 0, len(uniqueCutoverTimes))
	for t := range uniqueCutoverTimes {
		cutoverTimeAsc = append(cutoverTimeAsc, t)
	}
	sort.Sort(int64Asc(cutoverTimeAsc))

	return &activeRuleSet{
		version:            version,
		iterFn:             iterFn,
		newIDFn:            newIDFn,
		mappingRuleChanges: mappingRuleChanges,
		rollupRuleChanges:  rollupRuleChanges,
		cutoverTimesAsc:    cutoverTimeAsc,
	}
}

func (as *activeRuleSet) Match(id []byte, t time.Time) MatchResult {
	mappingCutoverNs, mappingPolicies := as.matchMappings(id, t)
	rollupCutoverNs, rollupResults := as.matchRollups(id, t)

	// NB(xichen): we take the latest cutover time across all rules matched. This is
	// used to determine whether the match result is valid for a given time.
	cutoverNs := int64(math.Max(float64(mappingCutoverNs), float64(rollupCutoverNs)))
	expireAtNs := as.nextCutover(cutoverNs)
	return newMatchResult(as.version, cutoverNs, expireAtNs, mappingPolicies, rollupResults)
}

func (as *activeRuleSet) matchMappings(id []byte, t time.Time) (int64, []policy.Policy) {
	// TODO(xichen): pool the policies
	var (
		cutoverNs int64
		policies  []policy.Policy
	)
	for _, ruleChanges := range as.mappingRuleChanges {
		rule := ruleChanges.ActiveRule(t)
		if rule == nil {
			continue
		}
		if cutoverNs < rule.cutoverNs {
			cutoverNs = rule.cutoverNs
		}
		if !rule.filter.Matches(id) {
			continue
		}
		policies = append(policies, rule.policies...)
	}
	return cutoverNs, resolvePolicies(policies)
}

func (as *activeRuleSet) matchRollups(id []byte, t time.Time) (int64, []rollupResult) {
	// TODO(xichen): pool the rollup targets.
	var (
		cutoverNs int64
		rollups   []rollupTarget
	)
	for _, ruleChanges := range as.rollupRuleChanges {
		rule := ruleChanges.ActiveRule(t)
		if rule == nil {
			continue
		}
		if !rule.filter.Matches(id) {
			continue
		}
		if cutoverNs < rule.cutoverNs {
			cutoverNs = rule.cutoverNs
		}
		for _, target := range rule.targets {
			found := false
			// If the new target has the same transformation as an existing one,
			// we merge their policies.
			for i := range rollups {
				if rollups[i].sameTransform(target) {
					rollups[i].Policies = append(rollups[i].Policies, target.Policies...)
					found = true
					break
				}
			}
			// Otherwise, we add a new rollup target.
			if !found {
				rollups = append(rollups, target.clone())
			}
		}
	}

	// Resolve the policies for each rollup target.
	for i := range rollups {
		rollups[i].Policies = resolvePolicies(rollups[i].Policies)
	}

	return cutoverNs, as.toRollupResults(id, rollups)
}

// toRollupResults encodes rollup target name and values into ids for each rollup target.
func (as *activeRuleSet) toRollupResults(id []byte, targets []rollupTarget) []rollupResult {
	// NB(r): This is n^2 however this should be quite fast still as
	// long as there is not an absurdly high number of rollup
	// targets for any given ID and that iterfn is alloc free.
	//
	// Even with a very high number of rules its still predicted that
	// any given ID would match a relatively low number of rollups.

	// TODO(xichen): pool tag pairs and rollup results.
	var tagPairs []TagPair
	rollups := make([]rollupResult, 0, len(targets))
	for _, target := range targets {
		tagPairs = tagPairs[:0]
		for _, tag := range target.Tags {
			iter := as.iterFn(id)
			for iter.Next() {
				name, value := iter.Current()
				if bytes.Equal(name, tag) {
					tagPairs = append(tagPairs, TagPair{Name: tag, Value: value})
					break
				}
			}
			iter.Close()
		}
		result := rollupResult{
			ID:       as.newIDFn(target.Name, tagPairs),
			Policies: target.Policies,
		}
		rollups = append(rollups, result)
	}

	return rollups
}

// nextCutover returns the next cutover time after t.
// NB(xichen): not using sort.Search to avoid a lambda capture.
func (as *activeRuleSet) nextCutover(t int64) int64 {
	i, j := 0, len(as.cutoverTimesAsc)
	for i < j {
		h := i + (j-i)/2
		if as.cutoverTimesAsc[h] <= t {
			i = h + 1
		} else {
			j = h
		}
	}
	if i < len(as.cutoverTimesAsc) {
		return as.cutoverTimesAsc[i]
	}
	return timeMax
}

// RuleSet is a set of rules associated with a namespace.
type RuleSet interface {
	// Namespace is the metrics namespace the ruleset applies to.
	Namespace() string

	// Version returns the ruleset version.
	Version() int

	// CutoverNs returns when the ruleset takes effect.
	CutoverNs() int64

	// TombStoned returns whether the ruleset is tombstoned.
	TombStoned() bool

	// ActiveSet returns the active ruleset at a given time.
	ActiveSet(t time.Time) Matcher
}

type ruleSet struct {
	uuid               string
	version            int
	namespace          string
	createdAtNs        int64
	lastUpdatedAtNs    int64
	tombStoned         bool
	cutoverNs          int64
	iterFn             filters.NewSortedTagIteratorFn
	newIDFn            NewIDFn
	mappingRuleChanges []*mappingRuleChanges
	rollupRuleChanges  []*rollupRuleChanges
}

// NewRuleSet creates a new ruleset
func NewRuleSet(version int, rs *schema.RuleSet, opts Options) (RuleSet, error) {
	if rs == nil {
		return nil, errNilRuleSetSchema
	}
	iterFn := opts.NewSortedTagIteratorFn()
	mappingRuleChanges := make([]*mappingRuleChanges, 0, len(rs.MappingRuleChanges))
	for _, ruleChanges := range rs.MappingRuleChanges {
		mc, err := newMappingRuleChanges(ruleChanges, iterFn)
		if err != nil {
			return nil, err
		}
		mappingRuleChanges = append(mappingRuleChanges, mc)
	}
	rollupRuleChanges := make([]*rollupRuleChanges, 0, len(rs.RollupRuleChanges))
	for _, ruleChanges := range rs.RollupRuleChanges {
		rc, err := newRollupRuleChanges(ruleChanges, iterFn)
		if err != nil {
			return nil, err
		}
		rollupRuleChanges = append(rollupRuleChanges, rc)
	}
	return &ruleSet{
		uuid:               rs.Uuid,
		version:            version,
		namespace:          rs.Namespace,
		createdAtNs:        rs.CreatedAt,
		lastUpdatedAtNs:    rs.LastUpdatedAt,
		tombStoned:         rs.Tombstoned,
		cutoverNs:          rs.CutoverTime,
		iterFn:             iterFn,
		newIDFn:            opts.NewIDFn(),
		mappingRuleChanges: mappingRuleChanges,
		rollupRuleChanges:  rollupRuleChanges,
	}, nil
}

func (rs *ruleSet) Namespace() string { return rs.namespace }
func (rs *ruleSet) Version() int      { return rs.version }
func (rs *ruleSet) CutoverNs() int64  { return rs.cutoverNs }
func (rs *ruleSet) TombStoned() bool  { return rs.tombStoned }

func (rs *ruleSet) ActiveSet(t time.Time) Matcher {
	mappingRuleChanges := make([]*mappingRuleChanges, 0, len(rs.mappingRuleChanges))
	for _, ruleChanges := range rs.mappingRuleChanges {
		activeRules := ruleChanges.ActiveRules(t)
		mappingRuleChanges = append(mappingRuleChanges, activeRules)
	}
	rollupRuleChanges := make([]*rollupRuleChanges, 0, len(rs.rollupRuleChanges))
	for _, ruleChanges := range rs.rollupRuleChanges {
		activeRules := ruleChanges.ActiveRules(t)
		rollupRuleChanges = append(rollupRuleChanges, activeRules)
	}
	return newActiveRuleSet(rs.version, rs.iterFn, rs.newIDFn, mappingRuleChanges, rollupRuleChanges)
}

// resolvePolicies resolves the conflicts among policies if any, following the rules below:
// * If two policies have the same resolution but different retention, the one with longer
// retention period is chosen
// * If two policies have the same retention but different resolution, the policy with higher
// resolution is chosen
// * If a policy has lower resolution and shorter retention than another policy, the policy
// is superseded by the other policy and therefore ignored.
func resolvePolicies(policies []policy.Policy) []policy.Policy {
	if len(policies) == 0 {
		return policies
	}
	sort.Sort(policy.ByResolutionAsc(policies))
	// curr is the index of the last policy kept so far.
	curr := 0
	for i := 1; i < len(policies); i++ {
		// If the policy has the same resolution, it must have either the same or shorter retention
		// period due to sorting, so we keep the one with longer retention period and ignore this
		// policy.
		if policies[curr].Resolution().Window == policies[i].Resolution().Window {
			continue
		}
		// Otherwise the policy has lower resolution, so if it has shorter or the same retention
		// period, we keep the one with higher resolution and longer retention period and ignore
		// this policy.
		if policies[curr].Retention() >= policies[i].Retention() {
			continue
		}
		// Now we are guaranteed the policy has lower resolution and higher retention than the
		// current one, so we want to keep it.
		curr++
		policies[curr] = policies[i]
	}
	return policies[:curr+1]
}

type int64Asc []int64

func (a int64Asc) Len() int           { return len(a) }
func (a int64Asc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64Asc) Less(i, j int) bool { return a[i] < a[j] }
