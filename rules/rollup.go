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
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/pborman/uuid"
)

var (
	emptyRollupTarget RollupTarget

	errNilRollupTargetSchema       = errors.New("nil rollup target schema")
	errNilRollupRuleSnapshotSchema = errors.New("nil rollup rule snapshot schema")
	errNilRollupRuleSchema         = errors.New("nil rollup rule schema")
)

// RollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be grouped and rolled up across the provided set of tags, named
// with the provided name, and aggregated and retained under the provided policies.
type RollupTarget struct {
	Name     []byte
	Tags     [][]byte
	Policies []policy.Policy
}

func newRollupTarget(target *schema.RollupTarget) (RollupTarget, error) {
	if target == nil {
		return emptyRollupTarget, errNilRollupTargetSchema
	}
	policies, err := policy.NewPoliciesFromSchema(target.Policies)
	if err != nil {
		return emptyRollupTarget, err
	}
	tags := make([]string, len(target.Tags))
	copy(tags, target.Tags)
	sort.Strings(tags)
	return RollupTarget{
		Name:     []byte(target.Name),
		Tags:     bytesArrayFromStringArray(tags),
		Policies: policies,
	}, nil
}

// NewRollupTargetFromFields ...
func NewRollupTargetFromFields(name string, tags []string, policies []policy.Policy) RollupTarget {
	return RollupTarget{Name: []byte(name), Tags: bytesArrayFromStringArray(tags), Policies: policies}
}

type rollupTargetJSON struct {
	Name     string          `json:"name"`
	Tags     []string        `json:"tags"`
	Policies []policy.Policy `json:"policies"`
}

func newRollupTargetJSON(rt RollupTarget) rollupTargetJSON {
	return rollupTargetJSON{Name: string(rt.Name), Tags: stringArrayFromBytesArray(rt.Tags), Policies: rt.Policies}
}

// MarshalJSON ...
func (t RollupTarget) MarshalJSON() ([]byte, error) {
	return json.Marshal(newRollupTargetJSON(t))
}

// UnmarshalJSON unmarshals JSON-encoded data into staged policies.
func (t *RollupTarget) UnmarshalJSON(data []byte) error {
	var tj rollupTargetJSON
	err := json.Unmarshal(data, &tj)
	if err != nil {
		return err
	}
	*t = tj.rollupTarget()
	return nil
}

func (tj rollupTargetJSON) rollupTarget() RollupTarget {
	return NewRollupTargetFromFields(tj.Name, tj.Tags, tj.Policies)
}

// sameTransform determines whether two targets have the same transformation.
func (t *RollupTarget) sameTransform(other RollupTarget) bool {
	if !bytes.Equal(t.Name, other.Name) {
		return false
	}
	if len(t.Tags) != len(other.Tags) {
		return false
	}
	for i := 0; i < len(t.Tags); i++ {
		if !bytes.Equal(t.Tags[i], other.Tags[i]) {
			return false
		}
	}
	return true
}

// clone clones a rollup target.
func (t *RollupTarget) clone() RollupTarget {
	name := make([]byte, len(t.Name))
	copy(name, t.Name)
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return RollupTarget{
		Name:     name,
		Tags:     bytesArrayCopy(t.Tags),
		Policies: policies,
	}
}

type rollupRuleSnapshotJSON struct {
	Name         string             `json:"name"`
	Tombstoned   bool               `json:"tombstoned"`
	CutoverNanos int64              `json:"cutoverTime"`
	TagFilters   map[string]string  `json:"filters"`
	Targets      []rollupTargetJSON `json:"targets"`
}

func newRollupRuleSnapshotJSON(rrs rollupRuleSnapshot) rollupRuleSnapshotJSON {
	targets := make([]rollupTargetJSON, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = newRollupTargetJSON(t)
	}

	return rollupRuleSnapshotJSON{
		Name:         rrs.name,
		Tombstoned:   rrs.tombstoned,
		CutoverNanos: rrs.cutoverNanos,
		TagFilters:   rrs.rawFilters,
		Targets:      targets,
	}
}

// MarshalJSON returns the JSON encoding of rollupRuleSnapshots
func (rrs rollupRuleSnapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(newRollupRuleSnapshotJSON(rrs))
}

// UnmarshalJSON unmarshals JSON-encoded data into rollupRuleSnapshots
func (rrs *rollupRuleSnapshot) UnmarshalJSON(data []byte) error {
	var rrsj rollupRuleSnapshotJSON
	err := json.Unmarshal(data, &rrsj)
	if err != nil {
		return err
	}
	*rrs = rrsj.rollupRuleSnapshot()
	return nil
}

func (rrsj rollupRuleSnapshotJSON) rollupRuleSnapshot() rollupRuleSnapshot {
	targets := make([]RollupTarget, len(rrsj.Targets))
	for i, t := range rrsj.Targets {
		targets[i] = t.rollupTarget()
	}

	return rollupRuleSnapshot{
		name:         rrsj.Name,
		tombstoned:   rrsj.Tombstoned,
		cutoverNanos: rrsj.CutoverNanos,
		rawFilters:   rrsj.TagFilters,
		targets:      targets,
	}
}

// Schema returns the schema representation of a rollup target
func (t RollupTarget) Schema() (*schema.RollupTarget, error) {
	res := &schema.RollupTarget{
		Name: string(t.Name),
	}

	policies := make([]*schema.Policy, len(t.Policies))
	for i, p := range t.Policies {
		policy, err := p.Schema()
		if err != nil {
			return nil, err
		}
		policies[i] = policy
	}
	res.Policies = policies
	res.Tags = stringArrayFromBytesArray(t.Tags)

	return res, nil
}

// rollupRuleSnapshot defines a rule snapshot such that if a metric matches the
// provided filters, it is rolled up using the provided list of rollup targets.
type rollupRuleSnapshot struct {
	name         string
	tombstoned   bool
	cutoverNanos int64
	filter       filters.Filter
	targets      []RollupTarget
	rawFilters   map[string]string
}

func newRollupRuleSnapshot(
	r *schema.RollupRuleSnapshot,
	opts filters.TagsFilterOptions,
) (*rollupRuleSnapshot, error) {
	if r == nil {
		return nil, errNilRollupRuleSnapshotSchema
	}
	targets := make([]RollupTarget, 0, len(r.Targets))
	for _, t := range r.Targets {
		target, err := newRollupTarget(t)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	filter, err := filters.NewTagsFilter(r.TagFilters, filters.Conjunction, opts)
	if err != nil {
		return nil, err
	}
	return &rollupRuleSnapshot{
		name:         r.Name,
		tombstoned:   r.Tombstoned,
		cutoverNanos: r.CutoverTime,
		filter:       filter,
		targets:      targets,
		rawFilters:   r.TagFilters,
	}, nil
}

// Schema returns the given MappingRuleSnapshot in protobuf form.
func (rrs rollupRuleSnapshot) Schema() (*schema.RollupRuleSnapshot, error) {
	res := &schema.RollupRuleSnapshot{
		Name:        rrs.name,
		Tombstoned:  rrs.tombstoned,
		CutoverTime: rrs.cutoverNanos,
		TagFilters:  rrs.rawFilters,
	}

	targets := make([]*schema.RollupTarget, len(rrs.targets))
	for i, t := range rrs.targets {
		target, err := t.Schema()
		if err != nil {
			return nil, err
		}
		targets[i] = target
	}
	res.Targets = targets

	return res, nil
}

// rollupRule stores rollup rule snapshots.
type rollupRule struct {
	uuid      string
	snapshots []*rollupRuleSnapshot
}

func newRollupRule(
	mc *schema.RollupRule,
	opts filters.TagsFilterOptions,
) (*rollupRule, error) {
	if mc == nil {
		return nil, errNilRollupRuleSchema
	}
	snapshots := make([]*rollupRuleSnapshot, 0, len(mc.Snapshots))
	for i := 0; i < len(mc.Snapshots); i++ {
		mr, err := newRollupRuleSnapshot(mc.Snapshots[i], opts)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, mr)
	}
	return &rollupRule{
		uuid:      mc.Uuid,
		snapshots: snapshots,
	}, nil
}

func newRollupRuleFromFields(
	name string,
	rawFilters map[string]string,
	targets []RollupTarget,
	cutoverTime int64,
	opts filters.TagsFilterOptions,
) (*rollupRule, error) {
	rr := rollupRule{uuid: uuid.New()}
	rr.addSnapshot(name, rawFilters, targets, cutoverTime, opts)
	return &rr, nil
}

// ActiveSnapshot returns the latest rule snapshot whose cutover time is earlier
// than or equal to timeNanos, or nil if not found.
func (rc *rollupRule) ActiveSnapshot(timeNanos int64) *rollupRuleSnapshot {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return nil
	}
	return rc.snapshots[idx]
}

// ActiveRule returns the rule containing snapshots that's in effect at time timeNanos
// and all future rules after time timeNanos.
func (rc *rollupRule) ActiveRule(timeNanos int64) *rollupRule {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return rc
	}
	return &rollupRule{uuid: rc.uuid, snapshots: rc.snapshots[idx:]}
}

func (rc *rollupRule) activeIndex(timeNanos int64) int {
	idx := len(rc.snapshots) - 1
	for idx >= 0 && rc.snapshots[idx].cutoverNanos > timeNanos {
		idx--
	}
	return idx
}

func (rc *rollupRule) Name() (string, error) {
	if len(rc.snapshots) == 0 {
		return "", errNoSnapshots
	}
	latest := rc.snapshots[len(rc.snapshots)-1]
	return latest.name, nil
}

func (rc *rollupRule) Tombstoned() bool {
	if len(rc.snapshots) == 0 {
		return true
	}

	latest := rc.snapshots[len(rc.snapshots)-1]
	return latest.tombstoned
}

func (rc *rollupRule) addSnapshot(
	name string,
	rawFilters map[string]string,
	rollupTargets []RollupTarget,
	cutoverTime int64,
	opts filters.TagsFilterOptions,
) error {
	filter, err := filters.NewTagsFilter(rawFilters, filters.Conjunction, opts)
	if err != nil {
		return err
	}

	snapshot := &rollupRuleSnapshot{
		name:         name,
		tombstoned:   false,
		cutoverNanos: cutoverTime,
		filter:       filter,
		targets:      rollupTargets,
		rawFilters:   rawFilters,
	}

	rc.snapshots = append(rc.snapshots, snapshot)
	return nil
}

func (rc *rollupRule) tombstone(cutoverTime int64) error {
	n, err := rc.Name()
	if err != nil {
		return err
	}

	if rc.Tombstoned() {
		return fmt.Errorf("%s is already tombstoned", n)
	}

	snapshot := *rc.snapshots[len(rc.snapshots)-1]
	snapshot.tombstoned = true
	snapshot.cutoverNanos = cutoverTime
	rc.snapshots = append(rc.snapshots, &snapshot)
	return nil
}

func (rc *rollupRule) revive(
	name string,
	rawFilters map[string]string,
	targets []RollupTarget,
	cutoverTime int64,
	opts filters.TagsFilterOptions,
) error {
	n, err := rc.Name()
	if err != nil {
		return err
	}
	if !rc.Tombstoned() {
		return fmt.Errorf("%s is not tombstoned", n)
	}
	rc.addSnapshot(name, rawFilters, targets, cutoverTime, opts)
	return nil
}

type rollupRuleJSON struct {
	UUID      string                   `json:"uuid"`
	Snapshots []rollupRuleSnapshotJSON `json:"snapshots"`
}

func newRollupRuleJSON(rc rollupRule) rollupRuleJSON {
	snapshots := make([]rollupRuleSnapshotJSON, len(rc.snapshots))
	for i, s := range rc.snapshots {
		snapshots[i] = newRollupRuleSnapshotJSON(*s)
	}
	return rollupRuleJSON{
		UUID:      rc.uuid,
		Snapshots: snapshots,
	}
}

// MarshalJSON returns the JSON encoding of mappingRuleSnapshots
func (rc rollupRule) MarshalJSON() ([]byte, error) {
	return json.Marshal(newRollupRuleJSON(rc))
}

// UnmarshalJSON unmarshals JSON-encoded data into mappingRuleSnapshots
func (rc *rollupRule) UnmarshalJSON(data []byte) error {
	var rrj rollupRuleJSON
	err := json.Unmarshal(data, &rrj)
	if err != nil {
		return err
	}
	*rc = rrj.rollupRule()
	return nil
}

func (rrj rollupRuleJSON) rollupRule() rollupRule {
	snapshots := make([]*rollupRuleSnapshot, len(rrj.Snapshots))
	for i, s := range rrj.Snapshots {
		newSnapshot := s.rollupRuleSnapshot()
		snapshots[i] = &newSnapshot
	}
	return rollupRule{
		uuid:      rrj.UUID,
		snapshots: snapshots,
	}
}

// Schema returns the given RollupRule in protobuf form.
func (rc rollupRule) Schema() (*schema.RollupRule, error) {
	res := &schema.RollupRule{
		Uuid: rc.uuid,
	}

	snapshots := make([]*schema.RollupRuleSnapshot, len(rc.snapshots))
	for i, s := range rc.snapshots {
		snapshot, err := s.Schema()
		if err != nil {
			return nil, err
		}
		snapshots[i] = snapshot
	}
	res.Snapshots = snapshots

	return res, nil
}

func bytesArrayFromStringArray(values []string) [][]byte {
	result := make([][]byte, len(values))
	for i, str := range values {
		result[i] = []byte(str)
	}
	return result
}

func stringArrayFromBytesArray(values [][]byte) []string {
	result := make([]string, len(values))
	for i, bytes := range values {
		result[i] = string(bytes)
	}
	return result
}

func bytesArrayCopy(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i, b := range values {
		result[i] = make([]byte, len(b))
		copy(result[i], b)
	}
	return result
}
