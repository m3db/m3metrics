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
	"errors"
	"sort"

	"github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"
	xbytes "github.com/m3db/m3metrics/x/bytes"
)

var (
	// emptyRollupTarget   rollupTarget
	emptyRollupTargetV2 rollupTargetV2

	errNilPipelineProto       = errors.New("nil pipeline proto")
	errNilRollupTargetProto   = errors.New("nil rollup target proto")
	errNilRollupTargetV2Proto = errors.New("nil rollup target v2 proto")
)

/*
// rollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be grouped and rolled up across the provided set of tags, named
// with the provided name, and aggregated and retained under the provided policies.
type rollupTarget struct {
	Name     []byte
	Tags     [][]byte
	Policies []policy.Policy
}

func newRollupTargetFromProto(pb *rulepb.RollupTarget) (rollupTarget, error) {
	if pb == nil {
		return emptyRollupTarget, errNilRollupTargetProto
	}
	policies, err := policy.NewPoliciesFromProto(pb.Policies)
	if err != nil {
		return emptyRollupTarget, err
	}
	tags := make([]string, len(pb.Tags))
	copy(tags, pb.Tags)
	sort.Strings(tags)
	return rollupTarget{
		Name:     []byte(pb.Name),
		Tags:     xbytes.ArraysFromStringArray(tags),
		Policies: policies,
	}, nil
}

func newRollupTargetsFromView(views []models.RollupTargetView) []rollupTarget {
	targets := make([]rollupTarget, len(views))
	for i, t := range views {
		targets[i] = newRollupTargetFromView(t)
	}
	return targets
}

func newRollupTargetFromView(rtv models.RollupTargetView) rollupTarget {
	return rollupTarget{
		Name:     []byte(rtv.Name),
		Tags:     xbytes.ArraysFromStringArray(rtv.Tags),
		Policies: rtv.Policies,
	}
}

func (t rollupTarget) rollupTargetView() models.RollupTargetView {
	return models.RollupTargetView{
		Name:     string(t.Name),
		Tags:     xbytes.ArraysToStringArray(t.Tags),
		Policies: t.Policies,
	}
}

// TODO: Evaluate if this function is needed for rule matching. If not remove it.
// SameTransform returns whether two rollup targets have the same transformation.
func (t *rollupTarget) SameTransform(other rollupTarget) bool {
	if !bytes.Equal(t.Name, other.Name) {
		return false
	}
	if len(t.Tags) != len(other.Tags) {
		return false
	}
	clonedTags := xbytes.ArraysToStringArray(t.Tags)
	sort.Strings(clonedTags)
	otherClonedTags := xbytes.ArraysToStringArray(other.Tags)
	sort.Strings(otherClonedTags)
	for i := 0; i < len(clonedTags); i++ {
		if clonedTags[i] != otherClonedTags[i] {
			return false
		}
	}
	return true
}

// clone clones a rollup target.
func (t *rollupTarget) clone() rollupTarget {
	name := make([]byte, len(t.Name))
	copy(name, t.Name)
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return rollupTarget{
		Name:     name,
		Tags:     xbytes.ArrayCopy(t.Tags),
		Policies: policies,
	}
}

// Proto returns the proto representation of a rollup target.
func (t *rollupTarget) Proto() (*rulepb.RollupTarget, error) {
	res := &rulepb.RollupTarget{
		Name: string(t.Name),
	}

	policies := make([]*policypb.Policy, len(t.Policies))
	for i, p := range t.Policies {
		policy, err := p.Proto()
		if err != nil {
			return nil, err
		}
		policies[i] = policy
	}
	res.Policies = policies
	res.Tags = xbytes.ArraysToStringArray(t.Tags)

	return res, nil
}
*/

// rollupTargetV2 dictates how to roll up metrics. Metrics associated with a rollup
// target will be rolled up as dictated by the operations in the pipeline, and stored
// under the provided storage policies.
type rollupTargetV2 struct {
	Pipeline        op.Pipeline
	StoragePolicies policy.StoragePolicies
}

// newRollupTargetV2FromV1Proto creates a new rollup target from v1 proto
// for backward compatibility purposes.
func newRollupTargetV2FromV1Proto(pb *rulepb.RollupTarget) (rollupTargetV2, error) {
	if pb == nil {
		return emptyRollupTargetV2, errNilRollupTargetProto
	}
	aggregationID, storagePolicies, err := toAggregationIDAndStoragePolicies(pb.Policies)
	if err != nil {
		return emptyRollupTargetV2, err
	}
	tags := make([]string, len(pb.Tags))
	copy(tags, pb.Tags)
	sort.Strings(tags)
	rollupOp := op.Union{
		Type: op.RollupType,
		Rollup: op.Rollup{
			NewName:       []byte(pb.Name),
			Tags:          xbytes.ArraysFromStringArray(tags),
			AggregationID: aggregationID,
		},
	}
	pipeline := op.NewPipeline([]op.Union{rollupOp})
	return rollupTargetV2{
		Pipeline:        pipeline,
		StoragePolicies: storagePolicies,
	}, nil
}

// newRollupTargetV2FromProto creates a new rollup target from v2 proto.
func newRollupTargetV2FromV2Proto(pb *rulepb.RollupTargetV2) (rollupTargetV2, error) {
	if pb == nil {
		return emptyRollupTargetV2, errNilRollupTargetV2Proto
	}
	pipeline, err := op.NewPipelineFromProto(pb.Pipeline)
	if err != nil {
		return emptyRollupTargetV2, err
	}
	storagePolicies, err := policy.NewStoragePoliciesFromProto(pb.StoragePolicies)
	if err != nil {
		return emptyRollupTargetV2, err
	}
	return rollupTargetV2{
		Pipeline:        pipeline,
		StoragePolicies: storagePolicies,
	}, nil
}

func newRollupTargetV2FromView(rtv models.RollupTargetView) rollupTargetV2 {
	return rollupTargetV2{
		Pipeline:        rtv.Pipeline,
		StoragePolicies: rtv.StoragePolicies,
	}
}

func (t rollupTargetV2) rollupTargetView() models.RollupTargetView {
	return models.RollupTargetView{
		Pipeline:        t.Pipeline,
		StoragePolicies: t.StoragePolicies,
	}
}

// clone clones a rollup target.
func (t *rollupTargetV2) clone() rollupTargetV2 {
	return rollupTargetV2{
		Pipeline:        t.Pipeline.Clone(),
		StoragePolicies: t.StoragePolicies.Clone(),
	}
}

// Proto returns the proto representation of a rollup target.
func (t *rollupTargetV2) Proto() (*rulepb.RollupTargetV2, error) {
	pipeline, err := t.Pipeline.Proto()
	if err != nil {
		return nil, err
	}
	storagePolicies, err := t.StoragePolicies.Proto()
	if err != nil {
		return nil, err
	}
	return &rulepb.RollupTargetV2{
		Pipeline:        pipeline,
		StoragePolicies: storagePolicies,
	}, nil
}

func newRollupTargetsV2FromView(views []models.RollupTargetView) []rollupTargetV2 {
	targets := make([]rollupTargetV2, 0, len(views))
	for _, t := range views {
		targets = append(targets, newRollupTargetV2FromView(t))
	}
	return targets
}
