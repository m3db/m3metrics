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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	merrors "github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3metrics/generated/proto/policypb"
	"github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"
	xbytes "github.com/m3db/m3metrics/x/bytes"
	xerrors "github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
	testUser                 = "test_user"
	testActiveRuleSetCmpOpts = []cmp.Option{
		cmp.AllowUnexported(activeRuleSet{}),
		cmp.AllowUnexported(mappingRule{}),
		cmp.AllowUnexported(mappingRuleSnapshot{}),
		cmp.AllowUnexported(rollupRule{}),
		cmp.AllowUnexported(rollupRuleSnapshot{}),
		cmpopts.IgnoreTypes(
			activeRuleSet{}.tagsFilterOpts,
			activeRuleSet{}.newRollupIDFn,
			activeRuleSet{}.isRollupIDFn,
		),
		cmpopts.IgnoreInterfaces(struct{ filters.Filter }{}),
		cmpopts.IgnoreInterfaces(struct{ aggregation.TypesOptions }{}),
	}
	testRuleSetCmpOpts = []cmp.Option{
		cmp.AllowUnexported(ruleSet{}),
		cmp.AllowUnexported(mappingRule{}),
		cmp.AllowUnexported(mappingRuleSnapshot{}),
		cmp.AllowUnexported(rollupRule{}),
		cmp.AllowUnexported(rollupRuleSnapshot{}),
		cmpopts.IgnoreTypes(
			ruleSet{}.tagsFilterOpts,
			ruleSet{}.newRollupIDFn,
			ruleSet{}.isRollupIDFn,
		),
		cmpopts.IgnoreInterfaces(struct{ filters.Filter }{}),
		cmpopts.IgnoreInterfaces(struct{ aggregation.TypesOptions }{}),
	}
)

func TestRuleSetProperties(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1
	rs := &rulepb.RuleSet{
		Uuid:               "ruleset",
		Namespace:          "namespace",
		CreatedAtNanos:     1234,
		LastUpdatedAtNanos: 5678,
		Tombstoned:         false,
		CutoverNanos:       34923,
	}
	newRuleSet, err := NewRuleSetFromProto(version, rs, opts)
	require.NoError(t, err)
	ruleSet := newRuleSet.(*ruleSet)

	require.Equal(t, "ruleset", ruleSet.uuid)
	require.Equal(t, []byte("namespace"), ruleSet.Namespace())
	require.Equal(t, 1, ruleSet.Version())
	require.Equal(t, int64(34923), ruleSet.CutoverNanos())
	require.Equal(t, false, ruleSet.Tombstoned())
}

func TestRuleSetActiveSet(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	inputs := []struct {
		activeSetTimeNanos   int64
		expectedMappingRules []*mappingRule
		expectedRollupRules  []*rollupRule
	}{
		{
			activeSetTimeNanos:   0,
			expectedMappingRules: rs.mappingRules,
			expectedRollupRules:  rs.rollupRules,
		},
		{
			activeSetTimeNanos: 30000,
			expectedMappingRules: []*mappingRule{
				&mappingRule{
					uuid:      rs.mappingRules[0].uuid,
					snapshots: rs.mappingRules[0].snapshots[2:],
				},
				&mappingRule{
					uuid:      rs.mappingRules[1].uuid,
					snapshots: rs.mappingRules[1].snapshots[1:],
				},
				rs.mappingRules[2],
				rs.mappingRules[3],
				rs.mappingRules[4],
			},
			expectedRollupRules: []*rollupRule{
				&rollupRule{
					uuid:      rs.rollupRules[0].uuid,
					snapshots: rs.rollupRules[0].snapshots[2:],
				},
				&rollupRule{
					uuid:      rs.rollupRules[1].uuid,
					snapshots: rs.rollupRules[1].snapshots[1:],
				},
				rs.rollupRules[2],
				rs.rollupRules[3],
				rs.rollupRules[4],
				rs.rollupRules[5],
			},
		},
		{
			activeSetTimeNanos: 200000,
			expectedMappingRules: []*mappingRule{
				&mappingRule{
					uuid:      rs.mappingRules[0].uuid,
					snapshots: rs.mappingRules[0].snapshots[2:],
				},
				&mappingRule{
					uuid:      rs.mappingRules[1].uuid,
					snapshots: rs.mappingRules[1].snapshots[2:],
				},
				&mappingRule{
					uuid:      rs.mappingRules[2].uuid,
					snapshots: rs.mappingRules[2].snapshots[1:],
				},
				rs.mappingRules[3],
				rs.mappingRules[4],
			},
			expectedRollupRules: []*rollupRule{
				&rollupRule{
					uuid:      rs.rollupRules[0].uuid,
					snapshots: rs.rollupRules[0].snapshots[2:],
				},
				&rollupRule{
					uuid:      rs.rollupRules[1].uuid,
					snapshots: rs.rollupRules[1].snapshots[2:],
				},
				&rollupRule{
					uuid:      rs.rollupRules[2].uuid,
					snapshots: rs.rollupRules[2].snapshots[1:],
				},
				rs.rollupRules[3],
				rs.rollupRules[4],
				rs.rollupRules[5],
			},
		},
	}

	for _, input := range inputs {
		as := rs.ActiveSet(input.activeSetTimeNanos).(*activeRuleSet)
		expected := newActiveRuleSet(
			version,
			input.expectedMappingRules,
			input.expectedRollupRules,
			rs.tagsFilterOpts,
			rs.newRollupIDFn,
			rs.isRollupIDFn,
			rs.aggTypesOpts,
		)
		require.True(t, cmp.Equal(expected, as, testActiveRuleSetCmpOpts...))
	}
}

func TestNewRuleSetFromProtoToProtoRoundtrip(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	rs, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	res, err := rs.Proto()
	require.NoError(t, err)
	require.Equal(t, proto, res)
}

func TestRuleSetMappingRules(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	mr, err := rs.MappingRules()
	require.NoError(t, err)
	require.True(t, len(mr) > 0)
	for _, m := range rs.mappingRules {
		require.Contains(t, mr, m.uuid)
		mrv, err := m.mappingRuleView(len(m.snapshots) - 1)
		require.NoError(t, err)
		require.Equal(t, mr[m.uuid][0], mrv)
	}
}

func TestRuleSetRollupRules(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	rr, err := rs.RollupRules()
	require.NoError(t, err)
	require.True(t, len(rr) > 0)
	for _, r := range rs.rollupRules {
		require.Contains(t, rr, r.uuid)
		rrv, err := r.rollupRuleView(len(r.snapshots) - 1)
		require.NoError(t, err)
		require.Equal(t, rr[r.uuid][0], rrv)
	}
}

func TestRuleSetLatest(t *testing.T) {
	proto := &rulepb.RuleSet{
		Namespace:    "testNamespace",
		CutoverNanos: 998234,
		MappingRules: testMappingRulesConfig(),
		RollupRules:  testRollupRulesConfig(),
	}
	rs, err := NewRuleSetFromProto(123, proto, testRuleSetOptions())
	require.NoError(t, err)
	latest, err := rs.Latest()
	require.NoError(t, err)

	expected := &models.RuleSetSnapshotView{
		Namespace:    "testNamespace",
		Version:      123,
		CutoverNanos: 998234,
		MappingRules: map[string]*models.MappingRuleView{
			"mappingRule1": &models.MappingRuleView{
				ID:            "mappingRule1",
				Name:          "mappingRule1.snapshot3",
				Tombstoned:    false,
				CutoverNanos:  30000,
				Filter:        "mtagName1:mtagValue1",
				AggregationID: aggregation.DefaultID,
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
				},
			},
			"mappingRule3": &models.MappingRuleView{
				ID:            "mappingRule3",
				Name:          "mappingRule3.snapshot2",
				Tombstoned:    false,
				CutoverNanos:  34000,
				Filter:        "mtagName1:mtagValue1",
				AggregationID: aggregation.DefaultID,
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
			"mappingRule4": &models.MappingRuleView{
				ID:            "mappingRule4",
				Name:          "mappingRule4.snapshot1",
				Tombstoned:    false,
				CutoverNanos:  24000,
				Filter:        "mtagName1:mtagValue2",
				AggregationID: aggregation.MustCompressTypes(aggregation.P999),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
			"mappingRule5": &models.MappingRuleView{
				ID:                 "mappingRule5",
				Name:               "mappingRule5.snapshot1",
				Tombstoned:         false,
				CutoverNanos:       100000,
				LastUpdatedAtNanos: 123456,
				LastUpdatedBy:      "test",
				Filter:             "mtagName1:mtagValue1",
				AggregationID:      aggregation.DefaultID,
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
		},
		RollupRules: map[string]*models.RollupRuleView{
			"rollupRule1": &models.RollupRuleView{
				ID:           "rollupRule1",
				Name:         "rollupRule1.snapshot3",
				Tombstoned:   false,
				CutoverNanos: 30000,
				Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Pipeline: op.NewPipeline([]op.Union{
							{
								Type: op.RollupType,
								Rollup: op.Rollup{
									NewName: b("rName1"),
									Tags:    bs("rtagName1", "rtagName2"),
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
						},
					},
				},
			},
			"rollupRule3": &models.RollupRuleView{
				ID:           "rollupRule3",
				Name:         "rollupRule3.snapshot2",
				Tombstoned:   false,
				CutoverNanos: 34000,
				Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Pipeline: op.NewPipeline([]op.Union{
							{
								Type: op.RollupType,
								Rollup: op.Rollup{
									NewName: b("rName3"),
									Tags:    bs("rtagName1", "rtagName2"),
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
			"rollupRule4": &models.RollupRuleView{
				ID:           "rollupRule4",
				Name:         "rollupRule4.snapshot1",
				Tombstoned:   false,
				CutoverNanos: 24000,
				Filter:       "rtagName1:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Pipeline: op.NewPipeline([]op.Union{
							{
								Type: op.RollupType,
								Rollup: op.Rollup{
									NewName: b("rName4"),
									Tags:    bs("rtagName1", "rtagName2"),
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
			"rollupRule5": &models.RollupRuleView{
				ID:           "rollupRule5",
				Name:         "rollupRule5.snapshot1",
				Tombstoned:   false,
				CutoverNanos: 24000,
				Filter:       "rtagName1:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Pipeline: op.NewPipeline([]op.Union{
							{
								Type: op.RollupType,
								Rollup: op.Rollup{
									NewName: b("rName5"),
									Tags:    bs("rtagName1"),
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute),
						},
					},
				},
			},
			"rollupRule6": &models.RollupRuleView{
				ID:           "rollupRule6",
				Name:         "rollupRule6.snapshot1",
				Tombstoned:   false,
				CutoverNanos: 100000,
				Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Pipeline: op.NewPipeline([]op.Union{
							{
								Type: op.RollupType,
								Rollup: op.Rollup{
									NewName: b("rName6"),
									Tags:    bs("rtagName1", "rtagName2"),
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
		},
	}
	require.Equal(t, expected, latest)
}

func TestRuleSetClone(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	rsClone := rs.Clone().(*ruleSet)
	require.True(t, cmp.Equal(rs, rsClone, testRuleSetCmpOpts...))
	for i, m := range rs.mappingRules {
		require.False(t, m == rsClone.mappingRules[i])
	}
	for i, r := range rs.rollupRules {
		require.False(t, r == rsClone.rollupRules[i])
	}

	rsClone.mappingRules = []*mappingRule{}
	rsClone.rollupRules = []*rollupRule{}
	require.NotEqual(t, rs.mappingRules, rsClone.mappingRules)
	require.NotEqual(t, rs.rollupRules, rsClone.rollupRules)
}

func TestRuleSetAddMappingRuleInvalidFilter(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	view := models.MappingRuleView{
		Name:   "testInvalidFilter",
		Filter: "tag1:value1 tag2:abc[def",
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		},
	}
	helper := NewRuleSetUpdateHelper(10)
	newID, err := rs.AddMappingRule(view, helper.NewUpdateMetadata(time.Now().UnixNano(), testUser))
	require.Error(t, err)
	require.Empty(t, newID)
	require.True(t, strings.Contains(err.Error(), "cannot add rule testInvalidFilter:"))
	_, ok := xerrors.InnerError(err).(merrors.ValidationError)
	require.True(t, ok)
}

func TestRuleSetAddMappingRuleNewRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	_, err = rs.getMappingRuleByName("foo")
	require.Equal(t, errRuleNotFound, err)

	view := models.MappingRuleView{
		Name:   "foo",
		Filter: "tag1:value tag2:value",
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		},
	}
	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	newID, err := rs.AddMappingRule(view, helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)
	mrs, err := rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, newID)

	mr, err := rs.getMappingRuleByName("foo")
	require.NoError(t, err)

	expected := &models.MappingRuleView{
		Name:          "foo",
		ID:            mr.uuid,
		Tombstoned:    false,
		CutoverNanos:  nowNanos + 10,
		Filter:        "tag1:value tag2:value",
		AggregationID: aggregation.DefaultID,
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		},
		LastUpdatedBy:      testUser,
		LastUpdatedAtNanos: nowNanos,
	}
	require.Equal(t, expected, mrs[mr.uuid][0])

	require.Equal(t, nowNanos+10, rs.cutoverNanos)
	require.Equal(t, testUser, rs.lastUpdatedBy)
	require.Equal(t, nowNanos, rs.lastUpdatedAtNanos)
}

func TestRuleSetAddMappingRuleDuplicateRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	mr, err := rs.getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, mr)

	view := models.MappingRuleView{
		Name:   "mappingRule5.snapshot1",
		Filter: "tag1:value tag2:value",
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		},
	}
	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	newID, err := rs.AddMappingRule(view, helper.NewUpdateMetadata(nowNanos, testUser))
	require.Error(t, err)
	require.Empty(t, newID)
	containedErr, ok := err.(xerrors.ContainedError)
	require.True(t, ok)
	err = containedErr.InnerError()
	_, ok = err.(merrors.RuleConflictError)
	require.True(t, ok)
}

func TestRuleSetAddMappingRuleReviveRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	mr, err := rs.getMappingRuleByName("mappingRule2.snapshot3")
	require.NoError(t, err)
	require.NotNil(t, mr)

	view := models.MappingRuleView{
		Name:          "mappingRule2.snapshot3",
		Filter:        "test:bar",
		AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		},
	}
	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	newID, err := rs.AddMappingRule(view, helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)
	mrs, err := rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, newID)

	mr, err = rs.getMappingRuleByID(newID)
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[len(mr.snapshots)-1].rawFilter, view.Filter)

	expected := &models.MappingRuleView{
		Name:          "mappingRule2.snapshot3",
		ID:            mr.uuid,
		Tombstoned:    false,
		CutoverNanos:  nowNanos + 10,
		Filter:        "test:bar",
		AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		},
		LastUpdatedBy:      testUser,
		LastUpdatedAtNanos: nowNanos,
	}
	require.Equal(t, expected, mrs[mr.uuid][0])

	require.Equal(t, nowNanos+10, rs.cutoverNanos)
	require.Equal(t, testUser, rs.lastUpdatedBy)
	require.Equal(t, nowNanos, rs.lastUpdatedAtNanos)
}

func TestRuleSetUpdateMappingRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	mr, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)

	mrs, err := rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, "mappingRule5")

	view := models.MappingRuleView{
		ID:     "mappingRule5",
		Name:   "mappingRule5.snapshot2",
		Filter: "tag3:value",
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
	}
	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	err = rs.UpdateMappingRule(view, helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)

	r, err := rs.getMappingRuleByID(mr.uuid)
	require.NoError(t, err)

	mrs, err = rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, r.uuid)

	expected := &models.MappingRuleView{
		Name:          "mappingRule5.snapshot2",
		ID:            mr.uuid,
		Tombstoned:    false,
		CutoverNanos:  nowNanos + 10,
		Filter:        "tag3:value",
		AggregationID: aggregation.DefaultID,
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
		LastUpdatedBy:      testUser,
		LastUpdatedAtNanos: nowNanos,
	}
	require.Equal(t, expected, mrs[mr.uuid][0])

	require.Equal(t, nowNanos+10, rs.cutoverNanos)
	require.Equal(t, testUser, rs.lastUpdatedBy)
	require.Equal(t, nowNanos, rs.lastUpdatedAtNanos)
}

func TestRuleSetDeleteMappingRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	mrs, err := rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, "mappingRule5")

	m, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.NotNil(t, m)

	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	err = rs.DeleteMappingRule("mappingRule5", helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)

	m, err = rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.True(t, m.tombstoned())
	require.Equal(t, nowNanos+10, m.snapshots[len(m.snapshots)-1].cutoverNanos)
	require.Equal(t, aggregation.DefaultID, m.snapshots[len(m.snapshots)-1].aggregationID)
	require.Nil(t, m.snapshots[len(m.snapshots)-1].storagePolicies)

	mrs, err = rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, "mappingRule5")
}

func TestRuleSetAddRollupRuleNewRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	_, err = rs.getRollupRuleByName("foo")
	require.Equal(t, errRuleNotFound, err)

	view := models.RollupRuleView{
		Name:   "foo",
		Filter: "tag1:value tag2:value",
		Targets: []models.RollupTargetView{
			{
				Pipeline: op.NewPipeline([]op.Union{
					{
						Type: op.RollupType,
						Rollup: op.Rollup{
							NewName:       b("blah"),
							Tags:          bs("a"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
	}
	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	newID, err := rs.AddRollupRule(view, helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)
	rrs, err := rs.RollupRules()
	require.Contains(t, rrs, newID)
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByName("foo")
	require.NoError(t, err)
	require.Contains(t, rrs, rr.uuid)

	expected := &models.RollupRuleView{
		Name:         "foo",
		ID:           rr.uuid,
		Tombstoned:   false,
		CutoverNanos: nowNanos + 10,
		Filter:       "tag1:value tag2:value",
		Targets: []models.RollupTargetView{
			{
				Pipeline: op.NewPipeline([]op.Union{
					{
						Type: op.RollupType,
						Rollup: op.Rollup{
							NewName:       b("blah"),
							Tags:          bs("a"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
		LastUpdatedBy:      testUser,
		LastUpdatedAtNanos: nowNanos,
	}
	require.Equal(t, expected, rrs[rr.uuid][0])

	require.Equal(t, nowNanos+10, rs.cutoverNanos)
	require.Equal(t, testUser, rs.lastUpdatedBy)
	require.Equal(t, nowNanos, rs.lastUpdatedAtNanos)
}

func TestRuleSetAddRollupRuleDuplicateRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	r, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	require.NotNil(t, r)

	view := models.RollupRuleView{
		Name:   "rollupRule5.snapshot1",
		Filter: "test:bar",
		Targets: []models.RollupTargetView{
			{
				Pipeline: op.NewPipeline([]op.Union{
					{
						Type: op.RollupType,
						Rollup: op.Rollup{
							NewName:       b("blah"),
							Tags:          bs("a"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
	}
	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	newID, err := rs.AddRollupRule(view, helper.NewUpdateMetadata(nowNanos, testUser))
	require.Error(t, err)
	require.Empty(t, newID)
	containedErr, ok := err.(xerrors.ContainedError)
	require.True(t, ok)
	err = containedErr.InnerError()
	_, ok = err.(merrors.RuleConflictError)
	require.True(t, ok)
}

func TestRuleSetAddRollupRuleReviveRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	rr, err := rs.getRollupRuleByID("rollupRule3")
	require.NoError(t, err)
	require.NotNil(t, rr)

	view := models.RollupRuleView{
		Name:   "rollupRule3.snapshot4",
		Filter: "test:bar",
		Targets: []models.RollupTargetView{
			{
				Pipeline: op.NewPipeline([]op.Union{
					{
						Type: op.RollupType,
						Rollup: op.Rollup{
							NewName:       b("blah"),
							Tags:          bs("a"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
	}
	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	newID, err := rs.AddRollupRule(view, helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)
	require.NotEmpty(t, newID)
	rrs, err := rs.RollupRules()
	require.NoError(t, err)
	require.Contains(t, rrs, newID)

	rr, err = rs.getRollupRuleByID(newID)
	require.NoError(t, err)
	require.Equal(t, rr.snapshots[len(rr.snapshots)-1].rawFilter, view.Filter)

	expected := &models.RollupRuleView{
		Name:         "rollupRule3.snapshot4",
		ID:           rr.uuid,
		Tombstoned:   false,
		CutoverNanos: nowNanos + 10,
		Filter:       "test:bar",
		Targets: []models.RollupTargetView{
			{
				Pipeline: op.NewPipeline([]op.Union{
					{
						Type: op.RollupType,
						Rollup: op.Rollup{
							NewName:       b("blah"),
							Tags:          bs("a"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
		LastUpdatedBy:      testUser,
		LastUpdatedAtNanos: nowNanos,
	}
	require.Equal(t, expected, rrs[rr.uuid][0])

	require.Equal(t, nowNanos+10, rs.cutoverNanos)
	require.Equal(t, testUser, rs.lastUpdatedBy)
	require.Equal(t, nowNanos, rs.lastUpdatedAtNanos)
}

func TestRuleSetUpdateRollupRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)

	view := models.RollupRuleView{
		ID:     "rollupRule5",
		Name:   "rollupRule5.snapshot2",
		Filter: "test:bar",
		Targets: []models.RollupTargetView{
			{
				Pipeline: op.NewPipeline([]op.Union{
					{
						Type: op.RollupType,
						Rollup: op.Rollup{
							NewName:       b("blah"),
							Tags:          bs("a"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
	}
	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	err = rs.UpdateRollupRule(view, helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)

	r, err := rs.getRollupRuleByID(rr.uuid)
	require.NoError(t, err)

	rrs, err := rs.RollupRules()
	require.NoError(t, err)
	require.Contains(t, rrs, r.uuid)

	expected := &models.RollupRuleView{
		Name:         "rollupRule5.snapshot2",
		ID:           rr.uuid,
		Tombstoned:   false,
		CutoverNanos: nowNanos + 10,
		Filter:       "test:bar",
		Targets: []models.RollupTargetView{
			{
				Pipeline: op.NewPipeline([]op.Union{
					{
						Type: op.RollupType,
						Rollup: op.Rollup{
							NewName:       b("blah"),
							Tags:          bs("a"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
		LastUpdatedBy:      testUser,
		LastUpdatedAtNanos: nowNanos,
	}
	require.Equal(t, expected, rrs[rr.uuid][0])

	require.Equal(t, nowNanos+10, rs.cutoverNanos)
	require.Equal(t, testUser, rs.lastUpdatedBy)
	require.Equal(t, nowNanos, rs.lastUpdatedAtNanos)
}

func TestRuleSetDeleteRollupRule(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	rrs, err := rs.RollupRules()
	require.NoError(t, err)
	require.Contains(t, rrs, "rollupRule5")

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)

	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	err = rs.DeleteRollupRule(rr.uuid, helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)

	rr, err = rs.getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	require.True(t, rr.tombstoned())

	require.Equal(t, nowNanos+10, rr.snapshots[len(rr.snapshots)-1].cutoverNanos)
	require.Nil(t, rr.snapshots[len(rr.snapshots)-1].targets)

	rrs, err = rs.RollupRules()
	require.NoError(t, err)
	require.Contains(t, rrs, "rollupRule5")
}

func TestRuleSetDelete(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	err = rs.Delete(helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)

	require.True(t, rs.Tombstoned())
	for _, m := range rs.mappingRules {
		require.True(t, m.tombstoned())
	}

	for _, r := range rs.rollupRules {
		require.True(t, r.tombstoned())
	}
}

func TestRuleSetRevive(t *testing.T) {
	var (
		version = 1
		proto   = testRuleSetProto()
		opts    = testRuleSetOptions()
	)
	res, err := NewRuleSetFromProto(version, proto, opts)
	require.NoError(t, err)
	rs := res.(*ruleSet)

	nowNanos := time.Now().UnixNano()
	helper := NewRuleSetUpdateHelper(10)
	err = rs.Delete(helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)

	err = rs.Revive(helper.NewUpdateMetadata(nowNanos, testUser))
	require.NoError(t, err)

	require.False(t, rs.Tombstoned())
	for _, m := range rs.mappingRules {
		require.True(t, m.tombstoned())
	}

	for _, r := range rs.rollupRules {
		require.True(t, r.tombstoned())
	}
}

func testRuleSetProto() *rulepb.RuleSet {
	return &rulepb.RuleSet{
		Uuid:               "ruleset",
		Namespace:          "namespace",
		CreatedAtNanos:     1234,
		LastUpdatedAtNanos: 5678,
		LastUpdatedBy:      "someone",
		Tombstoned:         false,
		CutoverNanos:       34923,
		MappingRules:       testMappingRulesConfig(),
		RollupRules:        testRollupRulesConfig(),
	}
}

func testMappingRulesConfig() []*rulepb.MappingRule {
	return []*rulepb.MappingRule{
		&rulepb.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*rulepb.MappingRuleSnapshot{
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 10000,
					Filter:       "mtagName1:mtagValue1",
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: 10 * time.Second.Nanoseconds(),
								Precision:  time.Second.Nanoseconds(),
							},
							Retention: &policypb.Retention{
								Period: 24 * time.Hour.Nanoseconds(),
							},
						},
					},
				},
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule1.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 20000,
					Filter:       "mtagName1:mtagValue1",
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: 10 * time.Second.Nanoseconds(),
								Precision:  time.Second.Nanoseconds(),
							},
							Retention: &policypb.Retention{
								Period: 6 * time.Hour.Nanoseconds(),
							},
						},
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: 5 * time.Minute.Nanoseconds(),
								Precision:  time.Minute.Nanoseconds(),
							},
							Retention: &policypb.Retention{
								Period: 48 * time.Hour.Nanoseconds(),
							},
						},
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: 10 * time.Minute.Nanoseconds(),
								Precision:  time.Minute.Nanoseconds(),
							},
							Retention: &policypb.Retention{
								Period: 48 * time.Hour.Nanoseconds(),
							},
						},
					},
				},
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule1.snapshot3",
					Tombstoned:   false,
					CutoverNanos: 30000,
					Filter:       "mtagName1:mtagValue1",
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: 30 * time.Second.Nanoseconds(),
								Precision:  time.Second.Nanoseconds(),
							},
							Retention: &policypb.Retention{
								Period: 6 * time.Hour.Nanoseconds(),
							},
						},
					},
				},
			},
		},
		&rulepb.MappingRule{
			Uuid: "mappingRule2",
			Snapshots: []*rulepb.MappingRuleSnapshot{
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule2.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 15000,
					Filter:       "mtagName1:mtagValue1",
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: 10 * time.Second.Nanoseconds(),
								Precision:  time.Second.Nanoseconds(),
							},
							Retention: &policypb.Retention{
								Period: 12 * time.Hour.Nanoseconds(),
							},
						},
					},
				},
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule2.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 22000,
					Filter:       "mtagName1:mtagValue1",
					AggregationTypes: []aggregationpb.AggregationType{
						aggregationpb.AggregationType_MIN,
					},
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: 10 * time.Second.Nanoseconds(),
								Precision:  time.Second.Nanoseconds(),
							},
							Retention: &policypb.Retention{
								Period: 2 * time.Hour.Nanoseconds(),
							},
						},
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &policypb.Retention{
								Period: int64(time.Hour),
							},
						},
					},
				},
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule2.snapshot3",
					Tombstoned:   true,
					CutoverNanos: 35000,
					Filter:       "mtagName1:mtagValue1",
					AggregationTypes: []aggregationpb.AggregationType{
						aggregationpb.AggregationType_MIN,
					},
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &policypb.Retention{
								Period: int64(2 * time.Hour),
							},
						},
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &policypb.Retention{
								Period: int64(time.Hour),
							},
						},
					},
				},
			},
		},
		&rulepb.MappingRule{
			Uuid: "mappingRule3",
			Snapshots: []*rulepb.MappingRuleSnapshot{
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule3.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 22000,
					Filter:       "mtagName1:mtagValue1",
					AggregationTypes: []aggregationpb.AggregationType{
						aggregationpb.AggregationType_MAX,
					},
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &policypb.Retention{
								Period: int64(12 * time.Hour),
							},
						},
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(5 * time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &policypb.Retention{
								Period: int64(48 * time.Hour),
							},
						},
					},
				},
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule3.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 34000,
					Filter:       "mtagName1:mtagValue1",
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &policypb.Retention{
								Period: int64(2 * time.Hour),
							},
						},
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &policypb.Retention{
								Period: int64(time.Hour),
							},
						},
					},
				},
			},
		},
		&rulepb.MappingRule{
			Uuid: "mappingRule4",
			Snapshots: []*rulepb.MappingRuleSnapshot{
				&rulepb.MappingRuleSnapshot{
					Name:         "mappingRule4.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 24000,
					Filter:       "mtagName1:mtagValue2",
					AggregationTypes: []aggregationpb.AggregationType{
						aggregationpb.AggregationType_P999,
					},
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &policypb.Retention{
								Period: int64(24 * time.Hour),
							},
						},
					},
				},
			},
		},
		&rulepb.MappingRule{
			Uuid: "mappingRule5",
			Snapshots: []*rulepb.MappingRuleSnapshot{
				&rulepb.MappingRuleSnapshot{
					Name:               "mappingRule5.snapshot1",
					Tombstoned:         false,
					CutoverNanos:       100000,
					LastUpdatedAtNanos: 123456,
					LastUpdatedBy:      "test",
					Filter:             "mtagName1:mtagValue1",
					StoragePolicies: []*policypb.StoragePolicy{
						&policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &policypb.Retention{
								Period: int64(24 * time.Hour),
							},
						},
					},
				},
			},
		},
	}
}

func testRollupRulesConfig() []*rulepb.RollupRule {
	return []*rulepb.RollupRule{
		&rulepb.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*rulepb.RollupRuleSnapshot{
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 10000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName1",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(24 * time.Hour),
									},
								},
							},
						},
					},
				},
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule1.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 20000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName1",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(6 * time.Hour),
									},
								},
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(5 * time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(48 * time.Hour),
									},
								},
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(48 * time.Hour),
									},
								},
							},
						},
					},
				},
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule1.snapshot3",
					Tombstoned:   false,
					CutoverNanos: 30000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName1",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(30 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(6 * time.Hour),
									},
								},
							},
						},
					},
				},
			},
		},
		&rulepb.RollupRule{
			Uuid: "rollupRule2",
			Snapshots: []*rulepb.RollupRuleSnapshot{
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule2.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 15000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName2",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(12 * time.Hour),
									},
								},
							},
						},
					},
				},
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule2.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 22000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName2",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(2 * time.Hour),
									},
								},
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(time.Hour),
									},
								},
							},
						},
					},
				},
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule2.snapshot3",
					Tombstoned:   true,
					CutoverNanos: 35000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName2",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(time.Hour),
									},
								},
							},
						},
					},
				},
			},
		},
		&rulepb.RollupRule{
			Uuid: "rollupRule3",
			Snapshots: []*rulepb.RollupRuleSnapshot{
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule3.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 22000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName3",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(12 * time.Hour),
									},
								},
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(24 * time.Hour),
									},
								},
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(5 * time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(48 * time.Hour),
									},
								},
							},
						},
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName3",
											Tags:    []string{"rtagName1"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(24 * time.Hour),
									},
								},
							},
						},
					},
				},
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule3.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 34000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName3",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(2 * time.Hour),
									},
								},
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(time.Hour),
									},
								},
							},
						},
					},
				},
			},
		},
		&rulepb.RollupRule{
			Uuid: "rollupRule4",
			Snapshots: []*rulepb.RollupRuleSnapshot{
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule4.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 24000,
					Filter:       "rtagName1:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName4",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(time.Hour),
									},
								},
							},
						},
					},
				},
			},
		},
		&rulepb.RollupRule{
			Uuid: "rollupRule5",
			Snapshots: []*rulepb.RollupRuleSnapshot{
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule5.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 24000,
					Filter:       "rtagName1:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName5",
											Tags:    []string{"rtagName1"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(time.Minute),
									},
								},
							},
						},
					},
				},
			},
		},
		&rulepb.RollupRule{
			Uuid: "rollupRule6",
			Snapshots: []*rulepb.RollupRuleSnapshot{
				&rulepb.RollupRuleSnapshot{
					Name:         "rollupRule6.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 100000,
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
					TargetsV2: []*rulepb.RollupTargetV2{
						&rulepb.RollupTargetV2{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "rName6",
											Tags:    []string{"rtagName1", "rtagName2"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								&policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(time.Hour),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func testTagsFilterOptions() filters.TagsFilterOptions {
	return filters.TagsFilterOptions{
		NameTagKey: []byte("name"),
		NameAndTagsFn: func(b []byte) ([]byte, []byte, error) {
			idx := bytes.Index(b, []byte("|"))
			if idx == -1 {
				return nil, b, nil
			}
			return b[:idx], b[idx+1:], nil
		},
		SortedTagIteratorFn: filters.NewMockSortedTagIterator,
	}
}

func mockNewID(name []byte, tags []id.TagPair) []byte {
	if len(tags) == 0 {
		return name
	}
	var buf bytes.Buffer
	buf.Write(name)
	if len(tags) > 0 {
		buf.WriteString("|")
		for idx, p := range tags {
			buf.Write(p.Name)
			buf.WriteString("=")
			buf.Write(p.Value)
			if idx < len(tags)-1 {
				buf.WriteString(",")
			}
		}
	}
	return buf.Bytes()
}

func testRuleSetOptions() Options {
	return NewOptions().
		SetTagsFilterOptions(testTagsFilterOptions()).
		SetNewRollupIDFn(mockNewID)
}

func b(v string) []byte       { return []byte(v) }
func bs(v ...string) [][]byte { return xbytes.ArraysFromStringArray(v) }

type testMatchInput struct {
	id                    string
	matchFrom             int64
	matchTo               int64
	metricType            metric.Type      // reverse matching only
	aggregationType       aggregation.Type // reverse matching only
	expireAtNanos         int64
	forExistingIDResult   metadata.StagedMetadatas
	forNewRollupIDsResult []IDWithMetadatas
}
