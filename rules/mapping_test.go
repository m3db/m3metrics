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
	"testing"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testMappingRuleSchema = &schema.MappingRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*schema.MappingRuleSnapshot{
			&schema.MappingRuleSnapshot{
				Name:        "foo",
				Tombstoned:  false,
				CutoverTime: 12345,
				TagFilters: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				Policies: []*schema.Policy{
					&schema.Policy{
						StoragePolicy: &schema.StoragePolicy{
							Resolution: &schema.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &schema.Retention{
								Period: int64(24 * time.Hour),
							},
						},
						AggregationTypes: []schema.AggregationType{
							schema.AggregationType_P999,
						},
					},
				},
			},
			&schema.MappingRuleSnapshot{
				Name:        "bar",
				Tombstoned:  true,
				CutoverTime: 67890,
				TagFilters: map[string]string{
					"tag3": "value3",
					"tag4": "value4",
				},
				Policies: []*schema.Policy{
					&schema.Policy{
						StoragePolicy: &schema.StoragePolicy{
							Resolution: &schema.Resolution{
								WindowSize: int64(time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &schema.Retention{
								Period: int64(24 * time.Hour),
							},
						},
					},
					&schema.Policy{
						StoragePolicy: &schema.StoragePolicy{
							Resolution: &schema.Resolution{
								WindowSize: int64(5 * time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &schema.Retention{
								Period: int64(48 * time.Hour),
							},
						},
					},
				},
			},
		},
	}
)

func TestMappingRuleSnapshotNilSchema(t *testing.T) {
	_, err := newMappingRuleSnapshot(nil, testTagsFilterOptions())
	require.Equal(t, err, errNilMappingRuleSnapshotSchema)
}

func TestNewMappingRuleNilSchema(t *testing.T) {
	_, err := newMappingRule(nil, testTagsFilterOptions())
	require.Equal(t, err, errNilMappingRuleSchema)
}

func TestNewMappingRule(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, "12669817-13ae-40e6-ba2f-33087b262c68", mr.uuid)
	expectedSnapshots := []struct {
		name         string
		tombstoned   bool
		cutoverNanos int64
		policies     []policy.Policy
	}{
		{
			name:         "foo",
			tombstoned:   false,
			cutoverNanos: 12345,
			policies: []policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), compressedP999),
			},
		},
		{
			name:         "bar",
			tombstoned:   true,
			cutoverNanos: 67890,
			policies: []policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
				policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
			},
		},
	}
	for i, snapshot := range expectedSnapshots {
		require.Equal(t, snapshot.name, mr.snapshots[i].name)
		require.Equal(t, snapshot.tombstoned, mr.snapshots[i].tombstoned)
		require.Equal(t, snapshot.cutoverNanos, mr.snapshots[i].cutoverNanos)
		require.Equal(t, snapshot.policies, mr.snapshots[i].policies)
	}
}

func TestMappingRuleActiveSnapshotNotFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Nil(t, mr.ActiveSnapshot(0))
}

func TestMappingRuleActiveSnapshotFound_Second(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[1], mr.ActiveSnapshot(100000))
}

func TestMappingRuleActiveSnapshotFound_First(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[0], mr.ActiveSnapshot(20000))
}

func TestMappingRuleActiveRuleNotFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr, mr.ActiveRule(0))
}

func TestMappingRuleActiveRuleFound_Second(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	expected := &mappingRule{
		uuid:      mr.uuid,
		snapshots: mr.snapshots[1:],
	}
	require.Equal(t, expected, mr.ActiveRule(100000))
}

func TestMappingRuleActiveRuleFound_First(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr, mr.ActiveRule(20000))
}

func TestMappingRuleSnapshotSchema(t *testing.T) {
	expectedSchema := testMappingRuleSchema.Snapshots[0]
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	schema, err := mr.snapshots[0].Schema()
	require.NoError(t, err)
	require.EqualValues(t, expectedSchema, schema)
}

func TestMappingRuleSchema(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	schema, err := mr.Schema()
	require.NoError(t, err)
	require.Equal(t, testMappingRuleSchema, schema)
}

func TestMarshalMappingRule(t *testing.T) {
	marshalledRule := `{
		"uuid":"12669817-13ae-40e6-ba2f-33087b262c68",
		"snapshots":[
			{"name":"foo",
			 "tombstoned":false,
			 "cutoverTime":12345,
			 "filters":{"tag1":"value1","tag2":"value2"},
			 "policies":["10s@1s:24h0m0s|P999"]
			},
			{"name":"bar",
			 "tombstoned":true,
			 "cutoverTime":67890,
			 "filters":{"tag3":"value3","tag4":"value4"},
			 "policies":["1m0s@1m:24h0m0s","5m0s@1m:48h0m0s"]}
		]
	}`
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	res, err := mr.MarshalJSON()
	require.NoError(t, err)

	var bb bytes.Buffer
	err = json.Compact(&bb, []byte(marshalledRule))
	require.NoError(t, err)

	require.Equal(t, bb.String(), string(res))
}

func TestUnmarshalMappingRule(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	data, err := mr.MarshalJSON()

	var mr2 mappingRule
	err = json.Unmarshal(data, &mr2)
	require.NoError(t, err)

	expected, err := mr.Schema()
	require.NoError(t, err)

	actual, err := mr2.Schema()
	require.NoError(t, err)

	require.Equal(t, expected, actual)
}

func TestNewMappingRuleFromFields(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	rawFilters := map[string]string{"tag3": "value3"}
	mr, err := newMappingRuleFromFields(
		"bar",
		rawFilters,
		[]policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID)},
		12345,
		filterOpts,
	)
	filter, err := filters.NewTagsFilter(rawFilters, filters.Conjunction, filterOpts)
	require.NoError(t, err)
	expectedSnapshot := mappingRuleSnapshot{
		name:         "bar",
		tombstoned:   false,
		cutoverNanos: 12345,
		filter:       filter,
		rawFilters:   rawFilters,
		policies:     []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID)},
	}

	require.NoError(t, err)
	n, err := mr.Name()
	require.NoError(t, err)

	require.Equal(t, n, "bar")
	require.False(t, mr.Tombstoned())
	require.Len(t, mr.snapshots, 1)
	require.Equal(t, mr.snapshots[0].cutoverNanos, expectedSnapshot.cutoverNanos)
	require.Equal(t, mr.snapshots[0].rawFilters, expectedSnapshot.rawFilters)
	require.Equal(t, mr.snapshots[0].policies, expectedSnapshot.policies)
	require.Equal(t, mr.snapshots[0].filter.String(), expectedSnapshot.filter.String())
}

func TestNameNoSnapshot(t *testing.T) {
	mr := mappingRule{
		uuid:      "blah",
		snapshots: []*mappingRuleSnapshot{},
	}
	_, err := mr.Name()
	require.Error(t, err)
}

func TestTombstonedNoSnapshot(t *testing.T) {
	mr := mappingRule{
		uuid:      "blah",
		snapshots: []*mappingRuleSnapshot{},
	}
	require.True(t, mr.Tombstoned())
}

func TestTombstoned(t *testing.T) {
	mr, _ := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.True(t, mr.Tombstoned())
}

// github.com/m3db/m3metrics/rules/mapping.go mappingRule.AddSnapshot
// github.com/m3db/m3metrics/rules/mapping.go mappingRule.Tombstone
