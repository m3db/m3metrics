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

package op

import (
	"encoding/json"
	"testing"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3metrics/generated/proto/transformationpb"
	"github.com/m3db/m3metrics/transformation"
	"github.com/m3db/m3metrics/x/bytes"
	yaml "gopkg.in/yaml.v2"

	"github.com/stretchr/testify/require"
)

var (
	testTransformationOp = Transformation{
		Type: transformation.PerSecond,
	}
	testTransformationOpProto = pipelinepb.TransformationOp{
		Type: transformationpb.TransformationType_PERSECOND,
	}
	testBadTransformationOpProto = pipelinepb.TransformationOp{
		Type: transformationpb.TransformationType_UNKNOWN,
	}
)

func TestAggregationEqual(t *testing.T) {
	inputs := []struct {
		a1       Aggregation
		a2       Aggregation
		expected bool
	}{
		{
			a1:       Aggregation{aggregation.Count},
			a2:       Aggregation{aggregation.Count},
			expected: true,
		},
		{
			a1:       Aggregation{aggregation.Count},
			a2:       Aggregation{aggregation.Sum},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.a1.Equal(input.a2))
		require.Equal(t, input.expected, input.a2.Equal(input.a1))
	}
}

func TestTransformationEqual(t *testing.T) {
	inputs := []struct {
		a1       Transformation
		a2       Transformation
		expected bool
	}{
		{
			a1:       Transformation{transformation.Absolute},
			a2:       Transformation{transformation.Absolute},
			expected: true,
		},
		{
			a1:       Transformation{transformation.Absolute},
			a2:       Transformation{transformation.PerSecond},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.a1.Equal(input.a2))
		require.Equal(t, input.expected, input.a2.Equal(input.a1))
	}
}

func TestTransformationClone(t *testing.T) {
	source := Transformation{transformation.Absolute}
	clone := source.Clone()
	require.Equal(t, source, clone)
	clone.Type = transformation.PerSecond
	require.Equal(t, transformation.Absolute, source.Type)
}

func TestPipelineString(t *testing.T) {
	inputs := []struct {
		p        Pipeline
		expected string
	}{
		{
			p: Pipeline{
				operations: []Union{
					{
						Type:        AggregationType,
						Aggregation: Aggregation{Type: aggregation.Last},
					},
					{
						Type:           TransformationType,
						Transformation: Transformation{Type: transformation.PerSecond},
					},
					{
						Type: RollupType,
						Rollup: Rollup{
							NewName:       b("foo"),
							Tags:          [][]byte{b("tag1"), b("tag2")},
							AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
						},
					},
				},
			},
			expected: "{operations: [{aggregation: Last}, {transformation: PerSecond}, {rollup: {name: foo, tags: [tag1, tag2], aggregation: Sum}}]}",
		},
		{
			p: Pipeline{
				operations: []Union{
					{
						Type: Type(10),
					},
				},
			},
			expected: "{operations: [{unknown op type: Type(10)}]}",
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}

func TestTransformationOpToProto(t *testing.T) {
	var pb pipelinepb.TransformationOp
	require.NoError(t, testTransformationOp.ToProto(&pb))
	require.Equal(t, testTransformationOpProto, pb)
}

func TestTransformationOpFromProto(t *testing.T) {
	var res Transformation
	require.NoError(t, res.FromProto(&testTransformationOpProto))
	require.Equal(t, testTransformationOp, res)
}

func TestTransformationOpFromProtoNilProto(t *testing.T) {
	var res Transformation
	require.Equal(t, errNilTransformationOpProto, res.FromProto(nil))
}

func TestTransformationOpFromProtoBadProto(t *testing.T) {
	var res Transformation
	require.Error(t, res.FromProto(&testBadTransformationOpProto))
}

func TestTransformationOpRoundTrip(t *testing.T) {
	var (
		pb  pipelinepb.TransformationOp
		res Transformation
	)
	require.NoError(t, testTransformationOp.ToProto(&pb))
	require.NoError(t, res.FromProto(&pb))
	require.Equal(t, testTransformationOp, res)
}

func TestRollupOpSameTransform(t *testing.T) {
	rollupOp := Rollup{
		NewName: b("foo"),
		Tags:    bs("bar1", "bar2"),
	}
	inputs := []struct {
		op     Rollup
		result bool
	}{
		{
			op:     Rollup{NewName: b("foo"), Tags: bs("bar1", "bar2")},
			result: true,
		},
		{
			op:     Rollup{NewName: b("foo"), Tags: bs("bar2", "bar1")},
			result: true,
		},
		{
			op:     Rollup{NewName: b("foo"), Tags: bs("bar1")},
			result: false,
		},
		{
			op:     Rollup{NewName: b("foo"), Tags: bs("bar1", "bar2", "bar3")},
			result: false,
		},
		{
			op:     Rollup{NewName: b("foo"), Tags: bs("bar1", "bar3")},
			result: false,
		},
		{
			op:     Rollup{NewName: b("baz"), Tags: bs("bar1", "bar2")},
			result: false,
		},
		{
			op:     Rollup{NewName: b("baz"), Tags: bs("bar2", "bar1")},
			result: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.result, rollupOp.SameTransform(input.op))
	}
}

func TestOpUnionMarshalJSON(t *testing.T) {
	inputs := []struct {
		op       Union
		expected string
	}{
		{
			op: Union{
				Type:        AggregationType,
				Aggregation: Aggregation{Type: aggregation.Sum},
			},
			expected: `{"aggregation":"Sum"}`,
		},
		{
			op: Union{
				Type:           TransformationType,
				Transformation: Transformation{Type: transformation.PerSecond},
			},
			expected: `{"transformation":"PerSecond"}`,
		},
		{
			op: Union{
				Type: RollupType,
				Rollup: Rollup{
					NewName:       b("testRollup"),
					Tags:          bs("tag1", "tag2"),
					AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
				},
			},
			expected: `{"rollup":{"newName":"testRollup","tags":["tag1","tag2"],"aggregation":["Min","Max"]}}`,
		},
		{
			op: Union{
				Type: RollupType,
				Rollup: Rollup{
					NewName:       b("testRollup"),
					Tags:          bs("tag1", "tag2"),
					AggregationID: aggregation.DefaultID,
				},
			},
			expected: `{"rollup":{"newName":"testRollup","tags":["tag1","tag2"]}}`,
		},
	}

	for _, input := range inputs {
		b, err := json.Marshal(input.op)
		require.NoError(t, err)
		require.Equal(t, input.expected, string(b))
	}
}

func TestOpUnionMarshalJSONError(t *testing.T) {
	op := Union{}
	_, err := json.Marshal(op)
	require.Error(t, err)
}

func TestOpUnionMarshalJSONRoundtrip(t *testing.T) {
	ops := []Union{
		{
			Type:        AggregationType,
			Aggregation: Aggregation{Type: aggregation.Sum},
		},
		{
			Type:           TransformationType,
			Transformation: Transformation{Type: transformation.PerSecond},
		},
		{
			Type: RollupType,
			Rollup: Rollup{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			},
		},
		{
			Type: RollupType,
			Rollup: Rollup{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.DefaultID,
			},
		},
	}

	for _, op := range ops {
		b, err := json.Marshal(op)
		require.NoError(t, err)
		var res Union
		require.NoError(t, json.Unmarshal(b, &res))
		require.Equal(t, op, res)
	}
}

func TestPipelineMarshalJSON(t *testing.T) {
	p := NewPipeline([]Union{
		{
			Type:        AggregationType,
			Aggregation: Aggregation{Type: aggregation.Sum},
		},
		{
			Type:           TransformationType,
			Transformation: Transformation{Type: transformation.PerSecond},
		},
		{
			Type: RollupType,
			Rollup: Rollup{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			},
		},
		{
			Type: RollupType,
			Rollup: Rollup{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.DefaultID,
			},
		},
	})
	b, err := json.Marshal(p)
	require.NoError(t, err)

	expected := `[{"aggregation":"Sum"},` +
		`{"transformation":"PerSecond"},` +
		`{"rollup":{"newName":"testRollup","tags":["tag1","tag2"],"aggregation":["Min","Max"]}},` +
		`{"rollup":{"newName":"testRollup","tags":["tag1","tag2"]}}]`
	require.Equal(t, expected, string(b))
}

func TestPipelineMarshalJSONRoundtrip(t *testing.T) {
	p := NewPipeline([]Union{
		{
			Type:        AggregationType,
			Aggregation: Aggregation{Type: aggregation.Sum},
		},
		{
			Type:           TransformationType,
			Transformation: Transformation{Type: transformation.PerSecond},
		},
		{
			Type: RollupType,
			Rollup: Rollup{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			},
		},
		{
			Type: RollupType,
			Rollup: Rollup{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.DefaultID,
			},
		},
	})
	b, err := json.Marshal(p)
	require.NoError(t, err)
	var res Pipeline
	require.NoError(t, json.Unmarshal(b, &res))
	require.Equal(t, p, res)
}

func TestPipelineUnmarshalYAML(t *testing.T) {
	input := `
- aggregation: Sum
- transformation: PerSecond
- rollup:
    newName: testRollup
    tags:
      - tag1
      - tag2
    aggregation: Min,Max
- rollup:
    newName: testRollup2
    tags:
      - tag3
      - tag4
`

	var pipeline Pipeline
	require.NoError(t, yaml.Unmarshal([]byte(input), &pipeline))

	expected := NewPipeline([]Union{
		{
			Type:        AggregationType,
			Aggregation: Aggregation{Type: aggregation.Sum},
		},
		{
			Type:           TransformationType,
			Transformation: Transformation{Type: transformation.PerSecond},
		},
		{
			Type: RollupType,
			Rollup: Rollup{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			},
		},
		{
			Type: RollupType,
			Rollup: Rollup{
				NewName:       b("testRollup2"),
				Tags:          bs("tag3", "tag4"),
				AggregationID: aggregation.DefaultID,
			},
		},
	})
	require.Equal(t, expected, pipeline)
}

func b(v string) []byte       { return []byte(v) }
func bs(v ...string) [][]byte { return bytes.ArraysFromStringArray(v) }
