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

// TODO(xichen): rename this package to pipeline.
package op

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3metrics/transformation"
	xbytes "github.com/m3db/m3metrics/x/bytes"
)

var (
	errNilAggregationOpProto    = errors.New("nil aggregation op proto message")
	errNilTransformationOpProto = errors.New("nil transformation op proto message")
	errNilRollupOpProto         = errors.New("nil rollup op proto message")
	errNilPipelineProto         = errors.New("nil pipeline proto message")
)

// Type defines the type of an operation.
type Type int

// List of supported operation types.
const (
	UnknownType Type = iota
	AggregationType
	TransformationType
	RollupType
)

// Aggregation is an aggregation operation.
type Aggregation struct {
	// Type of aggregation performed.
	Type aggregation.Type
}

// NewAggregationOpFromProto creates a new aggregation op from proto.
func NewAggregationOpFromProto(pb *pipelinepb.AggregationOp) (Aggregation, error) {
	var agg Aggregation
	if pb == nil {
		return agg, errNilAggregationOpProto
	}
	aggType, err := aggregation.NewTypeFromProto(pb.Type)
	if err != nil {
		return agg, err
	}
	agg.Type = aggType
	return agg, nil
}

// Clone clones the aggregation operation.
func (op Aggregation) Clone() Aggregation {
	return op
}

// Equal determines whether two aggregation operations are equal.
func (op Aggregation) Equal(other Aggregation) bool {
	return op.Type == other.Type
}

// Proto returns the proto message for the given aggregation operation.
func (op Aggregation) Proto() (*pipelinepb.AggregationOp, error) {
	pbOpType, err := op.Type.Proto()
	if err != nil {
		return nil, err
	}
	return &pipelinepb.AggregationOp{Type: pbOpType}, nil
}

func (op Aggregation) String() string {
	return op.Type.String()
}

// Transformation is a transformation operation.
type Transformation struct {
	// Type of transformation performed.
	Type transformation.Type
}

// NewTransformationOpFromProto creates a new transformation op from proto.
func NewTransformationOpFromProto(pb *pipelinepb.TransformationOp) (Transformation, error) {
	var tf Transformation
	if err := tf.FromProto(pb); err != nil {
		return Transformation{}, err
	}
	return tf, nil
}

// Equal determines whether two transformation operations are equal.
func (op Transformation) Equal(other Transformation) bool {
	return op.Type == other.Type
}

// Clone clones the transformation operation.
func (op Transformation) Clone() Transformation {
	return op
}

// Proto returns the proto message for the given transformation op.
func (op Transformation) Proto() (*pipelinepb.TransformationOp, error) {
	var pbOp pipelinepb.TransformationOp
	if err := op.ToProto(&pbOp); err != nil {
		return nil, err
	}
	return &pbOp, nil
}

func (op Transformation) String() string {
	return op.Type.String()
}

// ToProto converts the transformation op to a protobuf message in place.
func (op Transformation) ToProto(pb *pipelinepb.TransformationOp) error {
	return op.Type.ToProto(&pb.Type)
}

// FromProto converts the protobuf message to a transformation in place.
func (op *Transformation) FromProto(pb *pipelinepb.TransformationOp) error {
	if pb == nil {
		return errNilTransformationOpProto
	}
	return op.Type.FromProto(pb.Type)
}

// Rollup is a rollup operation.
// TODO(xichen): look into whether it's better to make aggregation ID a list of types instead.
type Rollup struct {
	// New metric name generated as a result of the rollup.
	NewName []byte
	// Dimensions along which the rollup is performed.
	Tags [][]byte
	// Types of aggregation performed within each unique dimension combination.
	AggregationID aggregation.ID
}

// NewRollupOpFromProto creates a new rollup op from proto.
// NB: the rollup tags are always sorted on construction.
func NewRollupOpFromProto(pb *pipelinepb.RollupOp) (Rollup, error) {
	var rollup Rollup
	if pb == nil {
		return rollup, errNilRollupOpProto
	}
	aggregationID, err := aggregation.NewIDFromProto(pb.AggregationTypes)
	if err != nil {
		return rollup, err
	}
	tags := make([]string, len(pb.Tags))
	copy(tags, pb.Tags)
	sort.Strings(tags)
	return Rollup{
		NewName:       []byte(pb.NewName),
		Tags:          xbytes.ArraysFromStringArray(tags),
		AggregationID: aggregationID,
	}, nil
}

// SameTransform returns true if the two rollup operations have the same rollup transformation
// (i.e., same new rollup metric name and same set of rollup tags).
func (op Rollup) SameTransform(other Rollup) bool {
	if !bytes.Equal(op.NewName, other.NewName) {
		return false
	}
	if len(op.Tags) != len(other.Tags) {
		return false
	}
	// Sort the tags and compare.
	clonedTags := xbytes.ArraysToStringArray(op.Tags)
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

// Equal returns true if two rollup operations are equal.
func (op Rollup) Equal(other Rollup) bool {
	if !op.AggregationID.Equal(other.AggregationID) {
		return false
	}
	return op.SameTransform(other)
}

// Clone clones the rollup operation.
func (op Rollup) Clone() Rollup {
	newName := make([]byte, len(op.NewName))
	copy(newName, op.NewName)
	return Rollup{
		NewName:       newName,
		Tags:          xbytes.ArrayCopy(op.Tags),
		AggregationID: op.AggregationID,
	}
}

// Proto returns the proto message for the given rollup op.
func (op Rollup) Proto() (*pipelinepb.RollupOp, error) {
	aggTypes, err := op.AggregationID.Types()
	if err != nil {
		return nil, err
	}
	pbAggTypes, err := aggTypes.Proto()
	if err != nil {
		return nil, err
	}
	return &pipelinepb.RollupOp{
		NewName:          string(op.NewName),
		Tags:             xbytes.ArraysToStringArray(op.Tags),
		AggregationTypes: pbAggTypes,
	}, nil
}

func (op Rollup) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	fmt.Fprintf(&b, "name: %s, ", op.NewName)
	b.WriteString("tags: [")
	for i, t := range op.Tags {
		fmt.Fprintf(&b, "%s", t)
		if i < len(op.Tags)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("], ")
	fmt.Fprintf(&b, "aggregation: %v", op.AggregationID)
	b.WriteString("}")
	return b.String()
}

// Union is a union of different types of operation.
type Union struct {
	Type           Type
	Aggregation    Aggregation
	Transformation Transformation
	Rollup         Rollup
}

// NewOpUnionFromProto creates a new operation union from proto.
func NewOpUnionFromProto(pb pipelinepb.PipelineOp) (Union, error) {
	var (
		u   Union
		err error
	)
	switch pb.Type {
	case pipelinepb.PipelineOp_AGGREGATION:
		u.Type = AggregationType
		u.Aggregation, err = NewAggregationOpFromProto(pb.Aggregation)
	case pipelinepb.PipelineOp_TRANSFORMATION:
		u.Type = TransformationType
		u.Transformation, err = NewTransformationOpFromProto(pb.Transformation)
	case pipelinepb.PipelineOp_ROLLUP:
		u.Type = RollupType
		u.Rollup, err = NewRollupOpFromProto(pb.Rollup)
	default:
		err = fmt.Errorf("unknown op type in proto: %v", pb.Type)
	}
	return u, err
}

// Equal determines whether two operation unions are equal.
func (u Union) Equal(other Union) bool {
	if u.Type != other.Type {
		return false
	}
	switch u.Type {
	case AggregationType:
		return u.Aggregation.Equal(other.Aggregation)
	case TransformationType:
		return u.Transformation.Equal(other.Transformation)
	case RollupType:
		return u.Rollup.Equal(other.Rollup)
	}
	return true
}

// Clone clones an operation union.
func (u Union) Clone() Union {
	clone := Union{Type: u.Type}
	switch u.Type {
	case AggregationType:
		clone.Aggregation = u.Aggregation.Clone()
	case TransformationType:
		clone.Transformation = u.Transformation.Clone()
	case RollupType:
		clone.Rollup = u.Rollup.Clone()
	}
	return clone
}

// Proto creates a proto message for the given operation.
func (u Union) Proto() (*pipelinepb.PipelineOp, error) {
	var (
		pbOp pipelinepb.PipelineOp
		err  error
	)
	switch u.Type {
	case AggregationType:
		pbOp.Type = pipelinepb.PipelineOp_AGGREGATION
		pbOp.Aggregation, err = u.Aggregation.Proto()
	case TransformationType:
		pbOp.Type = pipelinepb.PipelineOp_TRANSFORMATION
		pbOp.Transformation, err = u.Transformation.Proto()
	case RollupType:
		pbOp.Type = pipelinepb.PipelineOp_ROLLUP
		pbOp.Rollup, err = u.Rollup.Proto()
	default:
		err = fmt.Errorf("unknown op type: %v", u.Type)
	}
	return &pbOp, err
}

func (u Union) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	switch u.Type {
	case AggregationType:
		fmt.Fprintf(&b, "aggregation: %s", u.Aggregation.String())
	case TransformationType:
		fmt.Fprintf(&b, "transformation: %s", u.Transformation.String())
	case RollupType:
		fmt.Fprintf(&b, "rollup: %s", u.Rollup.String())
	default:
		fmt.Fprintf(&b, "unknown op type: %v", u.Type)
	}
	b.WriteString("}")
	return b.String()
}

// Pipeline is a pipeline of operations.
// TODO(xichen): need json marshaling / unmarshaling.
type Pipeline struct {
	// a list of pipeline operations.
	operations []Union
}

// NewPipeline creates a new pipeline.
func NewPipeline(ops []Union) Pipeline {
	return Pipeline{operations: ops}
}

// NewPipelineFromProto creates a new pipeline from proto.
func NewPipelineFromProto(pb *pipelinepb.Pipeline) (Pipeline, error) {
	if pb == nil {
		return Pipeline{}, errNilPipelineProto
	}
	operations := make([]Union, 0, len(pb.Ops))
	for _, pbOp := range pb.Ops {
		operation, err := NewOpUnionFromProto(pbOp)
		if err != nil {
			return Pipeline{}, err
		}
		operations = append(operations, operation)
	}
	return Pipeline{operations: operations}, nil
}

// Len returns the number of steps in a pipeline.
func (p Pipeline) Len() int { return len(p.operations) }

// IsEmpty determines whether a pipeline is empty.
func (p Pipeline) IsEmpty() bool { return p.Len() == 0 }

// At returns the operation at a given step.
func (p Pipeline) At(i int) Union { return p.operations[i] }

// Equal determines whether two pipelines are equal.
func (p Pipeline) Equal(other Pipeline) bool {
	if len(p.operations) != len(other.operations) {
		return false
	}
	for i := 0; i < len(p.operations); i++ {
		if !p.operations[i].Equal(other.operations[i]) {
			return false
		}
	}
	return true
}

// SubPipeline returns a sub-pipeline containing operations between step `startInclusive`
// and step `endExclusive` of the current pipeline.
func (p Pipeline) SubPipeline(startInclusive int, endExclusive int) Pipeline {
	return Pipeline{operations: p.operations[startInclusive:endExclusive]}
}

// Clone clones the pipeline.
func (p Pipeline) Clone() Pipeline {
	clone := make([]Union, len(p.operations))
	for i, op := range p.operations {
		clone[i] = op.Clone()
	}
	return Pipeline{operations: clone}
}

// Proto returns the proto message for a given pipeline.
func (p Pipeline) Proto() (*pipelinepb.Pipeline, error) {
	pbOps := make([]pipelinepb.PipelineOp, 0, len(p.operations))
	for _, op := range p.operations {
		pbOp, err := op.Proto()
		if err != nil {
			return nil, err
		}
		pbOps = append(pbOps, *pbOp)
	}
	return &pipelinepb.Pipeline{Ops: pbOps}, nil
}

func (p Pipeline) String() string {
	var b bytes.Buffer
	b.WriteString("{operations: [")
	for i, op := range p.operations {
		b.WriteString(op.String())
		if i < len(p.operations)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]}")
	return b.String()
}
