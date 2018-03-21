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

package applied

import (
	"bytes"
	"fmt"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/op"
)

var (
	// DefaultPipeline is a default pipeline.
	DefaultPipeline Pipeline
)

// Rollup captures the rollup metadata after the operation is applied against a metric ID.
type Rollup struct {
	// Metric ID generated as a result of the rollup.
	ID []byte
	// Type of aggregations performed within each unique dimension combination.
	AggregationID aggregation.ID
}

// Equal determines whether two rollup operations are equal.
func (op Rollup) Equal(other Rollup) bool {
	return op.AggregationID == other.AggregationID && bytes.Equal(op.ID, other.ID)
}

func (op Rollup) String() string {
	return fmt.Sprintf("{id: %s, aggregation: %v}", op.ID, op.AggregationID)
}

// Union is a union of different types of operation.
type Union struct {
	Type           op.Type
	Transformation op.Transformation
	Rollup         Rollup
}

// Equal determines whether two operation unions are equal.
func (u Union) Equal(other Union) bool {
	if u.Type != other.Type {
		return false
	}
	switch u.Type {
	case op.TransformationType:
		return u.Transformation.Equal(other.Transformation)
	case op.RollupType:
		return u.Rollup.Equal(other.Rollup)
	}
	return true
}

func (u Union) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	switch u.Type {
	case op.TransformationType:
		fmt.Fprintf(&b, "transformation: %s", u.Transformation.String())
	case op.RollupType:
		fmt.Fprintf(&b, "rollup: %s", u.Rollup.String())
	default:
		fmt.Fprintf(&b, "unknown op type: %v", u.Type)
	}
	b.WriteString("}")
	return b.String()
}

// Pipeline is a pipeline of operations.
type Pipeline struct {
	// a list of pipeline operations.
	Operations []Union
}

// IsEmpty determines whether a pipeline is empty.
func (p Pipeline) IsEmpty() bool {
	return len(p.Operations) == 0
}

// Equal determines whether two pipelines are equal.
func (p Pipeline) Equal(other Pipeline) bool {
	if len(p.Operations) != len(other.Operations) {
		return false
	}
	for i := 0; i < len(p.Operations); i++ {
		if !p.Operations[i].Equal(other.Operations[i]) {
			return false
		}
	}
	return true
}

func (p Pipeline) String() string {
	var b bytes.Buffer
	b.WriteString("{operations: [")
	for i, op := range p.Operations {
		b.WriteString(op.String())
		if i < len(p.Operations)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]}")
	return b.String()
}
