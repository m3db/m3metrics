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

// Rollup captures the rollup metadata after the operation is applied against a metric ID.
type Rollup struct {
	// Metric ID generated as a result of the rollup.
	ID []byte
	// Type of aggregation performed within each unique dimension combination.
	AggregationType aggregation.Type
}

func (op Rollup) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	fmt.Fprintf(&b, "id: %s, ", op.ID)
	fmt.Fprintf(&b, "aggregation: %v", op.AggregationType)
	b.WriteString("}")
	return b.String()
}

// Union is a union of different types of operation.
type Union struct {
	Type           op.Type
	Aggregation    op.Aggregation
	Transformation op.Transformation
	Rollup         Rollup
}

func (u Union) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	switch u.Type {
	case op.AggregationType:
		fmt.Fprintf(&b, "aggregation: %s", u.Aggregation.String())
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
