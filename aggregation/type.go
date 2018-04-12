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

package aggregation

import (
	"fmt"
	"strings"

	"github.com/m3db/m3metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3x/pool"
)

// Supported aggregation types.
const (
	UnknownType Type = iota
	Last
	Min
	Max
	Mean
	Median
	Count
	Sum
	SumSq
	Stdev
	P10
	P20
	P30
	P40
	P50
	P60
	P70
	P80
	P90
	P95
	P99
	P999
	P9999

	nextTypeID = iota
)

const (
	// maxTypeID is the largest id of all the valid aggregation types.
	// NB(cw) maxTypeID is guaranteed to be greater or equal
	// to len(ValidTypes).
	// Iff ids of all the valid aggregation types are consecutive,
	// maxTypeID == len(ValidTypes).
	maxTypeID = nextTypeID - 1

	typesSeparator = ","
)

var (
	emptyStruct struct{}

	// DefaultTypes is a default list of aggregation types.
	DefaultTypes Types

	// ValidTypes is the list of all the valid aggregation types.
	ValidTypes = map[Type]struct{}{
		Last:   emptyStruct,
		Min:    emptyStruct,
		Max:    emptyStruct,
		Mean:   emptyStruct,
		Median: emptyStruct,
		Count:  emptyStruct,
		Sum:    emptyStruct,
		SumSq:  emptyStruct,
		Stdev:  emptyStruct,
		P10:    emptyStruct,
		P20:    emptyStruct,
		P30:    emptyStruct,
		P40:    emptyStruct,
		P50:    emptyStruct,
		P60:    emptyStruct,
		P70:    emptyStruct,
		P80:    emptyStruct,
		P90:    emptyStruct,
		P95:    emptyStruct,
		P99:    emptyStruct,
		P999:   emptyStruct,
		P9999:  emptyStruct,
	}

	typeStringMap map[string]Type
)

// Type defines an aggregation function.
type Type int

// NewTypeFromSchema creates an aggregation type from a schema.
func NewTypeFromSchema(input aggregationpb.AggregationType) (Type, error) {
	aggType := Type(input)
	if !aggType.IsValid() {
		return UnknownType, fmt.Errorf("invalid aggregation type from schema: %s", input)
	}
	return aggType, nil
}

// ID returns the id of the Type.
func (a Type) ID() int {
	return int(a)
}

// IsValid checks if an Type is valid.
func (a Type) IsValid() bool {
	_, ok := ValidTypes[a]
	return ok
}

// IsValidForGauge if an Type is valid for Gauge.
func (a Type) IsValidForGauge() bool {
	switch a {
	case Last, Min, Max, Mean, Count, Sum, SumSq, Stdev:
		return true
	default:
		return false
	}
}

// IsValidForCounter if an Type is valid for Counter.
func (a Type) IsValidForCounter() bool {
	switch a {
	case Min, Max, Mean, Count, Sum, SumSq, Stdev:
		return true
	default:
		return false
	}
}

// IsValidForTimer if an Type is valid for Timer.
func (a Type) IsValidForTimer() bool {
	switch a {
	case Last:
		return false
	default:
		return true
	}
}

// Quantile returns the quantile represented by the Type.
func (a Type) Quantile() (float64, bool) {
	switch a {
	case P10:
		return 0.1, true
	case P20:
		return 0.2, true
	case P30:
		return 0.3, true
	case P40:
		return 0.4, true
	case P50, Median:
		return 0.5, true
	case P60:
		return 0.6, true
	case P70:
		return 0.7, true
	case P80:
		return 0.8, true
	case P90:
		return 0.9, true
	case P95:
		return 0.95, true
	case P99:
		return 0.99, true
	case P999:
		return 0.999, true
	case P9999:
		return 0.9999, true
	default:
		return 0, false
	}
}

// Schema returns the schema of the aggregation type.
func (a Type) Schema() (aggregationpb.AggregationType, error) {
	s := aggregationpb.AggregationType(a)
	if err := validateSchemaType(s); err != nil {
		return aggregationpb.AggregationType_UNKNOWN, err
	}
	return s, nil
}

// UnmarshalYAML unmarshals aggregation type from a string.
func (a *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	parsed, err := ParseType(str)
	if err != nil {
		return err
	}
	*a = parsed
	return nil
}

func validateSchemaType(a aggregationpb.AggregationType) error {
	_, ok := aggregationpb.AggregationType_name[int32(a)]
	if !ok {
		return fmt.Errorf("invalid schema aggregation type: %v", a)
	}
	return nil
}

// ParseType parses an aggregation type.
func ParseType(str string) (Type, error) {
	aggType, ok := typeStringMap[str]
	if !ok {
		return UnknownType, fmt.Errorf("invalid aggregation type: %s", str)
	}
	return aggType, nil
}

// Types is a list of Types.
type Types []Type

// NewTypesFromSchema creates a list of aggregation types from a schema.
func NewTypesFromSchema(input []aggregationpb.AggregationType) (Types, error) {
	res := make([]Type, len(input))
	for i, t := range input {
		aggType, err := NewTypeFromSchema(t)
		if err != nil {
			return DefaultTypes, err
		}
		res[i] = aggType
	}
	return res, nil
}

// UnmarshalYAML unmarshals aggregation types from a string.
func (aggTypes *Types) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	parsed, err := ParseTypes(str)
	if err != nil {
		return err
	}
	*aggTypes = parsed
	return nil
}

// Contains checks if the given type is contained in the aggregation types.
func (aggTypes Types) Contains(aggType Type) bool {
	for _, at := range aggTypes {
		if at == aggType {
			return true
		}
	}
	return false
}

// IsDefault checks if the Types is the default aggregation type.
func (aggTypes Types) IsDefault() bool {
	return len(aggTypes) == 0
}

// String is for debugging.
func (aggTypes Types) String() string {
	if len(aggTypes) == 0 {
		return ""
	}

	parts := make([]string, len(aggTypes))
	for i, aggType := range aggTypes {
		parts[i] = aggType.String()
	}
	return strings.Join(parts, typesSeparator)
}

// IsValidForGauge checks if the list of aggregation types is valid for Gauge.
func (aggTypes Types) IsValidForGauge() bool {
	for _, aggType := range aggTypes {
		if !aggType.IsValidForGauge() {
			return false
		}
	}
	return true
}

// IsValidForCounter checks if the list of aggregation types is valid for Counter.
func (aggTypes Types) IsValidForCounter() bool {
	for _, aggType := range aggTypes {
		if !aggType.IsValidForCounter() {
			return false
		}
	}
	return true
}

// IsValidForTimer checks if the list of aggregation types is valid for Timer.
func (aggTypes Types) IsValidForTimer() bool {
	for _, aggType := range aggTypes {
		if !aggType.IsValidForTimer() {
			return false
		}
	}
	return true
}

// PooledQuantiles returns all the quantiles found in the list
// of aggregation types. Using a floats pool if available.
//
// A boolean will also be returned to indicate whether the
// returned float slice is from the pool.
func (aggTypes Types) PooledQuantiles(p pool.FloatsPool) ([]float64, bool) {
	var (
		res         []float64
		initialized bool
		medianAdded bool
		pooled      bool
	)
	for _, aggType := range aggTypes {
		q, ok := aggType.Quantile()
		if !ok {
			continue
		}
		// Dedup P50 and Median.
		if aggType == P50 || aggType == Median {
			if medianAdded {
				continue
			}
			medianAdded = true
		}
		if !initialized {
			if p == nil {
				res = make([]float64, 0, len(aggTypes))
			} else {
				res = p.Get(len(aggTypes))
				pooled = true
			}
			initialized = true
		}
		res = append(res, q)
	}
	return res, pooled
}

// Schema returns the schema of the aggregation types.
func (aggTypes Types) Schema() ([]aggregationpb.AggregationType, error) {
	// This is the same as returning an empty slice from the functionality perspective.
	// It makes creating testing fixtures much simpler.
	if aggTypes == nil {
		return nil, nil
	}

	res := make([]aggregationpb.AggregationType, len(aggTypes))
	for i, aggType := range aggTypes {
		s, err := aggType.Schema()
		if err != nil {
			return nil, err
		}
		res[i] = s
	}

	return res, nil
}

// ParseTypes parses a list of aggregation types in the form of type1,type2,type3.
func ParseTypes(str string) (Types, error) {
	parts := strings.Split(str, typesSeparator)
	res := make(Types, len(parts))
	for i := range parts {
		aggType, err := ParseType(parts[i])
		if err != nil {
			return nil, err
		}
		res[i] = aggType
	}
	return res, nil
}

func init() {
	typeStringMap = make(map[string]Type, maxTypeID)
	for aggType := range ValidTypes {
		typeStringMap[aggType.String()] = aggType
	}
}
