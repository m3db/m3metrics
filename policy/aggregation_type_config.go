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

package policy

import (
	"fmt"

	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

// AggregationTypesConfiguration contains configuration for aggregation types.
type AggregationTypesConfiguration struct {
	// Default aggregation types for counter metrics.
	DefaultCounterAggregationTypes *AggregationTypes `yaml:"defaultCounterAggregationTypes"`

	// Default aggregation types for timer metrics.
	DefaultTimerAggregationTypes *AggregationTypes `yaml:"defaultTimerAggregationTypes"`

	// Default aggregation types for gauge metrics.
	DefaultGaugeAggregationTypes *AggregationTypes `yaml:"defaultGaugeAggregationTypes"`

	// Global type strings.
	TypeStrings map[AggregationType]string `yaml:"typeStrings"`

	// Type string overrides for Counter.
	CounterTypeStringOverrides map[AggregationType]string `yaml:"counterTypeStringOverrides"`

	// Type string overrides for Timer.
	TimerTypeStringOverrides map[AggregationType]string `yaml:"timerTypeStringOverrides"`

	// Type string overrides for Gauge.
	GaugeTypeStringOverrides map[AggregationType]string `yaml:"gaugeTypeStringOverrides"`

	// TypeStringTransformerType configs the type string transformer type.
	TypeStringTransformerType string `yaml:"typeStringTransformerType"`

	// Pool of aggregation types.
	AggregationTypesPool pool.ObjectPoolConfiguration `yaml:"aggregationTypesPool"`

	// Pool of quantile slices.
	QuantilesPool pool.BucketizedPoolConfiguration `yaml:"quantilesPool"`
}

// NewOptions creates a new Option.
func (c AggregationTypesConfiguration) NewOptions(instrumentOpts instrument.Options) (AggregationTypesOptions, error) {
	typeStringTypeTransformerFnType, err := parseTypeStringTransformerFnType(c.TypeStringTransformerType)
	if err != nil {
		return nil, err
	}

	opts := NewAggregationTypesOptions().SetTypeStringTransformerFn(typeStringTypeTransformerFnType)
	if c.DefaultCounterAggregationTypes != nil {
		opts = opts.SetDefaultCounterAggregationTypes(*c.DefaultCounterAggregationTypes)
	}
	if c.DefaultGaugeAggregationTypes != nil {
		opts = opts.SetDefaultGaugeAggregationTypes(*c.DefaultGaugeAggregationTypes)
	}
	if c.DefaultTimerAggregationTypes != nil {
		opts = opts.SetDefaultTimerAggregationTypes(*c.DefaultTimerAggregationTypes)
	}

	opts = opts.SetTypeStrings(parseTypeStringOverride(c.TypeStrings))
	opts = opts.SetCounterTypeStringOverrides(parseTypeStringOverride(c.CounterTypeStringOverrides))
	opts = opts.SetGaugeTypeStringOverrides(parseTypeStringOverride(c.GaugeTypeStringOverrides))
	opts = opts.SetTimerTypeStringOverrides(parseTypeStringOverride(c.TimerTypeStringOverrides))

	scope := instrumentOpts.MetricsScope()

	// Set aggregation types pool.
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("aggregation-types-pool"))
	aggTypesPoolOpts := c.AggregationTypesPool.NewObjectPoolOptions(iOpts)
	aggTypesPool := NewAggregationTypesPool(aggTypesPoolOpts)
	opts = opts.SetAggregationTypesPool(aggTypesPool)
	aggTypesPool.Init(func() AggregationTypes {
		return make(AggregationTypes, 0, len(ValidAggregationTypes))
	})

	// Set quantiles pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("quantile-pool"))
	quantilesPool := pool.NewFloatsPool(
		c.QuantilesPool.NewBuckets(),
		c.QuantilesPool.NewObjectPoolOptions(iOpts),
	)
	opts = opts.SetQuantilesPool(quantilesPool)
	quantilesPool.Init()

	return opts, opts.Validate()
}

func parseTypeStringOverride(m map[AggregationType]string) map[AggregationType][]byte {
	res := make(map[AggregationType][]byte, len(m))
	for aggType, s := range m {
		var bytes []byte
		if s != "" {
			// NB(cw) []byte("") is empty with a cap of 8.
			bytes = []byte(s)
		}
		res[aggType] = bytes
	}
	return res
}

type typeStringTypeTransformerFnType string

var (
	defaultTypeStringTransformerFnType       typeStringTypeTransformerFnType = "default"
	withDotPrefixTypeStringTransformerFnType typeStringTypeTransformerFnType = "withDotPrefix"

	validTypeStringTransformerFnTypes = []string{
		string(defaultTypeStringTransformerFnType),
		string(withDotPrefixTypeStringTransformerFnType),
	}
)

func parseTypeStringTransformerFnType(s string) (TypeStringTransformerFn, error) {
	fnType := defaultTypeStringTransformerFnType
	if s != "" {
		fnType = typeStringTypeTransformerFnType(s)
	}

	switch fnType {
	case defaultTypeStringTransformerFnType:
		return defaultTypeStringTransformerFn, nil
	case withDotPrefixTypeStringTransformerFnType:
		return withDotPrefixTypeStringTransformerFn, nil
	default:
		return nil, fmt.Errorf("invalid type string transformer function type: %s, supported types are: %v", s, validTypeStringTransformerFnTypes)
	}
}
