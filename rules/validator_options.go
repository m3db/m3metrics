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
	"time"

	"github.com/m3db/m3metrics/metric"
)

const (
	defaultDefaultCustomAggFunctionsEnabled = false
	defaultDefaultMinPolicyResolution       = time.Second
	defaultDefaultMaxPolicyResolution       = time.Hour
)

// MetricTypeFn determines the metric type based on a set of tag based filters.
type MetricTypeFn func(tagFilters map[string]string) metric.Type

// ValidatorOptions provide a set of options for the validator.
type ValidatorOptions interface {
	// SetDefaultCustomAggregationFunctionEnabled sets whether custom aggregation functions
	// are enabled by default.
	SetDefaultCustomAggregationFunctionEnabled(value bool) ValidatorOptions

	// DefaultCustomAggregationFunctionEnabled returns whether custom aggregation functions
	// are enabled by default.
	DefaultCustomAggregationFunctionEnabled() bool

	// SetDefaultMinPolicyResolution sets the default minimum policy resolution.
	SetDefaultMinPolicyResolution(value time.Duration) ValidatorOptions

	// DefaultMinPolicyResolution returns the default minimum policy resolution.
	DefaultMinPolicyResolution() time.Duration

	// SetDefaultMaxPolicyResolution sets the default maximum policy resolution.
	SetDefaultMaxPolicyResolution(value time.Duration) ValidatorOptions

	// DefaultMaxPolicyResolution returns the default maximum policy resolution.
	DefaultMaxPolicyResolution() time.Duration

	// SetMetricTypeFn sets the metric type function.
	SetMetricTypeFn(value MetricTypeFn) ValidatorOptions

	// MetricTypeFn returns the metric type function.
	MetricTypeFn() MetricTypeFn

	// SetCustomAggregationFunctionEnabledFor sets whether custom aggregation functions
	// are enabled for a given metric type.
	SetCustomAggregationFunctionEnabledFor(t metric.Type, value bool) ValidatorOptions

	// CustomAggregationFunctionEnabledFor returns whether custom aggregation functions
	// are enabled for a given metric type.
	CustomAggregationFunctionEnabledFor(t metric.Type) bool

	// SetMinPolicyResolutionFor sets the minimum policy resolution for a given metric type.
	SetMinPolicyResolutionFor(t metric.Type, value time.Duration) ValidatorOptions

	// MinPolicyResolutionFor returns the minimum policy resolution for a given metric type.
	MinPolicyResolutionFor(t metric.Type) time.Duration

	// SetMaxPolicyResolutionFor sets the maximum policy resolution for a given metric type.
	SetMaxPolicyResolutionFor(t metric.Type, value time.Duration) ValidatorOptions

	// MaxPolicyResolutionFor returns the maximum policy resolution for a given metric type.
	MaxPolicyResolutionFor(t metric.Type) time.Duration
}

type validationMetadata struct {
	customAggFunctionsEnabled bool
	minPolicyResolution       time.Duration
	maxPolicyResolution       time.Duration
}

type validatorOptions struct {
	defaultCustomAggFunctionsEnabled bool
	defaultMinPolicyResolution       time.Duration
	defaultMaxPolicyResolution       time.Duration
	metricTypeFn                     MetricTypeFn
	metadatasByType                  map[metric.Type]validationMetadata
}

// NewValidatorOptions create a new set of validator options.
func NewValidatorOptions() ValidatorOptions {
	return &validatorOptions{
		defaultCustomAggFunctionsEnabled: defaultDefaultCustomAggFunctionsEnabled,
		defaultMinPolicyResolution:       defaultDefaultMinPolicyResolution,
		defaultMaxPolicyResolution:       defaultDefaultMaxPolicyResolution,
		metadatasByType:                  make(map[metric.Type]validationMetadata),
	}
}

func (o *validatorOptions) SetDefaultCustomAggregationFunctionEnabled(value bool) ValidatorOptions {
	o.defaultCustomAggFunctionsEnabled = value
	return o
}

func (o *validatorOptions) DefaultCustomAggregationFunctionEnabled() bool {
	return o.defaultCustomAggFunctionsEnabled
}

func (o *validatorOptions) SetDefaultMinPolicyResolution(value time.Duration) ValidatorOptions {
	o.defaultMinPolicyResolution = value
	return o
}

func (o *validatorOptions) DefaultMinPolicyResolution() time.Duration {
	return o.defaultMinPolicyResolution
}

func (o *validatorOptions) SetDefaultMaxPolicyResolution(value time.Duration) ValidatorOptions {
	o.defaultMaxPolicyResolution = value
	return o
}

func (o *validatorOptions) DefaultMaxPolicyResolution() time.Duration {
	return o.defaultMaxPolicyResolution
}

func (o *validatorOptions) SetMetricTypeFn(value MetricTypeFn) ValidatorOptions {
	o.metricTypeFn = value
	return o
}

func (o *validatorOptions) MetricTypeFn() MetricTypeFn {
	return o.metricTypeFn
}

func (o *validatorOptions) SetCustomAggregationFunctionEnabledFor(t metric.Type, value bool) ValidatorOptions {
	metadata := o.findOrCreateMetadata(t)
	metadata.customAggFunctionsEnabled = value
	o.metadatasByType[t] = metadata
	return o
}

func (o *validatorOptions) CustomAggregationFunctionEnabledFor(t metric.Type) bool {
	if metadata, exists := o.metadatasByType[t]; exists {
		return metadata.customAggFunctionsEnabled
	}
	return o.defaultCustomAggFunctionsEnabled
}

func (o *validatorOptions) SetMinPolicyResolutionFor(t metric.Type, value time.Duration) ValidatorOptions {
	metadata := o.findOrCreateMetadata(t)
	metadata.minPolicyResolution = value
	o.metadatasByType[t] = metadata
	return o
}

func (o *validatorOptions) MinPolicyResolutionFor(t metric.Type) time.Duration {
	if metadata, exists := o.metadatasByType[t]; exists {
		return metadata.minPolicyResolution
	}
	return o.defaultMinPolicyResolution
}

func (o *validatorOptions) SetMaxPolicyResolutionFor(t metric.Type, value time.Duration) ValidatorOptions {
	metadata := o.findOrCreateMetadata(t)
	metadata.maxPolicyResolution = value
	o.metadatasByType[t] = metadata
	return o
}

func (o *validatorOptions) MaxPolicyResolutionFor(t metric.Type) time.Duration {
	if metadata, exists := o.metadatasByType[t]; exists {
		return metadata.maxPolicyResolution
	}
	return o.defaultMaxPolicyResolution
}

func (o *validatorOptions) findOrCreateMetadata(t metric.Type) validationMetadata {
	if metadata, found := o.metadatasByType[t]; found {
		return metadata
	}
	return validationMetadata{
		customAggFunctionsEnabled: o.defaultCustomAggFunctionsEnabled,
		minPolicyResolution:       o.defaultMinPolicyResolution,
		maxPolicyResolution:       o.defaultMaxPolicyResolution,
	}
}
