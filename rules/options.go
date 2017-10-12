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
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
)

// Options provide a set of options for rule matching.
type Options interface {
	// SetTagsFilterOptions sets the tags filter options.
	SetTagsFilterOptions(value filters.TagsFilterOptions) Options

	// TagsFilterOptions returns the tags filter options.
	TagsFilterOptions() filters.TagsFilterOptions

	// SetNewRollupIDFn sets the new rollup id function.
	SetNewRollupIDFn(value id.NewIDFn) Options

	// NewRollupIDFn returns the new rollup id function.
	NewRollupIDFn() id.NewIDFn

	// SetIsRollupIDFn sets the function that determines whether an id is a rollup id.
	SetIsRollupIDFn(value id.MatchIDFn) Options

	// IsRollupIDFn returns the function that determines whether an id is a rollup id.
	IsRollupIDFn() id.MatchIDFn

	// SetPolicyOptions sets the policy options.
	SetPolicyOptions(v policy.Options) Options

	// PolicyOptions returns the policy options.
	PolicyOptions() policy.Options
}

type options struct {
	tagsFilterOpts filters.TagsFilterOptions
	newRollupIDFn  id.NewIDFn
	isRollupIDFn   id.MatchIDFn
	pOpts          policy.Options
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{
		pOpts: policy.NewOptions(),
	}
}

func (o *options) SetTagsFilterOptions(value filters.TagsFilterOptions) Options {
	opts := *o
	opts.tagsFilterOpts = value
	return &opts
}

func (o *options) TagsFilterOptions() filters.TagsFilterOptions {
	return o.tagsFilterOpts
}

func (o *options) SetNewRollupIDFn(value id.NewIDFn) Options {
	opts := *o
	opts.newRollupIDFn = value
	return &opts
}

func (o *options) NewRollupIDFn() id.NewIDFn {
	return o.newRollupIDFn
}

func (o *options) SetIsRollupIDFn(value id.MatchIDFn) Options {
	opts := *o
	opts.isRollupIDFn = value
	return &opts
}

func (o *options) IsRollupIDFn() id.MatchIDFn {
	return o.isRollupIDFn
}

func (o *options) SetPolicyOptions(value policy.Options) Options {
	opts := *o
	opts.pOpts = value
	return &opts
}

func (o *options) PolicyOptions() policy.Options {
	return o.pOpts
}
