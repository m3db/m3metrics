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

package metadata

import (
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
)

var (
	// DefaultStagedMetadata represents a default staged metadata.
	DefaultStagedMetadata StagedMetadata
)

// ProcessingMode dictates how a metric should be processed.
type ProcessingMode int

// A list of supported processing mode.
const (
	// In standard mode, a metric is always processed (e.g., written to downstream)
	// locally. Additionally, the metric is forwarded as necessary if there are
	// more pipeline steps to complete.
	StandardMode ProcessingMode = iota

	// In forwarding mode, a metric is always forwarded as necessary if there are
	// more pipeline steps to complete. It is only processed locally if there are
	// no more pipeline stpes to complete.
	ForwardingMode
)

// AggregationPolicyMetadata contains metadata around how
// metrics should be aggregated and stored.
type AggregationPolicyMetadata struct {
	// List of aggregation types.
	AggregationID aggregation.ID

	// List of storage policies.
	StoragePolicies []policy.StoragePolicy
}

// IsDefault returns whether this is the default aggregation policy metadata.
func (m AggregationPolicyMetadata) IsDefault() bool {
	return m.AggregationID.IsDefault() && policy.IsDefaultStoragePolicies(m.StoragePolicies)
}

// PipelineMetadata contains pipeline metadata.
type PipelineMetadata struct {
	AggregationPolicyMetadata
	applied.Pipeline
}

// Metadata represents the metadata associated with a metric.
type Metadata struct {
	// Mode dictates how the associated metric should be processed.
	Mode ProcessingMode

	// Current controls the aggregation and storage plicies at the current step.
	Current AggregationPolicyMetadata

	// Pipelines contain the processing pipelines the metric is subject to.
	Pipelines []PipelineMetadata
}

// IsDefault returns whether this is the default metadata.
func (m Metadata) IsDefault() bool {
	return m.Mode == StandardMode && m.Current.IsDefault() && len(m.Pipelines) == 0
}

// ForwardMetadata represents the metadata information associated with forwarded metrics.
type ForwardMetadata struct {
	// List of aggregation types.
	AggregationID aggregation.ID

	// Storage policy.
	StoragePolicy policy.StoragePolicy

	// Pipeline of operations that may be applied to the metric.
	Pipeline applied.Pipeline
}

// StagedMetadata represents metadata with a staged cutover time.
type StagedMetadata struct {
	Metadata

	// Cutover is when the metadata is applicable.
	CutoverNanos int64

	// Tombstoned determines whether the associated metric has been tombstoned.
	Tombstoned bool
}

// StagedMetadatas contains a list of staged metadatas.
type StagedMetadatas []StagedMetadata

// IsDefault determines whether the list of staged metadata is a default list.
func (sms StagedMetadatas) IsDefault() bool {
	return len(sms) == 1 && sms[0].IsDefault()
}
