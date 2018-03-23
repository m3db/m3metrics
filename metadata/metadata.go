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

// PipelineType describes the type of a pipeline.
type PipelineType int

// A list of supported pipeline types.
const (
	// A standard pipeline is a full pipeline that has not been processed.
	// The first step of a standard pipeline is always an aggregation step.
	// Metrics associated with a standard pipeline are raw, unaggregated metrics.
	StandardType PipelineType = iota

	// A forwarding pipeline is a sub-pipeline whose previous steps have
	// been processed. There are no aggregation steps in a forwarding pipeline.
	// Metrics associated with a forwarding pipeline are produced from
	// previous steps of the same forwarding pipeline.
	ForwardingType
)

// AggregationMetadata dictates how metrics should be aggregated.
type AggregationMetadata struct {
	// List of aggregation types.
	AggregationID aggregation.ID

	// List of storage policies.
	StoragePolicies []policy.StoragePolicy
}

// IsDefault returns whether this is the default aggregation metadata.
func (m AggregationMetadata) IsDefault() bool {
	return m.AggregationID.IsDefault() && policy.IsDefaultStoragePolicies(m.StoragePolicies)
}

// StandardPipelineMetadata contains standard pipeline metadata.
type StandardPipelineMetadata struct {
	AggregationMetadata
	applied.Pipeline
}

// IsDefault returns whether this is the default standard pipeline metadata.
func (sm StandardPipelineMetadata) IsDefault() bool {
	return sm.AggregationMetadata.IsDefault() && sm.Pipeline.IsEmpty()
}

// ForwardingPipelineMetadata contains forwarding pipeline metadata.
type ForwardingPipelineMetadata struct {
	applied.Pipeline
}

// IsDefault returns whether this is the default forwarding pipeline metadata.
func (fm ForwardingPipelineMetadata) IsDefault() bool {
	return fm.Pipeline.IsEmpty()
}

// PipelineMetadataUnion is a union of different pipeline metadatas.
type PipelineMetadataUnion struct {
	Type       PipelineType
	Standard   []StandardPipelineMetadata
	Forwarding ForwardingPipelineMetadata
}

// IsDefault returns whether this is the default pipeline metadata.
func (pu PipelineMetadataUnion) IsDefault() bool {
	return (pu.Type == StandardType && len(pu.Standard) == 0) ||
		(pu.Type == ForwardingType && pu.Forwarding.IsDefault())
}

// Metadata represents the metadata associated with a metric.
type Metadata struct {
	AggregationMetadata

	Pipeline PipelineMetadataUnion
}

// IsDefault returns whether this is the default metadata.
func (m Metadata) IsDefault() bool {
	return m.AggregationMetadata.IsDefault() && m.Pipeline.IsDefault()
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

// IsDefault returns whether this is a default staged metadata.
func (sm StagedMetadata) IsDefault() bool {
	return sm.CutoverNanos == 0 && !sm.Tombstoned && sm.Metadata.IsDefault()
}

// StagedMetadatas contains a list of staged metadatas.
type StagedMetadatas []StagedMetadata

// IsDefault determines whether the list of staged metadata is a default list.
func (sms StagedMetadatas) IsDefault() bool {
	return len(sms) == 1 && sms[0].IsDefault()
}
