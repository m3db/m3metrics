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

// Metadata represents the metadata associated with a metric.
type Metadata struct {
	// List of aggregation types.
	AggregationID aggregation.ID

	// List of storage policies.
	StoragePolicies []policy.StoragePolicy

	// Pipeline of operations that may be applied to the metric.
	Pipeline applied.Pipeline
}

// IsDefault returns whether this is the default metadata.
func (m Metadata) IsDefault() bool {
	return m.AggregationID.IsDefault() &&
		policy.IsDefaultStoragePolicies(m.StoragePolicies) &&
		m.Pipeline.IsEmpty()
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
type StagedMetadatas []StagedMetadatas

// IsDefault determines whether the list of staged metadata is a default list.
func (sms StagedMetadatas) IsDefault() bool {
	return len(sms) == 1 && sms[0].IsDefault()
}
