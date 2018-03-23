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
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestStagedMetadatasIsDefault(t *testing.T) {
	inputs := []struct {
		metadatas StagedMetadatas
		expected  bool
	}{
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type:     StandardType,
							Standard: nil,
						},
					},
				},
			},
			expected: true,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type:       ForwardingType,
							Forwarding: ForwardingPipelineMetadata{},
						},
					},
				},
			},
			expected: true,
		},
		{
			metadatas: StagedMetadatas{},
			expected:  false,
		},
		{
			metadatas: StagedMetadatas{
				{
					CutoverNanos: 1234,
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type:     StandardType,
							Standard: nil,
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Tombstoned: true,
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type:     StandardType,
							Standard: nil,
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						AggregationMetadata: AggregationMetadata{
							AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
						},
						Pipeline: PipelineMetadataUnion{
							Type:     StandardType,
							Standard: nil,
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						AggregationMetadata: AggregationMetadata{
							StoragePolicies: []policy.StoragePolicy{
								policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							},
						},
						Pipeline: PipelineMetadataUnion{
							Type:     StandardType,
							Standard: nil,
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type: StandardType,
							Standard: []StandardPipelineMetadata{
								{
									AggregationMetadata: AggregationMetadata{
										AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
									},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type: StandardType,
							Standard: []StandardPipelineMetadata{
								{
									AggregationMetadata: AggregationMetadata{
										StoragePolicies: []policy.StoragePolicy{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
										},
									},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type: StandardType,
							Standard: []StandardPipelineMetadata{
								{
									Pipeline: applied.Pipeline{
										Operations: []applied.Union{
											{
												Type:   op.RollupType,
												Rollup: applied.Rollup{ID: []byte("foo")},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type: ForwardingType,
							Forwarding: ForwardingPipelineMetadata{
								Pipeline: applied.Pipeline{
									Operations: []applied.Union{
										{
											Type:   op.RollupType,
											Rollup: applied.Rollup{ID: []byte("foo")},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type:     StandardType,
							Standard: nil,
						},
					},
				},
				{
					Metadata: Metadata{
						Pipeline: PipelineMetadataUnion{
							Type:       ForwardingType,
							Forwarding: ForwardingPipelineMetadata{},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.metadatas.IsDefault())
	}
}
