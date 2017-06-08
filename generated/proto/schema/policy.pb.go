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

// Code generated by protoc-gen-go.
// source: policy.proto
// DO NOT EDIT!

package schema

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type AggregationType int32

const (
	AggregationType_UNKNOWN AggregationType = 0
	AggregationType_LAST    AggregationType = 1
	AggregationType_LOWER   AggregationType = 2
	AggregationType_UPPER   AggregationType = 3
	AggregationType_MEAN    AggregationType = 4
	AggregationType_MEDIAN  AggregationType = 5
	AggregationType_COUNT   AggregationType = 6
	AggregationType_SUM     AggregationType = 7
	AggregationType_SUMSQ   AggregationType = 8
	AggregationType_STDEV   AggregationType = 9
	AggregationType_P10     AggregationType = 10
	AggregationType_P20     AggregationType = 11
	AggregationType_P30     AggregationType = 12
	AggregationType_P40     AggregationType = 13
	AggregationType_P50     AggregationType = 14
	AggregationType_P60     AggregationType = 15
	AggregationType_P70     AggregationType = 16
	AggregationType_P80     AggregationType = 17
	AggregationType_P90     AggregationType = 18
	AggregationType_P95     AggregationType = 19
	AggregationType_P99     AggregationType = 20
	AggregationType_P999    AggregationType = 21
	AggregationType_P9999   AggregationType = 22
)

var AggregationType_name = map[int32]string{
	0:  "UNKNOWN",
	1:  "LAST",
	2:  "LOWER",
	3:  "UPPER",
	4:  "MEAN",
	5:  "MEDIAN",
	6:  "COUNT",
	7:  "SUM",
	8:  "SUMSQ",
	9:  "STDEV",
	10: "P10",
	11: "P20",
	12: "P30",
	13: "P40",
	14: "P50",
	15: "P60",
	16: "P70",
	17: "P80",
	18: "P90",
	19: "P95",
	20: "P99",
	21: "P999",
	22: "P9999",
}
var AggregationType_value = map[string]int32{
	"UNKNOWN": 0,
	"LAST":    1,
	"LOWER":   2,
	"UPPER":   3,
	"MEAN":    4,
	"MEDIAN":  5,
	"COUNT":   6,
	"SUM":     7,
	"SUMSQ":   8,
	"STDEV":   9,
	"P10":     10,
	"P20":     11,
	"P30":     12,
	"P40":     13,
	"P50":     14,
	"P60":     15,
	"P70":     16,
	"P80":     17,
	"P90":     18,
	"P95":     19,
	"P99":     20,
	"P999":    21,
	"P9999":   22,
}

func (x AggregationType) String() string {
	return proto.EnumName(AggregationType_name, int32(x))
}
func (AggregationType) EnumDescriptor() ([]byte, []int) { return fileDescriptor1, []int{0} }

type Resolution struct {
	WindowSize int64 `protobuf:"varint,1,opt,name=window_size,json=windowSize" json:"window_size,omitempty"`
	Precision  int64 `protobuf:"varint,2,opt,name=precision" json:"precision,omitempty"`
}

func (m *Resolution) Reset()                    { *m = Resolution{} }
func (m *Resolution) String() string            { return proto.CompactTextString(m) }
func (*Resolution) ProtoMessage()               {}
func (*Resolution) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{0} }

type Retention struct {
	Period int64 `protobuf:"varint,1,opt,name=period" json:"period,omitempty"`
}

func (m *Retention) Reset()                    { *m = Retention{} }
func (m *Retention) String() string            { return proto.CompactTextString(m) }
func (*Retention) ProtoMessage()               {}
func (*Retention) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{1} }

type StoragePolicy struct {
	Resolution *Resolution `protobuf:"bytes,1,opt,name=resolution" json:"resolution,omitempty"`
	Retention  *Retention  `protobuf:"bytes,2,opt,name=retention" json:"retention,omitempty"`
}

func (m *StoragePolicy) Reset()                    { *m = StoragePolicy{} }
func (m *StoragePolicy) String() string            { return proto.CompactTextString(m) }
func (*StoragePolicy) ProtoMessage()               {}
func (*StoragePolicy) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{2} }

func (m *StoragePolicy) GetResolution() *Resolution {
	if m != nil {
		return m.Resolution
	}
	return nil
}

func (m *StoragePolicy) GetRetention() *Retention {
	if m != nil {
		return m.Retention
	}
	return nil
}

type Policy struct {
	StoragePolicy    *StoragePolicy    `protobuf:"bytes,1,opt,name=storage_policy,json=storagePolicy" json:"storage_policy,omitempty"`
	AggregationTypes []AggregationType `protobuf:"varint,2,rep,packed,name=aggregation_types,json=aggregationTypes,enum=schema.AggregationType" json:"aggregation_types,omitempty"`
}

func (m *Policy) Reset()                    { *m = Policy{} }
func (m *Policy) String() string            { return proto.CompactTextString(m) }
func (*Policy) ProtoMessage()               {}
func (*Policy) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{3} }

func (m *Policy) GetStoragePolicy() *StoragePolicy {
	if m != nil {
		return m.StoragePolicy
	}
	return nil
}

func init() {
	proto.RegisterType((*Resolution)(nil), "schema.Resolution")
	proto.RegisterType((*Retention)(nil), "schema.Retention")
	proto.RegisterType((*StoragePolicy)(nil), "schema.StoragePolicy")
	proto.RegisterType((*Policy)(nil), "schema.Policy")
	proto.RegisterEnum("schema.AggregationType", AggregationType_name, AggregationType_value)
}

func init() { proto.RegisterFile("policy.proto", fileDescriptor1) }

var fileDescriptor1 = []byte{
	// 415 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x5c, 0x92, 0xc1, 0x6e, 0xd3, 0x40,
	0x10, 0x86, 0x71, 0xd2, 0x3a, 0xf5, 0xb8, 0x49, 0x27, 0x0b, 0x2d, 0x39, 0x20, 0x51, 0x85, 0x4b,
	0xc5, 0x21, 0x2c, 0x2e, 0x05, 0x2c, 0x71, 0x89, 0x88, 0x0f, 0xa8, 0x8d, 0x63, 0xd6, 0x31, 0x3d,
	0x46, 0x26, 0x5d, 0x99, 0x95, 0x8a, 0xd7, 0xf2, 0x1a, 0x55, 0xc9, 0x33, 0xf0, 0xbc, 0x9c, 0xd1,
	0xae, 0x1d, 0x4c, 0x7a, 0xfb, 0x3c, 0xf3, 0xfb, 0x9f, 0x4f, 0x96, 0xe1, 0xb8, 0x90, 0xf7, 0x62,
	0xbd, 0x99, 0x14, 0xa5, 0xac, 0x24, 0xb1, 0xd5, 0xfa, 0x07, 0xff, 0x99, 0x8e, 0xaf, 0x01, 0x18,
	0x57, 0xf2, 0xfe, 0x57, 0x25, 0x64, 0x4e, 0x5e, 0x82, 0xfb, 0x20, 0xf2, 0x3b, 0xf9, 0xb0, 0x52,
	0x62, 0xcb, 0x47, 0xd6, 0xb9, 0x75, 0xd1, 0x65, 0x50, 0x8f, 0x62, 0xb1, 0xe5, 0xe4, 0x05, 0x38,
	0x45, 0xc9, 0xd7, 0x42, 0x09, 0x99, 0x8f, 0x3a, 0x66, 0xdd, 0x0e, 0xc6, 0xaf, 0xc0, 0x61, 0xbc,
	0xe2, 0xb9, 0xe9, 0x3a, 0x03, 0xbb, 0xe0, 0xa5, 0x90, 0x77, 0x4d, 0x4d, 0xf3, 0x34, 0xae, 0xa0,
	0x1f, 0x57, 0xb2, 0x4c, 0x33, 0x1e, 0x19, 0x21, 0xe2, 0x01, 0x94, 0xff, 0x14, 0x4c, 0xd8, 0xf5,
	0xc8, 0xa4, 0xf6, 0x9b, 0xb4, 0x72, 0xec, 0xbf, 0x14, 0x79, 0x03, 0x4e, 0xb9, 0xbb, 0x64, 0x3c,
	0x5c, 0x6f, 0xd8, 0xbe, 0xd2, 0x2c, 0x58, 0x9b, 0x19, 0xff, 0xb6, 0xc0, 0x6e, 0xee, 0x7d, 0x82,
	0x81, 0xaa, 0x05, 0x56, 0xf5, 0x27, 0x69, 0x6e, 0x9e, 0xee, 0x0a, 0xf6, 0xf4, 0x58, 0x5f, 0xed,
	0xd9, 0xce, 0x60, 0x98, 0x66, 0x59, 0xc9, 0xb3, 0x54, 0xf7, 0xae, 0xaa, 0x4d, 0xc1, 0xd5, 0xa8,
	0x73, 0xde, 0xbd, 0x18, 0x78, 0xcf, 0x77, 0x05, 0xd3, 0x36, 0xb0, 0xdc, 0x14, 0x9c, 0x61, 0xba,
	0x3f, 0x50, 0xaf, 0xff, 0x58, 0x70, 0xf2, 0x28, 0x45, 0x5c, 0xe8, 0x25, 0xe1, 0x75, 0xb8, 0xb8,
	0x0d, 0xf1, 0x09, 0x39, 0x82, 0x83, 0x9b, 0x69, 0xbc, 0x44, 0x8b, 0x38, 0x70, 0x78, 0xb3, 0xb8,
	0x0d, 0x18, 0x76, 0x34, 0x26, 0x51, 0x14, 0x30, 0xec, 0xea, 0xfd, 0x3c, 0x98, 0x86, 0x78, 0x40,
	0x00, 0xec, 0x79, 0x30, 0xfb, 0x32, 0x0d, 0xf1, 0x50, 0x07, 0x3e, 0x2f, 0x92, 0x70, 0x89, 0x36,
	0xe9, 0x41, 0x37, 0x4e, 0xe6, 0xd8, 0xd3, 0xb3, 0x38, 0x99, 0xc7, 0x5f, 0xf1, 0xc8, 0xe0, 0x72,
	0x16, 0x7c, 0x43, 0x47, 0xaf, 0xa3, 0xb7, 0x14, 0xc1, 0x80, 0x47, 0xd1, 0x35, 0x70, 0x49, 0xf1,
	0xd8, 0xc0, 0x3b, 0x8a, 0x7d, 0x03, 0x57, 0x14, 0x07, 0x06, 0xde, 0x53, 0x3c, 0x31, 0xf0, 0x81,
	0x22, 0x1a, 0xf8, 0x48, 0x71, 0x68, 0xc0, 0xa7, 0x48, 0x6a, 0xb8, 0xc2, 0xa7, 0x35, 0xf8, 0xf8,
	0x4c, 0x2b, 0x46, 0xbe, 0xef, 0xe3, 0xa9, 0xbe, 0xab, 0xc9, 0xc7, 0xb3, 0xef, 0xb6, 0xf9, 0xfd,
	0x2e, 0xff, 0x06, 0x00, 0x00, 0xff, 0xff, 0xf4, 0x48, 0x68, 0x67, 0x8e, 0x02, 0x00, 0x00,
}
