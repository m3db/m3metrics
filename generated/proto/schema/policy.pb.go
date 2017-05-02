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

type Policy struct {
	Resolution *Resolution `protobuf:"bytes,1,opt,name=resolution" json:"resolution,omitempty"`
	Retention  *Retention  `protobuf:"bytes,2,opt,name=retention" json:"retention,omitempty"`
}

func (m *Policy) Reset()                    { *m = Policy{} }
func (m *Policy) String() string            { return proto.CompactTextString(m) }
func (*Policy) ProtoMessage()               {}
func (*Policy) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{2} }

func (m *Policy) GetResolution() *Resolution {
	if m != nil {
		return m.Resolution
	}
	return nil
}

func (m *Policy) GetRetention() *Retention {
	if m != nil {
		return m.Retention
	}
	return nil
}

type CompressedPolicy struct {
	Policy *Policy `protobuf:"bytes,1,opt,name=policy" json:"policy,omitempty"`
	Id     int64   `protobuf:"varint,2,opt,name=id" json:"id,omitempty"`
}

func (m *CompressedPolicy) Reset()                    { *m = CompressedPolicy{} }
func (m *CompressedPolicy) String() string            { return proto.CompactTextString(m) }
func (*CompressedPolicy) ProtoMessage()               {}
func (*CompressedPolicy) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{3} }

func (m *CompressedPolicy) GetPolicy() *Policy {
	if m != nil {
		return m.Policy
	}
	return nil
}

type ActivePolicies struct {
	CompressedPolicies []*CompressedPolicy `protobuf:"bytes,1,rep,name=compressedPolicies" json:"compressedPolicies,omitempty"`
	CutoverTime        int64               `protobuf:"varint,2,opt,name=cutoverTime" json:"cutoverTime,omitempty"`
}

func (m *ActivePolicies) Reset()                    { *m = ActivePolicies{} }
func (m *ActivePolicies) String() string            { return proto.CompactTextString(m) }
func (*ActivePolicies) ProtoMessage()               {}
func (*ActivePolicies) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{4} }

func (m *ActivePolicies) GetCompressedPolicies() []*CompressedPolicy {
	if m != nil {
		return m.CompressedPolicies
	}
	return nil
}

func init() {
	proto.RegisterType((*Resolution)(nil), "schema.Resolution")
	proto.RegisterType((*Retention)(nil), "schema.Retention")
	proto.RegisterType((*Policy)(nil), "schema.Policy")
	proto.RegisterType((*CompressedPolicy)(nil), "schema.CompressedPolicy")
	proto.RegisterType((*ActivePolicies)(nil), "schema.ActivePolicies")
}

func init() { proto.RegisterFile("policy.proto", fileDescriptor1) }

var fileDescriptor1 = []byte{
	// 269 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x91, 0x41, 0x4b, 0xfc, 0x30,
	0x10, 0xc5, 0x69, 0x17, 0x0a, 0x9d, 0xfe, 0x29, 0x7f, 0xe7, 0x20, 0x3d, 0x08, 0x96, 0x0a, 0xb2,
	0xa7, 0x0a, 0xf5, 0x13, 0x88, 0x17, 0xd1, 0x8b, 0x44, 0xef, 0xa2, 0xe9, 0x80, 0x03, 0xdb, 0x4e,
	0x48, 0xb2, 0xbb, 0xb8, 0xf8, 0xe1, 0x85, 0x34, 0xb5, 0xab, 0x78, 0xcc, 0xbc, 0x37, 0xef, 0xf7,
	0x92, 0xc0, 0x3f, 0x23, 0x1b, 0xd6, 0x1f, 0xad, 0xb1, 0xe2, 0x05, 0x33, 0xa7, 0xdf, 0x69, 0x78,
	0x6d, 0x1e, 0x00, 0x14, 0x39, 0xd9, 0x6c, 0x3d, 0xcb, 0x88, 0xe7, 0x50, 0xec, 0x79, 0xec, 0x65,
	0xff, 0xe2, 0xf8, 0x40, 0x55, 0x52, 0x27, 0xeb, 0x95, 0x82, 0x69, 0xf4, 0xc4, 0x07, 0xc2, 0x33,
	0xc8, 0x8d, 0x25, 0xcd, 0x8e, 0x65, 0xac, 0xd2, 0x20, 0x2f, 0x83, 0xe6, 0x02, 0x72, 0x45, 0x9e,
	0xc6, 0x90, 0x75, 0x0a, 0x99, 0x21, 0xcb, 0xd2, 0xc7, 0x98, 0x78, 0x6a, 0x06, 0xc8, 0x1e, 0x43,
	0x13, 0xec, 0x00, 0xec, 0x37, 0x3b, 0xb8, 0x8a, 0x0e, 0xdb, 0xa9, 0x58, 0xbb, 0xb4, 0x52, 0x47,
	0x2e, 0xbc, 0x82, 0xdc, 0xce, 0x88, 0x50, 0xa0, 0xe8, 0x4e, 0x96, 0x95, 0x28, 0xa8, 0xc5, 0xd3,
	0xdc, 0xc3, 0xff, 0x5b, 0x19, 0x8c, 0x25, 0xe7, 0xa8, 0x8f, 0xe0, 0x4b, 0xc8, 0xa6, 0xc7, 0x88,
	0xd0, 0x72, 0x4e, 0x98, 0x74, 0x15, 0x55, 0x2c, 0x21, 0xe5, 0x3e, 0x5e, 0x33, 0xe5, 0xbe, 0xf9,
	0x84, 0xf2, 0x46, 0x7b, 0xde, 0x51, 0xf0, 0x31, 0x39, 0xbc, 0x03, 0xd4, 0x3f, 0xd3, 0x99, 0x5c,
	0x95, 0xd4, 0xab, 0x75, 0xd1, 0x55, 0x73, 0xea, 0x6f, 0xbe, 0xfa, 0x63, 0x07, 0x6b, 0x28, 0xf4,
	0xd6, 0xcb, 0x8e, 0xec, 0x33, 0x0f, 0x14, 0xa1, 0xc7, 0xa3, 0xb7, 0x2c, 0xfc, 0xdc, 0xf5, 0x57,
	0x00, 0x00, 0x00, 0xff, 0xff, 0xa5, 0x67, 0x3b, 0x84, 0xc9, 0x01, 0x00, 0x00,
}
