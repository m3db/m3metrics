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
// source: rule.proto
// DO NOT EDIT!

package schema

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type MappingRule struct {
	Name       string            `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	TagFilters map[string]string `protobuf:"bytes,2,rep,name=tag_filters,json=tagFilters" json:"tag_filters,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	Policies   []*Policy         `protobuf:"bytes,3,rep,name=policies" json:"policies,omitempty"`
}

func (m *MappingRule) Reset()                    { *m = MappingRule{} }
func (m *MappingRule) String() string            { return proto.CompactTextString(m) }
func (*MappingRule) ProtoMessage()               {}
func (*MappingRule) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{0} }

func (m *MappingRule) GetTagFilters() map[string]string {
	if m != nil {
		return m.TagFilters
	}
	return nil
}

func (m *MappingRule) GetPolicies() []*Policy {
	if m != nil {
		return m.Policies
	}
	return nil
}

type RollupTarget struct {
	Name     string    `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Tags     []string  `protobuf:"bytes,2,rep,name=tags" json:"tags,omitempty"`
	Policies []*Policy `protobuf:"bytes,3,rep,name=policies" json:"policies,omitempty"`
}

func (m *RollupTarget) Reset()                    { *m = RollupTarget{} }
func (m *RollupTarget) String() string            { return proto.CompactTextString(m) }
func (*RollupTarget) ProtoMessage()               {}
func (*RollupTarget) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{1} }

func (m *RollupTarget) GetPolicies() []*Policy {
	if m != nil {
		return m.Policies
	}
	return nil
}

type RollupRule struct {
	Name       string            `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	TagFilters map[string]string `protobuf:"bytes,2,rep,name=tag_filters,json=tagFilters" json:"tag_filters,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	Targets    []*RollupTarget   `protobuf:"bytes,3,rep,name=targets" json:"targets,omitempty"`
}

func (m *RollupRule) Reset()                    { *m = RollupRule{} }
func (m *RollupRule) String() string            { return proto.CompactTextString(m) }
func (*RollupRule) ProtoMessage()               {}
func (*RollupRule) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{2} }

func (m *RollupRule) GetTagFilters() map[string]string {
	if m != nil {
		return m.TagFilters
	}
	return nil
}

func (m *RollupRule) GetTargets() []*RollupTarget {
	if m != nil {
		return m.Targets
	}
	return nil
}

type RuleSet struct {
	Namespace     string         `protobuf:"bytes,1,opt,name=namespace" json:"namespace,omitempty"`
	CreatedAt     int64          `protobuf:"varint,2,opt,name=created_at,json=createdAt" json:"created_at,omitempty"`
	LastUpdatedAt int64          `protobuf:"varint,3,opt,name=last_updated_at,json=lastUpdatedAt" json:"last_updated_at,omitempty"`
	Tombstoned    bool           `protobuf:"varint,4,opt,name=tombstoned" json:"tombstoned,omitempty"`
	Version       int32          `protobuf:"varint,5,opt,name=version" json:"version,omitempty"`
	Cutover       int64          `protobuf:"varint,6,opt,name=cutover" json:"cutover,omitempty"`
	MappingRules  []*MappingRule `protobuf:"bytes,7,rep,name=mapping_rules,json=mappingRules" json:"mapping_rules,omitempty"`
	RollupRules   []*RollupRule  `protobuf:"bytes,8,rep,name=rollup_rules,json=rollupRules" json:"rollup_rules,omitempty"`
}

func (m *RuleSet) Reset()                    { *m = RuleSet{} }
func (m *RuleSet) String() string            { return proto.CompactTextString(m) }
func (*RuleSet) ProtoMessage()               {}
func (*RuleSet) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{3} }

func (m *RuleSet) GetMappingRules() []*MappingRule {
	if m != nil {
		return m.MappingRules
	}
	return nil
}

func (m *RuleSet) GetRollupRules() []*RollupRule {
	if m != nil {
		return m.RollupRules
	}
	return nil
}

type Namespaces struct {
	Namespaces []string `protobuf:"bytes,1,rep,name=namespaces" json:"namespaces,omitempty"`
}

func (m *Namespaces) Reset()                    { *m = Namespaces{} }
func (m *Namespaces) String() string            { return proto.CompactTextString(m) }
func (*Namespaces) ProtoMessage()               {}
func (*Namespaces) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{4} }

func init() {
	proto.RegisterType((*MappingRule)(nil), "schema.MappingRule")
	proto.RegisterType((*RollupTarget)(nil), "schema.RollupTarget")
	proto.RegisterType((*RollupRule)(nil), "schema.RollupRule")
	proto.RegisterType((*RuleSet)(nil), "schema.RuleSet")
	proto.RegisterType((*Namespaces)(nil), "schema.Namespaces")
}

func init() { proto.RegisterFile("rule.proto", fileDescriptor1) }

var fileDescriptor1 = []byte{
	// 433 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xa4, 0x53, 0x5d, 0xab, 0x13, 0x31,
	0x10, 0x25, 0xdd, 0x7e, 0x4e, 0x7b, 0xbd, 0x32, 0xde, 0x87, 0x70, 0x51, 0x29, 0x2b, 0x48, 0x11,
	0xd9, 0x07, 0x45, 0xb8, 0x08, 0x3e, 0x5c, 0xfc, 0x78, 0x53, 0x24, 0x5e, 0x9f, 0x4b, 0xba, 0x8d,
	0xeb, 0x62, 0x76, 0xb3, 0x24, 0xb3, 0x85, 0xfe, 0x2e, 0xff, 0x89, 0xf8, 0x83, 0x64, 0x93, 0xa6,
	0xad, 0x5a, 0x05, 0xf1, 0x6d, 0xe6, 0x64, 0xe6, 0x64, 0xce, 0xc9, 0x04, 0xc0, 0xb6, 0x5a, 0x65,
	0x8d, 0x35, 0x64, 0x70, 0xe8, 0xf2, 0xcf, 0xaa, 0x92, 0x97, 0xb3, 0xc6, 0xe8, 0x32, 0xdf, 0x06,
	0x34, 0xfd, 0xc6, 0x60, 0xfa, 0x56, 0x36, 0x4d, 0x59, 0x17, 0xa2, 0xd5, 0x0a, 0x11, 0xfa, 0xb5,
	0xac, 0x14, 0x67, 0x73, 0xb6, 0x98, 0x08, 0x1f, 0xe3, 0x2b, 0x98, 0x92, 0x2c, 0x96, 0x9f, 0x4a,
	0x4d, 0xca, 0x3a, 0xde, 0x9b, 0x27, 0x8b, 0xe9, 0x93, 0x07, 0x59, 0xe0, 0xcb, 0x8e, 0xba, 0xb3,
	0x1b, 0x59, 0xbc, 0x09, 0x55, 0xaf, 0x6b, 0xb2, 0x5b, 0x01, 0xb4, 0x07, 0xf0, 0x11, 0x8c, 0xfd,
	0xcd, 0xa5, 0x72, 0x3c, 0xf1, 0x14, 0xb7, 0x22, 0xc5, 0x7b, 0x3f, 0x91, 0xd8, 0x9f, 0x5f, 0xbe,
	0x80, 0xf3, 0x5f, 0xa8, 0xf0, 0x36, 0x24, 0x5f, 0xd4, 0x76, 0x37, 0x57, 0x17, 0xe2, 0x05, 0x0c,
	0x36, 0x52, 0xb7, 0x8a, 0xf7, 0x3c, 0x16, 0x92, 0xe7, 0xbd, 0x2b, 0x96, 0xae, 0x60, 0x26, 0x8c,
	0xd6, 0x6d, 0x73, 0x23, 0x6d, 0xa1, 0xe8, 0xa4, 0x28, 0x84, 0x3e, 0xc9, 0x22, 0xa8, 0x99, 0x08,
	0x1f, 0xff, 0xcb, 0x88, 0xe9, 0x77, 0x06, 0x10, 0x2e, 0xf9, 0xa3, 0x6f, 0x2f, 0x4f, 0xf9, 0x96,
	0x46, 0xc6, 0x43, 0xf3, 0x5f, 0x6d, 0xcb, 0x60, 0x44, 0x5e, 0x45, 0x1c, 0xe9, 0xe2, 0x67, 0x82,
	0x20, 0x51, 0xc4, 0xa2, 0xff, 0xb5, 0xee, 0x6b, 0x0f, 0x46, 0xdd, 0x4c, 0x1f, 0x14, 0xe1, 0x5d,
	0x98, 0x74, 0x3a, 0x5c, 0x23, 0xf3, 0x28, 0xec, 0x00, 0xe0, 0x3d, 0x80, 0xdc, 0x2a, 0x49, 0x6a,
	0xbd, 0x94, 0xe4, 0x89, 0x12, 0x31, 0xd9, 0x21, 0xd7, 0x84, 0x0f, 0xe1, 0x5c, 0x4b, 0x47, 0xcb,
	0xb6, 0x59, 0xc7, 0x9a, 0xc4, 0xd7, 0x9c, 0x75, 0xf0, 0xc7, 0x80, 0x5e, 0x13, 0xde, 0x07, 0x20,
	0x53, 0xad, 0x1c, 0x99, 0x5a, 0xad, 0x79, 0x7f, 0xce, 0x16, 0x63, 0x71, 0x84, 0x20, 0x87, 0xd1,
	0x46, 0x59, 0x57, 0x9a, 0x9a, 0x0f, 0xe6, 0x6c, 0x31, 0x10, 0x31, 0xed, 0x4e, 0xf2, 0x96, 0xcc,
	0x46, 0x59, 0x3e, 0xf4, 0xcc, 0x31, 0xc5, 0x2b, 0x38, 0xab, 0xc2, 0x56, 0x2e, 0xbb, 0x0f, 0xe0,
	0xf8, 0xc8, 0x3b, 0x77, 0xe7, 0xc4, 0xca, 0x8a, 0x59, 0x75, 0x48, 0x1c, 0x3e, 0x83, 0x99, 0xf5,
	0xb6, 0xee, 0x1a, 0xc7, 0xbe, 0x11, 0x7f, 0x7f, 0x33, 0x31, 0xb5, 0xfb, 0xd8, 0xa5, 0x8f, 0x01,
	0xde, 0x45, 0x63, 0x5c, 0x27, 0x69, 0x6f, 0x93, 0xe3, 0xcc, 0x2f, 0xd8, 0x11, 0xb2, 0x1a, 0xfa,
	0xaf, 0xf7, 0xf4, 0x47, 0x00, 0x00, 0x00, 0xff, 0xff, 0xb9, 0x70, 0x44, 0xac, 0x9e, 0x03, 0x00,
	0x00,
}
