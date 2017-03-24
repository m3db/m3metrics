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
	Namespaces     []string `protobuf:"bytes,1,rep,name=namespaces" json:"namespaces,omitempty"`
	RulesetCutover int64    `protobuf:"varint,2,opt,name=ruleset_cutover,json=rulesetCutover" json:"ruleset_cutover,omitempty"`
	Version        int32    `protobuf:"varint,3,opt,name=version" json:"version,omitempty"`
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
	// 454 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xa4, 0x53, 0x4d, 0x8b, 0xd4, 0x40,
	0x10, 0x25, 0x93, 0xf9, 0xac, 0x99, 0xdd, 0x91, 0x72, 0x0f, 0xcd, 0xa2, 0x32, 0x44, 0xd0, 0xc1,
	0x43, 0x0e, 0x8a, 0xb0, 0x08, 0x1e, 0x96, 0x55, 0x6f, 0x8a, 0xb4, 0xeb, 0x39, 0xf4, 0x64, 0xda,
	0x18, 0x4c, 0xd2, 0xa1, 0xbb, 0x32, 0x30, 0xbf, 0xcb, 0x7f, 0x22, 0xfe, 0x20, 0x49, 0x77, 0x7a,
	0x3e, 0x74, 0x14, 0x64, 0x6f, 0x55, 0xaf, 0xab, 0x5e, 0xd7, 0x7b, 0x5d, 0x0d, 0xa0, 0x9b, 0x42,
	0xc6, 0xb5, 0x56, 0xa4, 0x70, 0x68, 0xd2, 0xaf, 0xb2, 0x14, 0x97, 0xb3, 0x5a, 0x15, 0x79, 0xba,
	0x75, 0x68, 0xf4, 0x23, 0x80, 0xe9, 0x7b, 0x51, 0xd7, 0x79, 0x95, 0xf1, 0xa6, 0x90, 0x88, 0xd0,
	0xaf, 0x44, 0x29, 0x59, 0xb0, 0x08, 0x96, 0x13, 0x6e, 0x63, 0x7c, 0x03, 0x53, 0x12, 0x59, 0xf2,
	0x25, 0x2f, 0x48, 0x6a, 0xc3, 0x7a, 0x8b, 0x70, 0x39, 0x7d, 0xfe, 0x38, 0x76, 0x7c, 0xf1, 0x41,
	0x77, 0x7c, 0x2b, 0xb2, 0x77, 0xae, 0xea, 0x6d, 0x45, 0x7a, 0xcb, 0x81, 0x76, 0x00, 0x3e, 0x83,
	0xb1, 0xbd, 0x39, 0x97, 0x86, 0x85, 0x96, 0xe2, 0xdc, 0x53, 0x7c, 0xb4, 0x13, 0xf1, 0xdd, 0xf9,
	0xe5, 0x6b, 0x98, 0xff, 0x46, 0x85, 0xf7, 0x20, 0xfc, 0x26, 0xb7, 0xdd, 0x5c, 0x6d, 0x88, 0x17,
	0x30, 0xd8, 0x88, 0xa2, 0x91, 0xac, 0x67, 0x31, 0x97, 0xbc, 0xea, 0x5d, 0x05, 0xd1, 0x0a, 0x66,
	0x5c, 0x15, 0x45, 0x53, 0xdf, 0x0a, 0x9d, 0x49, 0x3a, 0x29, 0x0a, 0xa1, 0x4f, 0x22, 0x73, 0x6a,
	0x26, 0xdc, 0xc6, 0xff, 0x33, 0x62, 0xf4, 0x33, 0x00, 0x70, 0x97, 0xfc, 0xd5, 0xb7, 0x9b, 0x53,
	0xbe, 0x45, 0x9e, 0x71, 0xdf, 0xfc, 0x4f, 0xdb, 0x62, 0x18, 0x91, 0x55, 0xe1, 0x47, 0xba, 0x38,
	0x26, 0x70, 0x12, 0xb9, 0x2f, 0xba, 0xab, 0x75, 0xdf, 0x7b, 0x30, 0x6a, 0x67, 0xfa, 0x24, 0x09,
	0x1f, 0xc0, 0xa4, 0xd5, 0x61, 0x6a, 0x91, 0x7a, 0x61, 0x7b, 0x00, 0x1f, 0x02, 0xa4, 0x5a, 0x0a,
	0x92, 0xeb, 0x44, 0x90, 0x25, 0x0a, 0xf9, 0xa4, 0x43, 0xae, 0x09, 0x9f, 0xc0, 0xbc, 0x10, 0x86,
	0x92, 0xa6, 0x5e, 0xfb, 0x9a, 0xd0, 0xd6, 0x9c, 0xb5, 0xf0, 0x67, 0x87, 0x5e, 0x13, 0x3e, 0x02,
	0x20, 0x55, 0xae, 0x0c, 0xa9, 0x4a, 0xae, 0x59, 0x7f, 0x11, 0x2c, 0xc7, 0xfc, 0x00, 0x41, 0x06,
	0xa3, 0x8d, 0xd4, 0x26, 0x57, 0x15, 0x1b, 0x2c, 0x82, 0xe5, 0x80, 0xfb, 0xb4, 0x3d, 0x49, 0x1b,
	0x52, 0x1b, 0xa9, 0xd9, 0xd0, 0x32, 0xfb, 0x14, 0xaf, 0xe0, 0xac, 0x74, 0x5b, 0x99, 0xb4, 0x1f,
	0xc0, 0xb0, 0x91, 0x75, 0xee, 0xfe, 0x89, 0x95, 0xe5, 0xb3, 0x72, 0x9f, 0x18, 0x7c, 0x09, 0x33,
	0x6d, 0x6d, 0xed, 0x1a, 0xc7, 0xb6, 0x11, 0xff, 0x7c, 0x33, 0x3e, 0xd5, 0xbb, 0xd8, 0x44, 0x0a,
	0xe0, 0x83, 0x37, 0xc6, 0xb4, 0x92, 0x76, 0x36, 0x19, 0x16, 0xd8, 0x05, 0x3b, 0x40, 0xf0, 0x29,
	0xcc, 0x2d, 0xbb, 0xa4, 0xc4, 0x0b, 0x70, 0xf6, 0x9d, 0x77, 0xf0, 0x4d, 0xa7, 0xe3, 0x40, 0x7b,
	0x78, 0xa4, 0x7d, 0x35, 0xb4, 0xbf, 0xf7, 0xc5, 0xaf, 0x00, 0x00, 0x00, 0xff, 0xff, 0x14, 0xdf,
	0x00, 0xa3, 0xe1, 0x03, 0x00, 0x00,
}
