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
	Name        string            `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Tombstoned  bool              `protobuf:"varint,2,opt,name=tombstoned" json:"tombstoned,omitempty"`
	CutoverTime int64             `protobuf:"varint,3,opt,name=cutover_time,json=cutoverTime" json:"cutover_time,omitempty"`
	TagFilters  map[string]string `protobuf:"bytes,4,rep,name=tag_filters,json=tagFilters" json:"tag_filters,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	Policies    []*Policy         `protobuf:"bytes,5,rep,name=policies" json:"policies,omitempty"`
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

type MappingRuleChanges struct {
	Uuid    string         `protobuf:"bytes,1,opt,name=uuid" json:"uuid,omitempty"`
	Changes []*MappingRule `protobuf:"bytes,2,rep,name=changes" json:"changes,omitempty"`
}

func (m *MappingRuleChanges) Reset()                    { *m = MappingRuleChanges{} }
func (m *MappingRuleChanges) String() string            { return proto.CompactTextString(m) }
func (*MappingRuleChanges) ProtoMessage()               {}
func (*MappingRuleChanges) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{1} }

func (m *MappingRuleChanges) GetChanges() []*MappingRule {
	if m != nil {
		return m.Changes
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
func (*RollupTarget) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{2} }

func (m *RollupTarget) GetPolicies() []*Policy {
	if m != nil {
		return m.Policies
	}
	return nil
}

type RollupRule struct {
	Name        string            `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Tombstoned  bool              `protobuf:"varint,2,opt,name=tombstoned" json:"tombstoned,omitempty"`
	CutoverTime int64             `protobuf:"varint,3,opt,name=cutover_time,json=cutoverTime" json:"cutover_time,omitempty"`
	TagFilters  map[string]string `protobuf:"bytes,4,rep,name=tag_filters,json=tagFilters" json:"tag_filters,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	Targets     []*RollupTarget   `protobuf:"bytes,5,rep,name=targets" json:"targets,omitempty"`
}

func (m *RollupRule) Reset()                    { *m = RollupRule{} }
func (m *RollupRule) String() string            { return proto.CompactTextString(m) }
func (*RollupRule) ProtoMessage()               {}
func (*RollupRule) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{3} }

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

type RollupRuleChanges struct {
	Uuid    string        `protobuf:"bytes,1,opt,name=uuid" json:"uuid,omitempty"`
	Changes []*RollupRule `protobuf:"bytes,2,rep,name=changes" json:"changes,omitempty"`
}

func (m *RollupRuleChanges) Reset()                    { *m = RollupRuleChanges{} }
func (m *RollupRuleChanges) String() string            { return proto.CompactTextString(m) }
func (*RollupRuleChanges) ProtoMessage()               {}
func (*RollupRuleChanges) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{4} }

func (m *RollupRuleChanges) GetChanges() []*RollupRule {
	if m != nil {
		return m.Changes
	}
	return nil
}

type RuleSet struct {
	Uuid               string                `protobuf:"bytes,1,opt,name=uuid" json:"uuid,omitempty"`
	Namespace          string                `protobuf:"bytes,2,opt,name=namespace" json:"namespace,omitempty"`
	CreatedAt          int64                 `protobuf:"varint,3,opt,name=created_at,json=createdAt" json:"created_at,omitempty"`
	LastUpdatedAt      int64                 `protobuf:"varint,4,opt,name=last_updated_at,json=lastUpdatedAt" json:"last_updated_at,omitempty"`
	Tombstoned         bool                  `protobuf:"varint,5,opt,name=tombstoned" json:"tombstoned,omitempty"`
	CutoverTime        int64                 `protobuf:"varint,6,opt,name=cutover_time,json=cutoverTime" json:"cutover_time,omitempty"`
	MappingRuleChanges []*MappingRuleChanges `protobuf:"bytes,7,rep,name=mapping_rule_changes,json=mappingRuleChanges" json:"mapping_rule_changes,omitempty"`
	RollupRuleChanges  []*RollupRuleChanges  `protobuf:"bytes,8,rep,name=rollup_rule_changes,json=rollupRuleChanges" json:"rollup_rule_changes,omitempty"`
}

func (m *RuleSet) Reset()                    { *m = RuleSet{} }
func (m *RuleSet) String() string            { return proto.CompactTextString(m) }
func (*RuleSet) ProtoMessage()               {}
func (*RuleSet) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{5} }

func (m *RuleSet) GetMappingRuleChanges() []*MappingRuleChanges {
	if m != nil {
		return m.MappingRuleChanges
	}
	return nil
}

func (m *RuleSet) GetRollupRuleChanges() []*RollupRuleChanges {
	if m != nil {
		return m.RollupRuleChanges
	}
	return nil
}

type Namespace struct {
	Name       string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Tombstoned bool   `protobuf:"varint,2,opt,name=tombstoned" json:"tombstoned,omitempty"`
	ExpireAt   int64  `protobuf:"varint,3,opt,name=expire_at,json=expireAt" json:"expire_at,omitempty"`
}

func (m *Namespace) Reset()                    { *m = Namespace{} }
func (m *Namespace) String() string            { return proto.CompactTextString(m) }
func (*Namespace) ProtoMessage()               {}
func (*Namespace) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{6} }

type Namespaces struct {
	Namespaces []*Namespace `protobuf:"bytes,1,rep,name=namespaces" json:"namespaces,omitempty"`
}

func (m *Namespaces) Reset()                    { *m = Namespaces{} }
func (m *Namespaces) String() string            { return proto.CompactTextString(m) }
func (*Namespaces) ProtoMessage()               {}
func (*Namespaces) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{7} }

func (m *Namespaces) GetNamespaces() []*Namespace {
	if m != nil {
		return m.Namespaces
	}
	return nil
}

func init() {
	proto.RegisterType((*MappingRule)(nil), "schema.MappingRule")
	proto.RegisterType((*MappingRuleChanges)(nil), "schema.MappingRuleChanges")
	proto.RegisterType((*RollupTarget)(nil), "schema.RollupTarget")
	proto.RegisterType((*RollupRule)(nil), "schema.RollupRule")
	proto.RegisterType((*RollupRuleChanges)(nil), "schema.RollupRuleChanges")
	proto.RegisterType((*RuleSet)(nil), "schema.RuleSet")
	proto.RegisterType((*Namespace)(nil), "schema.Namespace")
	proto.RegisterType((*Namespaces)(nil), "schema.Namespaces")
}

func init() { proto.RegisterFile("rule.proto", fileDescriptor1) }

var fileDescriptor1 = []byte{
	// 525 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xb4, 0x54, 0xdb, 0x8a, 0xd3, 0x40,
	0x18, 0x26, 0x49, 0x4f, 0xf9, 0x5b, 0x5d, 0x3b, 0xdb, 0x8b, 0x58, 0x0f, 0xd4, 0x08, 0x52, 0x44,
	0x03, 0xea, 0x8d, 0x08, 0x22, 0x65, 0x55, 0x10, 0x54, 0x64, 0xec, 0xe2, 0x8d, 0x10, 0xa6, 0xe9,
	0x98, 0x0d, 0xe6, 0xc4, 0xcc, 0x64, 0xb1, 0x0f, 0x22, 0xbe, 0x9d, 0xcf, 0x22, 0x99, 0xc9, 0x24,
	0x6d, 0x53, 0x2a, 0x28, 0x7b, 0xf7, 0xf7, 0xcb, 0xcf, 0x37, 0xdf, 0x61, 0x3a, 0x00, 0xac, 0x88,
	0xa9, 0x97, 0xb3, 0x4c, 0x64, 0xa8, 0xc7, 0x83, 0x0b, 0x9a, 0x90, 0xe9, 0x28, 0xcf, 0xe2, 0x28,
	0xd8, 0x28, 0xd4, 0xfd, 0x69, 0xc2, 0xf0, 0x03, 0xc9, 0xf3, 0x28, 0x0d, 0x71, 0x11, 0x53, 0x84,
	0xa0, 0x93, 0x92, 0x84, 0x3a, 0xc6, 0xcc, 0x98, 0xdb, 0x58, 0xce, 0xe8, 0x2e, 0x80, 0xc8, 0x92,
	0x15, 0x17, 0x59, 0x4a, 0xd7, 0x8e, 0x39, 0x33, 0xe6, 0x03, 0xbc, 0x85, 0xa0, 0x7b, 0x30, 0x0a,
	0x0a, 0x91, 0x5d, 0x52, 0xe6, 0x8b, 0x28, 0xa1, 0x8e, 0x35, 0x33, 0xe6, 0x16, 0x1e, 0x56, 0xd8,
	0x32, 0x4a, 0x28, 0x7a, 0x0d, 0x43, 0x41, 0x42, 0xff, 0x5b, 0x14, 0x0b, 0xca, 0xb8, 0xd3, 0x99,
	0x59, 0xf3, 0xe1, 0xd3, 0xfb, 0x9e, 0x92, 0xe4, 0x6d, 0x09, 0xf0, 0x96, 0x24, 0x7c, 0xab, 0xb6,
	0xde, 0xa4, 0x82, 0x6d, 0x30, 0x88, 0x1a, 0x40, 0x0f, 0x61, 0x20, 0xc5, 0x47, 0x94, 0x3b, 0x5d,
	0x49, 0x71, 0x5d, 0x53, 0x7c, 0x92, 0xa6, 0x70, 0xfd, 0x7d, 0xfa, 0x12, 0x4e, 0xf6, 0xa8, 0xd0,
	0x0d, 0xb0, 0xbe, 0xd3, 0x4d, 0x65, 0xad, 0x1c, 0xd1, 0x04, 0xba, 0x97, 0x24, 0x2e, 0xa8, 0x34,
	0x65, 0x63, 0xf5, 0xe3, 0x85, 0xf9, 0xdc, 0x70, 0xbf, 0x00, 0xda, 0x52, 0x75, 0x76, 0x41, 0xd2,
	0x90, 0xf2, 0x32, 0x9d, 0xa2, 0x88, 0xd6, 0x3a, 0x9d, 0x72, 0x46, 0x8f, 0xa1, 0x1f, 0xa8, 0xcf,
	0x8e, 0x29, 0x35, 0x9d, 0x1e, 0xb0, 0x85, 0xf5, 0x8e, 0xbb, 0x82, 0x11, 0xce, 0xe2, 0xb8, 0xc8,
	0x97, 0x84, 0x85, 0x54, 0x1c, 0x0c, 0x1c, 0x41, 0x47, 0x90, 0x50, 0xf1, 0xd9, 0x58, 0xce, 0x3b,
	0xde, 0xad, 0xe3, 0xde, 0xdd, 0x5f, 0x26, 0x80, 0x3a, 0xe4, 0x2a, 0x3b, 0x3d, 0x3b, 0xd4, 0xa9,
	0xab, 0x45, 0x35, 0xe7, 0x1f, 0xad, 0xd4, 0x83, 0xbe, 0x90, 0x41, 0xe8, 0x46, 0x27, 0xbb, 0x04,
	0x2a, 0x25, 0xac, 0x97, 0xfe, 0xb7, 0xd6, 0x73, 0x18, 0x37, 0xc2, 0x8e, 0xb5, 0xfa, 0x68, 0xbf,
	0x55, 0xd4, 0x36, 0xd6, 0x94, 0xfa, 0xdb, 0x84, 0x7e, 0x89, 0x7c, 0x56, 0x85, 0xb6, 0xd8, 0x6e,
	0x83, 0x5d, 0xa6, 0xce, 0x73, 0x12, 0x68, 0x51, 0x0d, 0x80, 0xee, 0x00, 0x04, 0x8c, 0x12, 0x41,
	0xd7, 0x3e, 0x11, 0x55, 0xd2, 0x76, 0x85, 0x2c, 0x04, 0x7a, 0x00, 0x27, 0x31, 0xe1, 0xc2, 0x2f,
	0xf2, 0xb5, 0xde, 0xe9, 0xc8, 0x9d, 0x6b, 0x25, 0x7c, 0xae, 0xd0, 0x85, 0xd8, 0xab, 0xb4, 0xfb,
	0xd7, 0x4a, 0x7b, 0xed, 0x4a, 0xdf, 0xc3, 0x24, 0x51, 0x97, 0xd6, 0x2f, 0x5f, 0x0e, 0x5f, 0x47,
	0xd0, 0x97, 0x11, 0x4c, 0x0f, 0x5c, 0xec, 0x2a, 0x43, 0x8c, 0x92, 0xf6, 0xbf, 0xe5, 0x1d, 0x9c,
	0x32, 0x19, 0xd6, 0x2e, 0xd9, 0x40, 0x92, 0xdd, 0x6c, 0xe7, 0xa9, 0xb9, 0xc6, 0x6c, 0x1f, 0x72,
	0xbf, 0x82, 0xfd, 0xb1, 0xce, 0xeb, 0x5f, 0xee, 0xf3, 0x2d, 0xb0, 0xe9, 0x8f, 0x3c, 0x62, 0xb4,
	0x89, 0x78, 0xa0, 0x80, 0x85, 0x70, 0x5f, 0x01, 0xd4, 0xec, 0x1c, 0x3d, 0x01, 0xa8, 0xbb, 0xe1,
	0x8e, 0x21, 0xd5, 0x8e, 0xb5, 0xda, 0x7a, 0x0f, 0x6f, 0x2d, 0xad, 0x7a, 0xf2, 0x31, 0x7d, 0xf6,
	0x27, 0x00, 0x00, 0xff, 0xff, 0xd0, 0x4e, 0x75, 0x67, 0x70, 0x05, 0x00, 0x00,
}
