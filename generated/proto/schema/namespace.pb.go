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
// source: namespace.proto
// DO NOT EDIT!

/*
Package schema is a generated protocol buffer package.

It is generated from these files:
	namespace.proto
	policy.proto
	rule.proto

It has these top-level messages:
	NamespaceSnapshot
	Namespace
	Namespaces
	Resolution
	Retention
	Policy
	MappingRuleSnapshot
	MappingRule
	RollupTarget
	RollupRuleSnapshot
	RollupRule
	RuleSet
*/
package schema

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type NamespaceSnapshot struct {
	ForRulesetVersion int32 `protobuf:"varint,1,opt,name=for_ruleset_version,json=forRulesetVersion" json:"for_ruleset_version,omitempty"`
	Tombstoned        bool  `protobuf:"varint,2,opt,name=tombstoned" json:"tombstoned,omitempty"`
}

func (m *NamespaceSnapshot) Reset()                    { *m = NamespaceSnapshot{} }
func (m *NamespaceSnapshot) String() string            { return proto.CompactTextString(m) }
func (*NamespaceSnapshot) ProtoMessage()               {}
func (*NamespaceSnapshot) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type Namespace struct {
	Name      string               `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Snapshots []*NamespaceSnapshot `protobuf:"bytes,2,rep,name=snapshots" json:"snapshots,omitempty"`
}

func (m *Namespace) Reset()                    { *m = Namespace{} }
func (m *Namespace) String() string            { return proto.CompactTextString(m) }
func (*Namespace) ProtoMessage()               {}
func (*Namespace) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *Namespace) GetSnapshots() []*NamespaceSnapshot {
	if m != nil {
		return m.Snapshots
	}
	return nil
}

type Namespaces struct {
	Namespaces []*Namespace `protobuf:"bytes,1,rep,name=namespaces" json:"namespaces,omitempty"`
}

func (m *Namespaces) Reset()                    { *m = Namespaces{} }
func (m *Namespaces) String() string            { return proto.CompactTextString(m) }
func (*Namespaces) ProtoMessage()               {}
func (*Namespaces) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *Namespaces) GetNamespaces() []*Namespace {
	if m != nil {
		return m.Namespaces
	}
	return nil
}

func init() {
	proto.RegisterType((*NamespaceSnapshot)(nil), "schema.NamespaceSnapshot")
	proto.RegisterType((*Namespace)(nil), "schema.Namespace")
	proto.RegisterType((*Namespaces)(nil), "schema.Namespaces")
}

func init() { proto.RegisterFile("namespace.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 201 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0xe2, 0xcf, 0x4b, 0xcc, 0x4d,
	0x2d, 0x2e, 0x48, 0x4c, 0x4e, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2b, 0x4e, 0xce,
	0x48, 0xcd, 0x4d, 0x54, 0x4a, 0xe6, 0x12, 0xf4, 0x83, 0x49, 0x05, 0xe7, 0x25, 0x16, 0x14, 0x67,
	0xe4, 0x97, 0x08, 0xe9, 0x71, 0x09, 0xa7, 0xe5, 0x17, 0xc5, 0x17, 0x95, 0xe6, 0xa4, 0x16, 0xa7,
	0x96, 0xc4, 0x97, 0xa5, 0x16, 0x15, 0x67, 0xe6, 0xe7, 0x49, 0x30, 0x2a, 0x30, 0x6a, 0xb0, 0x06,
	0x09, 0xa6, 0xe5, 0x17, 0x05, 0x41, 0x64, 0xc2, 0x20, 0x12, 0x42, 0x72, 0x5c, 0x5c, 0x25, 0xf9,
	0xb9, 0x49, 0xc5, 0x25, 0xf9, 0x79, 0xa9, 0x29, 0x12, 0x4c, 0x0a, 0x8c, 0x1a, 0x1c, 0x41, 0x48,
	0x22, 0x4a, 0x11, 0x5c, 0x9c, 0x70, 0x4b, 0x84, 0x84, 0xb8, 0x58, 0x40, 0x8e, 0x01, 0x9b, 0xc6,
	0x19, 0x04, 0x66, 0x0b, 0x99, 0x73, 0x71, 0x16, 0x43, 0x2d, 0x2f, 0x96, 0x60, 0x52, 0x60, 0xd6,
	0xe0, 0x36, 0x92, 0xd4, 0x83, 0xb8, 0x50, 0x0f, 0xc3, 0x79, 0x41, 0x08, 0xb5, 0x4a, 0xf6, 0x5c,
	0x5c, 0x70, 0xf9, 0x62, 0x21, 0x43, 0x2e, 0x2e, 0xb8, 0x3f, 0x8b, 0x25, 0x18, 0xc1, 0xe6, 0x08,
	0x62, 0x98, 0x13, 0x84, 0xa4, 0x28, 0x89, 0x0d, 0x1c, 0x1c, 0xc6, 0x80, 0x00, 0x00, 0x00, 0xff,
	0xff, 0xad, 0xc2, 0x51, 0xf8, 0x21, 0x01, 0x00, 0x00,
}
