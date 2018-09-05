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

// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: github.com/m3db/m3metrics/generated/proto/transformationpb/transformation.proto

package transformationpb // import "github.com/m3db/m3metrics/generated/proto/transformationpb"

import proto "github.com/gogo/protobuf/proto"
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
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type TransformationType int32

const (
	TransformationType_UNKNOWN   TransformationType = 0
	TransformationType_ABSOLUTE  TransformationType = 1
	TransformationType_PERSECOND TransformationType = 2
)

var TransformationType_name = map[int32]string{
	0: "UNKNOWN",
	1: "ABSOLUTE",
	2: "PERSECOND",
}
var TransformationType_value = map[string]int32{
	"UNKNOWN":   0,
	"ABSOLUTE":  1,
	"PERSECOND": 2,
}

func (x TransformationType) String() string {
	return proto.EnumName(TransformationType_name, int32(x))
}
func (TransformationType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_transformation_4f93e55befbb3e0c, []int{0}
}

func init() {
	proto.RegisterEnum("transformationpb.TransformationType", TransformationType_name, TransformationType_value)
}

func init() {
	proto.RegisterFile("github.com/m3db/m3metrics/generated/proto/transformationpb/transformation.proto", fileDescriptor_transformation_4f93e55befbb3e0c)
}

var fileDescriptor_transformation_4f93e55befbb3e0c = []byte{
	// 183 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xf2, 0x4f, 0xcf, 0x2c, 0xc9,
	0x28, 0x4d, 0xd2, 0x4b, 0xce, 0xcf, 0xd5, 0xcf, 0x35, 0x4e, 0x49, 0xd2, 0xcf, 0x35, 0xce, 0x4d,
	0x2d, 0x29, 0xca, 0x4c, 0x2e, 0xd6, 0x4f, 0x4f, 0xcd, 0x4b, 0x2d, 0x4a, 0x2c, 0x49, 0x4d, 0xd1,
	0x2f, 0x28, 0xca, 0x2f, 0xc9, 0xd7, 0x2f, 0x29, 0x4a, 0xcc, 0x2b, 0x4e, 0xcb, 0x2f, 0xca, 0x4d,
	0x2c, 0xc9, 0xcc, 0xcf, 0x2b, 0x48, 0x42, 0x13, 0xd0, 0x03, 0xab, 0x12, 0x12, 0x40, 0x57, 0xa6,
	0x65, 0xc7, 0x25, 0x14, 0x82, 0x22, 0x16, 0x52, 0x59, 0x90, 0x2a, 0xc4, 0xcd, 0xc5, 0x1e, 0xea,
	0xe7, 0xed, 0xe7, 0x1f, 0xee, 0x27, 0xc0, 0x20, 0xc4, 0xc3, 0xc5, 0xe1, 0xe8, 0x14, 0xec, 0xef,
	0x13, 0x1a, 0xe2, 0x2a, 0xc0, 0x28, 0xc4, 0xcb, 0xc5, 0x19, 0xe0, 0x1a, 0x14, 0xec, 0xea, 0xec,
	0xef, 0xe7, 0x22, 0xc0, 0xe4, 0x14, 0x72, 0xe2, 0x91, 0x1c, 0xe3, 0x85, 0x47, 0x72, 0x8c, 0x0f,
	0x1e, 0xc9, 0x31, 0x4e, 0x78, 0x2c, 0xc7, 0x70, 0xe1, 0xb1, 0x1c, 0xc3, 0x8d, 0xc7, 0x72, 0x0c,
	0x51, 0x56, 0xe4, 0x3b, 0x3e, 0x89, 0x0d, 0x2c, 0x6e, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff, 0x4f,
	0x21, 0x32, 0x93, 0x01, 0x01, 0x00, 0x00,
}
