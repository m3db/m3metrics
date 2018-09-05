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
// source: github.com/m3db/m3metrics/generated/proto/aggregationpb/aggregation.proto

package aggregationpb // import "github.com/m3db/m3metrics/generated/proto/aggregationpb"

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type AggregationType int32

const (
	AggregationType_UNKNOWN AggregationType = 0
	AggregationType_LAST    AggregationType = 1
	AggregationType_MIN     AggregationType = 2
	AggregationType_MAX     AggregationType = 3
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
	2:  "MIN",
	3:  "MAX",
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
	"MIN":     2,
	"MAX":     3,
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
func (AggregationType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_aggregation_770b4e4d9907e9d6, []int{0}
}

// AggregationID is a unique identifier uniquely identifying
// one or more aggregation types.
type AggregationID struct {
	Id uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (m *AggregationID) Reset()         { *m = AggregationID{} }
func (m *AggregationID) String() string { return proto.CompactTextString(m) }
func (*AggregationID) ProtoMessage()    {}
func (*AggregationID) Descriptor() ([]byte, []int) {
	return fileDescriptor_aggregation_770b4e4d9907e9d6, []int{0}
}
func (m *AggregationID) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *AggregationID) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_AggregationID.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *AggregationID) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AggregationID.Merge(dst, src)
}
func (m *AggregationID) XXX_Size() int {
	return m.Size()
}
func (m *AggregationID) XXX_DiscardUnknown() {
	xxx_messageInfo_AggregationID.DiscardUnknown(m)
}

var xxx_messageInfo_AggregationID proto.InternalMessageInfo

func (m *AggregationID) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func init() {
	proto.RegisterType((*AggregationID)(nil), "aggregationpb.AggregationID")
	proto.RegisterEnum("aggregationpb.AggregationType", AggregationType_name, AggregationType_value)
}
func (m *AggregationID) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *AggregationID) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Id != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintAggregation(dAtA, i, uint64(m.Id))
	}
	return i, nil
}

func encodeVarintAggregation(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *AggregationID) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Id != 0 {
		n += 1 + sovAggregation(uint64(m.Id))
	}
	return n
}

func sovAggregation(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozAggregation(x uint64) (n int) {
	return sovAggregation(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *AggregationID) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAggregation
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: AggregationID: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: AggregationID: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAggregation
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipAggregation(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAggregation
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipAggregation(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowAggregation
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowAggregation
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowAggregation
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthAggregation
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowAggregation
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipAggregation(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthAggregation = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowAggregation   = fmt.Errorf("proto: integer overflow")
)

func init() {
	proto.RegisterFile("github.com/m3db/m3metrics/generated/proto/aggregationpb/aggregation.proto", fileDescriptor_aggregation_770b4e4d9907e9d6)
}

var fileDescriptor_aggregation_770b4e4d9907e9d6 = []byte{
	// 324 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0xd1, 0xbd, 0x52, 0xf2, 0x40,
	0x18, 0x05, 0xe0, 0x2c, 0xff, 0x2c, 0x1f, 0xf0, 0x7e, 0xeb, 0xcf, 0x58, 0x45, 0xc7, 0xca, 0xb1,
	0x20, 0xab, 0x11, 0x31, 0x65, 0x14, 0x8a, 0x8c, 0x66, 0x01, 0x93, 0xa8, 0x63, 0x47, 0x48, 0x26,
	0xa6, 0x08, 0x61, 0x62, 0x2c, 0xbc, 0x0b, 0x2f, 0xcb, 0x92, 0xd2, 0xd2, 0x81, 0x3b, 0xf0, 0x0a,
	0x9c, 0x7d, 0x29, 0xc4, 0xd6, 0xee, 0xd9, 0x73, 0xce, 0xcc, 0xee, 0xcc, 0x52, 0x2b, 0x8a, 0xf3,
	0xa7, 0x17, 0xbf, 0x33, 0x4d, 0x13, 0x2d, 0xd1, 0x03, 0x5f, 0x4b, 0xf4, 0x24, 0xcc, 0xb3, 0x78,
	0xfa, 0xac, 0x45, 0xe1, 0x2c, 0xcc, 0x26, 0x79, 0x18, 0x68, 0xf3, 0x2c, 0xcd, 0x53, 0x6d, 0x12,
	0x45, 0x59, 0x18, 0x4d, 0xf2, 0x38, 0x9d, 0xcd, 0xfd, 0xcd, 0x53, 0x07, 0x7b, 0xd6, 0xfc, 0x35,
	0x38, 0xdc, 0xa7, 0x4d, 0xf3, 0x27, 0xb0, 0xfa, 0xac, 0x45, 0x0b, 0x71, 0xb0, 0x47, 0x0e, 0xc8,
	0x51, 0xe9, 0xb6, 0x10, 0x07, 0xc7, 0x5f, 0x84, 0xb6, 0x37, 0x16, 0xee, 0xeb, 0x3c, 0x64, 0x0d,
	0x5a, 0xf5, 0xc4, 0xb5, 0x18, 0xde, 0x0b, 0x50, 0x58, 0x8d, 0x96, 0x6e, 0x4c, 0xc7, 0x05, 0xc2,
	0xaa, 0xb4, 0x68, 0x5b, 0x02, 0x0a, 0x08, 0xf3, 0x01, 0x8a, 0xb2, 0xb3, 0x07, 0xa6, 0x80, 0x12,
	0xa3, 0xb4, 0x62, 0x0f, 0xfa, 0x96, 0x29, 0xa0, 0xcc, 0xea, 0xb4, 0x7c, 0x35, 0xf4, 0x84, 0x0b,
	0x15, 0xb9, 0x74, 0x3c, 0x1b, 0xaa, 0x32, 0x73, 0x3c, 0xdb, 0x19, 0x43, 0x0d, 0xe9, 0xf6, 0x07,
	0x77, 0x50, 0x97, 0xf5, 0xe8, 0x84, 0x03, 0x45, 0x9c, 0x72, 0x68, 0x20, 0x74, 0x0e, 0xff, 0x10,
	0x67, 0x1c, 0x9a, 0x88, 0x2e, 0x87, 0x16, 0xe2, 0x9c, 0x43, 0x1b, 0xd1, 0xe3, 0x00, 0x88, 0x0b,
	0x0e, 0xff, 0x11, 0x06, 0x07, 0xb6, 0x46, 0x17, 0xb6, 0xd6, 0x30, 0x60, 0x5b, 0x3e, 0x71, 0x64,
	0x18, 0x06, 0xec, 0xc8, 0x7b, 0xa5, 0x0c, 0xd8, 0xbd, 0x1c, 0xbf, 0x2f, 0x55, 0xb2, 0x58, 0xaa,
	0xe4, 0x73, 0xa9, 0x92, 0xb7, 0x95, 0xaa, 0x2c, 0x56, 0xaa, 0xf2, 0xb1, 0x52, 0x95, 0xc7, 0xde,
	0x1f, 0x7f, 0xc2, 0xaf, 0x60, 0xa8, 0x7f, 0x07, 0x00, 0x00, 0xff, 0xff, 0x8f, 0xd2, 0x79, 0x1e,
	0xcb, 0x01, 0x00, 0x00,
}
