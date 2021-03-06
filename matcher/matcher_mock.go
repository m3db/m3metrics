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

// Automatically generated by MockGen. DO NOT EDIT!
// Source: github.com/m3db/m3metrics/matcher (interfaces: Matcher)

package matcher

import (
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/rules"

	"github.com/golang/mock/gomock"
)

// Mock of Matcher interface
type MockMatcher struct {
	ctrl     *gomock.Controller
	recorder *_MockMatcherRecorder
}

// Recorder for MockMatcher (not exported)
type _MockMatcherRecorder struct {
	mock *MockMatcher
}

func NewMockMatcher(ctrl *gomock.Controller) *MockMatcher {
	mock := &MockMatcher{ctrl: ctrl}
	mock.recorder = &_MockMatcherRecorder{mock}
	return mock
}

func (_m *MockMatcher) EXPECT() *_MockMatcherRecorder {
	return _m.recorder
}

func (_m *MockMatcher) Close() error {
	ret := _m.ctrl.Call(_m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockMatcherRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}

func (_m *MockMatcher) ForwardMatch(_param0 id.ID, _param1 int64, _param2 int64) rules.MatchResult {
	ret := _m.ctrl.Call(_m, "ForwardMatch", _param0, _param1, _param2)
	ret0, _ := ret[0].(rules.MatchResult)
	return ret0
}

func (_mr *_MockMatcherRecorder) ForwardMatch(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ForwardMatch", arg0, arg1, arg2)
}
