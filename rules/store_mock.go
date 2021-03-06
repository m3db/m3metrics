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
// Source: github.com/m3db/m3metrics/rules (interfaces: Store)

package rules

import (
	"github.com/golang/mock/gomock"
)

// Mock of Store interface
type MockStore struct {
	ctrl     *gomock.Controller
	recorder *_MockStoreRecorder
}

// Recorder for MockStore (not exported)
type _MockStoreRecorder struct {
	mock *MockStore
}

func NewMockStore(ctrl *gomock.Controller) *MockStore {
	mock := &MockStore{ctrl: ctrl}
	mock.recorder = &_MockStoreRecorder{mock}
	return mock
}

func (_m *MockStore) EXPECT() *_MockStoreRecorder {
	return _m.recorder
}

func (_m *MockStore) Close() {
	_m.ctrl.Call(_m, "Close")
}

func (_mr *_MockStoreRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}

func (_m *MockStore) ReadNamespaces() (*Namespaces, error) {
	ret := _m.ctrl.Call(_m, "ReadNamespaces")
	ret0, _ := ret[0].(*Namespaces)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockStoreRecorder) ReadNamespaces() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ReadNamespaces")
}

func (_m *MockStore) ReadRuleSet(_param0 string) (RuleSet, error) {
	ret := _m.ctrl.Call(_m, "ReadRuleSet", _param0)
	ret0, _ := ret[0].(RuleSet)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockStoreRecorder) ReadRuleSet(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ReadRuleSet", arg0)
}

func (_m *MockStore) WriteAll(_param0 *Namespaces, _param1 MutableRuleSet) error {
	ret := _m.ctrl.Call(_m, "WriteAll", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockStoreRecorder) WriteAll(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WriteAll", arg0, arg1)
}

func (_m *MockStore) WriteRuleSet(_param0 MutableRuleSet) error {
	ret := _m.ctrl.Call(_m, "WriteRuleSet", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockStoreRecorder) WriteRuleSet(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WriteRuleSet", arg0)
}
