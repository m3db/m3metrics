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

package policy

import (
	"testing"
	"time"
)

var (
	testNowNs = time.Now().UnixNano()
)

func BenchmarkStagedPoliciesAsStruct(b *testing.B) {
	sp := NewStagedPolicies(testNowNs, false, defaultPolicies)
	for n := 0; n < b.N; n++ {
		validatePolicyByValue(b, sp)
	}
}

func BenchmarkStagedPoliciesAsPointer(b *testing.B) {
	sp := NewStagedPolicies(testNowNs, false, defaultPolicies)
	for n := 0; n < b.N; n++ {
		validatePolicyByPointer(b, &sp)
	}
}

func BenchmarkStagedPoliciesAsInterface(b *testing.B) {
	sp := &testStagedPolicies{cutoverNs: testNowNs, policies: defaultPolicies}
	for n := 0; n < b.N; n++ {
		validatePolicyByInterface(b, sp)
	}
}

func BenchmarkStagedPoliciesAsStructExported(b *testing.B) {
	sp := testStagedPolicies{cutoverNs: testNowNs, policies: defaultPolicies}
	for n := 0; n < b.N; n++ {
		validatePolicyByStructExported(b, sp)
	}
}

type testStagedPoliciesInt64 interface {
	CutoverNs() int64
}

// StagedPolicies represent a list of policies at a specified version.
type testStagedPolicies struct {
	cutoverNs  int64
	tombstoned bool
	policies   []Policy
}

func (v testStagedPolicies) ValCutoverNs() int64 {
	return v.cutoverNs
}

func (v *testStagedPolicies) CutoverNs() int64 {
	return v.cutoverNs
}

func validatePolicyByValue(b *testing.B, sp StagedPolicies) {
	if sp.CutoverNs != testNowNs {
		b.FailNow()
	}
}

func validatePolicyByPointer(b *testing.B, sp *StagedPolicies) {
	if sp.CutoverNs != testNowNs {
		b.FailNow()
	}
}

func validatePolicyByInterface(b *testing.B, sp testStagedPoliciesInt64) {
	if sp.CutoverNs() != testNowNs {
		b.FailNow()
	}
}

func validatePolicyByStructExported(b *testing.B, sp testStagedPolicies) {
	if sp.ValCutoverNs() != testNowNs {
		b.FailNow()
	}
}
