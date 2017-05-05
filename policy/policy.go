// Copyright (c) 2016 Uber Technologies, Inc.
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
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3x/time"
)

const (
	// InitPolicyVersion is the version of an uninitialized policy.
	InitPolicyVersion = -1

	// DefaultPolicyVersion is the version for the default policy.
	DefaultPolicyVersion = 0
)

var (
	// EmptyPolicy represents an empty policy.
	EmptyPolicy Policy

	// DefaultPolicies are the default policies.
	DefaultPolicies = []Policy{
		NewPolicy(10*time.Second, xtime.Second, 2*24*time.Hour),
		NewPolicy(time.Minute, xtime.Minute, 30*24*time.Hour),
	}

	// DefaultStagedPolicies represents a default staged policies.
	DefaultStagedPolicies StagedPolicies

	// DefaultPoliciesList represents a default policies list.
	DefaultPoliciesList = PoliciesList{DefaultStagedPolicies}

	errNilPolicySchema = errors.New("nil policy schema")
)

// Policy represents the resolution and retention period metric datapoints
// are stored at.
type Policy struct {
	resolution Resolution
	retention  Retention
}

// NewPolicy creates a new policy given a resolution window size and retention.
func NewPolicy(window time.Duration, precision xtime.Unit, retention time.Duration) Policy {
	return Policy{
		resolution: Resolution{
			Window:    window,
			Precision: precision,
		},
		retention: Retention(retention),
	}
}

// NewPolicyFromSchema creates a new policy from a schema policy.
func NewPolicyFromSchema(p *schema.Policy) (Policy, error) {
	if p == nil {
		return EmptyPolicy, errNilPolicySchema
	}
	precision := time.Duration(p.Resolution.Precision)
	unit, err := xtime.UnitFromDuration(precision)
	if err != nil {
		return EmptyPolicy, err
	}
	return Policy{
		resolution: Resolution{
			Window:    time.Duration(p.Resolution.WindowSize),
			Precision: unit,
		},
		retention: Retention(p.Retention.Period),
	}, nil
}

// NewPoliciesFromSchema creates multiple new policues from given schema policies.
func NewPoliciesFromSchema(policies []*schema.Policy) ([]Policy, error) {
	res := make([]Policy, 0, len(policies))
	for _, p := range policies {
		policy, err := NewPolicyFromSchema(p)
		if err != nil {
			return nil, err
		}
		res = append(res, policy)
	}
	return res, nil
}

// String is the string representation of a policy.
func (p Policy) String() string {
	return fmt.Sprintf("{resolution:%s,retention:%s}", p.resolution.String(), p.retention.String())
}

// Resolution returns the resolution of the policy.
func (p Policy) Resolution() Resolution {
	return p.resolution
}

// Retention return the retention of the policy.
func (p Policy) Retention() Retention {
	return p.retention
}

// ByResolutionAsc implements the sort.Sort interface to sort
// policies by resolution in ascending order, finest resolution first.
// If two policies have the same resolution, the one with longer
// retention period comes first.
type ByResolutionAsc []Policy

func (pr ByResolutionAsc) Len() int      { return len(pr) }
func (pr ByResolutionAsc) Swap(i, j int) { pr[i], pr[j] = pr[j], pr[i] }

func (pr ByResolutionAsc) Less(i, j int) bool {
	w1, w2 := pr[i].Resolution().Window, pr[j].Resolution().Window
	if w1 < w2 {
		return true
	}
	if w1 > w2 {
		return false
	}
	r1, r2 := pr[i].Retention(), pr[j].Retention()
	if r1 > r2 {
		return true
	}
	if r1 < r2 {
		return false
	}
	// NB(xichen): compare precision to ensure a deterministic ordering.
	return pr[i].Resolution().Precision < pr[i].Resolution().Precision
}

// StagedPolicies represent a list of policies at a specified version.
type StagedPolicies struct {
	// Cutover is when the policies take effect.
	CutoverNanos int64

	// Tombstoned determines whether the associated (rollup) metric has been tombstoned.
	Tombstoned bool

	// policies represent the list of policies.
	policies []Policy
}

// NewStagedPolicies create a new staged policies.
func NewStagedPolicies(cutoverNanos int64, tombstoned bool, policies []Policy) StagedPolicies {
	return StagedPolicies{CutoverNanos: cutoverNanos, Tombstoned: tombstoned, policies: policies}
}

// Reset resets the staged policies.
func (p *StagedPolicies) Reset() { *p = DefaultStagedPolicies }

// IsDefault returns whether this is a default staged policies.
func (p StagedPolicies) IsDefault() bool {
	return p.CutoverNanos == 0 && !p.Tombstoned && p.hasDefaultPolicies()
}

// Policies returns the policies and whether the policies are the default policies.
func (p StagedPolicies) Policies() ([]Policy, bool) {
	return p.policies, p.hasDefaultPolicies()
}

// SamePolicies returns whether two staged policies have the same policy list,
// assuming the policies are sorted in the same order.
func (p StagedPolicies) SamePolicies(other StagedPolicies) bool {
	currPolicies, currIsDefault := p.Policies()
	otherPolicies, otherIsDefault := other.Policies()
	if currIsDefault && otherIsDefault {
		return true
	}
	if currIsDefault || otherIsDefault {
		return false
	}
	if len(currPolicies) != len(otherPolicies) {
		return false
	}
	for i := 0; i < len(currPolicies); i++ {
		if currPolicies[i] != otherPolicies[i] {
			return false
		}
	}
	return true
}

// String is the representation of staged policies.
func (p StagedPolicies) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("{cutover:%s,tombstoned:%v,policies:[", time.Unix(0, p.CutoverNanos).String(), p.Tombstoned))
	for i := range p.policies {
		buf.WriteString(p.policies[i].String())
		if i < len(p.policies)-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString("]}")
	return buf.String()
}

func (p StagedPolicies) hasDefaultPolicies() bool {
	return len(p.policies) == 0
}

// PoliciesList is a list of staged policies.
type PoliciesList []StagedPolicies

// IsDefault determines whether this is a default policies list.
func (l PoliciesList) IsDefault() bool {
	return len(l) == 1 && l[0].IsDefault()
}
