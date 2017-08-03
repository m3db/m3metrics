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
// THE SOFTWARE

package rules

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3metrics/policy"
)

var (
	blankMappingRuleConfig = MappingRuleConfig{}

	// Input Errors
	errNoName     = errors.New("no name given")
	errNoNewName  = errors.New("no new name given")
	errNoFilters  = errors.New("no filters given")
	errNoPolicies = errors.New("no policies given")
	errNoTargets  = errors.New("no targets given")

	// Namespace Errors
	errNoSnapshots         = errors.New("namespace has no snapshots")
	errNamespaceTombstoned = errors.New("namespace is tombstoned")

	namespaceActionErrorFmt = "cannot %s namespace %s. %v"

	// Ruleset Errors
	errNoUpdate      = errors.New("no new config given")
	errNotTombstoned = errors.New("not tombstoned")

	ruleActionErrorFmt    = "cannot %s rule %s. %v"
	ruleSetActionErrorFmt = "cannot %s ruleset %s. %v"
)

// HandlerOptions holds common settings to all handlers.
type HandlerOptions struct {
	propagationDelay time.Duration
}

// NewHandlerOptions creates a new HanlderOptions struct.
func NewHandlerOptions(propDelay time.Duration) HandlerOptions {
	return HandlerOptions{propagationDelay: propDelay}
}

// MappingRuleConfig is the parts of a mapping rule that an end user would care about.
// This struct is provided to create or make updates to the mapping rule.
type MappingRuleConfig struct {
	Name     string            `json:"name" validate:"nonzero"`
	Filters  map[string]string `json:"filters" validate:"nonzero"`
	Policies []policy.Policy   `json:"policies" validate:"nonzero"`
}

// MappingRuleUpdate contains a MappingRuleConfig along with an ID for a mapping rule.
// The rule with that ID is meant to updated to the given config.
type MappingRuleUpdate struct {
	Config MappingRuleConfig `json:"config" validate:"nonzero"`
	Name   string            `json:"name" validate:"nonzero"`
}

// RollupRuleConfig is the parts of a rollup rule that an end user would care about.
// This struct is provided to create or make updates to the rollup rule.
type RollupRuleConfig struct {
	Name    string            `json:"name" validate:"nonzero"`
	Filters map[string]string `json:"filters" validate:"nonzero"`
	Targets []RollupTarget    `json:"targets" validate:"nonzero"`
}

// RollupRuleUpdate is a RollupRuleConfig along with an ID for a rollup rule.
// The rule with that ID is meant to updated with a given config.
type RollupRuleUpdate struct {
	Config RollupRuleConfig `json:"config" validate:"nonzero"`
	Name   string           `json:"name" validate:"nonzero"`
}

// RuleSetHandler exposes all of the methods to work with a RuleSet
type RuleSetHandler interface {
	// AppendMappingRule creates a new mapping rule and adds it to this ruleset.
	AddMappingRule(MappingRuleConfig) error

	// UpdateMappingRule creates a new mapping rule and adds it to this ruleset.
	UpdateMappingRule(MappingRuleUpdate) error

	// DeleteMappingRule deletes a mapping rule
	DeleteMappingRule(ruleName string) error

	// AppendRollupRule creates a new mapping rule and adds it to this ruleset.
	AddRollupRule(RollupRuleConfig) error

	// UpdateRollupRule creates a new mapping rule and adds it to this ruleset.
	UpdateRollupRule(RollupRuleUpdate) error

	// DeleteRollupRule deletes a rollup rule
	DeleteRollupRule(ruleName string) error

	// Tombstone tombstones this ruleset and all of its rules.
	Delete() error

	// Revive removes the tombstone from this ruleset. It does not revive any rules.
	Revive() error

	// Validate checks that this ruleset is good to use. e.g. not tombstoned
	Validate() error

	// Ruleset returns the latest state of the ruleset in this handler.
	RuleSet() RuleSet
}

// ruleSetHandler is a struct that all functionality should be defined on.
type ruleSetHandler struct {
	opts HandlerOptions
	rs   *ruleSet
}

func (h ruleSetHandler) RuleSet() RuleSet {
	return h.rs
}

func (h ruleSetHandler) AddMappingRule(mrc MappingRuleConfig) error {
	rs := h.rs
	m, err := rs.getMappingRuleByName(mrc.Name)
	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(h.opts.propagationDelay)
	if err != nil {
		if err != errNoSuchRule {
			return fmt.Errorf(ruleActionErrorFmt, "add", mrc.Name, err)
		}
		m, err := newMappingRuleFromFields(
			mrc.Name,
			mrc.Filters,
			mrc.Policies,
			cutoverTime,
			rs.tagsFilterOpts,
		)
		if err != nil {
			return fmt.Errorf(ruleActionErrorFmt, "add", mrc.Name, err)
		}
		rs.mappingRules = append(rs.mappingRules, m)
	} else {
		if err := m.revive(mrc.Name, mrc.Filters, mrc.Policies, cutoverTime, rs.tagsFilterOpts); err != nil {
			return fmt.Errorf(ruleActionErrorFmt, "revive", mrc.Name, err)
		}
	}

	rs.updateTimeStamps(updateTime, cutoverTime)
	return nil
}

func (h ruleSetHandler) UpdateMappingRule(mru MappingRuleUpdate) error {
	rs := h.rs
	m, err := rs.getMappingRuleByName(mru.Name)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", mru.Name, err)
	}

	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(h.opts.propagationDelay)
	mrc := mru.Config
	err = m.addSnapshot(mrc.Name, mrc.Filters, mrc.Policies, cutoverTime, rs.tagsFilterOpts)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", mru.Name, err)
	}
	rs.updateTimeStamps(updateTime, cutoverTime)
	return nil
}

func (h ruleSetHandler) DeleteMappingRule(name string) error {
	rs := h.rs
	m, err := rs.getMappingRuleByName(name)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", name, err)
	}
	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(h.opts.propagationDelay)
	if err := m.markTombstoned(cutoverTime); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", name, err)
	}
	rs.updateTimeStamps(updateTime, cutoverTime)
	return nil
}

func (h ruleSetHandler) AddRollupRule(rrc RollupRuleConfig) error {
	rs := h.rs
	r, err := rs.getRollupRuleByName(rrc.Name)
	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(h.opts.propagationDelay)
	if err != nil {
		if err != errNoSuchRule {
			return fmt.Errorf(ruleActionErrorFmt, "add", rrc.Name, err)
		}
		r, err = newRollupRuleFromFields(rrc.Name, rrc.Filters, rrc.Targets, cutoverTime, rs.tagsFilterOpts)
		if err != nil {
			return fmt.Errorf(ruleActionErrorFmt, "add", rrc.Name, err)
		}
		rs.rollupRules = append(rs.rollupRules, r)
	} else {
		if err := r.revive(rrc.Name, rrc.Filters, rrc.Targets, cutoverTime, rs.tagsFilterOpts); err != nil {
			return fmt.Errorf(ruleActionErrorFmt, "revive", rrc.Name, err)
		}
	}
	rs.updateTimeStamps(updateTime, cutoverTime)
	return nil
}

func (h ruleSetHandler) UpdateRollupRule(rru RollupRuleUpdate) error {
	rs := h.rs
	r, err := rs.getRollupRuleByName(rru.Name)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", rru.Name, err)
	}
	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(h.opts.propagationDelay)
	rrc := rru.Config
	err = r.addSnapshot(rrc.Name, rrc.Filters, rrc.Targets, cutoverTime, rs.tagsFilterOpts)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", rru.Name, err)
	}
	rs.updateTimeStamps(updateTime, cutoverTime)
	return nil
}

func (h ruleSetHandler) DeleteRollupRule(name string) error {
	rs := h.rs
	r, err := rs.getRollupRuleByName(name)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", name, err)
	}
	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(h.opts.propagationDelay)
	if err := r.markTombstoned(cutoverTime); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", name, err)
	}
	return nil
}

func (h ruleSetHandler) Delete() error {
	rs := h.rs
	if rs.tombstoned {
		return fmt.Errorf("%s is already tombstoned", string(rs.namespace))
	}

	rs.tombstoned = true
	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(h.opts.propagationDelay)
	rs.updateTimeStamps(updateTime, cutoverTime)

	// Make sure that all of the rules in the ruleset are tombstoned as well.
	for _, m := range rs.mappingRules {
		if t := m.Tombstoned(); !t {
			m.markTombstoned(cutoverTime)
		}
	}

	for _, r := range rs.rollupRules {
		if t := r.Tombstoned(); !t {
			r.markTombstoned(cutoverTime)
		}
	}

	return nil
}

func (h ruleSetHandler) Revive() error {
	rs := h.rs
	if !rs.Tombstoned() {
		return fmt.Errorf(ruleSetActionErrorFmt, "revive", string(rs.namespace), errNotTombstoned)
	}

	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(h.opts.propagationDelay)
	rs.tombstoned = false
	rs.updateTimeStamps(updateTime, cutoverTime)
	return nil
}

func (h ruleSetHandler) Validate() error {
	rs := h.rs
	if rs.Tombstoned() {
		return fmt.Errorf("ruleset %s is tombstoned", rs.namespace)
	}
	return nil
}

// NamespacesHandler exposes all of the methods to work with Namespaces
type NamespacesHandler interface {
	// Adds a namespace
	AddNamespace(string) error

	// Delete a namespace
	DeleteNamespace(string, int) error

	// Validate checks that this ruleset is good to use. E.i. not tombstoned
	ValidateNamespace(string) error

	// Namespaces fetches the current state of the namespaces object in the handler.
	Namespaces() *Namespaces
}

// Handler is a struct that all functionality should be defined on.
type namespacesHandler struct {
	namespaces *Namespaces
}

func (h namespacesHandler) Namespaces() *Namespaces {
	return h.namespaces
}

// ValidateNamespace validates whether a given namespace exists.
func (h namespacesHandler) ValidateNamespace(nsName string) error {
	ns, err := h.namespaces.Namespace(nsName)
	if err != nil {
		return err
	}

	if len(ns.Snapshots()) == 0 {
		return errNoSnapshots
	}

	if ns.Tombstoned() {
		return errNamespaceTombstoned
	}
	return nil
}

// AddNamespace adds a new namespace to the namespaces structure and persists it
func (h namespacesHandler) AddNamespace(nsName string) error {
	nss := h.namespaces
	existing, err := nss.Namespace(nsName)
	if err != nil {
		if err != errNamespaceNotFound {
			return fmt.Errorf(namespaceActionErrorFmt, "add", nsName, err)
		}
	}

	// Brand new namespace
	if err == errNamespaceNotFound {
		ns := Namespace{
			name: []byte(nsName),
			snapshots: []NamespaceSnapshot{
				NamespaceSnapshot{
					forRuleSetVersion: 1,
					tombstoned:        false,
				},
			},
		}

		nss.namespaces = append(nss.namespaces, ns)
		return nil
	}

	// Revive the namespace
	if err = existing.revive(); err != nil {
		return fmt.Errorf(namespaceActionErrorFmt, "revive", nsName, err)
	}

	return nil
}

// DeleteNamespace tombstones the given namespace mapping it to the given RuleSet version + 1
func (h namespacesHandler) DeleteNamespace(nsName string, forRuleSetVersion int) error {
	nss := h.namespaces
	existing, err := nss.Namespace(nsName)
	if err != nil {
		return fmt.Errorf(namespaceActionErrorFmt, "delete", nsName, err)
	}

	if err := existing.markTombstoned(forRuleSetVersion + 1); err != nil {
		return fmt.Errorf(namespaceActionErrorFmt, "delete", nsName, err)
	}

	return nil
}
