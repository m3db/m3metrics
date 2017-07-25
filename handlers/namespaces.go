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

package handlers

import (
	"fmt"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules"
)

var (
	errNoNameGiven         = fmt.Errorf("no name provided")
	errNoSuchNamespace     = fmt.Errorf("no such namespace")
	errNoSnapshots         = fmt.Errorf("namespace has no snapshots")
	errNamespaceTombstoned = fmt.Errorf("namespace is tombstoned")
)

// Namespaces returns the version and the persisted namespaces in kv store.
func (h Handler) Namespaces() (*rules.Namespaces, error) {
	value, err := h.store.Get(h.opts.NamespacesKey)
	if err != nil {
		return nil, err
	}

	version := value.Version()
	var namespaces schema.Namespaces
	if err := value.Unmarshal(&namespaces); err != nil {
		return nil, err
	}

	nss, err := rules.NewNamespaces(version, &namespaces)
	if err != nil {
		return nil, err
	}
	return &nss, err
}

// ValidateNamespace validates whether a given namespace exists.
func (h Handler) ValidateNamespace(ns *rules.Namespace) error {
	if len(ns.Snapshots()) == 0 {
		return errNoSnapshots
	}
	if ns.Tombstoned() {
		return errNamespaceTombstoned
	}
	return nil
}

// AddNamespace adds a new namespace to the namespaces structure and persists it
func (h Handler) AddNamespace(nss *rules.Namespaces, nsName string) (rules.RuleSet, error) {
	if len(nsName) == 0 {
		return nil, errNoNameGiven
	}

	err := nss.AddNamespace(nsName)
	if err != nil {
		return nil, err
	}

	rs, err := h.initRuleSet(nsName)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// DeleteNamespace adds a new namespace to the namespaces structure and persists it
func (h Handler) DeleteNamespace(nss *rules.Namespaces, nsName string) (rules.RuleSet, error) {
	rs, err := h.RuleSet(nsName)
	if err != nil {
		return nil, err
	}

	if err := rs.Tombstone(h.opts.PropagationDelay); err != nil {
		return nil, fmt.Errorf("Could not tombstone Ruleset: %s. %v", nsName, err)
	}

	if err := nss.DeleteNamespace(nsName, rs.Version()); err != nil {
		return nil, err
	}

	return rs, nil
}
