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
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules"
	"github.com/pborman/uuid"
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
	ns, err := rules.NewNamespaces(version, &namespaces)
	if err != nil {
		return nil, err
	}
	return &ns, err
}

// ValidateNamespace validates whether a given namespace exists.
func (h Handler) ValidateNamespace(namespaces *rules.Namespaces, namespaceName string) error {
	ns, err := namespaces.Namespace(namespaceName)
	if err != nil {
		return fmt.Errorf("error finding namespace with name %s: %v", namespaceName, err)
	}
	if ns == nil {
		return fmt.Errorf("namespace %s doesn't exist", namespaceName)
	}
	if len(ns.Snapshots()) == 0 {
		return fmt.Errorf("namespace %s has no snapshots", namespaceName)
	}
	if ns.Tombstoned() {
		return fmt.Errorf("namespace %s is tombstoned", namespaceName)
	}
	return nil
}

// AddNamespace adds a new namespace to the namespaces structure and persists it
func (h Handler) AddNamespace(namespaces *rules.Namespaces, namespaceName string) error {
	if err := namespaces.AddNamespace(namespaceName); err != nil {
		return err
	}

	now := time.Now().UnixNano()
	rs, err := rules.NewRuleSet(1, &schema.RuleSet{
		Uuid:          uuid.New(),
		Namespace:     namespaceName,
		CreatedAt:     now,
		LastUpdatedAt: now,
		Tombstoned:    false,
		CutoverTime:   now,
	}, rules.NewOptions())
	if err != nil {
		return err
	}

	if err := h.persistRuleSet(rs, namespaces); err != nil {
		return err
	}
	return nil
}

func (h Handler) persistNamespaces(nss *rules.Namespaces) error {
	nssSchema, err := nss.Schema()
	if err != nil {
		return err
	}

	namespacesCond := kv.NewCondition().
		SetKey(h.opts.NamespacesKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(nss.Version)

	conditions := []kv.Condition{
		namespacesCond,
	}

	ops := []kv.Op{
		kv.NewSetOp(h.opts.NamespacesKey, nssSchema),
	}

	if _, err := h.store.Commit(conditions, ops); err != nil {
		return err
	}

	return nil
}

// DeleteNamespace adds a new namespace to the namespaces structure and persists it
func (h Handler) DeleteNamespace(nss *rules.Namespaces, namespaceName string) error {
	if err := nss.DeleteNamespace(namespaceName); err != nil {
		return err
	}

	if err := h.persistNamespaces(nss); err != nil {
		return err
	}

	return nil
}
