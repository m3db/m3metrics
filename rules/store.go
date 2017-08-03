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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
)

var (
	errNilRuleSet    = errors.New("nil ruleset")
	errNilNamespaces = errors.New("nil namespaces")
)

// Store facilitates read/write operations to the backing kv store.
type Store interface {
	// Write saves the given ruleset and optional namespace to the backing store.
	// If saveNamespace is false, will only use the namesapces state to ensure that
	// the RuleSet is still valid.
	Write(RuleSet, *Namespaces, bool) error

	// ReadNamespaces returns the version and the persisted namespaces in kv store.
	ReadNamespaces() (*Namespaces, error)

	// ReadRuleSet returns the version and the persisted ruleset data in kv store.
	ReadRuleSet(namespaceName string) (RuleSet, error)

	// RuleSetKey generates a ruleset key for a given namespace
	ruleSetKey(namespaceName string) string
}

// StoreOptions is a configuration for the rules r/w store.
type StoreOptions struct {
	NamespacesKey string
	RuleSetKeyFmt string
}

type store struct {
	kvStore kv.TxnStore
	opts    StoreOptions
}

// NewStoreOptions creates a new store options struct.
func NewStoreOptions(namespacesKey string, rulesetKeyFmt string) StoreOptions {
	return StoreOptions{NamespacesKey: namespacesKey, RuleSetKeyFmt: rulesetKeyFmt}
}

// NewStore creates a new Store.
func NewStore(kvStore kv.TxnStore, opts StoreOptions) Store {
	return store{kvStore: kvStore, opts: opts}
}

func (s store) ReadNamespaces() (*Namespaces, error) {
	value, err := s.kvStore.Get(s.opts.NamespacesKey)
	if err != nil {
		return nil, err
	}

	version := value.Version()
	var namespaces schema.Namespaces
	if err := value.Unmarshal(&namespaces); err != nil {
		return nil, err
	}

	nss, err := NewNamespaces(version, &namespaces)
	if err != nil {
		return nil, err
	}
	return &nss, err
}

func (s store) ruleSetKey(ns string) string {
	return fmt.Sprintf(s.opts.RuleSetKeyFmt, ns)
}

func (s store) ReadRuleSet(nsName string) (RuleSet, error) {
	ruleSetKey := s.ruleSetKey(nsName)
	value, err := s.kvStore.Get(ruleSetKey)

	if err != nil {
		return nil, err
	}

	version := value.Version()
	var ruleSet schema.RuleSet
	if err := value.Unmarshal(&ruleSet); err != nil {
		return nil, fmt.Errorf("Could not fetch RuleSet %s: %v", nsName, err.Error())
	}
	rs, err := NewRuleSetFromSchema(version, &ruleSet, NewOptions())
	if err != nil {
		return nil, fmt.Errorf("Could not fetch RuleSet %s: %v", nsName, err.Error())
	}
	return rs, err
}

func (s store) Write(
	rs RuleSet,
	nss *Namespaces,
	saveNamespace bool,
) error {
	if rs == nil {
		return errNilRuleSet
	}

	if nss == nil {
		return errNilNamespaces
	}
	ruleSetKey := s.ruleSetKey(string(rs.Namespace()))

	namespacesKey := s.opts.NamespacesKey
	rsSchema, err := rs.Schema()
	if err != nil {
		return err
	}

	namespacesCond := kv.NewCondition().
		SetKey(namespacesKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(nss.Version())

	ruleSetCond := kv.NewCondition().
		SetKey(ruleSetKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(rs.Version())

	conditions := []kv.Condition{
		namespacesCond,
		ruleSetCond,
	}

	ops := []kv.Op{
		kv.NewSetOp(ruleSetKey, rsSchema),
	}
	if saveNamespace {
		nssSchema, err := nss.Schema()
		if err != nil {
			return err
		}
		ops = append(ops, kv.NewSetOp(namespacesKey, nssSchema))
	}

	if _, err := s.kvStore.Commit(conditions, ops); err != nil {
		return err
	}

	return nil
}
