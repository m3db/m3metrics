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
	"github.com/pborman/uuid"
)

// Namespaces returns the version ad the persisted namespaces in kv store.
func Namespaces(store kv.Store, namespacesKey string) (int, *schema.Namespaces, error) {
	value, err := store.Get(namespacesKey)
	if err != nil {
		return 0, nil, err
	}

	version := value.Version()
	var namespaces schema.Namespaces
	if err := value.Unmarshal(&namespaces); err != nil {
		return 0, nil, err
	}

	return version, &namespaces, nil
}

// ValidateNamespace validates whether a given namespace exists.
func ValidateNamespace(store kv.Store, namespacesKey string, namespaceName string) (int, *schema.Namespaces, *schema.Namespace, error) {
	namespacesVersion, namespaces, err := Namespaces(store, namespacesKey)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("could not read namespaces data: %v", err)
	}
	ns, err := Namespace(namespaces, namespaceName)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("error finding namespace with name %s: %v", namespaceName, err)
	}
	if ns == nil {
		return 0, nil, nil, fmt.Errorf("namespace %s doesn't exist", namespaceName)
	}
	if len(ns.Snapshots) == 0 {
		return 0, nil, nil, fmt.Errorf("namespace %s has no snapshots", namespaceName)
	}
	if ns.Snapshots[len(ns.Snapshots)-1].Tombstoned {
		return 0, nil, nil, fmt.Errorf("namespace %s is tombstoned", namespaceName)
	}
	return namespacesVersion, namespaces, ns, nil
}

// Namespace returns the namespace with a given name, or an error if there are
// multiple matches.
func Namespace(namespaces *schema.Namespaces, namespaceName string) (*schema.Namespace, error) {
	var namespace *schema.Namespace
	for _, ns := range namespaces.Namespaces {
		if ns.Name != namespaceName {
			continue
		}
		if namespace == nil {
			namespace = ns
		} else {
			return nil, errMultipleMatches
		}
	}

	if namespace == nil {
		return nil, kv.ErrNotFound
	}

	return namespace, nil
}

// CreateNamespace creates a blank namespace with a given name.
func CreateNamespace(store kv.TxnStore,
	namespaceKey, namespaceName, ruleSetKey string,
	propDelay time.Duration) error {
	// Validate namespace doesn't exist.
	namespacesVersion, namespaces, err := Namespaces(store, namespaceKey)
	if err != nil {
		return fmt.Errorf("could not read namespaces data: %v", err)
	}

	namespace, err := Namespace(namespaces, namespaceName)
	if err != nil {
		if err == errMultipleMatches {
			return kv.ErrAlreadyExists
		} else if err != kv.ErrNotFound {
			return err
		}
	}

	if namespace != nil {
		numSnapshots := len(namespace.Snapshots)
		if numSnapshots > 0 && !namespace.Snapshots[numSnapshots-1].Tombstoned {
			return kv.ErrAlreadyExists
		}
	}

	// Read the existing rules for the target service in case this is
	// a resurrected service.
	ruleSetVersion, ruleSet, err := RuleSet(store, ruleSetKey)
	if err != nil && err != kv.ErrNotFound {
		return fmt.Errorf("could not read ruleSet data for namespace %s: %v", namespaceName, err)
	}

	// Mark the ruleset alive.
	nowNs := time.Now().UnixNano()
	if err == kv.ErrNotFound {
		ruleSet = &schema.RuleSet{
			Uuid:          uuid.New(),
			Namespace:     namespaceName,
			CreatedAt:     nowNs,
			LastUpdatedAt: nowNs,
			Tombstoned:    false,
			CutoverTime:   NewCutoverNs(nowNs, 0, propDelay),
		}
	} else {
		if !ruleSet.Tombstoned {
			return fmt.Errorf("namespace %s does not exist or has been tombstoned but its ruleset is alive", namespaceName)
		}
		ruleSet.LastUpdatedAt = nowNs
		ruleSet.Tombstoned = false
		ruleSet.CutoverTime = NewCutoverNs(nowNs, ruleSet.CutoverTime, propDelay)
	}

	// Add the namespace to the list of registered namespaces.
	snapshot := &schema.NamespaceSnapshot{
		ForRulesetVersion: int32(ruleSetVersion + 1),
		Tombstoned:        false,
	}
	if namespace == nil {
		namespace = &schema.Namespace{
			Name:      namespaceName,
			Snapshots: []*schema.NamespaceSnapshot{snapshot},
		}
		namespaces.Namespaces = append(namespaces.Namespaces, namespace)
	} else {
		namespace.Snapshots = append(namespace.Snapshots, snapshot)
	}

	// Perform a transaction and only update if the services version and ruleSet
	// version were unchanged.
	servicesCond := kv.NewCondition().
		SetKey(namespaceKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(namespacesVersion)
	ruleSetCond := kv.NewCondition().
		SetKey(ruleSetKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(ruleSetVersion)
	conditions := []kv.Condition{
		servicesCond,
		ruleSetCond,
	}
	ops := []kv.Op{
		kv.NewSetOp(namespaceKey, namespaces),
		kv.NewSetOp(ruleSetKey, ruleSet),
	}
	if _, err := store.Commit(conditions, ops); err != nil {
		return fmt.Errorf("unable to update kv store: %v", err)
	}
	return nil
}
