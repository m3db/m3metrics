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

package rules

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
)

var (
	emptyNamespaceSnapshot NamespaceSnapshot
	emptyNamespace         Namespace
	emptyNamespaces        Namespaces

	errNilNamespaceSnapshotSchema = errors.New("nil namespace snapshot schema")
	errNilNamespaceSchema         = errors.New("nil namespace schema")
	errNilNamespacesSchema        = errors.New("nil namespaces schema")
	errNilNamespaceSnapshot       = errors.New("nil namespace snapshot")
	errMultipleNamespaceMatches   = errors.New("more than one namespace match found")
	errNamespaceExists            = errors.New("namespace already exists")
	errNoNamespaceSnapshots       = errors.New("namespace has no snapshots")
)

// NamespaceSnapshot defines a namespace snapshot for which rules are defined.
type NamespaceSnapshot struct {
	forRuleSetVersion int
	tombstoned        bool
}

func newNamespaceSnapshot(snapshot *schema.NamespaceSnapshot) (NamespaceSnapshot, error) {
	if snapshot == nil {
		return emptyNamespaceSnapshot, errNilNamespaceSnapshotSchema
	}
	return NamespaceSnapshot{
		forRuleSetVersion: int(snapshot.ForRulesetVersion),
		tombstoned:        snapshot.Tombstoned,
	}, nil
}

type namespaceSnapshotJSON struct {
	ForRuleSetVersion int  `json:"forRuleSetVersion"`
	Tombstoned        bool `json:"tombstoned"`
}

func newNamespaceSnapshotJSON(s NamespaceSnapshot) namespaceSnapshotJSON {
	return namespaceSnapshotJSON{ForRuleSetVersion: s.forRuleSetVersion, Tombstoned: s.tombstoned}
}

// MarshalJSON returns the JSON encoding of Namespaces.
func (s NamespaceSnapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(newNamespaceSnapshotJSON(s))
}

// UnmarshalJSON unmarshals JSON-encoded data into a Namespace.
func (s *NamespaceSnapshot) UnmarshalJSON(data []byte) error {
	var nsj namespaceSnapshotJSON
	err := json.Unmarshal(data, &nsj)
	if err != nil {
		return err
	}
	*s = *nsj.NamespaceSnapshot()
	return nil
}

func (sj namespaceSnapshotJSON) NamespaceSnapshot() *NamespaceSnapshot {
	return &NamespaceSnapshot{forRuleSetVersion: sj.ForRuleSetVersion, tombstoned: sj.Tombstoned}
}

// ForRuleSetVersion is the ruleset version this namespace change is related to.
func (s NamespaceSnapshot) ForRuleSetVersion() int { return s.forRuleSetVersion }

// Tombstoned determines whether the namespace has been tombstoned.
func (s NamespaceSnapshot) Tombstoned() bool { return s.tombstoned }

// Schema returns the given Namespace in protobuf form
func (s NamespaceSnapshot) Schema() *schema.NamespaceSnapshot {
	return &schema.NamespaceSnapshot{
		ForRulesetVersion: int32(s.forRuleSetVersion),
		Tombstoned:        s.tombstoned,
	}
}

// Namespace stores namespace snapshots.
type Namespace struct {
	name      []byte
	snapshots []NamespaceSnapshot
}

// newNameSpace creates a new namespace.
func newNameSpace(namespace *schema.Namespace) (Namespace, error) {
	if namespace == nil {
		return emptyNamespace, errNilNamespaceSchema
	}
	snapshots := make([]NamespaceSnapshot, 0, len(namespace.Snapshots))
	for _, snapshot := range namespace.Snapshots {
		s, err := newNamespaceSnapshot(snapshot)
		if err != nil {
			return emptyNamespace, err
		}
		snapshots = append(snapshots, s)
	}
	return Namespace{
		name:      []byte(namespace.Name),
		snapshots: snapshots,
	}, nil
}

type namespaceJSON struct {
	Name      string              `json:"name"`
	Snapshots []NamespaceSnapshot `json:"snapshots"`
}

func newNamespaceJSON(n Namespace) namespaceJSON {
	return namespaceJSON{Name: string(n.name), Snapshots: n.snapshots}
}

// MarshalJSON returns the JSON encoding of Namespaces.
func (n Namespace) MarshalJSON() ([]byte, error) {
	return json.Marshal(newNamespaceJSON(n))
}

// UnmarshalJSON unmarshals JSON-encoded data into a Namespace.
func (n *Namespace) UnmarshalJSON(data []byte) error {
	var nsj namespaceJSON
	err := json.Unmarshal(data, &nsj)
	if err != nil {
		return err
	}
	*n = *nsj.Namespace()
	return nil
}

func (nsj namespaceJSON) Namespace() *Namespace {
	return &Namespace{name: []byte(nsj.Name), snapshots: nsj.Snapshots}
}

// Name is the name of the namespace.
func (n Namespace) Name() []byte { return n.name }

// Snapshots return the namespace snapshots.
func (n Namespace) Snapshots() []NamespaceSnapshot { return n.snapshots }

// Schema returns the given Namespace in protobuf form
func (n Namespace) Schema() (*schema.Namespace, error) {
	if n.snapshots == nil {
		return nil, errNilNamespaceSnapshot
	}

	res := &schema.Namespace{
		Name: string(n.name),
	}

	snapshots := make([]*schema.NamespaceSnapshot, len(n.snapshots))
	for i, s := range n.snapshots {
		snapshots[i] = s.Schema()
	}
	res.Snapshots = snapshots

	return res, nil
}

// Tombstone ...
func (n *Namespace) tombstone(tombstonedRSVersion int) error {
	if n.Tombstoned() {
		return fmt.Errorf("%s is already tombstoned", string(n.name))
	}
	snapshot := NamespaceSnapshot{tombstoned: true, forRuleSetVersion: tombstonedRSVersion}
	n.snapshots = append(n.snapshots, snapshot)
	return nil
}

func (n *Namespace) revive() error {
	if !n.Tombstoned() {
		return fmt.Errorf("cannot revive %s. It is not tombstoned", string(n.name))
	}
	v := n.snapshots[len(n.snapshots)-1].forRuleSetVersion
	snapshot := NamespaceSnapshot{tombstoned: false, forRuleSetVersion: v}
	n.snapshots = append(n.snapshots, snapshot)
	return nil
}

// Tombstoned returns the tombstoned state for a given namespace.
func (n Namespace) Tombstoned() bool {
	if len(n.snapshots) == 0 {
		return true
	}
	return n.snapshots[len(n.snapshots)-1].tombstoned
}

// Namespaces store the list of namespaces for which rules are defined.
type Namespaces struct {
	version    int
	namespaces []Namespace
}

// NewNamespaces creates new namespaces.
func NewNamespaces(version int, namespaces *schema.Namespaces) (Namespaces, error) {
	if namespaces == nil {
		return emptyNamespaces, errNilNamespacesSchema
	}
	nss := make([]Namespace, 0, len(namespaces.Namespaces))
	for _, namespace := range namespaces.Namespaces {
		ns, err := newNameSpace(namespace)
		if err != nil {
			return emptyNamespaces, err
		}
		nss = append(nss, ns)
	}
	return Namespaces{
		version:    version,
		namespaces: nss,
	}, nil
}

type namespacesJSON struct {
	Version    int         `json:"version"`
	Namespaces []Namespace `json:"namespaces"`
}

func newNamespacesJSON(nss Namespaces) namespacesJSON {
	return namespacesJSON{Version: nss.version, Namespaces: nss.namespaces}
}

// MarshalJSON returns the JSON encoding of Namespaces.
func (nss Namespaces) MarshalJSON() ([]byte, error) {
	return json.Marshal(newNamespacesJSON(nss))
}

// UnmarshalJSON unmarshals JSON-encoded data into a Namespace.
func (nss *Namespaces) UnmarshalJSON(data []byte) error {
	var nsj namespacesJSON
	err := json.Unmarshal(data, &nsj)
	if err != nil {
		return err
	}
	*nss = *nsj.NewNamespaces()
	return nil
}

// Calling this NewNamespaces to not
func (nsj namespacesJSON) NewNamespaces() *Namespaces {
	return &Namespaces{version: nsj.Version, namespaces: nsj.Namespaces}
}

// Version returns the namespaces version.
func (nss Namespaces) Version() int { return nss.version }

// Namespaces returns the list of namespaces.
func (nss Namespaces) Namespaces() []Namespace { return nss.namespaces }

// Schema returns the given Namespaces slice in protobuf form.
func (nss Namespaces) Schema() (*schema.Namespaces, error) {
	res := &schema.Namespaces{}

	namespaces := make([]*schema.Namespace, len(nss.namespaces))
	for i, n := range nss.namespaces {
		namespace, err := n.Schema()
		if err != nil {
			return nil, err
		}
		namespaces[i] = namespace
	}
	res.Namespaces = namespaces

	return res, nil
}

// Namespace ...
func (nss *Namespaces) Namespace(name string) (*Namespace, error) {
	var res *Namespace
	if len(nss.namespaces) == 0 {
		return nil, kv.ErrNotFound
	}

	for i, ns := range nss.namespaces {
		if string(ns.name) != name {
			continue
		}

		if res == nil {
			res = &nss.namespaces[i]
		} else {
			return nil, errMultipleNamespaceMatches
		}
	}

	if res == nil {
		return nil, kv.ErrNotFound
	}

	return res, nil
}

// AddNamespace adds a blank namespace to the namespaces object
func (nss *Namespaces) AddNamespace(name string) error {
	existing, err := nss.Namespace(name)

	if err != nil {
		if err != kv.ErrNotFound {
			return fmt.Errorf("cannot add namespace %s. %v", name, err)
		}
	}

	// Brand new namespace
	if err == kv.ErrNotFound {
		ns := Namespace{
			name: []byte(name),
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
		return err
	}

	return nil
}

// DeleteNamespace tombstones a Namesapce as well as its cooresponding ruleset
func (nss *Namespaces) DeleteNamespace(nsName string, rsVersion int) error {
	ns, err := nss.Namespace(nsName)
	if err != nil {
		return fmt.Errorf("cannot delete %s. %v", nsName, err)
	}

	// The expected rule set version is the one before the ruleset gets tombstoned.
	// There is an expectation that the RuleSet associated with this namespace will get
	// deleted as well.
	if err := ns.tombstone(rsVersion + 1); err != nil {
		return err
	}
	return nil
}
