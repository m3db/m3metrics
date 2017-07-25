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
	"testing"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules"
	"github.com/stretchr/testify/require"
)

var (
	testNamespaces = &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "fooNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        false,
					},
				},
			},
			&schema.Namespace{
				Name: "barNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        true,
					},
				},
			},
		},
	}

	badNamespaces = &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{Name: "fooNs", Snapshots: nil},
		},
	}
)

func bootstrap() (*Handler, *rules.Namespaces, error) {
	h := testHandler()
	h.store.Set(testNamespaceKey, testNamespaces)
	nss, err := h.Namespaces()
	return h, nss, err
}

func TestNamespaces(t *testing.T) {
	_, nss, err := bootstrap()
	require.NoError(t, err)
	require.NotNil(t, nss.Namespaces)
}

func TestNamespacesError(t *testing.T) {
	h := testHandler()
	h.store.Set(testNamespaceKey, &schema.RollupRule{Uuid: "x"})
	nss, err := h.Namespaces()
	require.Error(t, err)
	require.Nil(t, nss)
}

func TestValidateNamespace(t *testing.T) {
	h, nss, err := bootstrap()
	require.NoError(t, err)
	ns, _ := nss.Namespace("fooNs")
	err = h.ValidateNamespace(ns)
	require.NoError(t, err)
}

func TestValidateNamespaceTombstoned(t *testing.T) {
	h, nss, _ := bootstrap()
	ns, _ := nss.Namespace("barNs")
	err := h.ValidateNamespace(ns)
	require.Error(t, err)
}

func TestValidateNamespaceNoSnaps(t *testing.T) {
	h, _, _ := bootstrap()
	h.store.Set(testNamespaceKey, badNamespaces)
	nss, err := h.Namespaces()
	require.NoError(t, err)
	ns, err := nss.Namespace("fooNs")
	require.NoError(t, err)
	err = h.ValidateNamespace(ns)
	require.Error(t, err)
}

func TestAddNamespace(t *testing.T) {
	h, nss, err := bootstrap()
	require.NoError(t, err)
	require.NotNil(t, nss)

	ns, err := nss.Namespace("testNs")
	require.Error(t, err)
	require.Nil(t, ns)

	rs, err := h.AddNamespace(nss, "testNs")
	require.NoError(t, err)

	writtenNs, err := nss.Namespace("testNs")
	require.NoError(t, err)

	h.Persist(rs, nss, true)

	nss, err = h.Namespaces()
	require.NoError(t, err)
	require.NotNil(t, nss)

	persistedNs, err := nss.Namespace("testNs")
	require.Equal(t, writtenNs, persistedNs)
}

func TestAddNamespaceBadInput(t *testing.T) {
	h, nss, _ := bootstrap()

	_, err := h.AddNamespace(nss, "")
	require.EqualError(t, err, errNoNameGiven.Error())
}

func TestAddNamespaceDuplicate(t *testing.T) {
	h, nss, _ := bootstrap()
	_, err := h.AddNamespace(nss, "fooNs")
	require.Error(t, err)
}

func TestRevive(t *testing.T) {
	h, nss, _ := bootstrap()
	h.store.Set(h.RuleSetKey("fooNs"), testRuleSet)
	rs, err := h.DeleteNamespace(nss, "fooNs")
	require.NoError(t, err)
	h.Persist(rs, nss, true)

	nss, err = h.Namespaces()
	require.NoError(t, err)
	ns, err := nss.Namespace("fooNs")
	rs, err = h.RuleSet("fooNs")
	require.NotNil(t, ns)
	require.NoError(t, err)

	require.True(t, ns.Tombstoned())
	require.True(t, rs.Tombstoned())

	rs, err = h.AddNamespace(nss, "fooNs")
	require.NoError(t, err)
	h.Persist(rs, nss, true)

	nss, err = h.Namespaces()
	ns, err = nss.Namespace("fooNs")
	rs, err = h.RuleSet("fooNs")
	require.False(t, ns.Tombstoned())
	require.False(t, rs.Tombstoned())
}

func TestAddNamespaceSerial(t *testing.T) {
	h, nss, _ := bootstrap()

	// Add + Save testNs
	rs, err := h.AddNamespace(nss, "testNs")
	require.NoError(t, err)
	err = h.Persist(rs, nss, true)
	require.NoError(t, err)

	// Get Latest state + Add + Save testNs2
	nss, err = h.Namespaces()
	require.NoError(t, err)
	rs, err = h.AddNamespace(nss, "testNs2")
	require.NoError(t, err)
	err = h.Persist(rs, nss, true)
	require.NoError(t, err)

	nss, err = h.Namespaces()
	require.NoError(t, err)
	_, err = nss.Namespace("testNs")
	require.NoError(t, err)
	_, err = nss.Namespace("testNs2")
	require.NoError(t, err)
}

func TestAddNamespaceParallel(t *testing.T) {
	h, nss, _ := bootstrap()
	nss2, err := h.Namespaces()
	require.NoError(t, err)

	rs, err := h.AddNamespace(nss, "testNs")
	require.NoError(t, err)
	err = h.Persist(rs, nss, true)
	require.NoError(t, err)

	// In this case the version condition check fails because the namespaces start the same.
	rs2, err := h.AddNamespace(nss2, "testNs2")
	err = h.Persist(rs2, nss2, true)
	require.Error(t, err)

	nss, err = h.Namespaces()
	require.NoError(t, err)
	_, err = nss.Namespace("testNs")
	require.NoError(t, err)
	_, err = nss.Namespace("testNs2")
	require.EqualError(t, err, kv.ErrNotFound.Error())
}

func TestDeleteNamespace(t *testing.T) {
	h, nss, _ := bootstrap()
	h.store.Set(h.RuleSetKey("fooNs"), testRuleSet)

	rs, err := h.DeleteNamespace(nss, "fooNs")
	require.NoError(t, err)
	ns, err := nss.Namespace("fooNs")
	require.NoError(t, err)
	require.True(t, ns.Tombstoned())
	err = h.Persist(rs, nss, true)

	rs, err = h.RuleSet("fooNs")
	require.NoError(t, err)
	require.True(t, rs.Tombstoned())

	nss, err = h.Namespaces()
	require.NoError(t, err)

	ns, err = nss.Namespace("fooNs")
	require.NoError(t, err)
	require.True(t, ns.Tombstoned())
}

func TestDeleteNamespaceNoRuleSet(t *testing.T) {
	h, nss, _ := bootstrap()
	// Nothing there
	_, err := h.DeleteNamespace(nss, "blah")
	require.Error(t, err)

	// No Namespace
	h.store.Set(h.RuleSetKey("blah"), testRuleSet)
	_, err = h.DeleteNamespace(nss, "blah")
	require.Error(t, err)

	// No ruleset
	_, err = h.DeleteNamespace(nss, "fooNs")
	require.Error(t, err)
}

func TestDeleteNamespaceDeleteTwice(t *testing.T) {
	h, nss, _ := bootstrap()
	h.store.Set(h.RuleSetKey("fooNs"), testRuleSet)
	_, err := h.DeleteNamespace(nss, "fooNs")
	require.NoError(t, err)

	_, err = h.DeleteNamespace(nss, "blah")
	require.Error(t, err)
}
