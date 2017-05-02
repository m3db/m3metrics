package policy

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	compressorPolicies = CompressionMap{
		Policy{
			resolution: Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			retention:  Retention(6 * time.Hour),
		}: 1,
		Policy{
			resolution: Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			retention:  Retention(2 * time.Hour),
		}: 2,
		Policy{
			resolution: Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			retention:  Retention(12 * time.Hour),
		}: 3,
		Policy{
			resolution: Resolution{Window: 5 * time.Minute, Precision: xtime.Minute},
			retention:  Retention(48 * time.Hour),
		}: 4,
		Policy{
			resolution: Resolution{Window: time.Minute, Precision: xtime.Minute},
			retention:  Retention(time.Hour),
		}: 5,
		Policy{
			resolution: Resolution{Window: time.Minute, Precision: xtime.Minute},
			retention:  Retention(24 * time.Hour),
		}: 6,
	}
)

func TestNewCompressionMap(t *testing.T) {
	sampleSchemaPolicy := &schema.Policy{
		Resolution: &schema.Resolution{WindowSize: int64(10 * time.Second), Precision: int64(time.Second)},
		Retention:  &schema.Retention{Period: int64(2 * time.Hour)},
	}
	samplePolicy, _ := NewPolicyFromSchema(sampleSchemaPolicy)

	tests := []struct {
		policies    []*schema.CompressedPolicy
		expectedErr bool
		expectedMap CompressionMap
	}{
		// Test case for when policies is nil.
		{nil, true, nil},
		// Test case for when a compressed policy is nil.
		{[]*schema.CompressedPolicy{nil}, true, nil},
		// Test case for when an internal policy is nil.
		{[]*schema.CompressedPolicy{&schema.CompressedPolicy{Policy: nil}}, true, nil},
		// Test case for valid policies.
		{
			[]*schema.CompressedPolicy{
				&schema.CompressedPolicy{
					Policy: sampleSchemaPolicy,
					Id:     1,
				},
			},
			false,
			CompressionMap{samplePolicy: 1},
		},
	}

	for _, test := range tests {
		actualMap, actualErr := NewCompressionMap(test.policies)
		if test.expectedErr {
			require.Error(t, actualErr)
			continue
		}
		require.NoError(t, actualErr)
		require.Equal(t, test.expectedMap, actualMap)
	}
}

func TestNoopCompressor(t *testing.T) {
	c := NewNoopCompressor()

	for policy := range compressorPolicies {
		_, ok := c.ID(policy)
		require.False(t, ok)
	}
}

func TestStaticCompressor(t *testing.T) {
	c := NewStaticCompressor(compressorPolicies)

	for policy, expectedID := range compressorPolicies {
		actualID, ok := c.ID(policy)
		require.True(t, ok)
		require.Equal(t, expectedID, actualID)
	}

	unusedPolicy := Policy{
		resolution: Resolution{Window: 10 * time.Minute, Precision: xtime.Minute},
		retention:  Retention(48 * time.Hour),
	}
	_, ok := c.ID(unusedPolicy)
	require.False(t, ok)
}

func TestDynamicCompressor(t *testing.T) {
	var (
		key      = "compression"
		policies = make(CompressionMap, len(compressorPolicies))
		store    = mem.NewStore()
	)

	for k, v := range compressorPolicies {
		policies[k] = v
	}
	proto := compressorProtoFromPolicies(policies)
	store.Set(key, proto)

	opts := NewOptions().
		SetCompressionKey(key).
		SetKVStore(store)
	c := NewDynamicCompressor(opts).(*dynamicCompressor)

	// Process the first value.
	val, _ := store.Get(key)
	compMap, err := c.toCompressionMap(val)
	require.NoError(t, err)
	err = c.process(compMap)
	require.NoError(t, err)

	for policy, expectedID := range compressorPolicies {
		actualID, ok := c.ID(policy)
		require.True(t, ok)
		require.Equal(t, expectedID, actualID)
	}

	// Insert a new policy.
	newPolicy := Policy{
		resolution: Resolution{Window: 10 * time.Minute, Precision: xtime.Minute},
		retention:  Retention(48 * time.Hour),
	}
	id := int64(len(compressorPolicies) + 1)
	policies[newPolicy] = id
	proto = compressorProtoFromPolicies(policies)
	store.Set(key, proto)

	val, _ = store.Get(key)
	compMap, err = c.toCompressionMap(val)
	require.NoError(t, err)
	err = c.process(compMap)
	require.NoError(t, err)

	actualID, ok := c.ID(newPolicy)
	require.True(t, ok)
	require.Equal(t, id, actualID)

	// Remove the new policy.
	delete(policies, newPolicy)
	proto = compressorProtoFromPolicies(policies)
	store.Set(key, proto)

	val, _ = store.Get(key)
	compMap, err = c.toCompressionMap(val)
	require.NoError(t, err)
	err = c.process(compMap)
	require.NoError(t, err)

	_, ok = c.ID(newPolicy)
	require.False(t, ok)
}

func compressorProtoFromPolicies(policies CompressionMap) proto.Message {
	activePolicies := &schema.ActivePolicies{}
	for policy, id := range policies {
		precision, err := xtime.DurationFromUnit(policy.resolution.Precision)
		if err != nil {
			continue
		}
		res := schema.Resolution{WindowSize: int64(policy.resolution.Window), Precision: int64(precision)}
		ret := schema.Retention{Period: int64(policy.retention)}
		p := schema.Policy{Resolution: &res, Retention: &ret}
		e := schema.CompressedPolicy{Policy: &p, Id: id}

		activePolicies.CompressedPolicies = append(activePolicies.CompressedPolicies, &e)
	}

	return activePolicies
}
