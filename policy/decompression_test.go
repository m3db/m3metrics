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
	decompressorPolicies = DecompressionMap{
		1: Policy{
			resolution: Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			retention:  Retention(6 * time.Hour),
		},
		2: Policy{
			resolution: Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			retention:  Retention(2 * time.Hour),
		},
		3: Policy{
			resolution: Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			retention:  Retention(12 * time.Hour),
		},
		4: Policy{
			resolution: Resolution{Window: 5 * time.Minute, Precision: xtime.Minute},
			retention:  Retention(48 * time.Hour),
		},
		5: Policy{
			resolution: Resolution{Window: time.Minute, Precision: xtime.Minute},
			retention:  Retention(time.Hour),
		},
		6: Policy{
			resolution: Resolution{Window: time.Minute, Precision: xtime.Minute},
			retention:  Retention(24 * time.Hour),
		},
	}
)

func TestNewDecompressionMap(t *testing.T) {
	sampleSchemaPolicy := &schema.Policy{
		Resolution: &schema.Resolution{WindowSize: int64(10 * time.Second), Precision: int64(time.Second)},
		Retention:  &schema.Retention{Period: int64(2 * time.Hour)},
	}
	samplePolicy, _ := NewPolicyFromSchema(sampleSchemaPolicy)

	tests := []struct {
		policies    []*schema.CompressedPolicy
		expectedErr bool
		expectedMap DecompressionMap
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
			DecompressionMap{1: samplePolicy},
		},
	}

	for _, test := range tests {
		actualMap, actualErr := NewDecompressionMap(test.policies)
		if test.expectedErr {
			require.Error(t, actualErr)
			continue
		}
		require.NoError(t, actualErr)
		require.Equal(t, test.expectedMap, actualMap)
	}
}

func TestNoopDecompressor(t *testing.T) {
	c := NewNoopDecompressor()

	for id := range decompressorPolicies {
		_, ok := c.Policy(id)
		require.False(t, ok)
	}
}

func TestStaticDecompressor(t *testing.T) {
	c := NewStaticDecompressor(decompressorPolicies)

	for id, expectedPolicy := range decompressorPolicies {
		actualPolicy, ok := c.Policy(id)
		require.True(t, ok)
		require.Equal(t, expectedPolicy, actualPolicy)
	}

	var unusedID int64 = 7
	_, ok := c.Policy(unusedID)
	require.False(t, ok)
}

func TestDynamicDecompressor(t *testing.T) {
	var (
		key      = "compression"
		policies = make(DecompressionMap, len(decompressorPolicies))
		store    = mem.NewStore()
	)

	for k, v := range decompressorPolicies {
		policies[k] = v
	}
	proto := decompressorProtoFromPolicies(policies)
	store.Set(key, proto)

	opts := NewOptions().
		SetCompressionKey(key).
		SetKVStore(store)
	d := NewDynamicDecompressor(opts).(*dynamicDecompressor)

	// Process the first value.
	val, _ := store.Get(key)
	decompMap, err := d.toDecompressionMap(val)
	require.NoError(t, err)
	err = d.process(decompMap)
	require.NoError(t, err)

	for id, expectedPolicy := range decompressorPolicies {
		actualPolicy, ok := d.Policy(id)
		require.True(t, ok)
		require.Equal(t, expectedPolicy, actualPolicy)
	}

	// Insert a new policy.
	newPolicy := Policy{
		resolution: Resolution{Window: 10 * time.Minute, Precision: xtime.Minute},
		retention:  Retention(48 * time.Hour),
	}
	id := int64(len(decompressorPolicies)) + 1
	policies[id] = newPolicy
	proto = decompressorProtoFromPolicies(policies)
	store.Set(key, proto)

	val, _ = store.Get(key)
	decompMap, err = d.toDecompressionMap(val)
	require.NoError(t, err)
	err = d.process(decompMap)
	require.NoError(t, err)

	actualPolicy, ok := d.Policy(id)
	require.True(t, ok)
	require.Equal(t, newPolicy, actualPolicy)

	// Remove the new policy.
	delete(policies, id)
	proto = decompressorProtoFromPolicies(policies)
	store.Set(key, proto)

	val, _ = store.Get(key)
	decompMap, err = d.toDecompressionMap(val)
	require.NoError(t, err)
	err = d.process(decompMap)
	require.NoError(t, err)

	_, ok = d.Policy(id)
	require.False(t, ok)
}

func decompressorProtoFromPolicies(policies DecompressionMap) proto.Message {
	activePolicies := &schema.ActivePolicies{}
	for id, policy := range policies {
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
