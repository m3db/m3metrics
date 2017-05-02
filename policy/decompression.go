package policy

import (
	"sync"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/generated/proto/schema"
)

// DecompressionMap is a mapping from integer to Policy.
type DecompressionMap map[int64]Policy

// NewDecompressionMap constructs a new DecompressionMap from a slice of
// compressed policies.
func NewDecompressionMap(policies []*schema.CompressedPolicy) (DecompressionMap, error) {
	if policies == nil {
		return nil, errNilPolicies
	}

	decompressionMap := make(DecompressionMap, len(policies))
	for _, compressedPolicy := range policies {
		if compressedPolicy == nil {
			return nil, errNilCompressedPolicy
		}

		internalPolicy := compressedPolicy.GetPolicy()
		if internalPolicy == nil {
			return nil, errNilInternalPolicy
		}

		policy, err := NewPolicyFromSchema(internalPolicy)
		if err != nil {
			return nil, err
		}
		decompressionMap[compressedPolicy.Id] = policy
	}
	return decompressionMap, nil
}

// Decompressor maintains a mapping of integers to policies. It is used to
// return a Policy from its corresponding integer encoding.
type Decompressor interface {
	Policy(id int64) (Policy, bool)
}

type noopDecompression struct{}

// NewNoopDecompressor returns a new Decompressor mapping which never matches an id.
func NewNoopDecompressor() Decompressor {
	return noopDecompression{}
}

func (m noopDecompression) Policy(id int64) (Policy, bool) {
	return EmptyPolicy, false
}

type staticDecompressor struct {
	policies DecompressionMap
}

// NewStaticDecompressor returns a new static Decompressor.
func NewStaticDecompressor(policies map[int64]Policy) Decompressor {
	return staticDecompressor{policies: policies}
}

func (m staticDecompressor) Policy(id int64) (Policy, bool) {
	p, ok := m.policies[id]
	if !ok {
		return EmptyPolicy, false
	}
	return p, true
}

type dynamicDecompressor struct {
	sync.RWMutex
	runtime.Value

	policies DecompressionMap

	proto *schema.ActivePolicies
}

// NewDynamicDecompressor returns a new dynamic Decompressor.
func NewDynamicDecompressor(opts Options) Decompressor {
	decompressor := &dynamicDecompressor{
		policies: make(DecompressionMap),
		proto:    &schema.ActivePolicies{},
	}

	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(opts.KVStore()).
		SetUnmarshalFn(decompressor.toDecompressionMap).
		SetProcessFn(decompressor.process)

	decompressor.Value = runtime.NewValue(opts.CompressionKey(), valueOpts)

	return decompressor
}

func (d *dynamicDecompressor) Policy(id int64) (Policy, bool) {
	d.RLock()
	p, ok := d.policies[id]
	d.RUnlock()
	if !ok {
		return EmptyPolicy, false
	}
	return p, true
}

func (d *dynamicDecompressor) process(value interface{}) error {
	d.Lock()

	p := value.(DecompressionMap)
	d.policies = p

	d.Unlock()
	return nil
}

func (d *dynamicDecompressor) toDecompressionMap(v kv.Value) (interface{}, error) {
	if v == nil {
		return nil, errNilValue
	}

	d.Lock()
	defer d.Unlock()

	err := v.Unmarshal(d.proto)
	if err != nil {
		return nil, err
	}

	compressedPolicies := d.proto.GetCompressedPolicies()
	if compressedPolicies == nil {
		return nil, errNilCompressedPolicies
	}

	return NewDecompressionMap(compressedPolicies)
}
