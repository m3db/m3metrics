package policy

import (
	"errors"
	"sync"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/generated/proto/schema"
)

var (
	errNilValue              = errors.New("nil value received")
	errNilPolicies           = errors.New("policies cannot be nil")
	errNilCompressedPolicy   = errors.New("compressed policy cannot be nil")
	errNilCompressedPolicies = errors.New("compressed policies cannot be nil")
	errNilInternalPolicy     = errors.New("internal policy for compressed policy cannot be nil")
)

// CompressionMap is a mapping from Policy to integer.
type CompressionMap map[Policy]int64

// NewCompressionMap constructs a new CompressionMap from a slice of
// compressed policies.
func NewCompressionMap(policies []*schema.CompressedPolicy) (CompressionMap, error) {
	if policies == nil {
		return nil, errNilPolicies
	}

	compressionMap := make(CompressionMap, len(policies))
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
		compressionMap[policy] = compressedPolicy.Id
	}

	return compressionMap, nil
}

// Compressor maintains a mapping of policies to integers. It is useful when
// encoding policies so that an integer representation of a policy can be
// sent instead of the entire Policy.
type Compressor interface {
	ID(p Policy) (int64, bool)
}

type noopCompressor struct{}

// NewNoopCompressor returns a new Compressor which never matches a policy.
func NewNoopCompressor() Compressor {
	return noopCompressor{}
}

func (m noopCompressor) ID(p Policy) (int64, bool) {
	return 0, false
}

type staticCompressor struct {
	policies CompressionMap
}

// NewStaticCompressor returns a new static Compressor.
func NewStaticCompressor(policies CompressionMap) Compressor {
	return staticCompressor{policies: policies}
}

func (m staticCompressor) ID(p Policy) (int64, bool) {
	i, ok := m.policies[p]
	return i, ok
}

type dynamicCompressor struct {
	sync.RWMutex
	runtime.Value

	policies CompressionMap

	proto *schema.ActivePolicies
}

// NewDynamicCompressor returns a new dynamic Compressor.
func NewDynamicCompressor(opts Options) Compressor {
	compressor := &dynamicCompressor{
		policies: make(CompressionMap),
		proto:    &schema.ActivePolicies{},
	}

	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(opts.KVStore()).
		SetUnmarshalFn(compressor.toCompressionMap).
		SetProcessFn(compressor.process)

	compressor.Value = runtime.NewValue(opts.CompressionKey(), valueOpts)

	return compressor
}

func (c *dynamicCompressor) ID(p Policy) (int64, bool) {
	c.RLock()
	i, ok := c.policies[p]
	c.RUnlock()
	return i, ok
}

func (c *dynamicCompressor) process(value interface{}) error {
	c.Lock()

	p := value.(CompressionMap)
	c.policies = p

	c.Unlock()
	return nil
}

func (c *dynamicCompressor) toCompressionMap(v kv.Value) (interface{}, error) {
	if v == nil {
		return nil, errNilValue
	}

	c.Lock()
	defer c.Unlock()

	if err := v.Unmarshal(c.proto); err != nil {
		return nil, err
	}

	compressedPolicies := c.proto.GetCompressedPolicies()
	if compressedPolicies == nil {
		return nil, errNilCompressedPolicies
	}

	return NewCompressionMap(compressedPolicies)
}
