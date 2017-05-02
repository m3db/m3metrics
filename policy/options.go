package policy

import (
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultInitWatchTimeout = 10 * time.Second
)

// Options provide a set of options for the policy compressor and decompressor.
type Options interface {
	// SetCompressionKey sets the kv key to watch for compression changes.
	SetCompressionKey(value string) Options

	// CompressionKey returns the kv key to watch for compression changes.
	CompressionKey() string

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInitWatchTimeout sets the initial watch timeout.
	SetInitWatchTimeout(value time.Duration) Options

	// InitWatchTimeout returns the initial watch timeout.
	InitWatchTimeout() time.Duration

	// SetKVStore sets the kv store.
	SetKVStore(value kv.Store) Options

	// KVStore returns the kv store.
	KVStore() kv.Store
}

type options struct {
	compressionKey   string
	instrumentOpts   instrument.Options
	initWatchTimeout time.Duration
	kvStore          kv.Store
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{
		instrumentOpts:   instrument.NewOptions(),
		initWatchTimeout: defaultInitWatchTimeout,
	}
}

func (o *options) SetCompressionKey(key string) Options {
	opts := *o
	opts.compressionKey = key
	return &opts
}

func (o *options) CompressionKey() string {
	return o.compressionKey
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetInitWatchTimeout(value time.Duration) Options {
	opts := *o
	opts.initWatchTimeout = value
	return &opts
}

func (o *options) InitWatchTimeout() time.Duration {
	return o.initWatchTimeout
}

func (o *options) SetKVStore(value kv.Store) Options {
	opts := *o
	opts.kvStore = value
	return &opts
}

func (o *options) KVStore() kv.Store {
	return o.kvStore
}
