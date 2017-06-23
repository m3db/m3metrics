package cache

import (
	"time"

	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

// Configuration is config used to create a matcher.Cache.
type Configuration struct {
	Capacity          int           `yaml:"capacity"`
	FreshDuration     time.Duration `yaml:"freshDuration"`
	StutterDuration   time.Duration `yaml:"stutterDuration"`
	EvictionBatchSize int           `yaml:"evictionBatchSize"`
	DeletionBatchSize int           `yaml:"deletionBatchSize"`
}

// NewCache creates a matcher.Cache.
func (cfg *Configuration) NewCache(
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) matcher.Cache {
	opts := NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts)
	if cfg.Capacity != 0 {
		opts = opts.SetCapacity(cfg.Capacity)
	}
	if cfg.FreshDuration != 0 {
		opts = opts.SetFreshDuration(cfg.FreshDuration)
	}
	if cfg.StutterDuration != 0 {
		opts = opts.SetStutterDuration(cfg.StutterDuration)
	}
	if cfg.EvictionBatchSize != 0 {
		opts = opts.SetEvictionBatchSize(cfg.EvictionBatchSize)
	}
	if cfg.DeletionBatchSize != 0 {
		opts = opts.SetDeletionBatchSize(cfg.DeletionBatchSize)
	}

	return NewCache(opts)
}
