package eventstore

import (
	"context"
	"fmt"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
)

// RetentionMode selects how excess snapshot history is removed.
type RetentionMode int

const (
	RetentionDelete RetentionMode = RetentionMode(storeoptions.RetentionDelete)
	RetentionTTL    RetentionMode = RetentionMode(storeoptions.RetentionTTL)
)

// The alias lets storage packages apply public Options through internal/storeoptions.
type options = storeoptions.Options

// Option configures common store settings.
type Option func(*options) error

// WithRetentionCount supplies a constructed retention setting.
// Omission and NoRetention both mean no snapshot history.
func WithRetentionCount(c RetentionCount) Option {
	return func(o *options) error {
		if err := c.validate(); err != nil {
			return err
		}
		if c.count == 0 {
			o.RetentionCount = nil
		} else {
			count := c.count
			o.RetentionCount = &count
		}
		return nil
	}
}

// WithRetentionMode supplies the requested removal mode, even when history is disabled.
// Each storage constructor decides whether it supports TTL.
func WithRetentionMode(m RetentionMode) Option {
	return func(o *options) error {
		o.RetentionMode = storeoptions.RetentionMode(m)
		return nil
	}
}

// WithTTLGraceSeconds supplies a non-negative grace period in integer seconds.
// Omission means zero. The value is used only by TTL retention.
func WithTTLGraceSeconds(seconds int64) Option {
	return func(o *options) error {
		if seconds < 0 {
			return newConfigurationError(fmt.Errorf("TTL grace seconds must be non-negative: %d", seconds))
		}
		o.TTLGraceSeconds = seconds
		return nil
	}
}

// WithRetentionFailureHandler stores an additional retention failure notification.
// It supplements required logging; applying this option does not call the handler.
func WithRetentionFailureHandler(h func(ctx context.Context, err error)) Option {
	return func(o *options) error {
		o.RetentionFailureHandler = h
		return nil
	}
}

func newConfigurationError(cause error) error {
	return &ConfigurationError{Cause: cause}
}
