// Package storeoptions assembles common settings without depending on public packages.
// Storage constructors apply eventstore.Option values here before storage-specific checks.
package storeoptions

import (
	"context"
	"fmt"
)

// RetentionMode is the internal representation shared with eventstore.
type RetentionMode int

const (
	RetentionDelete RetentionMode = iota + 1
	RetentionTTL
)

// Options contains the common settings passed to storage constructors.
// RetentionCount is nil for valid no-history settings; a present count is positive.
type Options struct {
	RetentionCount          *int
	RetentionMode           RetentionMode
	TTLGraceSeconds         int64
	RetentionFailureHandler func(context.Context, error)
}

// Apply assembles a fresh configuration, applying options in order.
// configurationError classifies errors owned by this boundary without an import cycle.
// Option errors are returned unchanged, preserving their classification and cause.
// Mode validation applies only when history is enabled; storage support is checked later.
func Apply[O ~func(*Options) error](configurationError func(error) error, opts ...O) (Options, error) {
	o := Options{RetentionMode: RetentionDelete}
	for i, opt := range opts {
		if opt == nil {
			return Options{}, configurationError(fmt.Errorf("option %d is nil", i))
		}
		if err := opt(&o); err != nil {
			return Options{}, err
		}
	}
	if o.RetentionCount != nil {
		if *o.RetentionCount < 1 {
			return Options{}, configurationError(fmt.Errorf("retention count must be positive: %d", *o.RetentionCount))
		}
		if o.RetentionMode != RetentionDelete && o.RetentionMode != RetentionTTL {
			return Options{}, configurationError(fmt.Errorf("invalid retention mode: %d", o.RetentionMode))
		}
	}
	if o.TTLGraceSeconds < 0 {
		return Options{}, configurationError(fmt.Errorf("TTL grace seconds must be non-negative: %d", o.TTLGraceSeconds))
	}
	return o, nil
}
