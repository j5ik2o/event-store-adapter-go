package eventstore

import (
	"errors"
	"fmt"
)

// RetentionCount specifies no snapshot history or a positive history count (S-1).
// Its zero value is invalid; use NoRetention to explicitly disable history.
type RetentionCount struct {
	count       int
	constructed bool
}

// NoRetention specifies that only the current snapshot is kept.
func NoRetention() RetentionCount {
	return RetentionCount{constructed: true}
}

// KeepLatest specifies how many snapshot history entries to keep.
// Counts less than one return a ConfigurationError.
func KeepLatest(n int) (RetentionCount, error) {
	if n < 1 {
		return RetentionCount{}, newConfigurationError(fmt.Errorf("retention count must be positive: %d", n))
	}
	return RetentionCount{count: n, constructed: true}, nil
}

func (c RetentionCount) validate() error {
	if !c.constructed {
		return newConfigurationError(errors.New("retention count is unconstructed"))
	}
	return nil
}
