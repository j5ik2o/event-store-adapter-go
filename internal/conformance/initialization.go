package conformance

import (
	"errors"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// NewInitializationInjection uses the runner's fault accounting for an internal
// configuration opening that does not implement the four-operation Store.
// Finish ends operation 0 and verifies applied counts, including unfired faults.
func NewInitializationInjection(specs []FaultSpec) (injection Injection, finish func() error) {
	return NewOperationInjection(0, specs)
}

// NewOperationInjection uses existing runner accounting for a single internal
// operation without requiring a complete four-operation Store.
// Finish closes that operation and checks actual fault application counts.
func NewOperationInjection(operation int, specs []FaultSpec) (injection Injection, finish func() error) {
	cur := &operationCursor{}
	cur.Set(operation)
	injection = Injection{Faults: newFaults(specs, cur), Hooks: testhook.New()}
	return injection, func() error {
		cur.Set(-1)
		if message := verifyFaults(injection.Faults); message != "" {
			return errors.New(message)
		}
		return nil
	}
}
