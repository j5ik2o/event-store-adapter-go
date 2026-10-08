// Package memory provides process-local storage with store-wide locking.
package memory

import (
	"bytes"
	"errors"
	"sync"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// Store owns records, immutable settings and a lock for the whole storage destination.
// Calls using the same *Store share these values; separate NewStore calls are isolated.
// Its zero value is invalid; use NewStore.
type Store struct {
	mu       sync.RWMutex
	records  map[string]*record
	settings storeoptions.Options
	hooks    *testhook.Hooks
}

type record struct {
	journal []storedEvent
}

type storedEvent struct {
	aggregateID string
	seqNr       eventstore.SeqNr
	occurredAt  time.Time
	manifest    string
	payload     []byte
}

func (e storedEvent) clone() storedEvent {
	e.payload = bytes.Clone(e.payload)
	return e
}

// NewStore applies common options once and rejects TTL retention, even without history.
// The store keeps its own copy of the applied settings.
func NewStore(opts ...eventstore.Option) (*Store, error) {
	settings, err := storeoptions.Apply(func(cause error) error {
		return &eventstore.ConfigurationError{Cause: cause}
	}, opts...)
	if err != nil {
		return nil, err
	}
	if settings.RetentionMode == storeoptions.RetentionTTL {
		return nil, &eventstore.ConfigurationError{Cause: errors.New("memory does not support TTL retention")}
	}
	if settings.RetentionCount != nil {
		count := *settings.RetentionCount
		settings.RetentionCount = &count
	}
	return &Store{records: make(map[string]*record), settings: settings}, nil
}

// observe copies the actual head and journal under the same read lock.
// It is an internal observation point, separate from the product's read operations.
func (s *Store) observe(aid string) (*storedEvent, []storedEvent) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	current := s.records[aid]
	if current == nil {
		return nil, nil
	}
	journal := make([]storedEvent, len(current.journal))
	for i, event := range current.journal {
		journal[i] = event.clone()
	}
	head := journal[len(journal)-1].clone()
	return &head, journal
}
