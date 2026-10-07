package testhook

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Phase names a point of an operation where a fault can be injected. The values are the
// phase enumeration of conformance/schema/common.schema.json.
type Phase string

const (
	PhaseSerializeEvent      Phase = "serialize-event"
	PhaseSerializeSnapshot   Phase = "serialize-snapshot"
	PhaseDeserializeEvent    Phase = "deserialize-event"
	PhaseDeserializeSnapshot Phase = "deserialize-snapshot"
	PhaseCommit              Phase = "commit"
	PhaseReadEvents          Phase = "read-events"
	PhaseReadSnapshot        Phase = "read-snapshot"
	PhaseRetentionQuery      Phase = "retention-query"
	PhaseRetentionDelete     Phase = "retention-delete"
	PhaseRetentionMark       Phase = "retention-mark"
	PhaseConfigurationRead   Phase = "configuration-read"
	PhaseConfigurationCreate Phase = "configuration-create"
)

var phases = []Phase{
	PhaseSerializeEvent, PhaseSerializeSnapshot, PhaseDeserializeEvent, PhaseDeserializeSnapshot,
	PhaseCommit, PhaseReadEvents, PhaseReadSnapshot, PhaseRetentionQuery, PhaseRetentionDelete,
	PhaseRetentionMark, PhaseConfigurationRead, PhaseConfigurationCreate,
}

// Phases returns every known phase.
func Phases() []Phase {
	return append([]Phase(nil), phases...)
}

// ParsePhase converts a phase name of the data into a Phase.
func ParsePhase(s string) (Phase, bool) {
	for _, p := range phases {
		if string(p) == s {
			return p, true
		}
	}
	return "", false
}

// Point describes where a hook is called.
type Point struct {
	Phase       Phase
	AggregateID string
	SeqNr       int64
	SeqNrs      []int64
	Payload     []byte
}

// InjectedError is the error that an injected fault returns.
type InjectedError struct {
	Phase   Phase
	Message string
}

func (e *InjectedError) Error() string {
	return fmt.Sprintf("injected fault at %s: %s", e.Phase, e.Message)
}

// Clock returns the current time.
type Clock func() time.Time

// Sleeper waits for a duration.
type Sleeper func(time.Duration)

// FailHook returns an error to make a phase fail, or nil to let it proceed.
type FailHook func(Point) error

// HistoryPagesHook returns the pages of history seq_nr values that the retention query reads.
// ok is false when the hook does not replace the query.
type HistoryPagesHook func(aid string, justWritten int64) (pages [][]int64, ok bool, err error)

// History is the internal history of one aggregate: unmarked (active) and marked seq_nr values.
type History struct {
	Active []int64
	Marked []int64
}

// HistoryReader reads the internal history of an aggregate.
type HistoryReader func(aid string) (History, error)

// Hooks is the registration point of the hooks. A nil *Hooks is valid and does nothing.
type Hooks struct {
	mu          sync.RWMutex
	clock       Clock
	sleeper     Sleeper
	fail        map[Phase][]FailHook
	historyPage HistoryPagesHook
	history     HistoryReader
}

// New returns an empty Hooks.
func New() *Hooks {
	return &Hooks{fail: map[Phase][]FailHook{}}
}

// SetClock injects the clock.
func (h *Hooks) SetClock(c Clock) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clock = c
}

// Now returns the injected time, or the real time when no clock is injected.
func (h *Hooks) Now() time.Time {
	if h != nil {
		h.mu.RLock()
		c := h.clock
		h.mu.RUnlock()
		if c != nil {
			return c()
		}
	}
	return time.Now()
}

// SetSleeper injects the wait.
func (h *Hooks) SetSleeper(s Sleeper) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sleeper = s
}

// Sleep waits through the injected sleeper, or really sleeps when none is injected.
func (h *Hooks) Sleep(d time.Duration) {
	if h != nil {
		h.mu.RLock()
		s := h.sleeper
		h.mu.RUnlock()
		if s != nil {
			s(d)
			return
		}
	}
	time.Sleep(d)
}

// OnFail registers a fault hook for a phase. Hooks run in registration order.
func (h *Hooks) OnFail(p Phase, f FailHook) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fail == nil {
		h.fail = map[Phase][]FailHook{}
	}
	h.fail[p] = append(h.fail[p], f)
}

// Fail calls the hooks of the point's phase and returns the first error.
func (h *Hooks) Fail(pt Point) error {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	hooks := append([]FailHook(nil), h.fail[pt.Phase]...)
	h.mu.RUnlock()
	for _, f := range hooks {
		if err := f(pt); err != nil {
			return err
		}
	}
	return nil
}

// OnHistoryPages registers the hook that replaces the retention query.
func (h *Hooks) OnHistoryPages(f HistoryPagesHook) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.historyPage = f
}

// HistoryPages calls the registered hook. ok is false when there is none.
func (h *Hooks) HistoryPages(aid string, justWritten int64) ([][]int64, bool, error) {
	if h == nil {
		return nil, false, nil
	}
	h.mu.RLock()
	f := h.historyPage
	h.mu.RUnlock()
	if f == nil {
		return nil, false, nil
	}
	return f(aid, justWritten)
}

// ProvideHistory registers the reader of the internal history.
func (h *Hooks) ProvideHistory(r HistoryReader) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.history = r
}

// History reads the internal history of an aggregate through the registered reader.
func (h *Hooks) History(aid string) (History, error) {
	if h == nil {
		return History{}, errors.New("testhook: no history reader is provided")
	}
	h.mu.RLock()
	r := h.history
	h.mu.RUnlock()
	if r == nil {
		return History{}, errors.New("testhook: no history reader is provided")
	}
	return r(aid)
}
