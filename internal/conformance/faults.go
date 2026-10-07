package conformance

import (
	"fmt"
	"strings"
	"sync"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

const (
	repeatCount = "count"
	repeatUntil = "until-operation-finishes"
)

// FaultSpec is a fault of a scenario.
type FaultSpec struct {
	// Operation is 0 for store creation and the 1-based step number otherwise.
	Operation int
	Phase     string
	Kind      string
	// Repeat is "count" or "until-operation-finishes". Count is used for "count".
	Repeat    string
	Count     int
	Details   map[string]any
	Injection string
}

// operationCursor holds the number of the operation that is running. It starts at 0 (store creation).
type operationCursor struct {
	mu sync.Mutex
	n  int
}

// Set moves the cursor to an operation.
func (c *operationCursor) Set(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n = n
}

func (c *operationCursor) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// Fault is a registered fault with its own firing count. The runner owns the count.
type Fault struct {
	Spec   FaultSpec
	index  int
	cursor *operationCursor
	mu     sync.Mutex
	fired  int
}

func newFaults(specs []FaultSpec, cur *operationCursor) []*Fault {
	out := make([]*Fault, len(specs))
	for i, s := range specs {
		out[i] = &Fault{Spec: s, index: i, cursor: cur}
	}
	return out
}

// TryApply reports whether the fault applies now and, if so, counts one firing. A fault applies
// only while its operation runs, and a count fault stops after its count.
func (f *Fault) TryApply() bool {
	if f.cursor.get() != f.Spec.Operation {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Spec.Repeat == repeatCount && f.fired >= f.Spec.Count {
		return false
	}
	f.fired++
	return true
}

// Fired returns how many times the fault was applied.
func (f *Fault) Fired() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fired
}

func (f *Fault) describe() string {
	return fmt.Sprintf("fault #%d (operation %d, phase %s, kind %s)", f.index, f.Spec.Operation, f.Spec.Phase, f.Spec.Kind)
}

// verifyFaults returns "" when every fault fired as specified: exactly Count times for count,
// at least once for until-operation-finishes. Otherwise it names the faults that did not.
func verifyFaults(faults []*Fault) string {
	var msgs []string
	for _, f := range faults {
		n := f.Fired()
		switch f.Spec.Repeat {
		case repeatCount:
			if n != f.Spec.Count {
				msgs = append(msgs, fmt.Sprintf("%s fired %d times, want exactly %d", f.describe(), n, f.Spec.Count))
			}
		default:
			if n < 1 {
				msgs = append(msgs, fmt.Sprintf("%s fired %d times, want at least 1", f.describe(), n))
			}
		}
	}
	return strings.Join(msgs, "; ")
}

// hookPhases are the phases that a storage-error or serialization-error fault can fail through testhook.
var hookPhases = map[testhook.Phase]bool{
	testhook.PhaseSerializeEvent: true, testhook.PhaseSerializeSnapshot: true,
	testhook.PhaseDeserializeEvent: true, testhook.PhaseDeserializeSnapshot: true,
	testhook.PhaseCommit: true, testhook.PhaseReadEvents: true, testhook.PhaseReadSnapshot: true,
	testhook.PhaseRetentionQuery: true, testhook.PhaseRetentionDelete: true,
}

// registerHookFaults registers on h the faults that go through testhook: storage-error and
// serialization-error on the hook phases, and sdk-response with history_pages on retention-query.
// Other faults (sdk-error, the other sdk-response, read-interleave) are applied by the backend
// through Injection.Faults.
func registerHookFaults(h *testhook.Hooks, faults []*Fault) {
	var pageFaults []*Fault
	for _, f := range faults {
		phase, ok := testhook.ParsePhase(f.Spec.Phase)
		if !ok || !hookPhases[phase] {
			continue
		}
		switch f.Spec.Kind {
		case "storage-error", "serialization-error":
			f := f
			h.OnFail(phase, func(testhook.Point) error {
				if f.TryApply() {
					return &testhook.InjectedError{Phase: phase, Message: f.Spec.Kind}
				}
				return nil
			})
		case "sdk-response":
			if phase == testhook.PhaseRetentionQuery {
				if _, ok := historyPages(f.Spec.Details); ok {
					pageFaults = append(pageFaults, f)
				}
			}
		}
	}
	if len(pageFaults) == 0 {
		return
	}
	h.OnHistoryPages(func(string, int64) ([][]int64, bool, error) {
		for _, f := range pageFaults {
			if f.TryApply() {
				pages, _ := historyPages(f.Spec.Details)
				return pages, true, nil
			}
		}
		return nil, false, nil
	})
}

// historyPages reads details.history_pages.
func historyPages(details map[string]any) ([][]int64, bool) {
	raw, ok := details["history_pages"].([]any)
	if !ok {
		return nil, false
	}
	pages := make([][]int64, 0, len(raw))
	for _, p := range raw {
		list, ok := p.([]any)
		if !ok {
			return nil, false
		}
		page := make([]int64, 0, len(list))
		for _, v := range list {
			n, err := bigIntFromJSONNumber(v)
			if err != nil || !n.IsInt64() {
				return nil, false
			}
			page = append(page, n.Int64())
		}
		pages = append(pages, page)
	}
	return pages, true
}
