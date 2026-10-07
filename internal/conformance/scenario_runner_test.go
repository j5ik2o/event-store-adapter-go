package conformance

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBackend stands in for a storage backend. It lives in tests only; the boundary
// (Backend and Store) has no production implementation in this change.
type fakeBackend struct {
	name       string
	injectable bool
	openErr    error
	opens      int32
	inFlight   int32
	maxFlight  int32
	calls      []string
	// hookOnPersist: persistEvent asks the hooks whether the commit phase fails.
	hookOnPersist bool
	// sdkTries: how many times persistEvent calls TryApply on the injected faults.
	sdkTries int
	// seedErr is returned by Seed when it is not nil.
	seedErr error
	// seedCalls counts the Seed calls, in order with the Open calls recorded in opens.
	seedCalls int32
	// active and marked are the histories that Open provides through the hooks.
	active []int64
	marked []int64
	// notifications is what the store reports as caught notifications.
	notifications []string
	// historyAid is the aid that the history reader was last called with.
	historyAid string
	// seq records "seed" and "open" in call order.
	seq []string
	// last is the store that Open returned last, to inspect its clock reads.
	last *fakeStore
}

func (b *fakeBackend) Name() string                { return b.name }
func (b *fakeBackend) Injectable(f FaultSpec) bool { return b.injectable }

func (b *fakeBackend) Seed(context.Context, []map[string]any) error {
	atomic.AddInt32(&b.seedCalls, 1)
	b.seq = append(b.seq, "seed")
	return b.seedErr
}

func (b *fakeBackend) Open(ctx context.Context, cfg StoreConfig, inj Injection) (Store, error) {
	atomic.AddInt32(&b.opens, 1)
	b.seq = append(b.seq, "open")
	inj.Hooks.ProvideHistory(func(aid string) (testhook.History, error) {
		b.historyAid = aid
		return testhook.History{Active: b.active, Marked: b.marked}, nil
	})
	if b.openErr != nil {
		return nil, b.openErr
	}
	st := &fakeStore{b: b, inj: inj, openTime: inj.Hooks.Now()}
	b.last = st
	return st, nil
}

type fakeStore struct {
	b        *fakeBackend
	inj      Injection
	events   []Event
	openTime time.Time
	// times records each operation's clock read, after openTime.
	times []time.Time
}

func (s *fakeStore) enter(name string) func() {
	n := atomic.AddInt32(&s.b.inFlight, 1)
	for {
		m := atomic.LoadInt32(&s.b.maxFlight)
		if n <= m || atomic.CompareAndSwapInt32(&s.b.maxFlight, m, n) {
			break
		}
	}
	s.b.calls = append(s.b.calls, name)
	return func() { atomic.AddInt32(&s.b.inFlight, -1) }
}

func (s *fakeStore) PersistEvent(ctx context.Context, ev Event) error {
	defer s.enter("persistEvent")()
	s.times = append(s.times, s.inj.Hooks.Now())
	if ev.SeqNr == 0 {
		return &OperationError{Category: "contract-violation", Message: "seq_nr is zero"}
	}
	for i := 0; i < s.b.sdkTries; i++ {
		for _, f := range s.inj.Faults {
			f.TryApply()
		}
	}
	if s.b.hookOnPersist {
		p, _ := testhook.ParsePhase("commit")
		if err := s.inj.Hooks.Fail(testhook.Point{Phase: p, AggregateID: ev.AggregateID.TypeName + "-" + ev.AggregateID.Value, SeqNr: ev.SeqNr}); err != nil {
			return &OperationError{Category: "storage", Message: err.Error()}
		}
	}
	s.events = append(s.events, ev)
	return nil
}

func (s *fakeStore) PersistEventAndSnapshot(ctx context.Context, ev Event, sn Snapshot) error {
	return s.PersistEvent(ctx, ev)
}

func (s *fakeStore) GetLatestSnapshotByID(ctx context.Context, aid AggregateIDArg) (SnapshotRead, error) {
	defer s.enter("getLatestSnapshotById")()
	return SnapshotRead{}, nil
}

func (s *fakeStore) GetEventsByIDSinceSeqNr(ctx context.Context, aid AggregateIDArg, since int64) ([]Event, error) {
	defer s.enter("getEventsByIdSinceSeqNr")()
	var out []Event
	for _, e := range s.events {
		if e.SeqNr >= since {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *fakeStore) Notifications() []string { return s.b.notifications }
func (s *fakeStore) Close() error            { return nil }

func fixtureEvent(seq int) string {
	return fmt.Sprintf(`{
  "aggregate_id": {"type_name": "Order", "value": "1"},
  "seq_nr": %d,
  "occurred_at": "2026-10-05T00:00:00.123456789Z",
  "payload": {"number": %d},
  "manifest": ""
}`, seq, seq)
}

func step(op, args, expect string) string {
	return `{"op":"` + op + `","arguments":` + args + `,"expect":` + expect + `}`
}

func stepObserve(op, args, expect, observe string) string {
	return `{"op":"` + op + `","arguments":` + args + `,"expect":` + expect + `,"observe":` + observe + `}`
}

func stepClock(op, args, expect string, epochSeconds int64) string {
	return `{"op":"` + op + `","arguments":` + args + `,"expect":` + expect + `,"clock_epoch_seconds":` + strconv.FormatInt(epochSeconds, 10) + `}`
}

func scenarioBody(init string, faults string, steps ...string) string {
	body := `{"id":"t","rules":["W-1"],"backends":["memory","dynamodb"],
	  "store":{"retention_count":null,"retention_mode":"delete"},
	  "fixtures":{"events":{"e0":` + fixtureEvent(0) + `,"e1":` + fixtureEvent(1) + `,"e2":` + fixtureEvent(2) + `},"snapshots":{}},
	  "steps":[`
	for i, s := range steps {
		if i > 0 {
			body += ","
		}
		body += s
	}
	body += `]`
	if init != "" {
		body += `,"initialization":` + init
	}
	if faults != "" {
		body += `,"faults":` + faults
	}
	return body + `}`
}

func mkCase(t *testing.T, body string) ScenarioCase {
	t.Helper()
	v, err := decodeStrictJSON([]byte(body))
	require.NoError(t, err)
	m := v.(map[string]any)
	plan, err := parseScenario(m)
	require.NoError(t, err)
	return ScenarioCase{ID: "t", Rules: []string{"W-1"}, Backends: []string{"memory", "dynamodb"}, Raw: m, Materialized: m, Plan: plan}
}

var (
	aidArgs        = `{"aggregate_id":{"type_name":"Order","value":"1"}}`
	persistOK      = step("persistEvent", `{"event":"e1"}`, `{"result":"success"}`)
	persistTwoOK   = step("persistEvent", `{"event":"e2"}`, `{"result":"success"}`)
	readFrom1Both  = step("getEventsByIdSinceSeqNr", `{"aggregate_id":{"type_name":"Order","value":"1"},"seq_nr":1}`, `{"result":"events","events":["e1","e2"]}`)
	readFrom1Only1 = step("getEventsByIdSinceSeqNr", `{"aggregate_id":{"type_name":"Order","value":"1"},"seq_nr":1}`, `{"result":"events","events":["e1"]}`)
)

func TestRunScenario(t *testing.T) {
	ctx := context.Background()

	t.Run("matching expectations succeed", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		c := mkCase(t, scenarioBody("", "", persistOK, persistTwoOK, readFrom1Both))
		r := runScenario(ctx, c, "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
		assert.Nil(t, r.FailedStep)
		assert.Equal(t, "memory", r.Backend)
		assert.Equal(t, "t", r.ID)
	})

	t.Run("a mismatch fails the scenario and reports the 1-based step", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		c := mkCase(t, scenarioBody("", "", persistOK, persistTwoOK, readFrom1Only1))
		r := runScenario(ctx, c, "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 3, *r.FailedStep)
		assert.NotEmpty(t, r.Reason)
	})

	t.Run("steps run in array order, one at a time, on one fresh store", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		c := mkCase(t, scenarioBody("", "", persistOK, persistTwoOK, readFrom1Both))
		r := runScenario(ctx, c, "memory", b)
		require.Equal(t, StatusSuccess, r.Status, r.Reason)
		assert.Equal(t, []string{"persistEvent", "persistEvent", "getEventsByIdSinceSeqNr"}, b.calls)
		assert.Equal(t, int32(1), b.maxFlight)
		assert.Equal(t, int32(1), b.opens)
	})

	t.Run("every scenario gets its own store", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		c := mkCase(t, scenarioBody("", "", persistOK, readFrom1Only1))
		for i := 0; i < 2; i++ {
			r := runScenario(ctx, c, "memory", b)
			require.Equal(t, StatusSuccess, r.Status, r.Reason)
		}
		assert.Equal(t, int32(2), b.opens)
	})

	t.Run("an error is compared by its category, not guessed from the message", func(t *testing.T) {
		contract := step("persistEvent", `{"event":"e0"}`, `{"result":"error","error":{"category":"contract-violation"}}`)
		b := &fakeBackend{name: "memory", injectable: true}
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", contract)), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)

		wrongCategory := step("persistEvent", `{"event":"e0"}`, `{"result":"error","error":{"category":"configuration"}}`)
		b = &fakeBackend{name: "memory", injectable: true}
		r = runScenario(ctx, mkCase(t, scenarioBody("", "", wrongCategory)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 1, *r.FailedStep)
	})

	t.Run("a plain error is not classified from its text", func(t *testing.T) {
		// The store reports an unclassified error whose text says "configuration".
		b := &fakeBackend{name: "memory", injectable: true, openErr: errors.New("configuration error")}
		init := `{"expect":{"error":{"category":"configuration"}}}`
		r := runScenario(ctx, mkCase(t, scenarioBody(init, "")), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
	})

	t.Run("an input the library rejects reaches the store unchecked and fails at that step", func(t *testing.T) {
		// seq_nr 0 must be rejected by the store (the fake), not pre-checked by the runner.
		b := &fakeBackend{name: "memory", injectable: true}
		bad := step("persistEvent", `{"event":"e0"}`, `{"result":"success"}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", bad)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 1, *r.FailedStep)
		assert.Equal(t, []string{"persistEvent"}, b.calls)
	})

	t.Run("an expected creation error is compared as a creation failure", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, openErr: &OperationError{Category: "configuration", Message: "retention_count must be positive"}}
		init := `{"expect":{"error":{"category":"configuration","message":{"must_contain":["retention_count"],"must_not_contain":[]}}}}`
		r := runScenario(ctx, mkCase(t, scenarioBody(init, "")), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("a creation error that does not match fails at step 0", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, openErr: &OperationError{Category: "storage", Message: "x"}}
		init := `{"expect":{"error":{"category":"configuration","message":{"must_contain":[],"must_not_contain":[]}}}}`
		r := runScenario(ctx, mkCase(t, scenarioBody(init, "")), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 0, *r.FailedStep)
	})

	t.Run("an expected creation error that does not happen fails", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		init := `{"expect":{"error":{"category":"configuration","message":{"must_contain":[],"must_not_contain":[]}}}}`
		r := runScenario(ctx, mkCase(t, scenarioBody(init, "")), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
	})

	t.Run("an unexpected creation error fails at step 0", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, openErr: &OperationError{Category: "storage", Message: "x"}}
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", persistOK)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 0, *r.FailedStep)
	})
}

func TestRunScenario_NotRun(t *testing.T) {
	ctx := context.Background()

	t.Run("a backend that is not connected leaves the scenario unverified", func(t *testing.T) {
		c := mkCase(t, scenarioBody("", "", persistOK))
		r := runScenario(ctx, c, "memory", nil)
		assert.Equal(t, StatusUnverified, r.Status)
		assert.NotEmpty(t, r.Reason)
		assert.Nil(t, r.FailedStep)
	})

	t.Run("milliseconds precision is not-applicable", func(t *testing.T) {
		c := mkCase(t, scenarioBody("", "", persistOK))
		c.TimePrecision = "milliseconds"
		b := &fakeBackend{name: "memory", injectable: true}
		r := runScenario(ctx, c, "memory", b)
		assert.Equal(t, StatusNotApplicable, r.Status)
		assert.Equal(t, int32(0), b.opens)
	})

	t.Run("a fault the backend cannot inject leaves the scenario unverified and the store unopened", func(t *testing.T) {
		faults := `[{"operation":1,"phase":"commit","kind":"storage-error","details":{},"repeat":{"mode":"count","count":1},"injection":"replace-request"}]`
		b := &fakeBackend{name: "memory", injectable: false}
		r := runScenario(context.Background(), mkCase(t, scenarioBody("", faults, persistOK)), "memory", b)
		assert.Equal(t, StatusUnverified, r.Status)
		assert.NotEmpty(t, r.Reason)
		assert.Equal(t, int32(0), b.opens)
	})
}

func TestRunScenario_FaultFiring(t *testing.T) {
	ctx := context.Background()
	commitFault := func(op, count int) string {
		return `[{"operation":` + itoa(op) + `,"phase":"commit","kind":"storage-error","details":{},"repeat":{"mode":"count","count":` + itoa(count) + `},"injection":"replace-request"}]`
	}
	expectStorageErr := step("persistEvent", `{"event":"e1"}`, `{"result":"error","error":{"category":"storage"}}`)

	t.Run("a hook fault that fires exactly as counted succeeds", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, hookOnPersist: true}
		r := runScenario(ctx, mkCase(t, scenarioBody("", commitFault(1, 1), expectStorageErr)), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("a fault that never fired fails the scenario and names the fault", func(t *testing.T) {
		// The store never asks the hooks, so the commit fault fires 0 times.
		b := &fakeBackend{name: "memory", injectable: true, hookOnPersist: false}
		r := runScenario(ctx, mkCase(t, scenarioBody("", commitFault(1, 1), persistOK)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		assert.Contains(t, r.Reason, "commit")
	})

	t.Run("a fault bound to operation 2 does not fire during operation 1", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, hookOnPersist: true}
		expectErr2 := step("persistEvent", `{"event":"e2"}`, `{"result":"error","error":{"category":"storage"}}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", commitFault(2, 1), persistOK, expectErr2)), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("an sdk fault is applied by the backend through the injected faults", func(t *testing.T) {
		faults := `[{"operation":1,"phase":"retention-query","kind":"sdk-response","details":{"history_pages":[[1]]},"repeat":{"mode":"count","count":2},"injection":"replace-response"}]`
		b := &fakeBackend{name: "memory", injectable: true, sdkTries: 2}
		r := runScenario(ctx, mkCase(t, scenarioBody("", faults, persistOK)), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)

		b = &fakeBackend{name: "memory", injectable: true, sdkTries: 1}
		r = runScenario(ctx, mkCase(t, scenarioBody("", faults, persistOK)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		assert.Contains(t, r.Reason, "retention-query")
	})

	t.Run("a count of 1 holds even when the backend asks three times", func(t *testing.T) {
		faults := `[{"operation":1,"phase":"retention-query","kind":"sdk-response","details":{"history_pages":[[1]]},"repeat":{"mode":"count","count":1},"injection":"replace-response"}]`
		b := &fakeBackend{name: "memory", injectable: true, sdkTries: 3}
		r := runScenario(ctx, mkCase(t, scenarioBody("", faults, persistOK)), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("a step mismatch is reported in preference to a fire-count mismatch", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, hookOnPersist: false}
		r := runScenario(ctx, mkCase(t, scenarioBody("", commitFault(1, 1), expectStorageErr)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 1, *r.FailedStep)
	})
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestRunScenario_Observe(t *testing.T) {
	ctx := context.Background()

	t.Run("a matching history and notifications observation succeeds", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, active: []int64{1}, notifications: []string{"retention-failure"}}
		s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
			`{"history":{"active":[1],"marked":[],"absent":[9]},"notifications":["retention-failure"]}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("a history active mismatch fails the scenario", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, active: []int64{1}}
		s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
			`{"history":{"active":[2],"marked":[],"absent":[]}}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 1, *r.FailedStep)
	})

	t.Run("a history marked mismatch fails the scenario", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, active: []int64{1}, marked: []int64{2}}
		s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
			`{"history":{"active":[1],"marked":[],"absent":[]}}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
	})

	t.Run("an object-form marked observation compares its seq_nr", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, marked: []int64{2}}
		s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
			`{"history":{"active":[],"marked":[{"seq_nr":2,"ttl":4102444740}],"absent":[]}}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("an object-form marked observation with a different seq_nr fails the scenario", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, marked: []int64{3}}
		s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
			`{"history":{"active":[],"marked":[{"seq_nr":2,"ttl":4102444740}],"absent":[]}}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
	})

	t.Run("a seq_nr that must be absent but is still in the history fails the scenario", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, active: []int64{1}, marked: []int64{2}}
		s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
			`{"history":{"active":[1],"marked":[2],"absent":[2]}}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
	})

	t.Run("a notifications mismatch fails the scenario", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true, notifications: []string{"other"}}
		s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
			`{"notifications":["retention-failure"]}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
	})

	t.Run("the history is read for the aggregate id of the step", func(t *testing.T) {
		// The step persists event e1, whose aggregate id is the pair Order/1.
		b := &fakeBackend{name: "memory", injectable: true, active: []int64{1}}
		s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
			`{"history":{"active":[1],"marked":[],"absent":[]}}`)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
		assert.Equal(t, "Order-1", b.historyAid)
	})
}

func TestRunScenario_Clock(t *testing.T) {
	ctx := context.Background()

	t.Run("the scenario clock and each step clock reach the store", func(t *testing.T) {
		const scenarioClock = int64(4102444800) // 2100-01-01T00:00:00Z
		const stepClock = int64(5000000000)     // 2128-06-11T13:33:20Z
		clock := `"clock":{"epoch_seconds":` + strconv.FormatInt(scenarioClock, 10) + `},`
		body := scenarioBodyClock(clock, persistOK, stepClockStep(stepClock))
		b := &fakeBackend{name: "memory", injectable: true}
		r := runScenario(ctx, mkCase(t, body), "memory", b)
		require.Equal(t, StatusSuccess, r.Status, r.Reason)
		require.NotNil(t, b.last)
		assert.Equal(t, time.Unix(scenarioClock, 0).UTC(), b.last.openTime)
		require.Len(t, b.last.times, 2)
		assert.Equal(t, time.Unix(scenarioClock, 0).UTC(), b.last.times[0])
		assert.Equal(t, time.Unix(stepClock, 0).UTC(), b.last.times[1])
	})

	t.Run("a step clock without a scenario clock reaches the store for that operation", func(t *testing.T) {
		const stepClock = int64(5000000000) // 2128-06-11T13:33:20Z
		b := &fakeBackend{name: "memory", injectable: true}
		before := time.Now().Add(-time.Second)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", stepClockStep(stepClock))), "memory", b)
		after := time.Now().Add(time.Second)
		require.Equal(t, StatusSuccess, r.Status, r.Reason)
		require.NotNil(t, b.last)
		assert.True(t, b.last.openTime.After(before) && b.last.openTime.Before(after), "store creation should use real time until the first step clock")
		require.Len(t, b.last.times, 1)
		assert.Equal(t, time.Unix(stepClock, 0).UTC(), b.last.times[0])
	})

	t.Run("a zero scenario clock explicitly selects the Unix epoch", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		body := scenarioBodyClock(`"clock":{"epoch_seconds":0},`, persistOK)
		r := runScenario(ctx, mkCase(t, body), "memory", b)
		require.Equal(t, StatusSuccess, r.Status, r.Reason)
		require.NotNil(t, b.last)
		assert.Equal(t, time.Unix(0, 0).UTC(), b.last.openTime)
	})

	t.Run("a scenario with no clock leaves the real time in place", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		before := time.Now().Add(-time.Second)
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", persistOK)), "memory", b)
		require.Equal(t, StatusSuccess, r.Status, r.Reason)
		require.NotNil(t, b.last)
		assert.True(t, b.last.openTime.After(before), "open time should be close to now")
	})
}

func TestRunScenario_UnwiredObservation(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		key, observation string
	}{
		{"history", `{"history":{"active":[1],"marked":[],"absent":[]}}`},
		{"notifications", `{"notifications":["retention-failure"]}`},
	} {
		t.Run("an unwired initialization "+tc.key+" observation leaves the scenario unverified", func(t *testing.T) {
			init := `{"expect":{"result":"success"},"observe":` + tc.observation + `}`
			b := &fakeBackend{name: "memory", injectable: true}
			r := runScenario(ctx, mkCase(t, scenarioBody(init, "")), "memory", b)
			assert.Equal(t, StatusUnverified, r.Status)
			assert.Contains(t, r.Reason, "initialization.observe."+tc.key)
			assert.Equal(t, int32(0), b.opens)
		})
	}

	for _, key := range []string{"items", "requests", "no_requests_in_phases", "request_count", "minimum_request_count"} {
		t.Run("an unwired "+key+" observation leaves the scenario unverified and the store unopened", func(t *testing.T) {
			s := stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`,
				`{"`+key+`":[]}`)
			b := &fakeBackend{name: "memory", injectable: true}
			r := runScenario(ctx, mkCase(t, scenarioBody("", "", s)), "memory", b)
			assert.Equal(t, StatusUnverified, r.Status)
			assert.NotEmpty(t, r.Reason)
			assert.Equal(t, int32(0), b.opens)
		})
	}
}

func TestRunScenario_SeedAndMemoryTTL(t *testing.T) {
	ctx := context.Background()

	t.Run("the seed runs before the store is created", func(t *testing.T) {
		body := scenarioBodySeed(`"seed":{"items":[{"table":"journal","attributes":{}}]},`, persistOK)
		b := &fakeBackend{name: "memory", injectable: true}
		r := runScenario(ctx, mkCase(t, body), "memory", b)
		require.Equal(t, StatusSuccess, r.Status, r.Reason)
		assert.Equal(t, int32(1), b.seedCalls)
		assert.Equal(t, []string{"seed", "open"}, b.seq)
	})

	t.Run("a seed failure fails at step 0 without opening the store", func(t *testing.T) {
		body := scenarioBodySeed(`"seed":{"items":[{"table":"journal","attributes":{}}]},`, persistOK)
		b := &fakeBackend{name: "memory", injectable: true, seedErr: errors.New("seed rejected")}
		r := runScenario(ctx, mkCase(t, body), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 0, *r.FailedStep)
		assert.Equal(t, int32(0), b.opens)
	})

	t.Run("a scenario without seed does not seed", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		r := runScenario(ctx, mkCase(t, scenarioBody("", "", persistOK)), "memory", b)
		require.Equal(t, StatusSuccess, r.Status, r.Reason)
		assert.Equal(t, int32(0), b.seedCalls)
		assert.Equal(t, []string{"open"}, b.seq)
	})

	t.Run("a scenario that requires ttl is not-applicable on memory", func(t *testing.T) {
		c := mkCase(t, scenarioBody("", "", persistOK))
		c.Requires = []string{"ttl"}
		b := &fakeBackend{name: "memory", injectable: true}
		r := runScenario(ctx, c, "memory", b)
		assert.Equal(t, StatusNotApplicable, r.Status)
		assert.Equal(t, int32(0), b.opens)
	})
}

func stepClockStep(epochSeconds int64) string {
	return stepClock("persistEvent", `{"event":"e2"}`, `{"result":"success"}`, epochSeconds)
}

func scenarioBodyClock(clock string, steps ...string) string {
	body := scenarioBody("", "", steps...)
	return strings.Replace(body, `"steps":[`, clock+`"steps":[`, 1)
}

func scenarioBodySeed(seed string, steps ...string) string {
	body := scenarioBody("", "", steps...)
	return strings.Replace(body, `"steps":[`, seed+`"steps":[`, 1)
}

func TestRunScenarioCases_NoBackend(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)

	results := runScenarioCases(context.Background(), d.Scenarios, nil)
	require.NotEmpty(t, results)

	expected := 0
	for _, c := range d.Scenarios {
		expected += len(c.Backends)
	}
	assert.Len(t, results, expected, "one result per scenario and backend")

	for _, r := range results {
		assert.Contains(t, []Status{StatusUnverified, StatusNotApplicable}, r.Status, r.ID)
		assert.NotEmpty(t, r.Reason, r.ID)
		assert.NotEmpty(t, r.Backend, r.ID)
	}

	t.Run("classifyCases reports no success and no failure for unconnected backends", func(t *testing.T) {
		for _, r := range classifyCases(d) {
			if r.Backend == "" {
				continue
			}
			assert.NotEqual(t, StatusSuccess, r.Status, r.ID)
			assert.NotEqual(t, StatusFailure, r.Status, r.ID)
		}
	})
}

func TestJSONEqual(t *testing.T) {
	parse := func(s string) any {
		v, err := decodeStrictJSON([]byte(s))
		require.NoError(t, err)
		return v
	}
	t.Run("key order and whitespace are ignored", func(t *testing.T) {
		assert.True(t, jsonEqual(parse(`{"a":1,"b":[1,2]}`), parse(`{ "b": [1, 2], "a": 1 }`)))
	})
	t.Run("array order matters", func(t *testing.T) {
		assert.False(t, jsonEqual(parse(`[1,2]`), parse(`[2,1]`)))
	})
	t.Run("null, true and 1 are all different", func(t *testing.T) {
		assert.False(t, jsonEqual(parse(`true`), parse(`1`)))
		assert.False(t, jsonEqual(parse(`null`), parse(`0`)))
		assert.False(t, jsonEqual(parse(`null`), parse(`false`)))
	})
	t.Run("numbers are compared by value", func(t *testing.T) {
		assert.True(t, jsonEqual(parse(`1`), parse(`1.0`)))
		assert.False(t, jsonEqual(parse(`9007199254740993`), parse(`9007199254740992`)))
	})
	t.Run("strings are not normalised", func(t *testing.T) {
		assert.False(t, jsonEqual(parse(`"\u00e9"`), parse(`"e\u0301"`)))
	})
}
