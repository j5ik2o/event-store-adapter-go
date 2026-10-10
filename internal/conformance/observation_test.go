package conformance

import (
	"context"
	"encoding/json"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

type observedBackend struct {
	*fakeBackend
	actual map[string]any
}

func (b *observedBackend) Observe(context.Context, int, StepPlan) (map[string]any, error) {
	return b.actual, nil
}

func TestObservationComparison(t *testing.T) {
	parse := func(raw string) map[string]any {
		value, err := decodeStrictJSON([]byte(raw))
		require.NoError(t, err)
		return value.(map[string]any)
	}
	for _, tc := range []struct {
		name, expected, actual string
		success                bool
	}{
		{"complete item", `{"items":[{"table":"head","attributes":{"events":"L"},"values":{"events":[{"seq_nr":"9007199254740993"}]},"nested_attributes":{"events[0]":{"seq_nr":"N","payload":"B"}},"binary_json":{"events[0].payload":{"number":9007199254740993}}}]}`, `{"items":[{"table":"head","attributes":{"events":"L"},"values":{"events":[{"seq_nr":"9007199254740993"}]},"nested_attributes":{"events[0]":{"seq_nr":"N","payload":"B"}},"binary_json":{"events[0].payload":{"number":9007199254740993}}}]}`, true},
		{"extra nested attribute", `{"items":[{"attributes":{"events":"L"},"nested_attributes":{"events[0]":{"seq_nr":"N"}}}]}`, `{"items":[{"attributes":{"events":"L"},"nested_attributes":{"events[0]":{"seq_nr":"N","version":"N"}}}]}`, false},
		{"extra payload value", `{"items":[{"attributes":{"payload":"B"},"binary_json":{"payload":{"number":1}}}]}`, `{"items":[{"attributes":{"payload":"B"},"binary_json":{"payload":{"number":1,"extra":true}}}]}`, false},
		{"generated binding mismatch", `{"items":[{"attributes":{"store_id":"S"},"bindings":{"store_id":"generated-store-id"}},{"attributes":{"store_id":"S"},"bindings":{"store_id":"generated-store-id"}}]}`, `{"items":[{"attributes":{"store_id":"S"},"values":{"store_id":"first"}},{"attributes":{"store_id":"S"},"values":{"store_id":"second"}}]}`, false},
		{"ordered distinct requests", `{"requests":[{"api":"Query"},{"api":"Query"}]}`, `{"requests":[{"api":"Query"},{"api":"Query"}]}`, true},
		{"one request cannot satisfy two", `{"requests":[{"api":"Query"},{"api":"Query"}]}`, `{"requests":[{"api":"Query"}]}`, false},
		{"wrong request order", `{"requests":[{"api":"Query"},{"api":"BatchGetItem"}]}`, `{"requests":[{"api":"BatchGetItem"},{"api":"Query"}]}`, false},
		{"counts and forbidden phase", `{"request_count":{"commit":1},"minimum_request_count":{"read-events":2},"no_requests_in_phases":["retention-query"]}`, `{"request_count":{"commit":1,"read-events":2}}`, true},
		{"unexpected phase", `{"no_requests_in_phases":["commit"]}`, `{"request_count":{"commit":1}}`, false},
		{"wrong exact count", `{"request_count":{"commit":1}}`, `{"request_count":{"commit":2}}`, false},
		{"too few pages", `{"minimum_request_count":{"read-events":2}}`, `{"request_count":{"read-events":1}}`, false},
		{"history and notifications", `{"history":{"active":[2,1],"marked":[{"seq_nr":3,"ttl":4102444860}],"absent":[4]},"notifications":["retention-failure"]}`, `{"history":{"active":[1,2],"marked":[{"ttl":4102444860,"seq_nr":3}]},"notifications":["retention-failure"]}`, true},
		{"history absent is present", `{"history":{"active":[1],"marked":[],"absent":[1]}}`, `{"history":{"active":[1],"marked":[]}}`, false},
		{"unknown observation", `{"unknown":true}`, `{"unknown":true}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &observedBackend{fakeBackend: &fakeBackend{}, actual: parse(tc.actual)}
			result := CaseResult{Operations: []OperationResult{{Number: 1}}}
			message := compareObservation(context.Background(), 1, StepPlan{Observe: parse(tc.expected)}, testhook.New(), nil, b, &result)
			require.Equal(t, tc.success, message == "", message)
			require.Equal(t, b.actual, result.Operations[0].Observation, "actual observation must survive a comparison failure")
		})
	}
	require.False(t, jsonSubset(json.Number("1"), "1"))
	require.True(t, jsonSubset(parse(`{"keys":["a","b"]}`), parse(`{"keys":["b","a"],"extra":true}`)))
	require.False(t, unorderedEqual([]any{"a"}, []any{"a", "b"}))
}

type ownedBackend struct {
	*fakeBackend
	cleaned          int
	cleanupErr       error
	preparedBackends []*fakeBackend
	cleanedAtPrepare []int
}

func (b *ownedBackend) Prepare(context.Context, *ScenarioPlan) (Backend, func() error, error) {
	b.cleanedAtPrepare = append(b.cleanedAtPrepare, b.cleaned)
	prepared := b.fakeBackend
	if len(b.preparedBackends) > 0 {
		prepared, b.preparedBackends = b.preparedBackends[0], b.preparedBackends[1:]
	}
	return prepared, func() error { b.cleaned++; return b.cleanupErr }, nil
}

func TestRunBackendOpenFailureNotifications(t *testing.T) {
	cause := &OperationError{Category: "configuration", Message: "retention_count must be positive"}
	failed := mkCase(t, scenarioBody(`{"expect":{"error":{"category":"configuration"}},"observe":{"notifications":[]}}`, ""))
	next := mkCase(t, scenarioBody(`{"expect":{"result":"success"},"observe":{"notifications":["retention-failure"]}}`, "",
		stepObserve("persistEvent", `{"event":"e1"}`, `{"result":"success"}`, `{"history":{"active":[1],"marked":[],"absent":[]},"notifications":["retention-failure"]}`)))
	first := &fakeBackend{name: "memory", openErr: cause}
	second := &fakeBackend{name: "memory", active: []int64{1}, notifications: []string{"retention-failure"}}
	owner := &ownedBackend{fakeBackend: first, preparedBackends: []*fakeBackend{first, second}}

	var results []CaseResult
	require.NotPanics(t, func() {
		results = RunBackend(t.Context(), &Data{Scenarios: []ScenarioCase{failed, next}}, owner)
	})

	require.Len(t, results, 2)
	require.Equal(t, StatusFailure, results[0].Status)
	require.Contains(t, results[0].Reason, "notifications")
	require.NotNil(t, results[0].FailedStep)
	require.Zero(t, *results[0].FailedStep)
	require.Equal(t, []OperationResult{{Number: 0, Result: "configuration: " + cause.Message}}, results[0].Operations)
	require.Equal(t, StatusSuccess, results[1].Status, results[1].Reason)
	require.Equal(t, []string{"persistEvent"}, second.calls)
	require.Equal(t, 2, owner.cleaned)
	require.Equal(t, []int{0, 1}, owner.cleanedAtPrepare, "the failed case must be cleaned before preparing the next case")
}
func TestScenarioOwnershipAndFaultReport(t *testing.T) {
	for _, failOpen := range []bool{false, true} {
		t.Run("open error="+strconv.FormatBool(failOpen), func(t *testing.T) {
			fake := &fakeBackend{name: "memory", injectable: true}
			if failOpen {
				fake.openErr = &OperationError{Category: "storage", Message: "open failed"}
			}
			b := &ownedBackend{fakeBackend: fake}
			c := mkCase(t, scenarioBody("", `[{"operation":1,"phase":"commit","kind":"storage-error","repeat":{"mode":"count","count":1}}]`, persistOK))
			result := runScenario(context.Background(), c, "memory", b)
			require.Equal(t, 1, b.cleaned)
			require.Equal(t, StatusFailure, result.Status)
			require.Len(t, result.Faults, 1)
			require.Equal(t, 0, result.Faults[0].Declaration)
			require.Zero(t, result.Faults[0].Applied)
			require.True(t, result.Faults[0].Unfired)
		})
	}
}
func TestJSONValuePreservesIntegers(t *testing.T) {
	value, err := JSONValue(map[string]any{"number": int64(9007199254740993)})
	require.NoError(t, err)
	require.Equal(t, json.Number("9007199254740993"), value.(map[string]any)["number"])
	_, err = JSONValue(make(chan int))
	require.Error(t, err)
}

func TestScenarioInputFixtureAccess(t *testing.T) {
	plan := &ScenarioPlan{
		Events: map[string]eventFixture{"input": {AID: AggregateIDArg{TypeName: "Order", Value: "9"}, SeqNr: big.NewInt(2), OccurredAt: "2026-10-05T00:00:00.123456789Z", Payload: map[string]any{"number": json.Number("9007199254740993")}, Manifest: "event"}},
		Snaps:  map[string]snapshotFixture{"input": {SeqNr: big.NewInt(2), Aggregate: map[string]any{"total": json.Number("9007199254740993")}, Manifest: "snapshot"}},
	}
	ev, err := plan.EventFixture("input")
	require.NoError(t, err)
	require.Equal(t, int64(2), ev.SeqNr)
	require.Equal(t, "2026-10-05T00:00:00.123456789Z", ev.OccurredAt.Format(time.RFC3339Nano))
	require.Equal(t, `{"number":9007199254740993}`, string(ev.Payload))
	sn, err := plan.SnapshotFixture("input")
	require.NoError(t, err)
	require.Equal(t, "snapshot", sn.Manifest)
	require.Equal(t, `{"total":9007199254740993}`, string(sn.Aggregate))
	_, err = plan.EventFixture("unknown")
	require.ErrorContains(t, err, "unknown event fixture")
	_, err = plan.SnapshotFixture("unknown")
	require.ErrorContains(t, err, "unknown snapshot fixture")
	require.Zero(t, OperationNumber(t.Context()))
	require.Equal(t, 7, OperationNumber(context.WithValue(t.Context(), operationContextKey{}, 7)))
}
