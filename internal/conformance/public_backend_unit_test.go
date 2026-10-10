package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/require"
)

type ownedMemoryBackend struct {
	*publicBackend
	cleaned          int
	cleanedAtPrepare []int
}

func (b *ownedMemoryBackend) Prepare(ctx context.Context, plan *conformance.ScenarioPlan) (conformance.Backend, func() error, error) {
	b.cleanedAtPrepare = append(b.cleanedAtPrepare, b.cleaned)
	prepared, cleanup, err := b.publicBackend.Prepare(ctx, plan)
	return prepared, func() error { b.cleaned++; return cleanup() }, err
}

func TestPublicMemoryOpenFailureObservations(t *testing.T) {
	data, err := conformance.LoadData(filepath.Join("..", "..", "conformance"))
	require.NoError(t, err)
	var failed conformance.ScenarioCase
	for _, scenario := range data.Scenarios {
		if scenario.ID == "core-retention-zero" {
			failed = scenario
			break
		}
	}
	require.NotNil(t, failed.Plan)
	_, cause := eventstore.KeepLatest(0)
	require.Error(t, cause)
	classified := conformance.ClassifyError(cause).(*conformance.OperationError)
	history := map[string]any{"active": []any{}, "marked": []any{}, "absent": []any{}}
	for _, tc := range []struct {
		name    string
		observe map[string]any
		status  conformance.Status
	}{
		{"history", map[string]any{"history": history}, conformance.StatusFailure},
		{"notifications", map[string]any{"notifications": []any{}}, conformance.StatusSuccess},
		{"no observation", nil, conformance.StatusSuccess},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := *failed.Plan
			init := *plan.Init
			init.Observe = tc.observe
			plan.Init = &init
			first := failed
			first.Plan = &plan
			next := failed
			next.Plan = &conformance.ScenarioPlan{Store: conformance.StoreConfig{RetentionMode: "delete"}, Init: &conformance.InitPlan{Observe: map[string]any{"history": history, "notifications": []any{}}}}
			owner := &ownedMemoryBackend{publicBackend: &publicBackend{name: "memory"}}

			var results []conformance.CaseResult
			require.NotPanics(t, func() {
				results = conformance.RunBackend(t.Context(), &conformance.Data{Scenarios: []conformance.ScenarioCase{first, next}}, owner)
			})

			require.Len(t, results, 2)
			require.Equal(t, tc.status, results[0].Status, results[0].Reason)
			require.Equal(t, classified.Category+": "+classified.Message, results[0].Operations[0].Result)
			if tc.status == conformance.StatusFailure {
				require.Contains(t, results[0].Reason, "history")
				require.NotNil(t, results[0].FailedStep)
				require.Zero(t, *results[0].FailedStep)
			}
			require.Equal(t, conformance.StatusSuccess, results[1].Status, results[1].Reason)
			require.NotEmpty(t, results[1].Operations[0].Observation)
			require.Equal(t, 2, owner.cleaned)
			require.Equal(t, []int{0, 1}, owner.cleanedAtPrepare)
		})
	}
}

func TestHookSerializerPreservesPayloadAndFailureCause(t *testing.T) {
	hooks := testhook.New()
	serializer := &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeEvent, deserialize: testhook.PhaseDeserializeEvent}
	payload := json.RawMessage(`{"number":9007199254740993,"label":"日本語"}`)
	encoded, err := serializer.Serialize(payload)
	require.NoError(t, err)
	decoded, err := serializer.Deserialize(encoded)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
	cause := errors.New("declared serializer fault")
	hooks.OnFail(testhook.PhaseSerializeEvent, func(testhook.Point) error { return cause })
	_, err = serializer.Serialize(payload)
	require.ErrorIs(t, err, cause)
	hooks.OnFail(testhook.PhaseDeserializeEvent, func(testhook.Point) error { return cause })
	_, err = serializer.Deserialize(encoded)
	require.ErrorIs(t, err, cause)
}
