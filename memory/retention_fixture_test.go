package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These are direct real-Store tests. They do not register a conformance backend
// or add successes to the conformance runner's required cases.
func TestRetentionDistributedCasesAgainstRealStore(t *testing.T) {
	data, err := conformance.LoadData("../conformance")
	require.NoError(t, err)
	for _, id := range []string{
		"core-retention-zero", "core-retention-delete-1", "core-retention-delete-2",
		"core-retention-default-current-only", "core-retention-failure-after-commit",
		"core-retention-query-failure", "core-gap-event", "core-snapshot-mismatch-2",
	} {
		t.Run(id, func(t *testing.T) {
			var scenario *conformance.ScenarioCase
			for i := range data.Scenarios {
				if data.Scenarios[i].ID == id {
					scenario = &data.Scenarios[i]
					break
				}
			}
			require.NotNil(t, scenario)
			plan := scenario.Plan
			require.NotNil(t, plan)
			require.Empty(t, plan.Seed)
			require.Equal(t, "delete", plan.Store.RetentionMode)
			log := captureRetentionLog(t)
			var notifications []string
			var notificationErrors []error
			opts := []eventstore.Option{
				eventstore.WithRetentionMode(eventstore.RetentionDelete),
				eventstore.WithRetentionFailureHandler(func(_ context.Context, err error) {
					notifications = append(notifications, "retention-failure")
					notificationErrors = append(notificationErrors, err)
				}),
			}
			var setupErr error
			if plan.Store.RetentionCount != nil {
				var count eventstore.RetentionCount
				count, setupErr = eventstore.KeepLatest(int(*plan.Store.RetentionCount))
				if setupErr == nil {
					opts = append(opts, eventstore.WithRetentionCount(count))
				}
			}
			var store *Store
			if setupErr == nil {
				store, setupErr = NewStore(opts...)
			}
			if plan.Init != nil && plan.Init.Error != nil {
				require.Nil(t, store)
				assertFixtureError(t, plan.Init.Error, setupErr)
				require.Empty(t, plan.Steps)
				t.Logf("%s: initialization rejected by real configuration boundary", id)
				return
			}
			require.NoError(t, setupErr)
			entry, err := New(store, eventstore.NewJSONSerializer[any](), eventstore.NewJSONSerializer[any]())
			require.NoError(t, err)
			store.hooks = testhook.New()
			store.hooks.ProvideHistory(func(aid string) (testhook.History, error) {
				_, actual := store.observeState(aid)
				history := testhook.History{Active: []int64{}, Marked: []int64{}}
				if actual != nil {
					for _, snapshot := range actual.history {
						history.Active = append(history.Active, int64(snapshot.seqNr))
					}
				}
				return history, nil
			})
			operation := 0
			fired := make([]int, len(plan.Faults))
			causes := make([]*testhook.InjectedError, len(plan.Faults))
			for i, fault := range plan.Faults {
				require.Equal(t, "count", fault.Repeat)
				phase, valid := testhook.ParsePhase(fault.Phase)
				require.True(t, valid)
				require.Contains(t, []testhook.Phase{testhook.PhaseRetentionQuery, testhook.PhaseRetentionDelete}, phase)
				switch fault.Kind {
				case "storage-error":
					require.Equal(t, "replace-request", fault.Injection)
					message, ok := fault.Details["message"].(string)
					require.True(t, ok)
					causes[i] = &testhook.InjectedError{Phase: phase, Message: message}
					store.hooks.OnFail(phase, func(testhook.Point) error {
						if operation != fault.Operation || fired[i] >= fault.Count {
							return nil
						}
						fired[i]++
						return causes[i]
					})
				case "sdk-response":
					require.Equal(t, testhook.PhaseRetentionQuery, phase)
					require.Equal(t, "replace-response", fault.Injection)
				default:
					t.Fatalf("unsupported fault in selected case: %s", fault.Kind)
				}
			}
			store.hooks.OnHistoryPages(func(string, int64) ([][]int64, bool, error) {
				for i, fault := range plan.Faults {
					if fault.Kind != "sdk-response" || operation != fault.Operation || fired[i] >= fault.Count {
						continue
					}
					pagesJSON, err := json.Marshal(fault.Details["history_pages"])
					require.NoError(t, err)
					var pages [][]int64
					require.NoError(t, json.Unmarshal(pagesJSON, &pages))
					fired[i]++
					return pages, true, nil
				}
				return nil, false, nil
			})
			for i, step := range plan.Steps {
				operation = i + 1
				aid := step.AID
				var actualSnapshot *eventstore.SnapshotRead[any]
				var actualEvents []eventstore.EventEnvelope[any]
				var operationErr error
				switch step.Op {
				case "persistEvent", "persistEventAndSnapshot":
					fixture, exists := plan.Events[step.Event]
					require.True(t, exists)
					aid = fixture.AID
					id, err := eventstore.NewAggregateID(aid.TypeName, aid.Value)
					require.NoError(t, err)
					occurredAt, err := time.Parse(time.RFC3339Nano, fixture.OccurredAt)
					require.NoError(t, err)
					event, err := eventstore.NewEventEnvelope(id, eventstore.SeqNr(fixture.SeqNr.Int64()), occurredAt, fixture.Payload, eventstore.WithManifest(fixture.Manifest))
					require.NoError(t, err)
					if step.Op == "persistEvent" {
						operationErr = entry.PersistEvent(t.Context(), event)
					} else {
						savedSnapshot, exists := plan.Snaps[step.Snapshot]
						require.True(t, exists)
						snapshot, err := eventstore.NewSnapshotEnvelope(savedSnapshot.Aggregate, eventstore.SeqNr(savedSnapshot.SeqNr.Int64()), eventstore.WithManifest(savedSnapshot.Manifest))
						require.NoError(t, err)
						operationErr = entry.PersistEventAndSnapshot(t.Context(), event, snapshot)
					}
				case "getLatestSnapshotById", "getEventsByIdSinceSeqNr":
					id, err := eventstore.NewAggregateID(aid.TypeName, aid.Value)
					require.NoError(t, err)
					if step.Op == "getLatestSnapshotById" {
						actualSnapshot, operationErr = entry.GetLatestSnapshotByID(t.Context(), id)
					} else {
						actualEvents, operationErr = entry.GetEventsByIDSinceSeqNr(t.Context(), id, eventstore.SeqNr(step.SeqNr.Int64()))
					}
				default:
					t.Fatalf("unsupported operation in selected case: %s", step.Op)
				}
				// Expectations are consumed only after the real operation has completed.
				if step.Expect.Kind == "error" {
					assertFixtureError(t, step.Expect.Error, operationErr)
				} else {
					require.NoError(t, operationErr)
					switch step.Expect.Kind {
					case "success":
					case "none":
						require.Nil(t, actualSnapshot)
					case "snapshot":
						require.NotNil(t, actualSnapshot)
						assert.Equal(t, step.Expect.HeadSeqNr.Int64(), int64(actualSnapshot.HeadSeqNr))
						if step.Expect.Snapshot == "" {
							assert.Nil(t, actualSnapshot.Snapshot)
						} else {
							require.NotNil(t, actualSnapshot.Snapshot)
							fixture := plan.Snaps[step.Expect.Snapshot]
							assert.Equal(t, fixture.SeqNr.Int64(), int64(actualSnapshot.Snapshot.SeqNr()))
							assert.Equal(t, fixture.Manifest, actualSnapshot.Snapshot.Manifest())
							assertFixtureJSON(t, fixture.Aggregate, actualSnapshot.Snapshot.Aggregate())
						}
					case "events":
						require.Len(t, actualEvents, len(step.Expect.Events))
						for j, name := range step.Expect.Events {
							fixture := plan.Events[name]
							actual := actualEvents[j]
							assert.Equal(t, fixture.AID.TypeName+"-"+fixture.AID.Value, actual.AggregateID())
							assert.Equal(t, fixture.SeqNr.Int64(), int64(actual.SeqNr()))
							occurredAt, err := time.Parse(time.RFC3339Nano, fixture.OccurredAt)
							require.NoError(t, err)
							assert.Equal(t, occurredAt, actual.OccurredAt())
							assert.Equal(t, fixture.Manifest, actual.Manifest())
							assertFixtureJSON(t, fixture.Payload, actual.Payload())
						}
					default:
						t.Fatalf("unsupported expectation in selected case: %s", step.Expect.Kind)
					}
				}
				for key, expected := range step.Observe {
					switch key {
					case "history":
						history, err := store.hooks.History(aid.TypeName + "-" + aid.Value)
						require.NoError(t, err)
						values := expected.(map[string]any)
						assertFixtureJSON(t, values["active"], history.Active)
						assertFixtureJSON(t, values["marked"], history.Marked)
						absentJSON, err := json.Marshal(values["absent"])
						require.NoError(t, err)
						var absent []int64
						require.NoError(t, json.Unmarshal(absentJSON, &absent))
						for _, seqNr := range absent {
							assert.NotContains(t, history.Active, seqNr)
							assert.NotContains(t, history.Marked, seqNr)
						}
						t.Logf("step %d actual history active=%v marked=%v", operation, history.Active, history.Marked)
					case "notifications":
						assertFixtureJSON(t, expected, notifications)
					default:
						t.Fatalf("unsupported observation in selected case: %s", key)
					}
				}
			}
			for i, fault := range plan.Faults {
				assert.Equal(t, fault.Count, fired[i], "fault %d", i)
				if causes[i] != nil {
					require.Len(t, notificationErrors, 1)
					requireKind(t, notificationErrors[0], eventstore.KindStorage)
					assert.ErrorIs(t, notificationErrors[0], causes[i])
				}
			}
			assert.Len(t, log.records, len(notificationErrors))
			t.Logf("%s: %d real operations compared to distributed expect/observe/error.rule; not a full conformance success", id, len(plan.Steps))
		})
	}
}

func assertFixtureError(t *testing.T, expected *conformance.ErrorExpect, actual error) {
	t.Helper()
	require.NotNil(t, expected)
	require.Error(t, actual)
	kind, known := map[string]eventstore.Kind{
		"configuration":      eventstore.KindConfiguration,
		"contract-violation": eventstore.KindContractViolation,
	}[expected.Category]
	require.True(t, known, "unexpected category in selected case: %s", expected.Category)
	requireKind(t, actual, kind)
	if expected.Rule != "" {
		var violation *eventstore.ContractViolationError
		require.True(t, errors.As(actual, &violation))
		assert.Equal(t, expected.Rule, violation.Rule)
	}
	for _, part := range expected.MustContain {
		assert.Contains(t, actual.Error(), part)
	}
	for _, part := range expected.MustNotContain {
		assert.NotContains(t, actual.Error(), part)
	}
	t.Logf("actual category=%s rule=%s error=%s", expected.Category, expected.Rule, actual)
}

func assertFixtureJSON(t *testing.T, expected, actual any) {
	t.Helper()
	want, err := json.Marshal(expected)
	require.NoError(t, err)
	got, err := json.Marshal(actual)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got), fmt.Sprint(actual))
}
