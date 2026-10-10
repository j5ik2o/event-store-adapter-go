package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// LayoutBackend verifies actual provisioned tables and publicly written items.
type LayoutBackend interface {
	RunLayout(context.Context, LayoutCase) CaseResult
}

// RunBackend executes all distributed cases and separately records exclusions.
// Applicability is derived from inputs, never from successful results.
func RunBackend(ctx context.Context, data *Data, backend Backend) []CaseResult {
	name := backend.Name()
	var results []CaseResult
	for _, c := range data.Values {
		res := CaseResult{ID: c.ID, Rules: nonNil(c.Rules), Backend: name}
		switch {
		case c.Operation == "fnv1a64":
			res.Status, res.Reason = StatusNotApplicable, reasonFnv1a64
		case c.TimePrecision == "milliseconds":
			res.Status, res.Reason = StatusNotApplicable, reasonMillis
		case c.Operation == "validateOccurredAt":
			res = runTimeCase(ctx, c, backend)
		default:
			res = runValueCase(c)
			res.Backend = name
		}
		results = append(results, res)
	}
	for _, c := range data.Scenarios {
		if !contains(c.Backends, name) {
			results = append(results, CaseResult{ID: c.ID, Rules: nonNil(c.Rules), Backend: name, Status: StatusNotApplicable, Reason: fmt.Sprintf("配布 backends=%v に %s がない", c.Backends, name)})
		} else {
			results = append(results, runScenario(ctx, c, name, backend))
		}
	}
	for _, c := range data.Layouts {
		res := CaseResult{ID: c.ID, Rules: nonNil(c.Rules), Backend: name}
		if name != "dynamodb" {
			res.Status, res.Reason = StatusNotApplicable, "配布の配置ケースは DynamoDB を対象とする"
		} else if b, ok := backend.(LayoutBackend); ok {
			res = b.RunLayout(ctx, c)
		} else {
			res.Status, res.Reason = StatusUnverified, reasonLayoutNoBackend
		}
		results = append(results, res)
	}
	return results
}

func runTimeCase(ctx context.Context, c ValueCase, b Backend) (res CaseResult) {
	res = CaseResult{ID: c.ID, Backend: b.Name(), Rules: nonNil(c.Rules), Expected: c.Expect}
	fail := func(err error) CaseResult { res.Status, res.Reason = StatusFailure, err.Error(); return res }
	plan := &ScenarioPlan{Store: StoreConfig{RetentionMode: "delete"}}
	if owner, ok := b.(ScenarioOwner); ok {
		prepared, cleanup, err := owner.Prepare(ctx, plan)
		if err != nil {
			return fail(err)
		}
		defer func() {
			if err := cleanup(); err != nil {
				res.Status, res.Reason = StatusFailure, err.Error()
			}
		}()
		b = prepared
	}
	hooks := testhook.New()
	store, err := b.Open(context.WithValue(ctx, operationContextKey{}, 0), plan.Store, Injection{Hooks: hooks, Plan: plan})
	res.Operations = append(res.Operations, OperationResult{Number: 0, Result: errorText(err)})
	if msg := compareObservation(ctx, 0, StepPlan{}, hooks, store, b, &res); msg != "" {
		return fail(fmt.Errorf("time initialization: %s", msg))
	}
	if err != nil {
		return fail(err)
	}
	if store == nil {
		return fail(fmt.Errorf("time initialization returned neither store nor error"))
	}
	defer store.Close()
	id := AggregateIDArg{TypeName: "ConformanceTime", Value: c.ID}
	n := c.Input.EventSeqNr.Int64()
	operation := 0
	record := func(step StepPlan, result any) error {
		res.Operations = append(res.Operations, OperationResult{Number: operation, Result: result})
		if msg := compareObservation(ctx, operation, step, hooks, store, b, &res); msg != "" {
			return fmt.Errorf("time operation %d: %s", operation, msg)
		}
		return nil
	}
	for seq := int64(1); seq < n; seq++ {
		operation++
		err := store.PersistEvent(context.WithValue(ctx, operationContextKey{}, operation), Event{AggregateID: id, SeqNr: seq, OccurredAt: time.Unix(0, 123000000).UTC(), Payload: []byte("{}")})
		if observationErr := record(StepPlan{Op: "persistEvent", AID: id}, errorText(err)); observationErr != nil {
			return fail(observationErr)
		}
		if err != nil {
			return fail(err)
		}
	}
	at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(c.Input.Raw["iso8601"]))
	if err != nil {
		return fail(err)
	}
	operation++
	err = store.PersistEvent(context.WithValue(ctx, operationContextKey{}, operation), Event{AggregateID: id, SeqNr: n, OccurredAt: at, Payload: []byte("{}")})
	if observationErr := record(StepPlan{Op: "persistEvent", AID: id}, errorText(err)); observationErr != nil {
		return fail(observationErr)
	}
	var actual any
	if err == nil {
		operation++
		events, readErr := store.GetEventsByIDSinceSeqNr(context.WithValue(ctx, operationContextKey{}, operation), id, 1)
		if observationErr := record(StepPlan{Op: "getEventsByIdSinceSeqNr", AID: id, SeqNr: big.NewInt(1)}, stepOutcome{op: "getEventsByIdSinceSeqNr", events: events, err: readErr}.describe()); observationErr != nil {
			return fail(observationErr)
		}
		if readErr != nil {
			return fail(readErr)
		}
		if len(events) != int(n) || !events[len(events)-1].OccurredAt.Equal(at) {
			return fail(fmt.Errorf("time readback differs from converted input %s", at))
		}
		actual = strconv.FormatInt(events[len(events)-1].OccurredAt.UnixNano(), 10)
	}
	res = compareValueResult(res, c.Expect, actual, err)
	res.Actual = map[string]any{"converted_input": at.Format(time.RFC3339Nano), "readback": actual, "result": res.Actual}
	return res
}

// ClassifyError uses the actual public error kind, retaining unclassified errors.
func ClassifyError(err error) error { return coreOperationError(err) }

// ApplicableCases derives the required set independently from execution results.
func ApplicableCases(data *Data, backend string) []string {
	var ids []string
	for _, c := range data.Values {
		if c.Operation != "fnv1a64" && c.TimePrecision != "milliseconds" {
			ids = append(ids, c.ID)
		}
	}
	for _, c := range data.Scenarios {
		if contains(c.Backends, backend) && c.TimePrecision != "milliseconds" && !(backend == "memory" && contains(c.Requires, "ttl")) {
			ids = append(ids, c.ID)
		}
	}
	if backend == "dynamodb" {
		for _, c := range data.Layouts {
			ids = append(ids, c.ID)
		}
	}
	return ids
}

// CheckRequiredCoverage rejects omissions and additions in the fixed required set.
func CheckRequiredCoverage(data *Data, required RequiredList) error {
	for _, name := range requiredBackends {
		toAny := func(ids []string) []any {
			a := make([]any, len(ids))
			for i, id := range ids {
				a[i] = id
			}
			return a
		}
		if !unorderedEqual(toAny(ApplicableCases(data, name)), toAny(required[name])) {
			return fmt.Errorf("required %s does not equal all applicable distributed cases", name)
		}
	}
	return nil
}

// JSONValue converts observations to the same lossless JSON value model as data.
func JSONValue(value any) (any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return decodeStrictJSON(raw)
}
