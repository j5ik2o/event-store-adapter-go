package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	awsdb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/dynamodb"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/storeoptions"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/j5ik2o/event-store-adapter-go/v5/memory"
)

type publicBackend struct {
	name           string
	environment    *dynamodbtest.Environment
	tables         *dynamodbtest.Tables
	recorder       *dynamodbtest.Recorder
	responses      *dynamodbtest.ResponseRecorder
	reads          *dynamodbtest.ReadObserver
	store          *publicStore
	mu             sync.Mutex
	notifications  map[int][]string
	waits          map[int][]time.Duration
	operation      int
	interleaved    map[int][]any
	partialDeletes map[int][]any
}

func (b *publicBackend) Name() string { return b.name }
func (b *publicBackend) Injectable(f conformance.FaultSpec) bool {
	if strings.HasPrefix(f.Phase, "serialize-") || strings.HasPrefix(f.Phase, "deserialize-") {
		return f.Kind == "serialization-error"
	}
	if b.name == "memory" {
		return f.Kind == "storage-error" || f.Phase == "retention-query" && f.Kind == "sdk-response"
	}
	return f.Kind == "storage-error" || f.Kind == "sdk-error" || f.Kind == "sdk-response" || f.Kind == "read-interleave"
}

func (b *publicBackend) Prepare(ctx context.Context, plan *conformance.ScenarioPlan) (conformance.Backend, func() error, error) {
	fresh := &publicBackend{name: b.name, environment: b.environment, notifications: map[int][]string{}, waits: map[int][]time.Duration{}, interleaved: map[int][]any{}, partialDeletes: map[int][]any{}}
	cleanup := func() error { return nil }
	if b.name == "dynamodb" {
		tables, err := b.environment.CreateTables(ctx, plan.Store.RetentionMode == "ttl")
		if err != nil {
			return nil, nil, err
		}
		fresh.tables = tables
		cleanup = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return tables.Close(ctx)
		}
	}
	return fresh, cleanup, nil
}

func (b *publicBackend) Seed(ctx context.Context, items []map[string]any) error {
	if b.tables == nil {
		return fmt.Errorf("Memory scenarios do not support physical seed items")
	}
	return b.tables.PutSeedItems(ctx, items)
}

func (b *publicBackend) options(cfg conformance.StoreConfig, hooks *testhook.Hooks) ([]eventstore.Option, error) {
	retention := eventstore.NoRetention()
	if cfg.RetentionCount != nil {
		var err error
		retention, err = eventstore.KeepLatest(int(*cfg.RetentionCount))
		if err != nil {
			return nil, err
		}
	}
	mode := eventstore.RetentionDelete
	if cfg.RetentionMode == "ttl" {
		mode = eventstore.RetentionTTL
	}
	opts := []eventstore.Option{eventstore.WithRetentionCount(retention), eventstore.WithRetentionMode(mode), eventstore.WithRetentionFailureHandler(func(ctx context.Context, _ error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		n := conformance.OperationNumber(ctx)
		b.notifications[n] = append(b.notifications[n], "retention-failure")
	}), func(o *storeoptions.Options) error { o.Hooks = hooks; return nil }}
	if cfg.TTLGraceSeconds != nil {
		opts = append(opts, eventstore.WithTTLGraceSeconds(*cfg.TTLGraceSeconds))
	}
	return opts, nil
}

func (b *publicBackend) config(cfg conformance.StoreConfig) dynamodb.Config {
	journal, _ := b.tables.TableName("journal")
	snapshot, _ := b.tables.TableName("snapshot")
	head, _ := b.tables.TableName("head")
	config := dynamodb.Config{JournalTableName: journal, SnapshotTableName: snapshot, HeadTableName: head, SnapshotHistoryIndexName: b.tables.HistoryIndexName()}
	if cfg.RetryLimit != nil {
		n := int(*cfg.RetryLimit)
		config.ConfigurationReadRetryLimit = &n
	}
	return config
}

func (b *publicBackend) Open(ctx context.Context, cfg conformance.StoreConfig, injection conformance.Injection) (conformance.Store, error) {
	hooks := injection.Hooks
	hooks.SetSleeper(func(d time.Duration) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.waits[b.operation] = append(b.waits[b.operation], d)
	})
	opts, err := b.options(cfg, hooks)
	if err != nil {
		return nil, conformance.ClassifyError(err)
	}
	events := &payloadTreeSerializer{raw: &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeEvent, deserialize: testhook.PhaseDeserializeEvent}}
	snapshots := &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeSnapshot, deserialize: testhook.PhaseDeserializeSnapshot}
	var store eventstore.EventStore[any, json.RawMessage]
	var state *memory.Store
	if b.name == "memory" {
		state, err = memory.NewStore(opts...)
		if err == nil {
			store, err = memory.New[any, json.RawMessage](state, events, snapshots)
		}
	} else {
		b.recorder = dynamodbtest.NewRecorder()
		b.responses = &dynamodbtest.ResponseRecorder{}
		b.reads = &dynamodbtest.ReadObserver{}
		b.connectReadPlans(cfg, injection)
		partial := func(input *awsdb.BatchWriteItemInput, output *awsdb.BatchWriteItemOutput, err error) {
			b.partialDeletes[b.operation] = append(b.partialDeletes[b.operation], map[string]any{"request": input, "response": output, "error": fmt.Sprint(err)})
		}
		client := b.environment.NewClient(b.recorder.APIOption, b.tables.ConfigurationAPIOption(injection), b.tables.CommitAPIOption(injection), b.tables.RetentionAPIOption(injection, partial), b.tables.ReadFailureAPIOption(injection), b.reads.APIOption, b.responses.APIOption)
		store, err = dynamodb.New(dynamodbtest.WithOperation(ctx, 0), client, b.config(cfg), events, snapshots, opts...)
	}
	if err != nil {
		return nil, conformance.ClassifyError(err)
	}
	b.store = &publicStore{actual: store, state: state, backend: b, hooks: hooks, written: map[int]any{}, read: map[int][]any{}}
	return b.store, nil
}

// CreateStore creates a second memory store in the middle of a scenario. A new store has its own
// records. A shared store is connected to the Store of the store it shares with through the public
// memory.New, so that both use the same records and the same lock. b.store stays the first store.
func (b *publicBackend) CreateStore(ctx context.Context, cfg conformance.StoreConfig, injection conformance.Injection, shared conformance.Store) (conformance.Store, error) {
	if b.name != "memory" {
		return nil, fmt.Errorf("createStore is only defined for the memory backend")
	}
	hooks := injection.Hooks
	var state *memory.Store
	if shared != nil {
		other, ok := shared.(*publicStore)
		if !ok || other.state == nil {
			return nil, fmt.Errorf("shared store is not a memory store")
		}
		state = other.state
	} else {
		opts, err := b.options(cfg, nil)
		if err != nil {
			return nil, conformance.ClassifyError(err)
		}
		if state, err = memory.NewStore(opts...); err != nil {
			return nil, conformance.ClassifyError(err)
		}
	}
	events := &payloadTreeSerializer{raw: &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeEvent, deserialize: testhook.PhaseDeserializeEvent}}
	snapshots := &hookSerializer{hooks: hooks, serialize: testhook.PhaseSerializeSnapshot, deserialize: testhook.PhaseDeserializeSnapshot}
	store, err := memory.New[any, json.RawMessage](state, events, snapshots)
	if err != nil {
		return nil, conformance.ClassifyError(err)
	}
	return &publicStore{actual: store, state: state, backend: b, hooks: hooks, written: map[int]any{}, read: map[int][]any{}}, nil
}

type hookSerializer struct {
	hooks                  *testhook.Hooks
	serialize, deserialize testhook.Phase
}

func (s *hookSerializer) Serialize(value json.RawMessage) ([]byte, error) {
	if err := s.hooks.Fail(testhook.Point{Phase: s.serialize}); err != nil {
		return nil, err
	}
	return eventstore.NewJSONSerializer[json.RawMessage]().Serialize(value)
}
func (s *hookSerializer) Deserialize(value []byte) (json.RawMessage, error) {
	if err := s.hooks.Fail(testhook.Point{Phase: s.deserialize}); err != nil {
		return nil, err
	}
	return eventstore.NewJSONSerializer[json.RawMessage]().Deserialize(value)
}

// payloadTreeSerializer serializes an event payload given as a decoded JSON tree (numbers are
// json.Number, so no precision is lost) through the same fault hooks as hookSerializer.
type payloadTreeSerializer struct{ raw *hookSerializer }

func (s *payloadTreeSerializer) Serialize(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, &eventstore.SerializationError{Cause: err}
	}
	return s.raw.Serialize(json.RawMessage(encoded))
}

func (s *payloadTreeSerializer) Deserialize(value []byte) (any, error) {
	decoded, err := s.raw.Deserialize(value)
	if err != nil {
		return nil, err
	}
	tree, err := decodePayloadTree(decoded)
	if err != nil {
		return nil, &eventstore.SerializationError{Cause: err}
	}
	return tree, nil
}

func decodePayloadTree(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var tree any
	if err := decoder.Decode(&tree); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing data after the payload")
	}
	return tree, nil
}

type fixtureID struct{ parts conformance.AggregateIDArg }

func (id fixtureID) TypeName() string { return id.parts.TypeName }
func (id fixtureID) Value() string    { return id.parts.Value }

type publicStore struct {
	actual  eventstore.EventStore[any, json.RawMessage]
	state   *memory.Store
	backend *publicBackend
	hooks   *testhook.Hooks
	mu      sync.Mutex
	// written is the payload tree that each operation passed to the library, and read is the trees
	// that each read operation got back. They are the objects the payload mutations change.
	written map[int]any
	read    map[int][]any
}

// publicEvent returns the event for the library and the payload tree inside it.
func publicEvent(ev conformance.Event) (eventstore.EventEnvelope[any], any, error) {
	tree, err := decodePayloadTree(ev.Payload)
	if err != nil {
		return eventstore.EventEnvelope[any]{}, nil, err
	}
	event, err := eventstore.NewEventEnvelope[any](fixtureID{parts: ev.AggregateID}, eventstore.SeqNr(ev.SeqNr), ev.OccurredAt, tree, eventstore.WithManifest(ev.Manifest))
	return event, tree, err
}
func publicSnapshot(sn conformance.Snapshot) (eventstore.SnapshotEnvelope[json.RawMessage], error) {
	return eventstore.NewSnapshotEnvelope(json.RawMessage(sn.Aggregate), eventstore.SeqNr(sn.SeqNr), eventstore.WithManifest(sn.Manifest))
}
func (s *publicStore) context(ctx context.Context) context.Context {
	s.backend.mu.Lock()
	s.backend.operation = conformance.OperationNumber(ctx)
	s.backend.mu.Unlock()
	return dynamodbtest.WithOperation(ctx, conformance.OperationNumber(ctx))
}
func (s *publicStore) PersistEvent(ctx context.Context, ev conformance.Event) error {
	ctx = s.context(ctx)
	event, tree, err := publicEvent(ev)
	if err == nil {
		s.recordWritten(ctx, tree)
		err = s.actual.PersistEvent(ctx, event)
	}
	return conformance.ClassifyError(err)
}
func (s *publicStore) PersistEventAndSnapshot(ctx context.Context, ev conformance.Event, sn conformance.Snapshot) error {
	ctx = s.context(ctx)
	event, tree, err := publicEvent(ev)
	if err != nil {
		return conformance.ClassifyError(err)
	}
	snapshot, err := publicSnapshot(sn)
	if err == nil {
		s.recordWritten(ctx, tree)
		err = s.actual.PersistEventAndSnapshot(ctx, event, snapshot)
	}
	return conformance.ClassifyError(err)
}
func (s *publicStore) GetLatestSnapshotByID(ctx context.Context, aid conformance.AggregateIDArg) (conformance.SnapshotRead, error) {
	result, err := s.actual.GetLatestSnapshotByID(s.context(ctx), fixtureID{parts: aid})
	if err != nil || result == nil {
		return conformance.SnapshotRead{}, conformance.ClassifyError(err)
	}
	out := conformance.SnapshotRead{Found: true, HeadSeqNr: int64(result.HeadSeqNr)}
	if result.Snapshot != nil {
		sn := result.Snapshot
		out.Snapshot = &conformance.Snapshot{SeqNr: int64(sn.SeqNr()), Manifest: sn.Manifest(), Aggregate: bytes.Clone(sn.Aggregate())}
	}
	return out, nil
}
func (s *publicStore) GetEventsByIDSinceSeqNr(ctx context.Context, aid conformance.AggregateIDArg, since int64) ([]conformance.Event, error) {
	ctx = s.context(ctx)
	events, err := s.actual.GetEventsByIDSinceSeqNr(ctx, fixtureID{parts: aid}, eventstore.SeqNr(since))
	if err != nil {
		return nil, conformance.ClassifyError(err)
	}
	out := []conformance.Event{}
	trees := []any{}
	for _, ev := range events {
		typeName, value, found := strings.Cut(ev.AggregateID(), "-")
		if !found {
			return nil, fmt.Errorf("returned aid has no delimiter")
		}
		// The payload tree is recorded as the library returned it, without a copy.
		tree := ev.Payload()
		payload, err := json.Marshal(tree)
		if err != nil {
			return nil, err
		}
		trees = append(trees, tree)
		out = append(out, conformance.Event{AggregateID: conformance.AggregateIDArg{TypeName: typeName, Value: value}, SeqNr: int64(ev.SeqNr()), OccurredAt: ev.OccurredAt(), Manifest: ev.Manifest(), Payload: payload})
	}
	s.mu.Lock()
	s.read[conformance.OperationNumber(ctx)] = trees
	s.mu.Unlock()
	return out, nil
}
func (s *publicStore) recordWritten(ctx context.Context, tree any) {
	s.mu.Lock()
	s.written[conformance.OperationNumber(ctx)] = tree
	s.mu.Unlock()
}

// MutateWrittenPayload changes the payload object that the operation passed to the library.
func (s *publicStore) MutateWrittenPayload(operation int, pointer string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tree, ok := s.written[operation]
	if !ok {
		return fmt.Errorf("operation %d wrote no payload", operation)
	}
	return conformance.ReplaceAtPointer(tree, pointer, value)
}

// MutateReadPayload changes the payload object that the read operation got back from the library.
func (s *publicStore) MutateReadPayload(operation, index int, pointer string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	trees := s.read[operation]
	if index < 0 || index >= len(trees) {
		return fmt.Errorf("operation %d read no payload at %d", operation, index)
	}
	return conformance.ReplaceAtPointer(trees[index], pointer, value)
}

func (s *publicStore) Notifications() []string {
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	return append([]string{}, s.backend.notifications[s.backend.operation]...)
}
func (s *publicStore) Close() error { return nil }

func (b *publicBackend) connectReadPlans(cfg conformance.StoreConfig, injection conformance.Injection) {
	var interleave *conformance.Fault
	var captured map[string]types.AttributeValue
	b.reads.Before = func(ctx context.Context, input any) error {
		if b.tables.ReadPhase(input) != "read-snapshot" {
			return nil
		}
		for _, f := range injection.Faults {
			if f.Spec.Kind != "read-interleave" || !f.CanApply() {
				continue
			}
			batch := input.(*awsdb.BatchGetItemInput)
			head, _ := b.tables.TableName("head")
			request := batch.RequestItems[head]
			if len(request.Keys) != 1 {
				return fmt.Errorf("interleave requires one real head key")
			}
			var err error
			captured, err = b.tables.GetItem(ctx, "head", request.Keys[0])
			if err != nil {
				return err
			}
			op := f.Spec.Details["interleaved_operation"].(map[string]any)
			args := op["arguments"].(map[string]any)
			ev, err := injection.Plan.EventFixture(fmt.Sprint(args["event"]))
			if err != nil {
				return err
			}
			sn, err := injection.Plan.SnapshotFixture(fmt.Sprint(args["snapshot"]))
			if err != nil {
				return err
			}
			opts, err := b.options(cfg, nil)
			if err != nil {
				return err
			}
			otherRequests := dynamodbtest.NewRecorder()
			otherResponses := &dynamodbtest.ResponseRecorder{}
			other, err := dynamodb.New(ctx, b.environment.NewClient(otherRequests.APIOption, otherResponses.APIOption), b.config(cfg), &payloadTreeSerializer{raw: &hookSerializer{hooks: testhook.New(), serialize: testhook.PhaseSerializeEvent, deserialize: testhook.PhaseDeserializeEvent}}, eventstore.NewJSONSerializer[json.RawMessage](), opts...)
			if err != nil {
				return err
			}
			event, _, err := publicEvent(ev)
			if err != nil {
				return err
			}
			snapshot, err := publicSnapshot(sn)
			if err != nil {
				return err
			}
			if err := other.PersistEventAndSnapshot(ctx, event, snapshot); err != nil {
				return err
			}
			n := conformance.OperationNumber(ctx)
			b.interleaved[n] = append(b.interleaved[n], map[string]any{"captured_head": captured, "requests": otherRequests.Requests(n), "responses": otherResponses.Results(n)})
			interleave = f
			break
		}
		return nil
	}
	b.reads.After = func(ctx context.Context, input, actual any) (any, error) {
		phase := b.tables.ReadPhase(input)
		if phase == "read-events" {
			return dynamodbtest.BoundEventQuery(actual.(*awsdb.QueryOutput))
		}
		if phase == "read-snapshot" {
			out := actual.(*awsdb.BatchGetItemOutput)
			if interleave != nil && interleave.CanApply() {
				_, err := interleave.TryApplyWith(func() error {
					head, _ := b.tables.TableName("head")
					out.Responses[head] = []map[string]types.AttributeValue{captured}
					return nil
				})
				interleave = nil
				return out, err
			}
			for _, f := range injection.Faults {
				if f.Spec.Phase != phase || f.Spec.Kind != "sdk-response" || !f.CanApply() {
					continue
				}
				_, err := f.TryApplyWith(func() error {
					var tables []string
					batch := input.(*awsdb.BatchGetItemInput)
					for _, raw := range f.Spec.Details["unprocessed_keys"].([]any) {
						parts := strings.Split(fmt.Sprint(raw), ":")
						table, err := b.tables.TableName(dynamodbtest.Table(parts[0]))
						if err != nil {
							return err
						}
						if len(batch.RequestItems[table].Keys) != 1 {
							return fmt.Errorf("unprocessed plan does not match requested key")
						}
						key := batch.RequestItems[table].Keys[0]
						aid, ok := key["aid"].(*types.AttributeValueMemberS)
						if !ok || aid.Value != parts[1] {
							return fmt.Errorf("unprocessed plan aid mismatch")
						}
						if len(parts) == 3 {
							sk, ok := key["skey"].(*types.AttributeValueMemberN)
							if !ok || sk.Value != parts[2] {
								return fmt.Errorf("unprocessed plan sort key mismatch")
							}
						}
						tables = append(tables, table)
					}
					var err error
					out, err = dynamodbtest.DeferReadKeys(batch, out, tables...)
					return err
				})
				return out, err
			}
		}
		return actual, nil
	}
}

func (b *publicBackend) Observe(ctx context.Context, operation int, step conformance.StepPlan) (map[string]any, error) {
	out := map[string]any{"notifications": append([]string{}, b.notifications[operation]...)}
	if _, requested := step.Observe["history"]; requested {
		aid := step.AID.TypeName + "-" + step.AID.Value
		active, marked := []any{}, []any{}
		if b.name == "memory" {
			if b.store == nil {
				return nil, fmt.Errorf("history observation requires an opened memory store")
			}
			h, err := b.store.hooks.History(aid)
			if err != nil {
				return nil, err
			}
			for _, n := range h.Active {
				active = append(active, n)
			}
			for _, n := range h.Marked {
				marked = append(marked, n)
			}
		} else {
			h, err := b.tables.History(ctx, aid)
			if err != nil {
				return nil, err
			}
			for _, n := range h.Active {
				active = append(active, n)
			}
			for _, n := range h.Marked {
				ttl, err := strconv.ParseInt(n.TTL, 10, 64)
				if err != nil {
					return nil, err
				}
				marked = append(marked, map[string]any{"seq_nr": n.SeqNr, "ttl": ttl})
			}
		}
		out["history"] = map[string]any{"active": active, "marked": marked}
	}
	if b.name == "dynamodb" {
		if b.recorder == nil {
			return map[string]any{"notifications": out["notifications"], "requests": []any{}, "request_count": map[string]any{}, "raw_requests": []any{}, "responses": []any{}, "reads": []any{}}, nil
		}
		requests, err := b.tables.ObserveRequests(ctx, b.recorder.Requests(operation), b.responses.Results(operation), step, b.waits[operation])
		if err != nil {
			return nil, err
		}
		out["requests"] = requests
		out["request_scope"] = dynamodbtest.RequestScope(b.recorder.Requests(operation))
		counts := map[string]any{}
		for _, r := range requests {
			phase := fmt.Sprint(r["phase"])
			n, _ := counts[phase].(int)
			counts[phase] = n + 1
		}
		out["request_count"] = counts
		items := []any{}
		if list, ok := step.Observe["items"].([]any); ok {
			for _, raw := range list {
				fixture := raw.(map[string]any)
				table := fmt.Sprint(fixture["table"])
				values := fixture["values"].(map[string]any)
				key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: fmt.Sprint(values["aid"])}}
				sortKey := ""
				if table == "journal" {
					sortKey = "seq_nr"
				}
				if table == "snapshot" {
					sortKey = "skey"
				}
				if sortKey != "" {
					key[sortKey] = &types.AttributeValueMemberN{Value: fmt.Sprint(values[sortKey])}
				}
				actual, err := b.tables.GetItem(ctx, dynamodbtest.Table(table), key)
				if err != nil {
					return nil, err
				}
				observation, err := dynamodbtest.ItemObservation(table, actual)
				if err != nil {
					return nil, err
				}
				items = append(items, observation)
			}
		}
		out["items"] = items
		out["raw_requests"] = b.recorder.Requests(operation)
		out["responses"] = b.responses.Results(operation)
		out["reads"] = b.reads.Results(operation)
		out["waits"] = b.waits[operation]
		out["interleaved_operations"] = b.interleaved[operation]
		out["partial_deletes"] = b.partialDeletes[operation]
	}
	value, err := conformance.JSONValue(out)
	if err != nil {
		return nil, err
	}
	return value.(map[string]any), nil
}
