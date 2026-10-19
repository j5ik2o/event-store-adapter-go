package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/j5ik2o/event-store-adapter-go/v5/internal/testhook"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memDB is the record of a fake memory store. Stores that share one memDB share
// their records, their optimistic lock and the retention setting of the storage.
type memDB struct {
	events map[string][]memRecord
	// retention is the retention count of the storage. nil keeps no history.
	retention *int64
	snapshots map[string]Snapshot
	history   map[string][]int64
}

type memRecord struct {
	event Event
	tree  any
}

func newMemDB(retention *int64) *memDB {
	return &memDB{
		events:    map[string][]memRecord{},
		retention: retention,
		snapshots: map[string]Snapshot{},
		history:   map[string][]int64{},
	}
}

// memFakeBackend is a fake storage backend that understands createStore and the payload
// mutations. Its switches model the wrong implementations that the runner must reject.
type memFakeBackend struct {
	// shareOnNew makes a "new" store share the records of the first store.
	shareOnNew bool
	// noCopy keeps, records and returns the payload tree without copying it.
	noCopy bool
	// ownSettingsOnShared makes a store created with storage "shared" use settings of its own
	// (no retention) instead of the settings of the storage it shares.
	ownSettingsOnShared bool
	first               *memFakeStore
	log                 []string
}

func (b *memFakeBackend) Name() string                                 { return "memory" }
func (b *memFakeBackend) Injectable(FaultSpec) bool                    { return false }
func (b *memFakeBackend) Seed(context.Context, []map[string]any) error { return nil }

func (b *memFakeBackend) newStore(label string, db *memDB, retention *int64) *memFakeStore {
	return &memFakeStore{b: b, label: label, db: db, retention: retention, written: map[int]any{}, read: map[int][]any{}}
}

func (b *memFakeBackend) Open(_ context.Context, cfg StoreConfig, inj Injection) (Store, error) {
	db := newMemDB(cfg.RetentionCount)
	inj.Hooks.ProvideHistory(func(aid string) (testhook.History, error) {
		return testhook.History{Active: append([]int64{}, db.history[aid]...), Marked: []int64{}}, nil
	})
	b.first = b.newStore("primary", db, db.retention)
	return b.first, nil
}

func (b *memFakeBackend) CreateStore(_ context.Context, cfg StoreConfig, _ Injection, shared Store) (Store, error) {
	if shared != nil {
		s, ok := shared.(*memFakeStore)
		if !ok {
			return nil, fmt.Errorf("unexpected shared store %T", shared)
		}
		retention := s.db.retention
		if b.ownSettingsOnShared {
			retention = nil
		}
		return b.newStore("shared", s.db, retention), nil
	}
	if b.shareOnNew {
		return b.newStore("second", b.first.db, b.first.db.retention), nil
	}
	return b.newStore("second", newMemDB(cfg.RetentionCount), cfg.RetentionCount), nil
}

type memFakeStore struct {
	b     *memFakeBackend
	label string
	db    *memDB
	// retention is the retention count that this instance applies.
	retention *int64
	written   map[int]any
	read      map[int][]any
}

func (s *memFakeStore) PersistEvent(ctx context.Context, ev Event) error {
	s.b.log = append(s.b.log, s.label+":persistEvent")
	key := ev.AggregateID.TypeName + "-" + ev.AggregateID.Value
	for _, r := range s.db.events[key] {
		if r.event.SeqNr == ev.SeqNr {
			return &OperationError{Category: "optimistic-lock", Message: fmt.Sprintf("%s seq_nr %d already exists", key, ev.SeqNr)}
		}
	}
	tree, err := decodeStrictJSON(ev.Payload)
	if err != nil {
		return err
	}
	s.written[OperationNumber(ctx)] = tree
	stored := tree
	if !s.b.noCopy {
		stored = deepCopy(tree)
	}
	s.db.events[key] = append(s.db.events[key], memRecord{event: ev, tree: stored})
	return nil
}

func (s *memFakeStore) PersistEventAndSnapshot(ctx context.Context, ev Event, sn Snapshot) error {
	if err := s.PersistEvent(ctx, ev); err != nil {
		return err
	}
	key := ev.AggregateID.TypeName + "-" + ev.AggregateID.Value
	s.db.snapshots[key] = sn
	if s.retention == nil {
		s.db.history[key] = nil
		return nil
	}
	h := append(s.db.history[key], ev.SeqNr)
	if int64(len(h)) > *s.retention {
		h = h[int64(len(h))-*s.retention:]
	}
	s.db.history[key] = h
	return nil
}

func (s *memFakeStore) GetLatestSnapshotByID(_ context.Context, aid AggregateIDArg) (SnapshotRead, error) {
	s.b.log = append(s.b.log, s.label+":getLatestSnapshotById")
	key := aid.TypeName + "-" + aid.Value
	recs := s.db.events[key]
	if len(recs) == 0 {
		return SnapshotRead{}, nil
	}
	read := SnapshotRead{Found: true, HeadSeqNr: recs[len(recs)-1].event.SeqNr}
	if sn, ok := s.db.snapshots[key]; ok {
		read.Snapshot = &sn
	}
	return read, nil
}

func (s *memFakeStore) GetEventsByIDSinceSeqNr(ctx context.Context, aid AggregateIDArg, since int64) ([]Event, error) {
	s.b.log = append(s.b.log, s.label+":getEventsByIdSinceSeqNr")
	var out []Event
	var trees []any
	for _, r := range s.db.events[aid.TypeName+"-"+aid.Value] {
		if r.event.SeqNr < since {
			continue
		}
		tree := r.tree
		if !s.b.noCopy {
			tree = deepCopy(tree)
		}
		raw, err := json.Marshal(tree)
		if err != nil {
			return nil, err
		}
		ev := r.event
		ev.Payload = raw
		out = append(out, ev)
		trees = append(trees, tree)
	}
	s.read[OperationNumber(ctx)] = trees
	return out, nil
}

func (s *memFakeStore) MutateWrittenPayload(operation int, pointer string, value any) error {
	tree, ok := s.written[operation]
	if !ok {
		return fmt.Errorf("operation %d wrote nothing", operation)
	}
	return ReplaceAtPointer(tree, pointer, value)
}

func (s *memFakeStore) MutateReadPayload(operation, index int, pointer string, value any) error {
	trees := s.read[operation]
	if index < 0 || index >= len(trees) {
		return fmt.Errorf("operation %d read no event at %d", operation, index)
	}
	return ReplaceAtPointer(trees[index], pointer, value)
}

func (s *memFakeStore) Notifications() []string { return nil }
func (s *memFakeStore) Close() error            { return nil }

// stepOn is a step that runs on a named store.
func stepOn(store, op, args, expect string) string {
	return `{"op":"` + op + `","store":"` + store + `","arguments":` + args + `,"expect":` + expect + `}`
}

const (
	aidOrder1  = `{"aggregate_id":{"type_name":"Order","value":"1"}}`
	createNew  = `{"name":"second","storage":"new"}`
	createSame = `{"name":"second","storage":"shared","shared_with":"primary"}`
)

var (
	createSecondNew    = step("createStore", createNew, `{"result":"success"}`)
	createSecondShared = step("createStore", createSame, `{"result":"success"}`)
	secondNone         = stepOn("second", "getLatestSnapshotById", aidOrder1, `{"result":"none"}`)
	primaryHeadIs1     = step("getLatestSnapshotById", aidOrder1, `{"result":"snapshot","head_seq_nr":1}`)
)

func parseRaw(t *testing.T, body string) (*ScenarioPlan, error) {
	t.Helper()
	v, err := decodeStrictJSON([]byte(body))
	require.NoError(t, err)
	return parseScenario(v.(map[string]any))
}

func TestParseScenario_Stores(t *testing.T) {
	t.Run("a step that names a created store is read with that store", func(t *testing.T) {
		c := mkCase(t, scenarioBody("", "", persistOK, createSecondNew, secondNone))
		require.Len(t, c.Plan.Steps, 3)
		create := c.Plan.Steps[1]
		assert.Equal(t, "createStore", create.Op)
		assert.Equal(t, "second", create.CreateName)
		assert.Equal(t, "new", create.Storage)
		assert.Equal(t, "second", c.Plan.Steps[2].Store)
		assert.Equal(t, "primary", c.Plan.Steps[0].Store)
	})

	t.Run("a shared store records the store it shares with", func(t *testing.T) {
		c := mkCase(t, scenarioBody("", "", persistOK, createSecondShared))
		assert.Equal(t, "shared", c.Plan.Steps[1].Storage)
		assert.Equal(t, "primary", c.Plan.Steps[1].SharedWith)
	})

	for name, steps := range map[string][]string{
		"the reserved name primary cannot be created": {step("createStore", `{"name":"primary","storage":"new"}`, `{"result":"success"}`)},
		"a name cannot be created twice":              {createSecondNew, step("createStore", createNew, `{"result":"success"}`)},
		"a step cannot name a store that is not yet created": {
			createSecondNew,
			stepOn("third", "getLatestSnapshotById", aidOrder1, `{"result":"none"}`),
		},
		"a step cannot name a store before it is created": {secondNone, createSecondNew},
		"a shared store cannot share with an unknown store": {
			step("createStore", `{"name":"second","storage":"shared","shared_with":"other"}`, `{"result":"success"}`),
		},
		"a shared store needs shared_with": {
			step("createStore", `{"name":"second","storage":"shared"}`, `{"result":"success"}`),
		},
		"an unknown storage kind is rejected": {
			step("createStore", `{"name":"second","storage":"copy"}`, `{"result":"success"}`),
		},
	} {
		steps := steps
		t.Run(name, func(t *testing.T) {
			_, err := parseRaw(t, scenarioBody("", "", steps...))
			assert.Error(t, err)
		})
	}
}

func mutateWritten(pointer, value string) string {
	return step("mutateWrittenPayload", `{"operation":1,"pointer":"`+pointer+`","value":`+value+`}`, `{"result":"success"}`)
}

func mutateRead(pointer, value string) string {
	return step("mutateReadPayload", `{"operation":2,"event":"e1","pointer":"`+pointer+`","value":`+value+`}`, `{"result":"success"}`)
}

func TestParseScenario_Mutation(t *testing.T) {
	t.Run("a written mutation refers to its persist step", func(t *testing.T) {
		c := mkCase(t, scenarioBody("", "", persistOK, mutateWritten("/number", "99")))
		s := c.Plan.Steps[1]
		assert.Equal(t, "mutateWrittenPayload", s.Op)
		assert.Equal(t, 1, s.MutateOperation)
		assert.Equal(t, "/number", s.Pointer)
		assert.True(t, jsonEqual(json.Number("99"), s.Value))
	})

	t.Run("a read mutation refers to the read step and the position of the event in it", func(t *testing.T) {
		c := mkCase(t, scenarioBody("", "", persistOK, readFrom1Only1, mutateRead("/number", "99")))
		s := c.Plan.Steps[2]
		assert.Equal(t, "mutateReadPayload", s.Op)
		assert.Equal(t, 2, s.MutateOperation)
		assert.Equal(t, 0, s.MutateIndex)
		assert.Equal(t, "/number", s.Pointer)
	})

	rejected := map[string][]string{
		"an empty pointer":                                   {persistOK, mutateWritten("", "99")},
		"a pointer without a leading slash":                  {persistOK, mutateWritten("number", "99")},
		"a pointer with an invalid escape":                   {persistOK, mutateWritten("/number~2", "99")},
		"a pointer to a member the payload lacks":            {persistOK, mutateWritten("/missing", "99")},
		"a pointer that descends into a number":              {persistOK, mutateWritten("/number/0", "99")},
		"a value equal to the current value":                 {persistOK, mutateWritten("/number", "1")},
		"a read mutation with a pointer the event lacks":     {persistOK, readFrom1Only1, mutateRead("/missing", "99")},
		"a read mutation with the current value":             {persistOK, readFrom1Only1, mutateRead("/number", "1")},
		"a written mutation of a step that is not a persist": {readFrom1Only1, mutateWritten("/number", "99")},
		"a read mutation of an event the read step does not return": {
			persistOK, readFrom1Only1,
			step("mutateReadPayload", `{"operation":2,"event":"e2","pointer":"/number","value":99}`, `{"result":"success"}`),
		},
		"a mutation of a step that comes later": {mutateWritten("/number", "99"), persistOK},
	}
	for name, steps := range rejected {
		steps := steps
		t.Run(name, func(t *testing.T) {
			_, err := parseRaw(t, scenarioBody("", "", steps...))
			assert.Error(t, err)
		})
	}
}

func TestReplaceAtPointer(t *testing.T) {
	decode := func(t *testing.T, s string) any {
		t.Helper()
		v, err := decodeStrictJSON([]byte(s))
		require.NoError(t, err)
		return v
	}

	t.Run("it replaces the value in place, decoding ~1 and ~0", func(t *testing.T) {
		root := decode(t, `{"items":[{"qty":1}],"a/b":{"~k":0}}`)
		require.NoError(t, ReplaceAtPointer(root, "/items/0/qty", json.Number("99")))
		require.NoError(t, ReplaceAtPointer(root, "/a~1b/~0k", json.Number("5")))
		assert.True(t, jsonEqual(decode(t, `{"items":[{"qty":99}],"a/b":{"~k":5}}`), root))
	})

	t.Run("it stores a copy of the value, not the value itself", func(t *testing.T) {
		root := decode(t, `{"x":1}`)
		value := decode(t, `{"y":[1]}`)
		require.NoError(t, ReplaceAtPointer(root, "/x", value))
		value.(map[string]any)["y"].([]any)[0] = json.Number("2")
		assert.True(t, jsonEqual(decode(t, `{"x":{"y":[1]}}`), root))
	})

	for _, pointer := range []string{"", "items/0", "/a~2", "/items/5/qty", "/items/00/qty", "/items/-/qty", "/missing/qty"} {
		pointer := pointer
		t.Run("it rejects pointer "+pointer+" and leaves the payload unchanged", func(t *testing.T) {
			root := decode(t, `{"items":[{"qty":1}]}`)
			assert.Error(t, ReplaceAtPointer(root, pointer, json.Number("99")))
			assert.True(t, jsonEqual(decode(t, `{"items":[{"qty":1}]}`), root))
		})
	}

	t.Run("it rejects a value equal to the current value", func(t *testing.T) {
		root := decode(t, `{"items":[{"qty":1}]}`)
		assert.Error(t, ReplaceAtPointer(root, "/items/0/qty", json.Number("1")))
	})
}

func TestRunScenario_CreateStore(t *testing.T) {
	ctx := context.Background()
	independent := scenarioBody("", "", persistOK, createSecondNew, secondNone, primaryHeadIs1)
	shared := scenarioBody("", "",
		persistOK, createSecondShared,
		stepOn("second", "persistEvent", `{"event":"e1"}`, `{"result":"error","error":{"category":"optimistic-lock"}}`),
		stepOn("second", "persistEvent", `{"event":"e2"}`, `{"result":"success"}`),
		step("getLatestSnapshotById", aidOrder1, `{"result":"snapshot","head_seq_nr":2}`),
	)

	t.Run("a new store is empty and independent of the first store", func(t *testing.T) {
		b := &memFakeBackend{}
		r := runScenario(ctx, mkCase(t, independent), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("steps run on the store they name", func(t *testing.T) {
		b := &memFakeBackend{}
		r := runScenario(ctx, mkCase(t, independent), "memory", b)
		require.Equal(t, StatusSuccess, r.Status, r.Reason)
		assert.Equal(t, []string{"primary:persistEvent", "second:getLatestSnapshotById", "primary:getLatestSnapshotById"}, b.log)
	})

	t.Run("a shared store shares the records and the optimistic lock", func(t *testing.T) {
		b := &memFakeBackend{}
		r := runScenario(ctx, mkCase(t, shared), "memory", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("a store that shares its records although the data asked for a new one fails at the empty read", func(t *testing.T) {
		b := &memFakeBackend{shareOnNew: true}
		r := runScenario(ctx, mkCase(t, independent), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 3, *r.FailedStep)
	})

	t.Run("a backend that cannot create a store fails the createStore step", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		r := runScenario(ctx, mkCase(t, independent), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 2, *r.FailedStep)
	})

	realCases := loadMemoryCases(t, "memory-store-new-independent", "memory-store-shared")

	t.Run("the distributed cases succeed on a correct store", func(t *testing.T) {
		for _, id := range []string{"memory-store-new-independent", "memory-store-shared"} {
			r := runScenario(ctx, realCases[id], "memory", &memFakeBackend{})
			assert.Equal(t, StatusSuccess, r.Status, "%s: %s", id, r.Reason)
		}
	})

	t.Run("the distributed shared configuration case succeeds on a store that uses the settings of the shared storage", func(t *testing.T) {
		c := loadMemoryCases(t, "memory-store-shared-configuration")["memory-store-shared-configuration"]
		r := runScenario(ctx, c, "memory", &memFakeBackend{})
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("the distributed shared configuration case rejects a shared store that keeps its own default settings", func(t *testing.T) {
		c := loadMemoryCases(t, "memory-store-shared-configuration")["memory-store-shared-configuration"]
		r := runScenario(ctx, c, "memory", &memFakeBackend{ownSettingsOnShared: true})
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 2, *r.FailedStep)
	})

	t.Run("the distributed independent case rejects a new store that shares its records", func(t *testing.T) {
		r := runScenario(ctx, realCases["memory-store-new-independent"], "memory", &memFakeBackend{shareOnNew: true})
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 3, *r.FailedStep)
	})
}

func loadMemoryCases(t *testing.T, ids ...string) map[string]ScenarioCase {
	t.Helper()
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	byID := map[string]ScenarioCase{}
	for _, c := range d.Scenarios {
		byID[c.ID] = c
	}
	out := map[string]ScenarioCase{}
	for _, id := range ids {
		c, ok := byID[id]
		require.True(t, ok, "case %s is not in the data", id)
		out[id] = c
	}
	return out
}

func TestRunScenario_PayloadMutation(t *testing.T) {
	ctx := context.Background()
	written := scenarioBody("", "", persistOK, mutateWritten("/number", "99"), readFrom1Only1)
	readBack := scenarioBody("", "", persistOK, readFrom1Only1, mutateRead("/number", "99"), readFrom1Only1)

	t.Run("a store that keeps its own copy returns the written value after a written mutation", func(t *testing.T) {
		r := runScenario(ctx, mkCase(t, written), "memory", &memFakeBackend{})
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("a store that keeps its own copy returns the written value after a read mutation", func(t *testing.T) {
		r := runScenario(ctx, mkCase(t, readBack), "memory", &memFakeBackend{})
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
	})

	t.Run("a store that keeps the written tree is caught by a written mutation", func(t *testing.T) {
		r := runScenario(ctx, mkCase(t, written), "memory", &memFakeBackend{noCopy: true})
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 3, *r.FailedStep)
	})

	t.Run("a store that returns its stored tree is caught by a read mutation", func(t *testing.T) {
		r := runScenario(ctx, mkCase(t, readBack), "memory", &memFakeBackend{noCopy: true})
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 4, *r.FailedStep)
	})

	t.Run("a store that cannot mutate payloads fails the mutation step", func(t *testing.T) {
		b := &fakeBackend{name: "memory", injectable: true}
		r := runScenario(ctx, mkCase(t, written), "memory", b)
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 2, *r.FailedStep)
	})

	realCases := loadMemoryCases(t, "memory-payload-written-mutation", "memory-payload-read-mutation")

	t.Run("the distributed cases succeed on a store that copies", func(t *testing.T) {
		for id, c := range realCases {
			r := runScenario(ctx, c, "memory", &memFakeBackend{})
			assert.Equal(t, StatusSuccess, r.Status, "%s: %s", id, r.Reason)
		}
	})

	t.Run("the distributed written case rejects a store that keeps the written tree", func(t *testing.T) {
		r := runScenario(ctx, realCases["memory-payload-written-mutation"], "memory", &memFakeBackend{noCopy: true})
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 3, *r.FailedStep)
	})

	t.Run("the distributed read case rejects a store that returns its stored tree", func(t *testing.T) {
		r := runScenario(ctx, realCases["memory-payload-read-mutation"], "memory", &memFakeBackend{noCopy: true})
		assert.Equal(t, StatusFailure, r.Status)
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 4, *r.FailedStep)
	})
}
