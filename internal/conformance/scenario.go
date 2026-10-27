package conformance

import (
	"fmt"
	"math/big"
	"sort"
)

// primaryStore is the reserved name of the store that the scenario opens first.
const primaryStore = "primary"

// ScenarioPlan is a scenario with its structure read and its references checked.
type ScenarioPlan struct {
	Store StoreConfig
	Seed  []map[string]any
	// onlySQLite is true when the backends of the scenario are exactly [sqlite]: only then are the SQLite-only operations known.
	onlySQLite bool
	// SeedRowDeletionPolicyDays is seed.row_deletion_policy_days: the INTERVAL of the row deletion policy of the Spanner
	// history table, or nil when the table has no policy.
	SeedRowDeletionPolicyDays *int64
	// SeedConfiguration is seed.configuration: the configuration a SQLite destination holds before the store is opened.
	SeedConfiguration map[string]any
	Init              *InitPlan
	Events            map[string]eventFixture
	Snaps             map[string]snapshotFixture
	Steps             []StepPlan
	Faults            []FaultSpec
	Requires          []string
	// ClockStart is the clock of the scenario (clock.epoch_seconds), if any.
	ClockStart *int64
}

// InitPlan is the initialization block. Error is nil when a success is expected.
type InitPlan struct {
	Error   *ErrorExpect
	Observe map[string]any
}

// ErrorExpect is an expected error.
type ErrorExpect struct {
	Category       string
	Rule           string
	MustContain    []string
	MustNotContain []string
}

// StepPlan is one operation of the scenario.
type StepPlan struct {
	Op string
	// Store names the store that runs the step. It is primaryStore unless the data says otherwise.
	Store string
	// CreateName, Storage and SharedWith are the arguments of createStore.
	CreateName, Storage, SharedWith string
	// MutateOperation is the one-based operation that mutateWrittenPayload and mutateReadPayload refer to.
	MutateOperation int
	// MutateIndex is the position, in the events that the referred read returned, of the event to mutate.
	MutateIndex int
	// Pointer and Value are the JSON Pointer position and the new value of a payload mutation.
	Pointer  string
	Value    any
	Event    string
	Snapshot string
	AID      AggregateIDArg
	SeqNr    *big.Int
	Expect   Expectation
	Observe  map[string]any
	// ClockEpochSeconds moves the clock before the operation.
	ClockEpochSeconds *int64
}

// Expectation is the expect of a step. Kind is success, none, snapshot, events or error.
type Expectation struct {
	Kind      string
	Error     *ErrorExpect
	HeadSeqNr *big.Int
	// Snapshot is the snapshot fixture name; empty means null.
	Snapshot string
	Events   []string
}

type eventFixture struct {
	AID        AggregateIDArg
	SeqNr      *big.Int
	OccurredAt string
	Payload    any
	Manifest   string
}

type snapshotFixture struct {
	SeqNr     *big.Int
	Aggregate any
	Manifest  string
}

// parseScenario reads a materialized scenario. Schema validation has already run, so a
// missing or mistyped element, or a reference to an unknown fixture or step, is a data error.
func parseScenario(m map[string]any) (*ScenarioPlan, error) {
	p := &ScenarioPlan{Events: map[string]eventFixture{}, Snaps: map[string]snapshotFixture{}}
	var err error
	if backends, listErr := stringList(m["backends"]); listErr == nil {
		p.onlySQLite = len(backends) == 1 && backends[0] == "sqlite"
	}

	store, ok := m["store"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("store is not an object")
	}
	if p.Store, err = parseStoreConfig(store); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if c, present := m["clock"]; present {
		cm, ok := c.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("clock is not an object")
		}
		n, err := optInt64(cm, "epoch_seconds")
		if err != nil || n == nil {
			return nil, fmt.Errorf("clock.epoch_seconds is not an integer")
		}
		p.ClockStart = n
		p.Store.ClockEpochSeconds = n
	}
	if r, present := m["requires"]; present {
		if p.Requires, err = stringList(r); err != nil {
			return nil, fmt.Errorf("requires: %w", err)
		}
	}
	if s, present := m["seed"]; present {
		sm, ok := s.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("seed is not an object")
		}
		_, hasItems := sm["items"]
		configuration, hasConfiguration := sm["configuration"]
		if hasItems == hasConfiguration {
			return nil, fmt.Errorf("seed needs exactly one of items and configuration")
		}
		if hasConfiguration {
			cm, ok := configuration.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("seed.configuration is not an object")
			}
			p.SeedConfiguration = cm
			if p.SeedRowDeletionPolicyDays, err = optInt64(sm, "row_deletion_policy_days"); err != nil {
				return nil, fmt.Errorf("seed.row_deletion_policy_days: %w", err)
			}
		} else {
			items, ok := sm["items"].([]any)
			if !ok {
				return nil, fmt.Errorf("seed.items is not an array")
			}
			for i, it := range items {
				im, ok := it.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("seed.items[%d] is not an object", i)
				}
				p.Seed = append(p.Seed, im)
			}
		}
	}

	fixtures, ok := m["fixtures"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("fixtures is not an object")
	}
	if err := parseFixtures(p, fixtures); err != nil {
		return nil, fmt.Errorf("fixtures: %w", err)
	}

	if init, present := m["initialization"]; present {
		im, ok := init.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("initialization is not an object")
		}
		expect, ok := im["expect"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("initialization.expect is not an object")
		}
		p.Init = &InitPlan{}
		if e, present := expect["error"]; present {
			if p.Init.Error, err = parseErrorExpect(e); err != nil {
				return nil, fmt.Errorf("initialization.expect.error: %w", err)
			}
		}
		if obs, ok := im["observe"].(map[string]any); ok {
			p.Init.Observe = obs
		}
	}

	steps, ok := m["steps"].([]any)
	if !ok {
		return nil, fmt.Errorf("steps is not an array")
	}
	for i, s := range steps {
		sp, err := parseStep(p, s)
		if err != nil {
			return nil, fmt.Errorf("steps[%d]: %w", i, err)
		}
		p.Steps = append(p.Steps, sp)
	}

	if f, present := m["faults"]; present {
		list, ok := f.([]any)
		if !ok {
			return nil, fmt.Errorf("faults is not an array")
		}
		for i, item := range list {
			spec, err := parseFault(item)
			if err != nil {
				return nil, fmt.Errorf("faults[%d]: %w", i, err)
			}
			if spec.Operation > len(p.Steps) {
				return nil, fmt.Errorf("faults[%d]: operation %d is beyond the %d steps", i, spec.Operation, len(p.Steps))
			}
			p.Faults = append(p.Faults, spec)
		}
	}
	return p, nil
}

// EventFixture returns an input fixture for a declared interleaved operation.
func (p *ScenarioPlan) EventFixture(name string) (Event, error) {
	f, ok := p.Events[name]
	if !ok {
		return Event{}, fmt.Errorf("unknown event fixture %q", name)
	}
	return toEvent(f)
}

// SnapshotFixture returns an input fixture for a declared interleaved operation.
func (p *ScenarioPlan) SnapshotFixture(name string) (Snapshot, error) {
	f, ok := p.Snaps[name]
	if !ok {
		return Snapshot{}, fmt.Errorf("unknown snapshot fixture %q", name)
	}
	return toSnapshot(f)
}

// storeConfigKeys are the store settings that parseStoreConfig interprets. The schema also allows
// settings of storages this language does not provide; a scenario of a provided storage that sets
// one of them is rejected instead of running with the setting silently dropped.
var storeConfigKeys = map[string]bool{
	"retention_count":   true,
	"retention_mode":    true,
	"ttl_grace_seconds": true,
	"layout_version":    true,
	"retry_limit":       true,
	"change_feed":       true,

	"lock_wait_milliseconds": true,

	"transaction_retry_limit": true,
	"ttl_grace_days":          true,
}

func parseStoreConfig(m map[string]any) (StoreConfig, error) {
	var c StoreConfig
	var err error
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !storeConfigKeys[k] {
			return c, fmt.Errorf("%s is not a store setting this runner interprets", k)
		}
	}
	if v, present := m["retention_count"]; present && v != nil {
		n, err := bigIntFromJSONNumber(v)
		if err != nil || !n.IsInt64() {
			return c, fmt.Errorf("retention_count is not an int64")
		}
		x := n.Int64()
		c.RetentionCount = &x
	}
	c.RetentionMode, _ = m["retention_mode"].(string)
	if c.RetentionMode == "" {
		return c, fmt.Errorf("retention_mode is missing")
	}
	if c.TTLGraceSeconds, err = optInt64(m, "ttl_grace_seconds"); err != nil {
		return c, err
	}
	if c.LayoutVersion, err = optInt64(m, "layout_version"); err != nil {
		return c, err
	}
	if c.RetryLimit, err = optInt64(m, "retry_limit"); err != nil {
		return c, err
	}
	if c.LockWaitMilliseconds, err = optInt64(m, "lock_wait_milliseconds"); err != nil {
		return c, err
	}
	if c.TTLGraceDays, err = optInt64(m, "ttl_grace_days"); err != nil {
		return c, err
	}
	if c.TransactionRetryLimit, err = optInt64(m, "transaction_retry_limit"); err != nil {
		return c, err
	}
	if v, present := m["change_feed"]; present {
		b, ok := v.(bool)
		if !ok {
			return c, fmt.Errorf("change_feed is not a boolean")
		}
		c.ChangeFeed = b
	}
	return c, nil
}

func optInt64(m map[string]any, key string) (*int64, error) {
	v, present := m[key]
	if !present || v == nil {
		return nil, nil
	}
	n, err := bigIntFromJSONNumber(v)
	if err != nil || !n.IsInt64() {
		return nil, fmt.Errorf("%s is not an int64", key)
	}
	x := n.Int64()
	return &x, nil
}

func parseAID(v any) (AggregateIDArg, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return AggregateIDArg{}, fmt.Errorf("aggregate_id is not an object")
	}
	t, tok := m["type_name"].(string)
	val, vok := m["value"].(string)
	if !tok || !vok {
		return AggregateIDArg{}, fmt.Errorf("aggregate_id needs string type_name and value")
	}
	return AggregateIDArg{TypeName: t, Value: val}, nil
}

func parseFixtures(p *ScenarioPlan, f map[string]any) error {
	events, ok := f["events"].(map[string]any)
	if !ok {
		return fmt.Errorf("events is not an object")
	}
	for name, v := range events {
		m, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("event %q is not an object", name)
		}
		aid, err := parseAID(m["aggregate_id"])
		if err != nil {
			return fmt.Errorf("event %q: %w", name, err)
		}
		seq, err := bigIntFromJSONNumber(m["seq_nr"])
		if err != nil {
			return fmt.Errorf("event %q: seq_nr: %w", name, err)
		}
		at, _ := m["occurred_at"].(string)
		manifest, _ := m["manifest"].(string)
		p.Events[name] = eventFixture{AID: aid, SeqNr: seq, OccurredAt: at, Payload: m["payload"], Manifest: manifest}
	}
	snaps, ok := f["snapshots"].(map[string]any)
	if !ok {
		return fmt.Errorf("snapshots is not an object")
	}
	for name, v := range snaps {
		m, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("snapshot %q is not an object", name)
		}
		seq, err := bigIntFromJSONNumber(m["seq_nr"])
		if err != nil {
			return fmt.Errorf("snapshot %q: seq_nr: %w", name, err)
		}
		manifest, _ := m["manifest"].(string)
		p.Snaps[name] = snapshotFixture{SeqNr: seq, Aggregate: m["aggregate"], Manifest: manifest}
	}
	return nil
}

func parseErrorExpect(v any) (*ErrorExpect, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("error is not an object")
	}
	e := &ErrorExpect{}
	e.Category, _ = m["category"].(string)
	if e.Category == "" {
		return nil, fmt.Errorf("error.category is missing")
	}
	e.Rule, _ = m["rule"].(string)
	if msg, present := m["message"]; present {
		mm, ok := msg.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("error.message is not an object")
		}
		var err error
		if e.MustContain, err = stringList(mm["must_contain"]); err != nil {
			return nil, fmt.Errorf("must_contain: %w", err)
		}
		if e.MustNotContain, err = stringList(mm["must_not_contain"]); err != nil {
			return nil, fmt.Errorf("must_not_contain: %w", err)
		}
	}
	return e, nil
}

func parseStep(p *ScenarioPlan, v any) (StepPlan, error) {
	var s StepPlan
	m, ok := v.(map[string]any)
	if !ok {
		return s, fmt.Errorf("not an object")
	}
	s.Op, _ = m["op"].(string)
	args, ok := m["arguments"].(map[string]any)
	if !ok {
		return s, fmt.Errorf("arguments is not an object")
	}
	var err error
	known := map[string]bool{primaryStore: true}
	for _, prev := range p.Steps {
		if prev.Op == "createStore" {
			known[prev.CreateName] = true
		}
	}
	s.Store = primaryStore
	if name, present := m["store"]; present {
		if s.Store, ok = name.(string); !ok || !known[s.Store] {
			return s, fmt.Errorf("store %v is not a created store", name)
		}
	}
	switch s.Op {
	case "createStore":
		if err := parseCreateStore(&s, args, known); err != nil {
			return s, err
		}
	case "mutateWrittenPayload", "mutateReadPayload":
		if err := parseMutation(p, &s, args); err != nil {
			return s, err
		}
	case "persistEvent", "persistEventAndSnapshot":
		s.Event, _ = args["event"].(string)
		if _, ok := p.Events[s.Event]; !ok {
			return s, fmt.Errorf("unknown event fixture %q", s.Event)
		}
		if s.Op == "persistEventAndSnapshot" {
			s.Snapshot, _ = args["snapshot"].(string)
			if _, ok := p.Snaps[s.Snapshot]; !ok {
				return s, fmt.Errorf("unknown snapshot fixture %q", s.Snapshot)
			}
		}
		s.AID = p.Events[s.Event].AID
	case "holdWriteLock", "releaseWriteLock":
		if !p.onlySQLite {
			return s, fmt.Errorf("unsupported op %q", s.Op)
		}
		if len(args) != 0 || m["store"] != nil {
			return s, fmt.Errorf("%s takes no arguments and no store", s.Op)
		}
	case "getLatestSnapshotById":
		if s.AID, err = parseAID(args["aggregate_id"]); err != nil {
			return s, err
		}
	case "getEventsByIdSinceSeqNr":
		if s.AID, err = parseAID(args["aggregate_id"]); err != nil {
			return s, err
		}
		if s.SeqNr, err = bigIntFromJSONNumber(args["seq_nr"]); err != nil {
			return s, fmt.Errorf("seq_nr: %w", err)
		}
	default:
		return s, fmt.Errorf("unsupported op %q", s.Op)
	}
	expect, ok := m["expect"].(map[string]any)
	if !ok {
		return s, fmt.Errorf("expect is not an object")
	}
	if s.Expect, err = parseExpectation(p, expect); err != nil {
		return s, fmt.Errorf("expect: %w", err)
	}
	if obs, present := m["observe"]; present {
		if s.Observe, ok = obs.(map[string]any); !ok {
			return s, fmt.Errorf("observe is not an object")
		}
	}
	if s.ClockEpochSeconds, err = optInt64(m, "clock_epoch_seconds"); err != nil {
		return s, err
	}
	return s, nil
}

func parseExpectation(p *ScenarioPlan, m map[string]any) (Expectation, error) {
	var e Expectation
	var err error
	if errv, present := m["error"]; present {
		e.Kind = "error"
		e.Error, err = parseErrorExpect(errv)
		return e, err
	}
	e.Kind, _ = m["result"].(string)
	switch e.Kind {
	case "success", "none":
	case "snapshot":
		if e.HeadSeqNr, err = bigIntFromJSONNumber(m["head_seq_nr"]); err != nil {
			return e, fmt.Errorf("head_seq_nr: %w", err)
		}
		if name, ok := m["snapshot"].(string); ok {
			if _, known := p.Snaps[name]; !known {
				return e, fmt.Errorf("unknown snapshot fixture %q", name)
			}
			e.Snapshot = name
		}
	case "events":
		if e.Events, err = stringList(m["events"]); err != nil {
			return e, fmt.Errorf("events: %w", err)
		}
		for _, name := range e.Events {
			if _, known := p.Events[name]; !known {
				return e, fmt.Errorf("unknown event fixture %q", name)
			}
		}
	default:
		return e, fmt.Errorf("unsupported result %q", e.Kind)
	}
	return e, nil
}

func parseFault(v any) (FaultSpec, error) {
	var f FaultSpec
	m, ok := v.(map[string]any)
	if !ok {
		return f, fmt.Errorf("not an object")
	}
	op, err := bigIntFromJSONNumber(m["operation"])
	if err != nil || !op.IsInt64() || op.Sign() < 0 || op.Int64() > 1<<31 {
		return f, fmt.Errorf("operation is not a small non-negative integer")
	}
	f.Operation = int(op.Int64())
	f.Phase, _ = m["phase"].(string)
	f.Kind, _ = m["kind"].(string)
	f.Injection, _ = m["injection"].(string)
	f.Details, _ = m["details"].(map[string]any)
	rep, ok := m["repeat"].(map[string]any)
	if !ok {
		return f, fmt.Errorf("repeat is not an object")
	}
	switch rep["mode"] {
	case repeatCount:
		n, err := bigIntFromJSONNumber(rep["count"])
		if err != nil || !n.IsInt64() || n.Int64() < 1 || n.Int64() > 1<<31 {
			return f, fmt.Errorf("repeat.count is not a positive integer")
		}
		f.Repeat, f.Count = repeatCount, int(n.Int64())
	case repeatUntil:
		f.Repeat = repeatUntil
	default:
		return f, fmt.Errorf("unsupported repeat.mode %v", rep["mode"])
	}
	return f, nil
}

func parseCreateStore(s *StepPlan, args map[string]any, known map[string]bool) error {
	s.CreateName, _ = args["name"].(string)
	if s.CreateName == "" || known[s.CreateName] {
		return fmt.Errorf("createStore name %q is empty, reserved or already created", s.CreateName)
	}
	s.Storage, _ = args["storage"].(string)
	switch s.Storage {
	case "new":
		if _, present := args["shared_with"]; present {
			return fmt.Errorf("createStore with storage new must not have shared_with")
		}
	case "shared":
		s.SharedWith, _ = args["shared_with"].(string)
		if !known[s.SharedWith] {
			return fmt.Errorf("createStore shared_with %q is not a created store", s.SharedWith)
		}
	default:
		return fmt.Errorf("createStore storage %q is not new or shared", s.Storage)
	}
	return nil
}

// parseMutation reads a payload mutation. The referred step is checked here, and so is the
// pointer against the payload that the mutation will find at that position.
func parseMutation(p *ScenarioPlan, s *StepPlan, args map[string]any) error {
	op, err := bigIntFromJSONNumber(args["operation"])
	if err != nil || !op.IsInt64() || op.Int64() < 1 || op.Int64() > int64(len(p.Steps)) {
		return fmt.Errorf("operation must refer to an earlier step")
	}
	s.MutateOperation = int(op.Int64())
	ref := p.Steps[s.MutateOperation-1]
	s.Store = ref.Store
	s.Pointer, _ = args["pointer"].(string)
	s.Value = args["value"]
	var target string
	switch s.Op {
	case "mutateWrittenPayload":
		if (ref.Op != "persistEvent" && ref.Op != "persistEventAndSnapshot") || ref.Expect.Kind != "success" {
			return fmt.Errorf("operation %d is not a successful persist", s.MutateOperation)
		}
		target = ref.Event
	case "mutateReadPayload":
		if ref.Op != "getEventsByIdSinceSeqNr" || ref.Expect.Kind != "events" {
			return fmt.Errorf("operation %d is not a read that returned events", s.MutateOperation)
		}
		event, _ := args["event"].(string)
		s.MutateIndex = -1
		for i, name := range ref.Expect.Events {
			if name == event {
				s.MutateIndex = i
				break
			}
		}
		if s.MutateIndex < 0 {
			return fmt.Errorf("event %q is not returned by operation %d", event, s.MutateOperation)
		}
		target = event
	}
	// Trial run on a copy: the pointer must exist and the value must differ from the current one.
	return ReplaceAtPointer(deepCopy(p.Events[target].Payload), s.Pointer, s.Value)
}

// checkSQLiteOnlyUse rejects the misuse that conformance/README.md defines as a data error: lock_wait_milliseconds and
// seed.configuration are used by a scenario whose backends are exactly ["sqlite"], and a scenario that holds the write lock
// releases it before it ends, names the rule SQ-7, sets store.lock_wait_milliseconds and holds the lock while a persist
// that is expected to fail with a storage error runs. Whether holdWriteLock and releaseWriteLock may appear at all is
// decided earlier by parseStep, which keeps rejecting them in other scenarios as an unsupported operation.
func checkSQLiteOnlyUse(p *ScenarioPlan, backends, rules []string) error {
	onlySQLite := len(backends) == 1 && backends[0] == "sqlite"
	if p.Store.LockWaitMilliseconds != nil && !onlySQLite {
		return fmt.Errorf("lock_wait_milliseconds is only for a scenario whose backends are exactly [sqlite]")
	}
	onlySpanner := len(backends) == 1 && backends[0] == "spanner"
	if p.SeedConfiguration != nil && !onlySQLite && !onlySpanner {
		return fmt.Errorf("seed.configuration is only for a scenario whose backends are exactly [sqlite] or exactly [spanner]")
	}
	held, failedWhileHeld, usesLock := false, false, false
	for _, step := range p.Steps {
		switch step.Op {
		case "holdWriteLock":
			usesLock = true
			if held {
				return fmt.Errorf("holdWriteLock while the lock is already held")
			}
			held = true
		case "releaseWriteLock":
			usesLock = true
			if !held {
				return fmt.Errorf("releaseWriteLock without a held lock")
			}
			held = false
		case "persistEvent", "persistEventAndSnapshot":
			if held && step.Expect.Kind == "error" && step.Expect.Error != nil && step.Expect.Error.Category == "storage" {
				failedWhileHeld = true
			}
		}
	}
	if !usesLock {
		return nil
	}
	if !contains(rules, "SQ-7") {
		return fmt.Errorf("a scenario that holds the write lock needs the rule SQ-7")
	}
	if p.Store.LockWaitMilliseconds == nil {
		return fmt.Errorf("a scenario that holds the write lock needs store.lock_wait_milliseconds")
	}
	if held {
		return fmt.Errorf("the scenario ends while the write lock is held")
	}
	if !failedWhileHeld {
		return fmt.Errorf("no persist expects a storage error while the write lock is held")
	}
	return nil
}

// checkSpannerOnlyUse rejects the misuse that conformance/README.md defines as a data error for Spanner: the settings
// transaction_retry_limit and ttl_grace_days and seed.row_deletion_policy_days are for a scenario whose backends are
// exactly ["spanner"]; a Spanner TTL scenario names ttl_grace_days; and a transaction-aborted fault is used only by such
// a scenario that names the rule SPN-7, sets transaction_retry_limit and fails the commit by replacing the request.
func checkSpannerOnlyUse(p *ScenarioPlan, backends, rules []string) error {
	onlySpanner := len(backends) == 1 && backends[0] == "spanner"
	if !onlySpanner {
		if p.Store.TransactionRetryLimit != nil {
			return fmt.Errorf("transaction_retry_limit is only for a scenario whose backends are exactly [spanner]")
		}
		if p.Store.TTLGraceDays != nil {
			return fmt.Errorf("ttl_grace_days is only for a scenario whose backends are exactly [spanner]")
		}
		if p.SeedRowDeletionPolicyDays != nil {
			return fmt.Errorf("seed.row_deletion_policy_days is only for a scenario whose backends are exactly [spanner]")
		}
	}
	if onlySpanner && p.Store.RetentionMode == "ttl" && p.Store.TTLGraceDays == nil {
		return fmt.Errorf("a Spanner scenario with retention_mode ttl needs store.ttl_grace_days")
	}
	for _, f := range p.Faults {
		if f.Kind != "transaction-aborted" {
			continue
		}
		switch {
		case !onlySpanner:
			return fmt.Errorf("a transaction-aborted fault is only for a scenario whose backends are exactly [spanner]")
		case f.Phase != "commit" || f.Injection != "replace-request" || f.Repeat != repeatCount:
			return fmt.Errorf("a transaction-aborted fault needs phase commit, injection replace-request and a count repeat")
		case !contains(rules, "SPN-7"):
			return fmt.Errorf("a scenario with a transaction-aborted fault needs the rule SPN-7")
		case p.Store.TransactionRetryLimit == nil:
			return fmt.Errorf("a scenario with a transaction-aborted fault needs store.transaction_retry_limit")
		}
	}
	return nil
}
