package conformance

import (
	"fmt"
	"math/big"
)

// ScenarioPlan is a scenario with its structure read and its references checked.
type ScenarioPlan struct {
	Store    StoreConfig
	Seed     []map[string]any
	Init     *InitPlan
	Events   map[string]eventFixture
	Snaps    map[string]snapshotFixture
	Steps    []StepPlan
	Faults   []FaultSpec
	Requires []string
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
	Op       string
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

func parseStoreConfig(m map[string]any) (StoreConfig, error) {
	var c StoreConfig
	var err error
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
	switch s.Op {
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
