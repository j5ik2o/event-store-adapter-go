package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

const (
	reasonNoBackend     = "保存先の境界が未接続のため、場面を実行していない（設計 7.2 の4番以降）"
	reasonUnwired       = "観測を検査する手段がまだなく、場面を検証できない"
	reasonNotInjectable = "保存先が差し込めない障害を持つため、場面を検証できない（設計 5.4）"
	reasonMemoryTTL     = "TTL 方式を要求する場面は DynamoDB だけで、メモリでは対象外（設計 5.2 の1）"
)

// runScenarioCases runs every scenario on every backend it lists. backends maps a backend name
// to its boundary; a name that is missing means the backend is not connected.
func runScenarioCases(ctx context.Context, cases []ScenarioCase, backends map[string]Backend) []CaseResult {
	var out []CaseResult
	for _, c := range cases {
		for _, name := range c.Backends {
			out = append(out, runScenario(ctx, c, name, backends[name]))
		}
	}
	return out
}

// runScenario runs one scenario on one backend following design 5.2. The first decision wins:
// not-applicable, unverified, the first step that does not match, then the firing counts.
func runScenario(ctx context.Context, c ScenarioCase, backend string, b Backend) CaseResult {
	res := CaseResult{ID: c.ID, Rules: nonNil(c.Rules), Backend: backend}
	set := func(s Status, reason string) CaseResult { res.Status, res.Reason = s, reason; return res }

	plan := c.Plan
	switch {
	case c.TimePrecision == "milliseconds":
		return set(StatusNotApplicable, reasonMillis)
	case backend == "memory" && contains(c.Requires, "ttl"):
		return set(StatusNotApplicable, reasonMemoryTTL)
	case b == nil:
		return set(StatusUnverified, reasonNoBackend)
	case plan.UnwiredObservation != "":
		return set(StatusUnverified, reasonUnwired+": "+plan.UnwiredObservation)
	}
	for _, f := range plan.Faults {
		if !b.Injectable(f) {
			return set(StatusUnverified, fmt.Sprintf("%s: operation %d, phase %s, kind %s", reasonNotInjectable, f.Operation, f.Phase, f.Kind))
		}
	}

	cursor := &operationCursor{}
	faults := newFaults(plan.Faults, cursor)
	hooks := testhook.New()
	hooks.SetSleeper(func(time.Duration) {})
	var clock atomic.Int64
	needsClock := plan.ClockStart != nil
	for _, s := range plan.Steps {
		if s.ClockEpochSeconds != nil {
			needsClock = true
			break
		}
	}
	if needsClock {
		if plan.ClockStart != nil {
			clock.Store(*plan.ClockStart)
		}
		hooks.SetClock(func() time.Time { return time.Unix(clock.Load(), 0).UTC() })
	}
	registerHookFaults(hooks, faults)

	fail := func(step int, reason string, expected, actual any) CaseResult {
		res.Status, res.Reason, res.Expected, res.Actual = StatusFailure, reason, expected, actual
		res.FailedStep = &step
		return res
	}

	if len(plan.Seed) > 0 {
		if err := b.Seed(ctx, plan.Seed); err != nil {
			return fail(0, "seed に失敗した: "+err.Error(), nil, err.Error())
		}
	}
	store, openErr := b.Open(ctx, plan.Store, Injection{Faults: faults, Hooks: hooks})
	if store != nil {
		defer store.Close()
	}
	if plan.Init != nil && plan.Init.Error != nil {
		if msg := matchError(plan.Init.Error, openErr); msg != "" {
			return fail(0, "生成: "+msg, plan.Init.Error, errorText(openErr))
		}
		return finish(res, faults)
	}
	if openErr != nil {
		return fail(0, "生成に失敗した: "+openErr.Error(), "success", errorText(openErr))
	}
	if store == nil {
		return fail(0, "生成がストアもエラーも返さなかった", "success", nil)
	}

	for i, s := range plan.Steps {
		n := i + 1
		cursor.Set(n)
		if s.ClockEpochSeconds != nil {
			clock.Store(*s.ClockEpochSeconds)
		}
		out := execStep(ctx, store, plan, s)
		if msg := checkExpectation(plan, s, out); msg != "" {
			return fail(n, msg, s.Expect, out.describe())
		}
		if msg := checkObservation(s, hooks, store); msg != "" {
			return fail(n, msg, s.Observe, nil)
		}
	}
	return finish(res, faults)
}

func finish(res CaseResult, faults []*Fault) CaseResult {
	if msg := verifyFaults(faults); msg != "" {
		res.Status, res.Reason = StatusFailure, "発火の数が合わない: "+msg
		return res
	}
	res.Status, res.Reason = StatusSuccess, ""
	return res
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func errorText(err error) string {
	if err == nil {
		return "success"
	}
	var oe *OperationError
	if errors.As(err, &oe) {
		return oe.Category + ": " + oe.Message
	}
	return "unclassified: " + err.Error()
}

type stepOutcome struct {
	err    error
	snap   SnapshotRead
	events []Event
	op     string
}

func (o stepOutcome) describe() any {
	if o.err != nil {
		return errorText(o.err)
	}
	switch o.op {
	case "getLatestSnapshotById":
		return fmt.Sprintf("found=%v head_seq_nr=%d", o.snap.Found, o.snap.HeadSeqNr)
	case "getEventsByIdSinceSeqNr":
		seqs := make([]int64, len(o.events))
		for i, e := range o.events {
			seqs[i] = e.SeqNr
		}
		return fmt.Sprintf("events seq_nr=%v", seqs)
	}
	return "success"
}

func toEvent(f eventFixture) (Event, error) {
	if !f.SeqNr.IsInt64() {
		return Event{}, fmt.Errorf("seq_nr %s does not fit in int64", f.SeqNr)
	}
	at, err := time.Parse(time.RFC3339Nano, f.OccurredAt)
	if err != nil {
		return Event{}, fmt.Errorf("occurred_at: %w", err)
	}
	payload, err := json.Marshal(f.Payload)
	if err != nil {
		return Event{}, err
	}
	return Event{AggregateID: f.AID, SeqNr: f.SeqNr.Int64(), OccurredAt: at, Payload: payload, Manifest: f.Manifest}, nil
}

func toSnapshot(f snapshotFixture) (Snapshot, error) {
	if !f.SeqNr.IsInt64() {
		return Snapshot{}, fmt.Errorf("seq_nr %s does not fit in int64", f.SeqNr)
	}
	agg, err := json.Marshal(f.Aggregate)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{SeqNr: f.SeqNr.Int64(), Aggregate: agg, Manifest: f.Manifest}, nil
}

// execStep converts the fixtures into the arguments and calls the store. A conversion that
// fails is the failure of this operation; the runner does not validate the envelope itself.
func execStep(ctx context.Context, st Store, plan *ScenarioPlan, s StepPlan) stepOutcome {
	out := stepOutcome{op: s.Op}
	switch s.Op {
	case "persistEvent":
		ev, err := toEvent(plan.Events[s.Event])
		if err != nil {
			out.err = err
			return out
		}
		out.err = st.PersistEvent(ctx, ev)
	case "persistEventAndSnapshot":
		ev, err := toEvent(plan.Events[s.Event])
		if err != nil {
			out.err = err
			return out
		}
		sn, err := toSnapshot(plan.Snaps[s.Snapshot])
		if err != nil {
			out.err = err
			return out
		}
		out.err = st.PersistEventAndSnapshot(ctx, ev, sn)
	case "getLatestSnapshotById":
		out.snap, out.err = st.GetLatestSnapshotByID(ctx, s.AID)
	case "getEventsByIdSinceSeqNr":
		if !s.SeqNr.IsInt64() {
			out.err = fmt.Errorf("seq_nr %s does not fit in int64", s.SeqNr)
			return out
		}
		out.events, out.err = st.GetEventsByIDSinceSeqNr(ctx, s.AID, s.SeqNr.Int64())
	}
	return out
}

// matchError compares an error by its category, then by the message rules. It returns "" on a match.
func matchError(exp *ErrorExpect, err error) string {
	if err == nil {
		return fmt.Sprintf("エラー（%s）を期待したが成功した", exp.Category)
	}
	var oe *OperationError
	if !errors.As(err, &oe) {
		return fmt.Sprintf("エラー（%s）を期待したが、分類のないエラーだった: %v", exp.Category, err)
	}
	if oe.Category != exp.Category {
		return fmt.Sprintf("エラーの分類が合わない: want %s, got %s", exp.Category, oe.Category)
	}
	msg := err.Error()
	if exp.Rule != "" && !strings.Contains(msg, exp.Rule) {
		return fmt.Sprintf("エラーのメッセージに規則 %q がない: %q", exp.Rule, msg)
	}
	for _, s := range exp.MustContain {
		if !strings.Contains(msg, s) {
			return fmt.Sprintf("エラーのメッセージに %q がない: %q", s, msg)
		}
	}
	for _, s := range exp.MustNotContain {
		if strings.Contains(msg, s) {
			return fmt.Sprintf("エラーのメッセージに %q が含まれている: %q", s, msg)
		}
	}
	return ""
}

func checkExpectation(plan *ScenarioPlan, s StepPlan, o stepOutcome) string {
	e := s.Expect
	if e.Kind == "error" {
		return matchError(e.Error, o.err)
	}
	if o.err != nil {
		return fmt.Sprintf("%s を期待したがエラーになった: %s", e.Kind, errorText(o.err))
	}
	switch e.Kind {
	case "success":
		return ""
	case "none":
		if o.snap.Found {
			return "結果なしを期待したが、スナップショットが返った"
		}
		return ""
	case "snapshot":
		if !o.snap.Found {
			return "スナップショットを期待したが、結果なしだった"
		}
		if o.snap.HeadSeqNr != e.HeadSeqNr.Int64() || !e.HeadSeqNr.IsInt64() {
			return fmt.Sprintf("head_seq_nr が合わない: want %s, got %d", e.HeadSeqNr, o.snap.HeadSeqNr)
		}
		if e.Snapshot == "" {
			if o.snap.Snapshot != nil {
				return "スナップショットは null を期待したが、値が返った"
			}
			return ""
		}
		if o.snap.Snapshot == nil {
			return fmt.Sprintf("スナップショット %s を期待したが null だった", e.Snapshot)
		}
		want, err := toSnapshot(plan.Snaps[e.Snapshot])
		if err != nil {
			return err.Error()
		}
		return diffSnapshot(want, *o.snap.Snapshot)
	case "events":
		if len(o.events) != len(e.Events) {
			return fmt.Sprintf("イベントの件数が合わない: want %d, got %d", len(e.Events), len(o.events))
		}
		for i, name := range e.Events {
			want, err := toEvent(plan.Events[name])
			if err != nil {
				return err.Error()
			}
			if msg := diffEvent(want, o.events[i]); msg != "" {
				return fmt.Sprintf("events[%d] (%s): %s", i, name, msg)
			}
		}
	}
	return ""
}

func jsonBytesEqual(a, b []byte) bool {
	av, err1 := decodeStrictJSON(a)
	bv, err2 := decodeStrictJSON(b)
	return err1 == nil && err2 == nil && jsonEqual(av, bv)
}

func diffEvent(want, got Event) string {
	switch {
	case want.AggregateID != got.AggregateID:
		return fmt.Sprintf("aggregate_id が合わない: want %v, got %v", want.AggregateID, got.AggregateID)
	case want.SeqNr != got.SeqNr:
		return fmt.Sprintf("seq_nr が合わない: want %d, got %d", want.SeqNr, got.SeqNr)
	case !want.OccurredAt.Equal(got.OccurredAt):
		return fmt.Sprintf("occurred_at が合わない: want %s, got %s", want.OccurredAt, got.OccurredAt)
	case want.Manifest != got.Manifest:
		return fmt.Sprintf("manifest が合わない: want %q, got %q", want.Manifest, got.Manifest)
	case !jsonBytesEqual(want.Payload, got.Payload):
		return fmt.Sprintf("payload が合わない: want %s, got %s", want.Payload, got.Payload)
	}
	return ""
}

func diffSnapshot(want, got Snapshot) string {
	switch {
	case want.SeqNr != got.SeqNr:
		return fmt.Sprintf("スナップショットの seq_nr が合わない: want %d, got %d", want.SeqNr, got.SeqNr)
	case want.Manifest != got.Manifest:
		return fmt.Sprintf("スナップショットの manifest が合わない: want %q, got %q", want.Manifest, got.Manifest)
	case !jsonBytesEqual(want.Aggregate, got.Aggregate):
		return fmt.Sprintf("スナップショットの aggregate が合わない: want %s, got %s", want.Aggregate, got.Aggregate)
	}
	return ""
}

func int64s(v any) ([]int64, bool) {
	list, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]int64, 0, len(list))
	for _, x := range list {
		var n *big.Int
		var err error
		if m, ok := x.(map[string]any); ok {
			n, err = bigIntFromJSONNumber(m["seq_nr"])
		} else {
			n, err = bigIntFromJSONNumber(x)
		}
		if err != nil || !n.IsInt64() {
			return nil, false
		}
		out = append(out, n.Int64())
	}
	return out, true
}

func sorted(a []int64) []int64 {
	b := append([]int64{}, a...)
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	return b
}

// checkObservation checks observe.history (through testhook) and observe.notifications.
func checkObservation(s StepPlan, hooks *testhook.Hooks, st Store) string {
	if h, ok := s.Observe["history"].(map[string]any); ok {
		active, ok1 := int64s(h["active"])
		marked, ok2 := int64s(h["marked"])
		absent, ok3 := int64s(h["absent"])
		if !ok1 || !ok2 || !ok3 {
			return "observe.history の形が不正"
		}
		got, err := hooks.History(s.AID.TypeName + "-" + s.AID.Value)
		if err != nil {
			return "履歴を読めない: " + err.Error()
		}
		if !reflect.DeepEqual(sorted(active), sorted(got.Active)) {
			return fmt.Sprintf("history.active が合わない: want %v, got %v", sorted(active), sorted(got.Active))
		}
		if !reflect.DeepEqual(sorted(marked), sorted(got.Marked)) {
			return fmt.Sprintf("history.marked が合わない: want %v, got %v", sorted(marked), sorted(got.Marked))
		}
		present := map[int64]bool{}
		for _, n := range append(append([]int64{}, got.Active...), got.Marked...) {
			present[n] = true
		}
		for _, n := range absent {
			if present[n] {
				return fmt.Sprintf("history.absent の %d が履歴に残っている", n)
			}
		}
	}
	if n, ok := s.Observe["notifications"].([]any); ok {
		want := make([]string, 0, len(n))
		for _, x := range n {
			str, _ := x.(string)
			want = append(want, str)
		}
		got := st.Notifications()
		if got == nil {
			got = []string{}
		}
		if !reflect.DeepEqual(want, got) {
			return fmt.Sprintf("notifications が合わない: want %v, got %v", want, got)
		}
	}
	return ""
}

// jsonEqual compares two decoded JSON values (from decodeStrictJSON). Object key order is ignored,
// array order is kept, numbers are compared by value, and booleans, null, numbers and strings
// are never equal to one another. Strings are compared as they are.
func jsonEqual(a, b any) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case string:
		y, ok := b.(string)
		return ok && x == y
	case json.Number:
		y, ok := b.(json.Number)
		if !ok {
			return false
		}
		rx, ok1 := new(big.Rat).SetString(x.String())
		ry, ok2 := new(big.Rat).SetString(y.String())
		return ok1 && ok2 && rx.Cmp(ry) == 0
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !jsonEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, present := y[k]
			if !present || !jsonEqual(v, w) {
				return false
			}
		}
		return true
	}
	return false
}
