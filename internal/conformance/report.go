package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
)

// Status is the state of a case in the report.
type Status string

const (
	StatusSuccess         Status = "success"
	StatusFailure         Status = "failure"
	StatusNotApplicable   Status = "not-applicable"
	StatusUnverified      Status = "unverified"
	StatusUnrepresentable Status = "unrepresentable"
)

var allStatuses = []Status{StatusSuccess, StatusFailure, StatusNotApplicable, StatusUnverified, StatusUnrepresentable}

// CaseResult is the report entry of one case.
type CaseResult struct {
	ID         string   `json:"id"`
	Rules      []string `json:"rules"`
	Backends   []string `json:"backends"`
	Status     Status   `json:"status"`
	Reason     string   `json:"reason"`
	FailedStep *int     `json:"failed_step"`
	Expected   any      `json:"expected"`
	Actual     any      `json:"actual"`
}

// Implementation identifies the implementation under test.
type Implementation struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// BackendState tells whether a backend is connected to the runner.
type BackendState struct {
	Name      string `json:"name"`
	Connected bool   `json:"connected"`
}

// Report is the content of conformance-report.json.
type Report struct {
	DataVersion    string                    `json:"data_version"`
	Manifest       ManifestResult            `json:"manifest"`
	Language       string                    `json:"language"`
	Implementation Implementation            `json:"implementation"`
	Backends       []BackendState            `json:"backends"`
	Summary        map[Status]int            `json:"summary"`
	Rules          map[string]map[Status]int `json:"rules"`
	Cases          []CaseResult              `json:"cases"`
	Required       GateResult                `json:"required"`
	Errors         []string                  `json:"errors"`
}

const (
	reasonFnv1a64 = "最初のメジャーにはハッシュを使う保存先がなく、FNV-1a 64 は段階 5 で実行する（設計 5.1）"
	reasonMillis  = "Go の time.Time はナノ秒精度であり、representation.time_precision が milliseconds のケースは対象外（設計 5.1）"
	reasonNoStore = "場面と値の表の操作は実行していない。中核と保存先に未接続（設計 7.2 の2番）"
)

// classifyCases decides the status and the reason of every case. Nothing is executed yet,
// so the status is either not-applicable or unverified, never success.
func classifyCases(d *Data) []CaseResult {
	classify := func(id string, rules, backends []string, fnv bool, precision string) CaseResult {
		r := CaseResult{ID: id, Rules: nonNil(rules), Backends: nonNil(backends)}
		switch {
		case fnv:
			r.Status, r.Reason = StatusNotApplicable, reasonFnv1a64
		case precision == "milliseconds":
			r.Status, r.Reason = StatusNotApplicable, reasonMillis
		default:
			r.Status, r.Reason = StatusUnverified, reasonNoStore
		}
		return r
	}
	var out []CaseResult
	for _, c := range d.Values {
		out = append(out, classify(c.ID, c.Rules, nil, c.Operation == "fnv1a64", c.TimePrecision))
	}
	for _, c := range d.Scenarios {
		out = append(out, classify(c.ID, c.Rules, c.Backends, false, c.TimePrecision))
	}
	for _, c := range d.Layouts {
		out = append(out, classify(c.ID, c.Rules, []string{"dynamodb"}, false, ""))
	}
	return out
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func zeroCounts() map[Status]int {
	m := make(map[Status]int, len(allStatuses))
	for _, s := range allStatuses {
		m[s] = 0
	}
	return m
}

// BuildReport assembles the report. Every status key is present in the counts.
func BuildReport(m ManifestResult, results []CaseResult, gate GateResult, errs []string) Report {
	version := "(devel)"
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		version = bi.Main.Version
	}
	if m.Mismatches == nil {
		m.Mismatches = []string{}
	}
	if gate.Violations == nil {
		gate.Violations = []string{}
	}
	lists := RequiredList{}
	for _, b := range requiredBackends {
		lists[b] = []string{}
	}
	for b, ids := range gate.Lists {
		lists[b] = nonNil(ids)
	}
	gate.Lists = lists
	if errs == nil {
		errs = []string{}
	}
	if results == nil {
		results = []CaseResult{}
	}
	r := Report{
		DataVersion:    DataVersion,
		Manifest:       m,
		Language:       "go",
		Implementation: Implementation{Name: "github.com/j5ik2o/event-store-adapter-go/v2", Version: version},
		Backends:       []BackendState{{Name: "memory"}, {Name: "dynamodb"}},
		Summary:        zeroCounts(),
		Rules:          map[string]map[Status]int{},
		Cases:          results,
		Required:       gate,
		Errors:         errs,
	}
	for _, c := range results {
		r.Summary[c.Status]++
		for _, rule := range c.Rules {
			if r.Rules[rule] == nil {
				r.Rules[rule] = zeroCounts()
			}
			r.Rules[rule][c.Status]++
		}
	}
	return r
}

// WriteReport writes the report as indented JSON.
func WriteReport(path string, r Report) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// FailureReasons lists why the runner must fail: the manifest does not match, a required
// case is failed or unverified, or the runner itself had an error. Unverified and
// not-applicable cases outside the required list never fail the run.
func (r Report) FailureReasons() []string {
	var reasons []string
	if !r.Manifest.Verified {
		reasons = append(reasons, fmt.Sprintf("manifest does not match the data: %v", r.Manifest.Mismatches))
	}
	for _, v := range r.Required.Violations {
		reasons = append(reasons, "required case: "+v)
	}
	for _, e := range r.Errors {
		reasons = append(reasons, "runner error: "+e)
	}
	return reasons
}
