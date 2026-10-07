package conformance

import (
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// DataVersion is the only supported version of the conformance data.
const DataVersion = "1.0.0"

// ValueInput is the input of a value-table case. Integers are kept without loss of precision.
type ValueInput struct {
	Raw              map[string]any
	SeqNr            *big.Int
	EventSeqNr       *big.Int
	EpochNanoseconds *big.Int
}

// ValueCase is a case of values/*.json. It is loaded but not executed.
type ValueCase struct {
	ID            string
	File          string
	Rules         []string
	Operation     string
	Input         ValueInput
	Expect        map[string]any
	TimePrecision string
}

// ScenarioCase is a scenario case. Materialized is Raw with generators expanded.
type ScenarioCase struct {
	ID            string
	File          string
	Rules         []string
	Backends      []string
	Requires      []string
	TimePrecision string
	Raw           map[string]any
	Materialized  map[string]any
	// Plan is Materialized read into its steps, faults and expectations.
	Plan *ScenarioPlan
}

// LayoutCase is a case of dynamodb/layout.json.
type LayoutCase struct {
	ID    string
	File  string
	Rules []string
	Raw   map[string]any
}

// Data is everything loaded from conformance/.
type Data struct {
	Values    []ValueCase
	Scenarios []ScenarioCase
	Layouts   []LayoutCase
	// Exclusions are the rules of coverage.json that have no case.
	Exclusions []RuleExclusion
}

// valueInputShape lists the keys of the input of a value-table operation.
type valueInputShape struct {
	required []string
	optional []string
}

// valueInputShapes is the set of supported operations, with the input each one takes.
// The schema accepts any of the four shapes for any operation, so this ties them together.
var valueInputShapes = map[string]valueInputShape{
	"buildAid":           {required: []string{"aggregate_id"}, optional: []string{"user_string"}},
	"validateOccurredAt": {required: []string{"iso8601", "epoch_nanoseconds", "event_seq_nr"}},
	"validateSeqNr":      {required: []string{"seq_nr", "context"}},
	"fnv1a64":            {required: []string{"utf8"}},
}

func checkValueInputShape(op string, in map[string]any) error {
	shape := valueInputShapes[op]
	allowed := map[string]bool{}
	for _, k := range shape.required {
		allowed[k] = true
		if _, present := in[k]; !present {
			return fmt.Errorf("input of %s needs %q", op, k)
		}
	}
	for _, k := range shape.optional {
		allowed[k] = true
	}
	for k := range in {
		if !allowed[k] {
			return fmt.Errorf("input of %s must not have %q", op, k)
		}
	}
	return nil
}

// expectedFormat returns the top-level format that the JSON file at rel (slash separated)
// must declare. Files under schema/ are JSON Schema documents and are not covered.
func expectedFormat(rel string) (string, bool) {
	switch {
	case rel == "manifest.json":
		return "manifest", true
	case rel == "coverage.json":
		return "coverage", true
	case rel == "dynamodb/layout.json":
		return "layout", true
	case path.Dir(rel) == "values" && strings.HasSuffix(rel, ".json"):
		return "values", true
	case path.Dir(rel) == "dynamodb" && strings.HasSuffix(rel, ".json"):
		return "scenarios", true
	case strings.HasPrefix(rel, "scenarios/") && strings.HasSuffix(rel, ".json"):
		return "scenarios", true
	}
	return "", false
}

// RuleExclusion is a rule of coverage.json that has no case, with the reason.
type RuleExclusion struct {
	Rule   string `json:"rule"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type dataDoc struct {
	rel    string
	format string
	obj    map[string]any
}

// LoadData reads every file under root, validates each data JSON against its schema in
// conformance/schema, checks format and version, loads the cases and expands generators.
// Schema validation runs before generators are expanded.
func LoadData(root string) (*Data, error) {
	schemas := map[string]any{}
	var docs []dataDoc
	err := filepath.WalkDir(root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if e.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%s: symbolic links are not allowed", rel)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		if !strings.HasSuffix(rel, ".json") {
			return nil
		}
		doc, err := decodeStrictJSON(raw)
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		if strings.HasPrefix(rel, "schema/") {
			schemas[rel] = doc
			return nil
		}
		format, ok := expectedFormat(rel)
		if !ok {
			return fmt.Errorf("%s: no expected format is defined for this JSON file", rel)
		}
		obj, ok := doc.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: top level is not an object", rel)
		}
		docs = append(docs, dataDoc{rel: rel, format: format, obj: obj})
		return nil
	})
	if err != nil {
		return nil, err
	}

	formats := map[string]bool{}
	var formatList []string
	for _, dd := range docs {
		if !formats[dd.format] {
			formats[dd.format] = true
			formatList = append(formatList, dd.format)
		}
	}
	set, err := newSchemaSet(schemas, formatList)
	if err != nil {
		return nil, err
	}

	d := &Data{}
	ids := map[string]string{}
	for _, dd := range docs {
		if err := set.validate(dd.format, dd.obj); err != nil {
			return nil, fmt.Errorf("%s: schema validation: %w", dd.rel, err)
		}
		if dd.obj["format"] != dd.format {
			return nil, fmt.Errorf("%s: format is %v, want %s", dd.rel, dd.obj["format"], dd.format)
		}
		if dd.obj["version"] != DataVersion {
			return nil, fmt.Errorf("%s: version is %v, want %s", dd.rel, dd.obj["version"], DataVersion)
		}
		switch dd.format {
		case "values", "scenarios", "layout":
			err = loadCases(d, dd.rel, dd.format, dd.obj, ids)
		case "coverage":
			err = loadExclusions(d, dd.rel, dd.obj)
		}
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

func loadExclusions(d *Data, rel string, obj map[string]any) error {
	list, ok := obj["exclusions"].([]any)
	if !ok {
		return fmt.Errorf("%s: exclusions is not an array", rel)
	}
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: exclusions[%d] is not an object", rel, i)
		}
		rule, rok := m["rule"].(string)
		status, sok := m["status"].(string)
		reason, nok := m["reason"].(string)
		if !rok || !sok || !nok {
			return fmt.Errorf("%s: exclusions[%d] needs string rule, status and reason", rel, i)
		}
		d.Exclusions = append(d.Exclusions, RuleExclusion{Rule: rule, Status: status, Reason: reason})
	}
	return nil
}

func loadCases(d *Data, rel, format string, obj map[string]any, ids map[string]string) error {
	list, ok := obj["cases"].([]any)
	if !ok {
		return fmt.Errorf("%s: cases is not an array", rel)
	}
	for i, item := range list {
		c, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: cases[%d] is not an object", rel, i)
		}
		id, ok := c["id"].(string)
		if !ok || id == "" {
			return fmt.Errorf("%s: cases[%d].id is not a non-empty string", rel, i)
		}
		if prev, dup := ids[id]; dup {
			return fmt.Errorf("%s: duplicate case id %q (also in %s)", rel, id, prev)
		}
		ids[id] = rel
		rules, err := stringList(c["rules"])
		if err != nil {
			return fmt.Errorf("%s: case %q: rules: %w", rel, id, err)
		}
		precision, err := timePrecision(c)
		if err != nil {
			return fmt.Errorf("%s: case %q: %w", rel, id, err)
		}
		switch format {
		case "values":
			vc, err := newValueCase(rel, id, rules, precision, c)
			if err != nil {
				return fmt.Errorf("%s: case %q: %w", rel, id, err)
			}
			d.Values = append(d.Values, vc)
		case "scenarios":
			backends, err := stringList(c["backends"])
			if err != nil {
				return fmt.Errorf("%s: case %q: backends: %w", rel, id, err)
			}
			var requires []string
			if r, present := c["requires"]; present {
				if requires, err = stringList(r); err != nil {
					return fmt.Errorf("%s: case %q: requires: %w", rel, id, err)
				}
			}
			mat, err := materialize(c)
			if err != nil {
				return fmt.Errorf("%s: case %q: %w", rel, id, err)
			}
			plan, err := parseScenario(mat)
			if err != nil {
				return fmt.Errorf("%s: case %q: %w", rel, id, err)
			}
			d.Scenarios = append(d.Scenarios, ScenarioCase{
				ID: id, File: rel, Rules: rules, Backends: backends, Requires: requires,
				TimePrecision: precision, Raw: c, Materialized: mat, Plan: plan,
			})
		case "layout":
			d.Layouts = append(d.Layouts, LayoutCase{ID: id, File: rel, Rules: rules, Raw: c})
		}
	}
	return nil
}

func newValueCase(rel, id string, rules []string, precision string, c map[string]any) (ValueCase, error) {
	op, _ := c["operation"].(string)
	if _, known := valueInputShapes[op]; !known {
		return ValueCase{}, fmt.Errorf("unsupported operation %v", c["operation"])
	}
	in, ok := c["input"].(map[string]any)
	if !ok {
		return ValueCase{}, fmt.Errorf("input is not an object")
	}
	if err := checkValueInputShape(op, in); err != nil {
		return ValueCase{}, err
	}
	expect, ok := c["expect"].(map[string]any)
	if !ok {
		return ValueCase{}, fmt.Errorf("expect is not an object")
	}
	vi := ValueInput{Raw: in}
	var err error
	if v, present := in["seq_nr"]; present {
		if vi.SeqNr, err = bigIntFromJSONNumber(v); err != nil {
			return ValueCase{}, fmt.Errorf("input.seq_nr: %w", err)
		}
	}
	if v, present := in["event_seq_nr"]; present {
		if vi.EventSeqNr, err = bigIntFromJSONNumber(v); err != nil {
			return ValueCase{}, fmt.Errorf("input.event_seq_nr: %w", err)
		}
	}
	if v, present := in["epoch_nanoseconds"]; present {
		if vi.EpochNanoseconds, err = bigIntFromDecimalString(v); err != nil {
			return ValueCase{}, fmt.Errorf("input.epoch_nanoseconds: %w", err)
		}
	}
	return ValueCase{ID: id, File: rel, Rules: rules, Operation: op, Input: vi, Expect: expect, TimePrecision: precision}, nil
}

func timePrecision(c map[string]any) (string, error) {
	rep, present := c["representation"]
	if !present {
		return "", nil
	}
	m, ok := rep.(map[string]any)
	if !ok {
		return "", fmt.Errorf("representation is not an object")
	}
	tp, present := m["time_precision"]
	if !present {
		return "", nil
	}
	s, _ := tp.(string)
	if s != "nanoseconds" && s != "milliseconds" {
		return "", fmt.Errorf("unsupported representation.time_precision %v", tp)
	}
	return s, nil
}

func stringList(v any) ([]string, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("not an array")
	}
	out := make([]string, 0, len(list))
	for _, x := range list {
		s, ok := x.(string)
		if !ok {
			return nil, fmt.Errorf("contains a non-string element")
		}
		out = append(out, s)
	}
	return out, nil
}
