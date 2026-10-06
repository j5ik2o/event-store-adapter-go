package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestConformance is the entry point used by CI: it loads the data, verifies the manifest,
// classifies every case, evaluates required.json, always writes the report, and only then decides failure.
func TestConformance(t *testing.T) {
	var errs []string
	root := dataRoot()

	m, err := VerifyManifest(root)
	if err != nil {
		errs = append(errs, "manifest: "+err.Error())
	}

	var results []CaseResult
	d, err := LoadData(root)
	if err != nil {
		errs = append(errs, "load: "+err.Error())
	} else {
		results = classifyCases(d)
	}

	var gate GateResult
	req, err := LoadRequired("required.json")
	if err != nil {
		errs = append(errs, "required: "+err.Error())
	} else if g, err := EvaluateRequired(req, results); err != nil {
		errs = append(errs, "required: "+err.Error())
	} else {
		gate = g
	}

	rep := BuildReport(m, results, gate, errs)

	path := os.Getenv("CONFORMANCE_REPORT")
	if path == "" {
		path = filepath.Join(t.TempDir(), "conformance-report.json")
	}
	if err := WriteReport(path, rep); err != nil {
		t.Fatalf("write report: %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var top struct {
		Summary map[string]int `json:"summary"`
	}
	if err := json.Unmarshal(b, &top); err != nil {
		t.Fatalf("report is not valid json: %v", err)
	}
	t.Logf("summary: %v", top.Summary)
	if top.Summary["success"] != 0 {
		t.Fatalf("nothing is executed yet, success must be 0: %v", top.Summary)
	}

	if reasons := rep.FailureReasons(); len(reasons) > 0 {
		t.Fatalf("conformance failed: %v", reasons)
	}
}
