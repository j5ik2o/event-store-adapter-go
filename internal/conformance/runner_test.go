package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/stretchr/testify/require"
)

// TestConformance preserves the existing CI entry and artifact path.
func TestConformance(t *testing.T) {
	path := os.Getenv("CONFORMANCE_REPORT")
	if path == "" {
		t.Skip("CONFORMANCE_REPORT is not set")
	}
	ctx := context.Background()
	root := filepath.Join("..", "..", "conformance")
	report, err := runConformance(ctx, root, "required.json", path, func(ctx context.Context) (*dynamodbtest.Environment, error) {
		environment, err := dynamodbtest.Start(ctx)
		if err == nil {
			t.Cleanup(func() { require.NoError(t, environment.Close(context.Background())) })
		}
		return environment, err
	})
	t.Logf("summary: %v", report.Summary)
	require.NoError(t, err)
}

// runConformance retains preparation diagnostics and attempts every report before
// returning failure. The start function owns cleanup of a successfully started environment.
func runConformance(ctx context.Context, root, requiredPath, path string, start func(context.Context) (*dynamodbtest.Environment, error)) (conformance.Report, error) {
	var causes []error
	record := func(stage string, err error) {
		if err != nil {
			causes = append(causes, fmt.Errorf("%s: %w", stage, err))
		}
	}
	manifest, err := conformance.VerifyManifest(root)
	record("manifest", err)
	data, err := conformance.LoadData(root)
	record("load", err)
	required, err := conformance.LoadRequired(requiredPath)
	record("required", err)
	var excluded []conformance.RuleExclusion
	if data != nil {
		excluded = data.Exclusions
		if required != nil {
			record("required coverage", conformance.CheckRequiredCoverage(data, required))
		}
	}
	var results []conformance.CaseResult
	backends := []conformance.BackendState{{Name: "memory"}, {Name: "dynamodb"}}
	if len(causes) == 0 {
		environment, err := start(ctx)
		record("setup", err)
		if err == nil {
			for i, backend := range []*publicBackend{{name: "memory"}, {name: "dynamodb", environment: environment}} {
				actual := conformance.RunBackend(ctx, data, backend)
				results = append(results, actual...)
				backends[i].Connected = true
				list := conformance.RequiredList{backend.name: required[backend.name]}
				report := conformance.BuildReport(manifest, actual, excluded, conformance.GateResult{Lists: list}, nil)
				report.Backends = []conformance.BackendState{backends[i]}
				_, err := finishConformanceReport(filepath.Join(filepath.Dir(path), backend.name+"-conformance-report.json"), report, list)
				record(backend.name+" report", err)
			}
		}
	}
	diagnostics := make([]string, len(causes))
	for i, cause := range causes {
		diagnostics[i] = cause.Error()
	}
	report := conformance.BuildReport(manifest, results, excluded, conformance.GateResult{Lists: required}, diagnostics)
	report.Backends = backends
	report, err = finishConformanceReport(path, report, required)
	return report, errors.Join(append(causes, err)...)
}

// finishConformanceReport evaluates the real gate and writes diagnostics before
// applying the existing case and report failure criteria.
func finishConformanceReport(path string, report conformance.Report, required conformance.RequiredList) (conformance.Report, error) {
	var causes []error
	if required != nil {
		gate, err := conformance.EvaluateRequired(required, report.Cases)
		if err != nil {
			cause := fmt.Errorf("required: %w", err)
			causes = append(causes, cause)
			report.Errors = append(report.Errors, cause.Error())
		} else {
			report.Required.Violations = gate.Violations
		}
	}
	if err := conformance.WriteReport(path, report); err != nil {
		causes = append(causes, fmt.Errorf("write report: %w", err))
	}
	for _, result := range report.Cases {
		if result.Status == conformance.StatusFailure || result.Status == conformance.StatusUnverified {
			causes = append(causes, fmt.Errorf("%s/%s: %s", result.Backend, result.ID, result.Reason))
		}
	}
	for _, reason := range report.FailureReasons() {
		causes = append(causes, errors.New(reason))
	}
	return report, errors.Join(causes...)
}

func TestConformanceSetupFailureReport(t *testing.T) {
	for _, writable := range []bool{true, false} {
		t.Run(fmt.Sprintf("writable=%t", writable), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "conformance-report.json")
			if !writable {
				path = filepath.Join(t.TempDir(), "missing", "conformance-report.json")
			}
			cause := errors.New("declared container startup failure")
			starts := 0

			report, err := runConformance(t.Context(), filepath.Join("..", "..", "conformance"), "required.json", path, func(context.Context) (*dynamodbtest.Environment, error) {
				starts++
				return nil, cause
			})

			require.ErrorIs(t, err, cause)
			require.Equal(t, 1, starts)
			require.Contains(t, report.Errors, "setup: "+cause.Error())
			require.True(t, report.Manifest.Verified)
			require.Len(t, report.Required.Lists["memory"], 60)
			require.Len(t, report.Required.Lists["dynamodb"], 104)
			require.Empty(t, report.Cases)
			require.Equal(t, []conformance.BackendState{{Name: "memory"}, {Name: "dynamodb"}}, report.Backends)
			require.NotEmpty(t, report.FailureReasons())
			if writable {
				raw, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				var saved conformance.Report
				require.NoError(t, json.Unmarshal(raw, &saved))
				require.Contains(t, saved.Errors, "setup: "+cause.Error())
				require.Equal(t, report.Backends, saved.Backends)
				require.NotEmpty(t, saved.FailureReasons())
			} else {
				var writeErr *os.PathError
				require.ErrorAs(t, err, &writeErr)
				require.Equal(t, path, writeErr.Path)
				require.Equal(t, "open", writeErr.Op)
				require.ErrorIs(t, writeErr, os.ErrNotExist)
				require.NoFileExists(t, path)
			}
		})
	}
}

func TestConformancePreparationFailureReport(t *testing.T) {
	for _, stage := range []string{"manifest", "load", "required", "required coverage"} {
		t.Run(stage, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "conformance")
			require.NoError(t, os.CopyFS(root, os.DirFS(filepath.Join("..", "..", "conformance"))))
			requiredPath := "required.json"
			switch stage {
			case "manifest":
				require.NoError(t, os.Remove(filepath.Join(root, "manifest.json")))
			case "load":
				require.NoError(t, os.WriteFile(filepath.Join(root, "values", "aid.json"), []byte(`{`), 0o644))
			case "required":
				requiredPath = filepath.Join(t.TempDir(), "missing-required.json")
			case "required coverage":
				required, err := conformance.LoadRequired(requiredPath)
				require.NoError(t, err)
				required["memory"] = required["memory"][1:]
				raw, err := json.Marshal(required)
				require.NoError(t, err)
				requiredPath = filepath.Join(t.TempDir(), "required.json")
				require.NoError(t, os.WriteFile(requiredPath, raw, 0o644))
			}
			path := filepath.Join(t.TempDir(), "conformance-report.json")
			starts := 0

			report, err := runConformance(t.Context(), root, requiredPath, path, func(context.Context) (*dynamodbtest.Environment, error) {
				starts++
				return nil, errors.New("setup must not run with invalid preparation")
			})

			require.ErrorContains(t, err, stage+": ")
			require.Zero(t, starts)
			require.Empty(t, report.Cases)
			require.Equal(t, []conformance.BackendState{{Name: "memory"}, {Name: "dynamodb"}}, report.Backends)
			raw, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			var saved conformance.Report
			require.NoError(t, json.Unmarshal(raw, &saved))
			require.Contains(t, strings.Join(saved.Errors, "\n"), stage+": ")
			require.NotEmpty(t, saved.FailureReasons())
		})
	}
}

func TestFinishConformanceReport(t *testing.T) {
	required, err := conformance.LoadRequired("required.json")
	require.NoError(t, err)
	id := required["memory"][0]
	for _, tc := range []struct {
		name       string
		status     conformance.Status
		missing    bool
		listed     bool
		verified   bool
		success    bool
		gateError  bool
		violations bool
	}{
		{name: "gate evaluation error", missing: true, listed: true, verified: true, gateError: true},
		{name: "required failure", status: conformance.StatusFailure, listed: true, verified: true, violations: true},
		{name: "required unverified", status: conformance.StatusUnverified, listed: true, verified: true, violations: true},
		{name: "case failure outside required", status: conformance.StatusFailure, verified: true},
		{name: "manifest mismatch", status: conformance.StatusSuccess, listed: true},
		{name: "success", status: conformance.StatusSuccess, listed: true, verified: true, success: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			list := conformance.RequiredList{"memory": {}}
			if tc.listed {
				list["memory"] = []string{id}
			}
			var results []conformance.CaseResult
			if !tc.missing {
				results = []conformance.CaseResult{{ID: id, Backend: "memory", Status: tc.status, Reason: "declared result"}}
			}
			report := conformance.BuildReport(conformance.ManifestResult{Verified: tc.verified}, results, nil, conformance.GateResult{Lists: list}, nil)
			path := filepath.Join(t.TempDir(), "conformance-report.json")

			report, err := finishConformanceReport(path, report, list)

			if tc.success {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			raw, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			var saved conformance.Report
			require.NoError(t, json.Unmarshal(raw, &saved))
			require.Equal(t, report.Errors, saved.Errors)
			require.Equal(t, tc.gateError, len(saved.Errors) > 0)
			require.Equal(t, tc.violations, len(saved.Required.Violations) > 0)
			require.Equal(t, len(results), len(saved.Cases))
		})
	}
}

func TestPublicMemoryConformance(t *testing.T) {
	data, err := conformance.LoadData(filepath.Join("..", "..", "conformance"))
	require.NoError(t, err)
	results := conformance.RunBackend(context.Background(), data, &publicBackend{name: "memory"})
	success := 0
	for _, result := range results {
		if result.Status == conformance.StatusSuccess {
			success++
		}
		if result.Status == conformance.StatusFailure || result.Status == conformance.StatusUnverified {
			t.Errorf("%s: %s", result.ID, result.Reason)
		}
	}
	require.Equal(t, len(conformance.ApplicableCases(data, "memory")), success)
}
