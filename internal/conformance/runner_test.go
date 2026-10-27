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

	"github.com/j5ik2o/event-store-adapter-go/v5/internal/bigtabletest"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/spannertest"
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
	}, startSpannerEnvironment(t), startBigtableEnvironment(t))
	t.Logf("summary: %v", report.Summary)
	require.NoError(t, err)
}

// startSpannerEnvironment starts the real emulator for the conformance run; the test owns its shutdown.
func startSpannerEnvironment(t *testing.T) func(context.Context) (*spannertest.Environment, error) {
	return func(ctx context.Context) (*spannertest.Environment, error) {
		environment, err := spannertest.Start(ctx)
		if err == nil {
			t.Cleanup(func() { require.NoError(t, environment.Close(context.Background())) })
		}
		return environment, err
	}
}

// startBigtableEnvironment starts the real emulator for the conformance run; the test owns its shutdown.
func startBigtableEnvironment(t *testing.T) func(context.Context) (*bigtabletest.Environment, error) {
	return func(ctx context.Context) (*bigtabletest.Environment, error) {
		environment, err := bigtabletest.Start(ctx)
		if err == nil {
			t.Cleanup(func() { require.NoError(t, environment.Close(context.Background())) })
		}
		return environment, err
	}
}

// runConformance retains preparation diagnostics and attempts every report before
// returning failure. The start function owns cleanup of a successfully started environment.
func runConformance(ctx context.Context, root, requiredPath, path string, start func(context.Context) (*dynamodbtest.Environment, error), startSpanner func(context.Context) (*spannertest.Environment, error), startBigtable func(context.Context) (*bigtabletest.Environment, error)) (conformance.Report, error) {
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
	backends := []conformance.BackendState{{Name: "memory"}, {Name: "dynamodb"}, {Name: "sqlite"}, {Name: "spanner"}, {Name: "bigtable"}}
	if len(causes) == 0 {
		environment, err := start(ctx)
		record("setup", err)
		spannerEnvironment, spannerErr := startSpanner(ctx)
		record("spanner setup", spannerErr)
		var spannerDatabase *spannertest.Database
		if spannerErr == nil {
			var databaseErr error
			spannerDatabase, databaseErr = spannerEnvironment.CreateDatabase(ctx)
			record("spanner database", databaseErr)
		}
		bigtableEnvironment, bigtableErr := startBigtable(ctx)
		record("bigtable setup", bigtableErr)
		if err == nil && spannerDatabase != nil && bigtableErr == nil {
			for i, run := range []struct {
				name    string
				backend conformance.Backend
			}{
				{"memory", &publicBackend{name: "memory"}},
				{"dynamodb", &publicBackend{name: "dynamodb", environment: environment}},
				{"sqlite", newSQLiteBackend()},
				{"spanner", newSpannerBackend(spannerDatabase)},
				{"bigtable", newBigtableBackend(bigtableEnvironment)},
			} {
				actual := conformance.RunBackend(ctx, data, run.backend)
				results = append(results, actual...)
				backends[i].Connected = true
				list := conformance.RequiredList{run.name: required[run.name]}
				report := conformance.BuildReport(manifest, actual, excluded, conformance.GateResult{Lists: list}, nil)
				report.Backends = []conformance.BackendState{backends[i]}
				_, err := finishConformanceReport(filepath.Join(filepath.Dir(path), run.name+"-conformance-report.json"), report, list)
				record(run.name+" report", err)
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
			spannerCause := errors.New("declared emulator startup failure")
			bigtableCause := errors.New("declared bigtable emulator startup failure")
			starts, spannerStarts, bigtableStarts := 0, 0, 0

			report, err := runConformance(t.Context(), filepath.Join("..", "..", "conformance"), "required.json", path, func(context.Context) (*dynamodbtest.Environment, error) {
				starts++
				return nil, cause
			}, func(context.Context) (*spannertest.Environment, error) {
				spannerStarts++
				return nil, spannerCause
			}, func(context.Context) (*bigtabletest.Environment, error) {
				bigtableStarts++
				return nil, bigtableCause
			})

			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, spannerCause, "a Spanner emulator that does not start is a failure, never a skip")
			require.Equal(t, 1, starts)
			require.ErrorIs(t, err, bigtableCause, "a Bigtable emulator that does not start is a failure, never a skip")
			require.Equal(t, 1, spannerStarts)
			require.Equal(t, 1, bigtableStarts)
			require.Contains(t, report.Errors, "setup: "+cause.Error())
			require.Contains(t, report.Errors, "spanner setup: "+spannerCause.Error())
			require.Contains(t, report.Errors, "bigtable setup: "+bigtableCause.Error())
			require.True(t, report.Manifest.Verified)
			require.Len(t, report.Required.Lists["memory"], 67)
			require.Len(t, report.Required.Lists["dynamodb"], 106)
			require.Len(t, report.Required.Lists["sqlite"], 68)
			require.Len(t, report.Required.Lists["spanner"], 79)
			require.Len(t, report.Required.Lists["bigtable"], 87)
			require.Empty(t, report.Cases)
			require.Equal(t, []conformance.BackendState{{Name: "memory"}, {Name: "dynamodb"}, {Name: "sqlite"}, {Name: "spanner"}, {Name: "bigtable"}}, report.Backends)
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
			}, func(context.Context) (*spannertest.Environment, error) {
				starts++
				return nil, errors.New("spanner setup must not run with invalid preparation")
			}, func(context.Context) (*bigtabletest.Environment, error) {
				starts++
				return nil, errors.New("bigtable setup must not run with invalid preparation")
			})

			require.ErrorContains(t, err, stage+": ")
			require.Zero(t, starts)
			require.Empty(t, report.Cases)
			require.Equal(t, []conformance.BackendState{{Name: "memory"}, {Name: "dynamodb"}, {Name: "sqlite"}, {Name: "spanner"}, {Name: "bigtable"}}, report.Backends)
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
	var unrepresentable []string
	for _, result := range results {
		if result.Status == conformance.StatusSuccess {
			success++
		}
		if result.Status == conformance.StatusUnrepresentable {
			unrepresentable = append(unrepresentable, result.ID)
			require.NotEmpty(t, result.Reason, result.ID)
		}
		if result.Status == conformance.StatusFailure || result.Status == conformance.StatusUnverified {
			t.Errorf("%s: %s", result.ID, result.Reason)
		}
	}
	// The public options cannot ask for a change feed, so that case is reported with a reason.
	require.Equal(t, []string{"memory-change-feed-unsupported"}, unrepresentable)
	require.Equal(t, len(conformance.ApplicableCases(data, "memory")), success+len(unrepresentable))
}

// TestPublicSQLiteConformance runs every applicable case against real SQLite files, with no skipped
// or unrepresentable case: the unsupported change feed is a successful rejection, not an exclusion.
func TestPublicSQLiteConformance(t *testing.T) {
	data, err := conformance.LoadData(filepath.Join("..", "..", "conformance"))
	require.NoError(t, err)
	required, err := conformance.LoadRequired("required.json")
	require.NoError(t, err)
	results := conformance.RunBackend(context.Background(), data, newSQLiteBackend())
	success := 0
	for _, result := range results {
		if result.Backend != "sqlite" {
			continue
		}
		switch result.Status {
		case conformance.StatusSuccess:
			success++
		case conformance.StatusNotApplicable:
		default:
			t.Errorf("%s: %s: %s", result.ID, result.Status, result.Reason)
		}
	}
	applicable := conformance.ApplicableCases(data, "sqlite")
	require.Len(t, applicable, 68)
	require.Equal(t, len(applicable), success)
	gate, err := conformance.EvaluateRequired(conformance.RequiredList{"sqlite": required["sqlite"]}, results)
	require.NoError(t, err)
	require.Empty(t, gate.Violations)
}

// TestPublicSpannerConformance runs every applicable case against a real Spanner emulator. No case is skipped or
// unrepresentable: the unsupported change feed is a successful rejection, and a case that does not run fails the gate.
func TestPublicSpannerConformance(t *testing.T) {
	data, err := conformance.LoadData(filepath.Join("..", "..", "conformance"))
	require.NoError(t, err)
	required, err := conformance.LoadRequired("required.json")
	require.NoError(t, err)
	ctx := context.Background()
	environment, err := startSpannerEnvironment(t)(ctx)
	require.NoError(t, err)
	database, err := environment.CreateDatabase(ctx)
	require.NoError(t, err)

	results := conformance.RunBackend(ctx, data, newSpannerBackend(database))

	success := 0
	for _, result := range results {
		if result.Backend != "spanner" {
			continue
		}
		switch result.Status {
		case conformance.StatusSuccess:
			success++
		case conformance.StatusNotApplicable:
		default:
			t.Errorf("%s: %s: %s", result.ID, result.Status, result.Reason)
		}
	}
	applicable := conformance.ApplicableCases(data, "spanner")
	require.Len(t, applicable, 79)
	require.Equal(t, len(applicable), success, "every applicable case succeeds: zero failures and zero unverified")
	gate, err := conformance.EvaluateRequired(conformance.RequiredList{"spanner": required["spanner"]}, results)
	require.NoError(t, err)
	require.Empty(t, gate.Violations)
	require.ElementsMatch(t, applicable, required["spanner"])
}

// TestPublicBigtableConformance runs every applicable case against a real Bigtable emulator. No case is skipped or
// unrepresentable: the unsupported change feed is a successful rejection, and a case that does not run fails the gate.
func TestPublicBigtableConformance(t *testing.T) {
	data, err := conformance.LoadData(filepath.Join("..", "..", "conformance"))
	require.NoError(t, err)
	required, err := conformance.LoadRequired("required.json")
	require.NoError(t, err)
	ctx := context.Background()
	environment, err := startBigtableEnvironment(t)(ctx)
	require.NoError(t, err)

	results := conformance.RunBackend(ctx, data, newBigtableBackend(environment))

	success := 0
	for _, result := range results {
		if result.Backend != "bigtable" {
			continue
		}
		switch result.Status {
		case conformance.StatusSuccess:
			success++
		case conformance.StatusNotApplicable:
		default:
			t.Errorf("%s: %s: %s", result.ID, result.Status, result.Reason)
		}
	}
	applicable := conformance.ApplicableCases(data, "bigtable")
	require.Len(t, applicable, 87)
	require.Equal(t, len(applicable), success, "every applicable case succeeds: zero failures and zero unverified")
	gate, err := conformance.EvaluateRequired(conformance.RequiredList{"bigtable": required["bigtable"]}, results)
	require.NoError(t, err)
	require.Empty(t, gate.Violations)
	require.ElementsMatch(t, applicable, required["bigtable"])
}
