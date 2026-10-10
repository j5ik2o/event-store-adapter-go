package conformance_test

import (
	"context"
	"os"
	"path/filepath"
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
	manifest, err := conformance.VerifyManifest(root)
	require.NoError(t, err)
	data, err := conformance.LoadData(root)
	require.NoError(t, err)
	required, err := conformance.LoadRequired("required.json")
	require.NoError(t, err)
	require.NoError(t, conformance.CheckRequiredCoverage(data, required))
	environment, err := dynamodbtest.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Close(context.Background())) })
	var results []conformance.CaseResult
	for _, backend := range []*publicBackend{{name: "memory"}, {name: "dynamodb", environment: environment}} {
		actual := conformance.RunBackend(ctx, data, backend)
		results = append(results, actual...)
		gate, err := conformance.EvaluateRequired(conformance.RequiredList{backend.name: required[backend.name]}, actual)
		require.NoError(t, err)
		report := conformance.BuildReport(manifest, actual, data.Exclusions, gate, nil)
		report.Backends = []conformance.BackendState{{Name: backend.name, Connected: true}}
		require.NoError(t, conformance.WriteReport(filepath.Join(filepath.Dir(path), backend.name+"-conformance-report.json"), report))
	}
	gate, err := conformance.EvaluateRequired(required, results)
	require.NoError(t, err)
	report := conformance.BuildReport(manifest, results, data.Exclusions, gate, nil)
	report.Backends = []conformance.BackendState{{Name: "memory", Connected: true}, {Name: "dynamodb", Connected: true}}
	require.NoError(t, conformance.WriteReport(path, report))
	for _, result := range results {
		if result.Status == conformance.StatusFailure || result.Status == conformance.StatusUnverified {
			t.Errorf("%s/%s: %s", result.Backend, result.ID, result.Reason)
		}
	}
	require.Empty(t, report.FailureReasons())
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
