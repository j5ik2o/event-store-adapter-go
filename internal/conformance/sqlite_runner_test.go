package conformance

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sqliteFake records the order of the SQLite-only operations next to Seed and Open.
type sqliteFake struct {
	*fakeBackend
	seeded map[string]any
}

func (b *sqliteFake) SeedConfiguration(_ context.Context, values map[string]any) error {
	b.seq = append(b.seq, "seed-configuration")
	b.seeded = values
	return nil
}

func (b *sqliteFake) HoldWriteLock(context.Context) error {
	b.seq = append(b.seq, "hold-lock")
	return nil
}

func (b *sqliteFake) ReleaseWriteLock(context.Context) error {
	b.seq = append(b.seq, "release-lock")
	return nil
}

func scenarioByID(t *testing.T, d *Data, id string) ScenarioCase {
	t.Helper()
	for _, c := range d.Scenarios {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("scenario %s is not in the data", id)
	return ScenarioCase{}
}

func TestRunScenario_SQLiteSeededConfiguration(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	c := scenarioByID(t, d, "sqlite-seeded-keep-count-mismatch")
	require.NotNil(t, c.Plan.SeedConfiguration)

	t.Run("the declared configuration is written after the destination is provisioned and before the store is opened", func(t *testing.T) {
		b := &sqliteFake{fakeBackend: &fakeBackend{name: "sqlite", injectable: true, openErr: &OperationError{Category: "configuration", Message: "mismatch"}}}
		r := runScenario(context.Background(), c, "sqlite", b)
		assert.Equal(t, StatusSuccess, r.Status, r.Reason)
		assert.Equal(t, []string{"seed-configuration", "open"}, b.seq)
		assert.Equal(t, json.Number("2"), b.seeded["keep_snapshot_count"])
		assert.Equal(t, "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11", b.seeded["store_id"])
	})

	t.Run("a backend that cannot seed the configuration leaves the scenario unverified instead of passing", func(t *testing.T) {
		b := &fakeBackend{name: "sqlite", injectable: true, openErr: &OperationError{Category: "configuration", Message: "mismatch"}}
		r := runScenario(context.Background(), c, "sqlite", b)
		assert.NotEqual(t, StatusSuccess, r.Status)
		assert.Equal(t, int32(0), b.opens, "the store must not be opened without the seeded configuration")
	})

	t.Run("a configuration mismatch that the store accepts is a failure", func(t *testing.T) {
		b := &sqliteFake{fakeBackend: &fakeBackend{name: "sqlite", injectable: true}}
		r := runScenario(context.Background(), c, "sqlite", b)
		assert.Equal(t, StatusFailure, r.Status, "an Open that succeeds must not satisfy the configuration error expectation")
	})
}

func TestRunScenario_SQLiteWriteLock(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	c := scenarioByID(t, d, "sqlite-write-lock-busy")
	require.NotNil(t, c.Plan.Store.LockWaitMilliseconds)
	assert.EqualValues(t, 100, *c.Plan.Store.LockWaitMilliseconds)

	t.Run("the lock is taken after the first append and before the append that must fail", func(t *testing.T) {
		b := &sqliteFake{fakeBackend: &fakeBackend{name: "sqlite", injectable: true}}
		r := runScenario(context.Background(), c, "sqlite", b)
		// The fake store accepts the append that the data expects to fail, so the run stops there.
		require.NotNil(t, r.FailedStep)
		assert.Equal(t, 3, *r.FailedStep)
		assert.Equal(t, []string{"open", "hold-lock"}, b.seq)
		assert.Equal(t, []string{"persistEvent", "persistEvent"}, b.calls)
	})

	t.Run("a backend that cannot hold the lock leaves the scenario unverified", func(t *testing.T) {
		b := &fakeBackend{name: "sqlite", injectable: true}
		r := runScenario(context.Background(), c, "sqlite", b)
		assert.NotEqual(t, StatusSuccess, r.Status)
	})
}
