package spannertest

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/api/iterator"
)

func TestEmulatorImageIsPinnedByDigest(t *testing.T) {
	assert.Regexp(t, regexp.MustCompile(`^gcr\.io/cloud-spanner-emulator/emulator:1\.5\.58@sha256:[0-9a-f]{64}$`), emulatorImage)
	assert.Contains(t, emulatorImage, "c6f3402f2599684f295a0fdefb6fbbbfb18a0e43e309ff5456ccb452a4570a79", "the digest used by the JavaScript implementation")
}

func TestLayoutDDLDerivesFiveTableNamesFromThePrefix(t *testing.T) {
	statements := LayoutDDL("esa", 0)

	require.Len(t, statements, 5)
	joined := strings.Join(statements, "\n")
	for _, name := range []string{"esa_journal", "esa_head", "esa_snapshot_current", "esa_snapshot_history", "esa_config"} {
		assert.Contains(t, joined, "CREATE TABLE "+name+" ", name)
	}
	assert.Contains(t, joined, "hash_prefix BYTES(8) NOT NULL")
	assert.Contains(t, joined, "occurred_at_ns INT64 NOT NULL", "occurred time is INT64 nanoseconds, not TIMESTAMP")
	assert.NotContains(t, joined, "ROW DELETION POLICY", "delete mode has no row deletion policy")
}

func TestLayoutDDLAddsTheRowDeletionPolicyOnlyForTTL(t *testing.T) {
	joined := strings.Join(LayoutDDL("esa", 7), "\n")

	assert.Contains(t, joined, "ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL 7 DAY))")
}

func TestStartFailureIsReportedNotSkipped(t *testing.T) {
	original := createContainer
	t.Cleanup(func() { createContainer = original })
	cause := errors.New("declared container startup failure")
	createContainer = func(context.Context, testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		return nil, cause
	}

	environment, err := Start(t.Context())

	require.ErrorIs(t, err, cause)
	assert.Nil(t, environment)
}

// TestEmulatorLifecycle runs against a real emulator: instance, database, per-prefix DDL, config row, client, close.
func TestEmulatorLifecycle(t *testing.T) {
	previous, hadPrevious := os.LookupEnv("SPANNER_EMULATOR_HOST")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	environment, err := Start(ctx)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, environment.Close(context.Background()))
		}
	})
	assert.NotEmpty(t, os.Getenv("SPANNER_EMULATOR_HOST"), "clients must reach the emulator, never a real service")

	database, err := environment.CreateDatabase(ctx)
	require.NoError(t, err)
	require.NoError(t, database.ApplyDDL(ctx, LayoutDDL("esa", 0)...))
	require.NoError(t, database.ApplyDDL(ctx, LayoutDDL("other", 7)...), "several prefixes live in one database")
	one := int64(1)
	require.NoError(t, database.InsertConfig(ctx, "esa", ConfigRow{
		StoreID: "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11", LayoutVersion: 1, KeepSnapshotCount: &one, RetentionMode: "delete",
	}))

	client, err := database.NewClient(ctx)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	tables := queryStrings(t, client, "SELECT table_name FROM information_schema.tables WHERE table_schema = '' AND STARTS_WITH(table_name, 'esa_') ORDER BY table_name")
	assert.Equal(t, []string{"esa_config", "esa_head", "esa_journal", "esa_snapshot_current", "esa_snapshot_history"}, tables)
	policy := queryStrings(t, client, "SELECT row_deletion_policy_expression FROM information_schema.tables WHERE table_name = 'other_snapshot_history'")
	require.Len(t, policy, 1)
	assert.Contains(t, policy[0], "INTERVAL 7 DAY")
	stored := queryStrings(t, client, "SELECT CONCAT(store_id, '|', retention_mode, '|', CAST(keep_snapshot_count AS STRING)) FROM esa_config WHERE singleton = 1")
	assert.Equal(t, []string{"5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11|delete|1"}, stored)

	second, err := environment.CreateDatabase(ctx)
	require.NoError(t, err)
	secondClient, err := second.NewClient(ctx)
	require.NoError(t, err)
	t.Cleanup(secondClient.Close)
	assert.Empty(t, queryStrings(t, secondClient, "SELECT table_name FROM information_schema.tables WHERE table_schema = '' AND STARTS_WITH(table_name, 'esa_')"), "a new database starts empty")

	require.NoError(t, environment.Close(context.Background()))
	closed = true
	restored, has := os.LookupEnv("SPANNER_EMULATOR_HOST")
	assert.Equal(t, hadPrevious, has)
	assert.Equal(t, previous, restored, "Close restores the environment variable it changed")
}

func queryStrings(t *testing.T, client *spanner.Client, sql string) []string {
	t.Helper()
	rows := client.Single().Query(context.Background(), spanner.NewStatement(sql))
	defer rows.Stop()
	out := []string{}
	for {
		row, err := rows.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)
			return out
		}
		var value spanner.NullString
		require.NoError(t, row.Columns(&value))
		out = append(out, value.StringVal)
	}
}
