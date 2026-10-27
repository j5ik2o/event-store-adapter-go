package bigtabletest

import (
	"context"
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	gbigtable "cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestEmulatorImageIsPinnedByDigest(t *testing.T) {
	assert.Regexp(t, regexp.MustCompile(`^gcr\.io/google\.com/cloudsdktool/google-cloud-cli:587\.0\.0-emulators@sha256:[0-9a-f]{64}$`), emulatorImage)
	assert.Contains(t, emulatorImage, "8d8573471c489a409f03891d2c24d177fa531276184f7002b9a2d853d1b3ed54", "the digest used by the Rust and Java implementations")
}

func TestConfigJSONHasTheFiveFieldsOfTheConfigurationRow(t *testing.T) {
	three := int64(3)
	assert.JSONEq(t, `{"store_id":"6f1b3a52-9d0c-4f5e-8a7b-1c2d3e4f5a6b","layout_version":1,"keep_snapshot_count":"3","retention_mode":"delete","envelope_limit_bytes":4096}`,
		ConfigJSON("6f1b3a52-9d0c-4f5e-8a7b-1c2d3e4f5a6b", &three, 4096))
	assert.JSONEq(t, `{"store_id":"x","layout_version":1,"keep_snapshot_count":null,"retention_mode":"delete","envelope_limit_bytes":1048576}`,
		ConfigJSON("x", nil, 1048576))
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

// TestEmulatorLifecycle runs against a real emulator: start, tables with the documented families, a configuration
// row, a raw round trip, close. It restores BIGTABLE_EMULATOR_HOST afterwards.
func TestEmulatorLifecycle(t *testing.T) {
	previous, had := os.LookupEnv("BIGTABLE_EMULATOR_HOST")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	environment, err := Start(ctx)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = environment.Close(context.Background())
		}
		if had {
			_ = os.Setenv("BIGTABLE_EMULATOR_HOST", previous)
		} else {
			_ = os.Unsetenv("BIGTABLE_EMULATOR_HOST")
		}
	})
	admin, err := environment.NewAdminClient(ctx)
	require.NoError(t, err)
	defer admin.Close()
	data, err := environment.NewClient(ctx)
	require.NoError(t, err)
	defer data.Close()

	require.NoError(t, CreateTables(ctx, admin, "journal", "head"))
	require.NoError(t, WriteConfig(ctx, data, "journal", ConfigJSON("6f1b3a52-9d0c-4f5e-8a7b-1c2d3e4f5a6b", nil, 1048576)))

	info, err := admin.TableInfo(ctx, "journal")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"event", "history", "config"}, info.Families)
	for _, family := range info.FamilyInfos {
		if family.Name != "config" {
			assert.Equal(t, gbigtable.MaxVersionsGCPolicy(1), family.FullGCPolicy, family.Name)
		}
	}
	row, err := data.Open("journal").ReadRow(ctx, "\x00")
	require.NoError(t, err)
	require.NotEmpty(t, row["config"])

	require.NoError(t, environment.Close(ctx))
	closed = true
}
