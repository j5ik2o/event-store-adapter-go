package spanner_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/spannertest"
	"github.com/j5ik2o/event-store-adapter-go/v5/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDerivesTheFiveTableNamesFromThePrefix(t *testing.T) {
	l := provision(t, layoutSpec{})

	entry, err := l.tryOpen(l.raw, l.config())

	require.NoError(t, err)
	id := aggregate(t, "Account", "prefix")
	require.NoError(t, entry.PersistEventAndSnapshot(t.Context(), eventAt(t, id, 1, "one"), snapshotAt(t, 1, "state")))
	assert.EqualValues(t, 1, l.count(t, "journal"))
	assert.EqualValues(t, 1, l.count(t, "head"))
	assert.EqualValues(t, 1, l.count(t, "snapshot_current"))
	assert.EqualValues(t, 1, l.count(t, "config"))
}

func TestNewRejectsPrefixesThatAreNotIdentifiersBeforeReadingStorage(t *testing.T) {
	longest := strings.Repeat("a", 128-len("_snapshot_history")+1)
	for name, prefix := range map[string]string{
		"hyphen":                        "esa-x",
		"leading digit":                 "1esa",
		"empty":                         "",
		"space":                         "esa x",
		"derived name over 128 letters": longest,
	} {
		t.Run(name, func(t *testing.T) {
			client, recorder := newRecordingClient(t)

			entry, err := spanner.New(t.Context(), client, spanner.Config{TablePrefix: prefix},
				eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())

			requireKind(t, err, eventstore.KindConfiguration)
			assert.Nil(t, entry)
			assert.Empty(t, recorder.all(), "no request may reach the destination")
		})
	}
}

func TestNewAcceptsTheLongestPrefixWhoseDerivedNamesFit(t *testing.T) {
	prefix := "p" + strings.Repeat("a", 128-len("_snapshot_history")-1)
	require.Len(t, prefix+"_snapshot_history", 128)
	require.NoError(t, database.ApplyDDL(t.Context(), spannertest.LayoutDDL(prefix, 0)...))
	require.NoError(t, database.InsertConfig(t.Context(), prefix, spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, RetentionMode: "delete"}))
	client := newClient(t)

	_, err := spanner.New(t.Context(), client, spanner.Config{TablePrefix: prefix},
		eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())

	require.NoError(t, err)
}

func TestNewRejectsInvalidSettingsBeforeReadingStorage(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  func(prefix string) spanner.Config
		opts func(t *testing.T) []eventstore.Option
	}{
		{"change feed", func(p string) spanner.Config { return spanner.Config{TablePrefix: p, ChangeFeed: true} }, nil},
		{"negative retry limit", func(p string) spanner.Config { return spanner.Config{TablePrefix: p, TransactionRetryLimit: ptr(-1)} }, nil},
		{"grace days without ttl mode", func(p string) spanner.Config { return spanner.Config{TablePrefix: p, TTLGraceDays: 7} }, nil},
		{"ttl mode without grace days", func(p string) spanner.Config { return spanner.Config{TablePrefix: p} }, func(t *testing.T) []eventstore.Option {
			return []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL), keepOption(t, 1)}
		}},
		{"negative grace days", func(p string) spanner.Config { return spanner.Config{TablePrefix: p, TTLGraceDays: -1} }, func(t *testing.T) []eventstore.Option {
			return []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL), keepOption(t, 1)}
		}},
		{"ttl mode without a retention count", func(p string) spanner.Config { return spanner.Config{TablePrefix: p, TTLGraceDays: 7} }, func(*testing.T) []eventstore.Option {
			return []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL)}
		}},
		{"grace seconds option", func(p string) spanner.Config { return spanner.Config{TablePrefix: p, TTLGraceDays: 7} }, func(t *testing.T) []eventstore.Option {
			return []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL), keepOption(t, 1), eventstore.WithTTLGraceSeconds(3600)}
		}},
		{"nil option", func(p string) spanner.Config { return spanner.Config{TablePrefix: p} }, func(*testing.T) []eventstore.Option {
			return []eventstore.Option{nil}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, recorder := newRecordingClient(t)
			var opts []eventstore.Option
			if tc.opts != nil {
				opts = tc.opts(t)
			}

			_, err := spanner.New(t.Context(), client, tc.cfg("esa"), eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string](), opts...)

			requireKind(t, err, eventstore.KindConfiguration)
			assert.Empty(t, recorder.all(), "invalid settings must be rejected before the destination is read")
		})
	}
}

func TestNewRejectsNilClientAndNilSerializers(t *testing.T) {
	_, err := spanner.New(t.Context(), nil, spanner.Config{TablePrefix: "esa"}, eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())
	requireKind(t, err, eventstore.KindConfiguration)

	l := provision(t, layoutSpec{})
	_, err = spanner.New[string, string](t.Context(), l.raw, l.config(), nil, eventstore.NewJSONSerializer[string]())
	requireKind(t, err, eventstore.KindConfiguration)
	_, err = spanner.New[string, string](t.Context(), l.raw, l.config(), eventstore.NewJSONSerializer[string](), nil)
	requireKind(t, err, eventstore.KindConfiguration)
}

func TestNewAcceptsRetryLimitZeroAndDefault(t *testing.T) {
	l := provision(t, layoutSpec{})
	for _, limit := range []*int{nil, ptr(0), ptr(5)} {
		cfg := l.config()
		cfg.TransactionRetryLimit = limit
		_, err := l.tryOpen(l.raw, cfg)
		require.NoError(t, err)
	}
}

func TestNewAcceptsTTLLayoutWhenGraceDaysAgreeInPolicyConfigAndClient(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(1)), graceDays: 7})

	_, err := l.tryOpen(l.raw, spanner.Config{TablePrefix: l.prefix, TTLGraceDays: 7},
		eventstore.WithRetentionMode(eventstore.RetentionTTL), keepOption(t, 1))

	require.NoError(t, err)
}

func TestNewAcceptsLayoutMatchingKeepCountOnReopenWithAFreshClient(t *testing.T) {
	l := provision(t, layoutSpec{keep: ptr(int64(2))})
	first := l.newEntry(t)
	id := aggregate(t, "Account", "restart")
	require.NoError(t, first.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))

	restarted := l.store(t, newClient(t))

	events, err := restarted.GetEventsByIDSinceSeqNr(t.Context(), id, 0)
	require.NoError(t, err)
	require.Len(t, events, 1, "the same destination opened again after a restart must see the committed data")
	require.NoError(t, restarted.PersistEvent(t.Context(), eventAt(t, id, 2, "two")))
}

func TestNewRejectsAnUnprovisionedPrefixWithoutCreatingAnything(t *testing.T) {
	prefix := "unprovisioned"
	client := newClient(t)
	before := queryInt(t, client, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ''", nil)

	_, err := spanner.New(t.Context(), client, spanner.Config{TablePrefix: prefix},
		eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())

	requireKind(t, err, eventstore.KindConfiguration)
	assert.Equal(t, before, queryInt(t, client, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ''", nil), "New must not run DDL")
	assert.Zero(t, queryInt(t, client, "SELECT COUNT(*) FROM information_schema.tables WHERE STARTS_WITH(table_name, 'unprovisioned_')", nil))
}

func TestNewRejectsLayoutsThatDoNotMatchTheProfile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		spec   layoutSpec
		cfg    func(prefix string) spanner.Config
		opts   func(t *testing.T) []eventstore.Option
		mutate func(t *testing.T, l *layout)
	}{
		{name: "config row is absent", spec: layoutSpec{skipConfig: true}},
		{name: "layout version differs", spec: layoutSpec{config: &spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 2, RetentionMode: "delete"}}},
		{name: "store id is not a lowercase UUID", spec: layoutSpec{config: &spannertest.ConfigRow{StoreID: "6F1B3A52-9D0C-4F5E-8A7B-1C2D3E4F5A6B", LayoutVersion: 1, RetentionMode: "delete"}}},
		{name: "config row asks for a change feed", spec: layoutSpec{config: &spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, RetentionMode: "delete", ChangeFeedEnabled: true}}},
		{name: "keep count of the config differs from the client", spec: layoutSpec{config: &spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, KeepSnapshotCount: ptr(int64(2)), RetentionMode: "delete"}},
			opts: func(t *testing.T) []eventstore.Option { return []eventstore.Option{keepOption(t, 1)} }},
		{name: "client keeps history but the config has none", spec: layoutSpec{},
			opts: func(t *testing.T) []eventstore.Option { return []eventstore.Option{keepOption(t, 1)} }},
		{name: "config keeps history but the client has none", spec: layoutSpec{keep: ptr(int64(1))},
			cfg:  func(p string) spanner.Config { return spanner.Config{TablePrefix: p} },
			opts: func(t *testing.T) []eventstore.Option { return nil }},
		{name: "config says ttl but the client deletes", spec: layoutSpec{config: &spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, KeepSnapshotCount: ptr(int64(1)), RetentionMode: "ttl", TTLGraceDays: ptr(int64(7))}},
			opts: func(t *testing.T) []eventstore.Option { return []eventstore.Option{keepOption(t, 1)} }},
		{name: "grace days of the policy is 8 while the config says 7", spec: layoutSpec{keep: ptr(int64(1)), graceDays: 8, config: &spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, KeepSnapshotCount: ptr(int64(1)), RetentionMode: "ttl", TTLGraceDays: ptr(int64(7))}},
			cfg:  func(p string) spanner.Config { return spanner.Config{TablePrefix: p, TTLGraceDays: 7} },
			opts: func(t *testing.T) []eventstore.Option { return ttlOptions(t) }},
		{name: "grace days of the client differ from policy and config", spec: layoutSpec{keep: ptr(int64(1)), graceDays: 7},
			cfg:  func(p string) spanner.Config { return spanner.Config{TablePrefix: p, TTLGraceDays: 6} },
			opts: func(t *testing.T) []eventstore.Option { return ttlOptions(t) }},
		{name: "delete mode but the history table has a row deletion policy", spec: layoutSpec{keep: ptr(int64(1)), graceDays: 7, config: &spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, KeepSnapshotCount: ptr(int64(1)), RetentionMode: "delete"}},
			cfg:  func(p string) spanner.Config { return spanner.Config{TablePrefix: p} },
			opts: func(t *testing.T) []eventstore.Option { return []eventstore.Option{keepOption(t, 1)} }},
		{name: "ttl mode but the history table has no row deletion policy", spec: layoutSpec{keep: ptr(int64(1)), config: &spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, KeepSnapshotCount: ptr(int64(1)), RetentionMode: "ttl", TTLGraceDays: ptr(int64(7))}},
			cfg:  func(p string) spanner.Config { return spanner.Config{TablePrefix: p, TTLGraceDays: 7} },
			opts: func(t *testing.T) []eventstore.Option { return ttlOptions(t) }},
		{name: "a table is missing", mutate: func(t *testing.T, l *layout) {
			require.NoError(t, database.ApplyDDL(t.Context(), "DROP TABLE "+l.table("snapshot_current")))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := provision(t, tc.spec)
			if tc.mutate != nil {
				tc.mutate(t, l)
			}
			cfg := l.config()
			if tc.cfg != nil {
				cfg = tc.cfg(l.prefix)
			}
			var opts []eventstore.Option
			if tc.opts != nil {
				opts = tc.opts(t)
			}

			entry, err := l.tryOpen(l.raw, cfg, opts...)

			requireKind(t, err, eventstore.KindConfiguration)
			assert.Nil(t, entry)
		})
	}
}

func ttlOptions(t *testing.T) []eventstore.Option {
	t.Helper()
	return []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL), keepOption(t, 1)}
}

func TestNewRejectsTablesThatDifferFromTheSharedStorageFormat(t *testing.T) {
	// Each definition differs from the JavaScript v5 DDL in exactly one way that changes the stored format.
	for name, replace := range map[string][2]string{
		"occurred time is a TIMESTAMP":      {"occurred_at_ns INT64 NOT NULL", "occurred_at_ns TIMESTAMP NOT NULL"},
		"hash prefix is an INT64":           {"hash_prefix BYTES(8) NOT NULL", "hash_prefix INT64 NOT NULL"},
		"journal key lacks the hash prefix": {"PRIMARY KEY (hash_prefix, aid, seq_nr)", "PRIMARY KEY (aid, seq_nr)"},
	} {
		t.Run(name, func(t *testing.T) {
			prefix := fmt.Sprintf("m%d", prefixCount.Add(1))
			statements := javascriptDDL(prefix, 0)
			require.Contains(t, statements[0], replace[0], "the journal definition must contain the text being replaced")
			statements[0] = strings.Replace(statements[0], replace[0], replace[1], 1)
			require.NoError(t, database.ApplyDDL(t.Context(), statements...))
			require.NoError(t, database.InsertConfig(t.Context(), prefix, spannertest.ConfigRow{StoreID: validStoreID, LayoutVersion: 1, RetentionMode: "delete"}))

			_, err := spanner.New(context.Background(), newClient(t), spanner.Config{TablePrefix: prefix},
				eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())

			requireKind(t, err, eventstore.KindConfiguration)
		})
	}
}

func TestNewReportsAnUnreadableDestinationAsStorageError(t *testing.T) {
	l := provision(t, layoutSpec{})
	client, err := database.NewClient(t.Context())
	require.NoError(t, err)
	client.Close()

	_, err = l.tryOpen(client, l.config())

	requireKind(t, err, eventstore.KindStorage)
}

func TestNewRejectsNullValuesInTheConfigRowAsConfigurationErrors(t *testing.T) {
	for _, column := range []struct{ name, notNull string }{
		{"store_id", "store_id STRING(36) NOT NULL"},
		{"retention_mode", "retention_mode STRING(8) NOT NULL"},
		{"change_feed_enabled", "change_feed_enabled BOOL NOT NULL"},
	} {
		for _, null := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s null=%t", column.name, null), func(t *testing.T) {
				prefix := fmt.Sprintf("n%d", prefixCount.Add(1))
				statements := spannertest.LayoutDDL(prefix, 0)
				config := len(statements) - 1
				require.Contains(t, statements[config], column.notNull, "the config definition must contain the text being replaced")
				statements[config] = strings.Replace(statements[config], column.notNull, strings.TrimSuffix(column.notNull, " NOT NULL"), 1)
				require.NoError(t, database.ApplyDDL(t.Context(), statements...))
				values := map[string]string{"store_id": "'" + validStoreID + "'", "retention_mode": "'delete'", "change_feed_enabled": "FALSE"}
				if null {
					values[column.name] = "NULL"
				}
				raw := newClient(t)
				execute(t, raw, fmt.Sprintf("INSERT INTO %s_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled) VALUES (1, %s, 1, NULL, %s, NULL, %s)",
					prefix, values["store_id"], values["retention_mode"], values["change_feed_enabled"]), nil)

				entry, err := spanner.New(t.Context(), raw, spanner.Config{TablePrefix: prefix},
					eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]())

				if !null {
					require.NoError(t, err, "the same definition with a real value opens")
					return
				}
				requireKind(t, err, eventstore.KindConfiguration)
				assert.Nil(t, entry)
			})
		}
	}
}
