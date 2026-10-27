package bigtable_test

import (
	"context"
	"fmt"
	"testing"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
	"github.com/j5ik2o/event-store-adapter-go/v5/internal/bigtabletest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func serializers() (eventstore.Serializer[string], eventstore.Serializer[string]) {
	return eventstore.NewJSONSerializer[string](), eventstore.NewJSONSerializer[string]()
}

func TestNewAcceptsAProvisionedLayoutAndWritesNothing(t *testing.T) {
	l := provision(t, layoutSpec{})
	beforeJournal, beforeHead := l.journalKeys(t), rowKeys(t, l.data, l.head)

	entry, err := l.tryOpen(l.config())

	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, beforeJournal, l.journalKeys(t), "New writes no row (BT-D1)")
	assert.Equal(t, beforeHead, rowKeys(t, l.data, l.head))
	assert.Equal(t, []string{"\x00"}, beforeJournal, "only the configuration row exists")
}

func TestNewRejectsEveryLayoutDefectAsAConfigurationError(t *testing.T) {
	t.Run("a missing column family", func(t *testing.T) {
		n := tableSeq.Add(1)
		journal, head := fmt.Sprintf("journal%d", n), fmt.Sprintf("head%d", n)
		admin, err := emulator.NewAdminClient(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = admin.Close() })
		require.NoError(t, admin.CreateTable(t.Context(), journal))
		for _, family := range []string{"event", "config"} { // "history" is missing
			require.NoError(t, admin.CreateColumnFamily(t.Context(), journal, family))
			require.NoError(t, admin.SetGCPolicy(t.Context(), journal, family, gbigtable.MaxVersionsPolicy(1)))
		}
		require.NoError(t, admin.CreateTable(t.Context(), head))
		for _, family := range []string{"head", "snapshot", "config"} {
			require.NoError(t, admin.CreateColumnFamily(t.Context(), head, family))
			require.NoError(t, admin.SetGCPolicy(t.Context(), head, family, gbigtable.MaxVersionsPolicy(1)))
		}
		data := newDataClient(t)

		es, ss := serializers()

		entry, err := bigtable.New(t.Context(), data, admin, bigtable.Config{JournalTableID: journal, HeadTableID: head}, es, ss)

		requireKind(t, err, eventstore.KindConfiguration)
		assert.Nil(t, entry)
	})

	t.Run("a data family that keeps two versions", func(t *testing.T) {
		l := provision(t, layoutSpec{})
		require.NoError(t, l.admin.SetGCPolicy(t.Context(), l.journal, "event", gbigtable.MaxVersionsPolicy(2)))

		_, err := l.tryOpen(l.config())

		requireKind(t, err, eventstore.KindConfiguration)
	})

	t.Run("a data family with an age rule", func(t *testing.T) {
		l := provision(t, layoutSpec{})
		require.NoError(t, l.admin.SetGCPolicy(t.Context(), l.head, "head", gbigtable.MaxAgePolicy(24*3600*1e9)))

		_, err := l.tryOpen(l.config())

		requireKind(t, err, eventstore.KindConfiguration)
	})

	for name, spec := range map[string]layoutSpec{
		"a missing journal configuration row": {skipJournalConfig: true},
		"a missing head configuration row":    {skipHeadConfig: true},
		"configuration rows that differ":      {headConfig: bigtabletest.ConfigJSON("11111111-2222-4333-8444-555555555555", nil, 1048576)},
		"a layout_version other than 1":       {journalConfig: `{"store_id":"` + validStoreID + `","layout_version":2,"keep_snapshot_count":null,"retention_mode":"delete","envelope_limit_bytes":1048576}`, headConfig: `{"store_id":"` + validStoreID + `","layout_version":2,"keep_snapshot_count":null,"retention_mode":"delete","envelope_limit_bytes":1048576}`},
		"an upper-case store_id":              {journalConfig: bigtabletest.ConfigJSON("6F1B3A52-9D0C-4F5E-8A7B-1C2D3E4F5A6B", nil, 1048576), headConfig: bigtabletest.ConfigJSON("6F1B3A52-9D0C-4F5E-8A7B-1C2D3E4F5A6B", nil, 1048576)},
		"a retention_mode other than delete":  {journalConfig: `{"store_id":"` + validStoreID + `","layout_version":1,"keep_snapshot_count":null,"retention_mode":"ttl","envelope_limit_bytes":1048576}`, headConfig: `{"store_id":"` + validStoreID + `","layout_version":1,"keep_snapshot_count":null,"retention_mode":"ttl","envelope_limit_bytes":1048576}`},
		"invalid JSON":                        {journalConfig: "{", headConfig: "{"},
	} {
		t.Run(name, func(t *testing.T) {
			l := provision(t, spec)

			_, err := l.tryOpen(l.config())

			requireKind(t, err, eventstore.KindConfiguration)
		})
	}

	t.Run("a configuration row that records a different retention count than the client", func(t *testing.T) {
		l := provision(t, layoutSpec{keep: ptr(int64(3))})

		_, err := l.tryOpen(l.config(), keepOption(t, 4))
		requireKind(t, err, eventstore.KindConfiguration)

		_, err = l.tryOpen(l.config())
		requireKind(t, err, eventstore.KindConfiguration)
	})

	t.Run("a configuration row that records a different envelope limit than the client", func(t *testing.T) {
		l := provision(t, layoutSpec{limit: 4096})
		cfg := l.config()
		cfg.EnvelopeLimitBytes = 8192

		_, err := l.tryOpen(cfg)

		requireKind(t, err, eventstore.KindConfiguration)
	})
}

func TestNewValidatesOptionsBeforeAnyRequest(t *testing.T) {
	data, admin := recordingClients(t)
	ttl := eventstore.WithRetentionMode(eventstore.RetentionTTL)
	for name, tc := range map[string]struct {
		cfg  bigtable.Config
		opts []eventstore.Option
	}{
		"a TTL retention mode":               {bigtable.Config{JournalTableID: "j", HeadTableID: "h"}, []eventstore.Option{ttl}},
		"a TTL retention mode with a count":  {bigtable.Config{JournalTableID: "j", HeadTableID: "h"}, []eventstore.Option{ttl, keepOption(t, 2)}},
		"the change feed":                    {bigtable.Config{JournalTableID: "j", HeadTableID: "h", ChangeFeed: true}, nil},
		"an envelope limit of 16":            {bigtable.Config{JournalTableID: "j", HeadTableID: "h", EnvelopeLimitBytes: 16}, nil},
		"an envelope limit of 8388609":       {bigtable.Config{JournalTableID: "j", HeadTableID: "h", EnvelopeLimitBytes: 8388609}, nil},
		"the same journal and head table ID": {bigtable.Config{JournalTableID: "same", HeadTableID: "same"}, nil},
		"an empty journal table ID":          {bigtable.Config{HeadTableID: "h"}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			es, ss := serializers()

			entry, err := bigtable.New(t.Context(), data.client, admin.client, tc.cfg, es, ss, tc.opts...)

			requireKind(t, err, eventstore.KindConfiguration)
			assert.Nil(t, entry)
			assert.Zero(t, data.recorder.count()+admin.recorder.count(), "no request may reach the destination")
		})
	}
}

func TestNewAcceptsTheEnvelopeLimitBoundaries(t *testing.T) {
	for _, limit := range []int{17, 8388608} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			l := provision(t, layoutSpec{limit: limit})

			_, err := l.tryOpen(l.config())

			require.NoError(t, err)
		})
	}
}

func TestNewRejectsNilClients(t *testing.T) {
	l := provision(t, layoutSpec{})
	es, ss := serializers()
	_, err := bigtable.New(t.Context(), nil, l.admin, l.config(), es, ss)
	requireKind(t, err, eventstore.KindConfiguration)
	_, err = bigtable.New(t.Context(), l.data, nil, l.config(), es, ss)
	requireKind(t, err, eventstore.KindConfiguration)
}

func TestNewReportsAnUnreachableDestinationAsAStorageError(t *testing.T) {
	l := provision(t, layoutSpec{})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	es, ss := serializers()

	_, err := bigtable.New(canceled, l.data, l.admin, l.config(), es, ss)

	requireKind(t, err, eventstore.KindStorage)
}

// TestConfigurationRow compares the two configuration rows by value (BT-13): key order and spaces do not matter,
// but the type of keep_snapshot_count, duplicate keys and leading zeros do.
func TestConfigurationRow(t *testing.T) {
	keepThree := layoutSpec{keep: ptr(int64(3))}
	const tail = `"layout_version":1,"retention_mode":"delete"}`
	id := `"store_id":"` + validStoreID + `"`
	open := func(t *testing.T, journal, head string) error {
		spec := keepThree
		spec.journalConfig, spec.headConfig = journal, head
		l := provision(t, spec)
		_, err := l.tryOpen(l.config(), keepOption(t, 3))
		return err
	}

	t.Run("a different key order and spacing with the same values is accepted", func(t *testing.T) {
		journal := `{ "envelope_limit_bytes":1048576, "keep_snapshot_count":"3", ` + id + `, ` + tail
		head := `{"retention_mode":"delete","layout_version":1,"keep_snapshot_count":"3","envelope_limit_bytes":1048576,` + id + `}`
		require.NoError(t, open(t, journal, head))
	})
	t.Run("a JSON number as the retention count is rejected", func(t *testing.T) {
		row := `{` + id + `,"keep_snapshot_count":3,"envelope_limit_bytes":1048576,` + tail
		requireKind(t, open(t, row, row), eventstore.KindConfiguration)
	})
	t.Run("a duplicate key is rejected", func(t *testing.T) {
		row := `{` + id + `,"keep_snapshot_count":"3","envelope_limit_bytes":1048576,"retention_mode":"delete","retention_mode":"delete","layout_version":1}`
		requireKind(t, open(t, row, row), eventstore.KindConfiguration)
	})
	t.Run("a leading zero in the retention count is rejected", func(t *testing.T) {
		row := `{` + id + `,"keep_snapshot_count":"03","envelope_limit_bytes":1048576,` + tail
		requireKind(t, open(t, row, row), eventstore.KindConfiguration)
	})
	t.Run("a missing field is rejected", func(t *testing.T) {
		row := `{` + id + `,"keep_snapshot_count":"3",` + tail
		requireKind(t, open(t, row, row), eventstore.KindConfiguration)
	})
	t.Run("an exponent in the retention count is rejected", func(t *testing.T) {
		row := `{` + id + `,"keep_snapshot_count":"3e0","envelope_limit_bytes":1048576,` + tail
		requireKind(t, open(t, row, row), eventstore.KindConfiguration)
	})
}
