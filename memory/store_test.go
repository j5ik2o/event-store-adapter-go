package memory

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEvent(t *testing.T, value string, seqNr eventstore.SeqNr, payload []byte) eventstore.EventEnvelope[[]byte] {
	t.Helper()
	id, err := eventstore.NewAggregateID("Account", value)
	require.NoError(t, err)
	event, err := eventstore.NewEventEnvelope(id, seqNr, time.Unix(1720000000, 123456789), payload,
		eventstore.WithManifest(" event/v1\x00日本語 "))
	require.NoError(t, err)
	return event
}

func requireKind(t *testing.T, err error, expected eventstore.Kind) {
	t.Helper()
	require.Error(t, err)
	kind, ok := eventstore.KindOf(err)
	require.True(t, ok, "error must use the existing classification: %v", err)
	require.Equal(t, expected, kind)
}

func TestNewStoreSettings(t *testing.T) {
	count, err := eventstore.KeepLatest(3)
	require.NoError(t, err)
	positiveCount := 3
	for _, tc := range []struct {
		name  string
		opts  []eventstore.Option
		count *int
		mode  storeoptions.RetentionMode
		grace int64
	}{
		{name: "omitted", mode: storeoptions.RetentionDelete},
		{name: "explicit no retention", opts: []eventstore.Option{eventstore.WithRetentionCount(eventstore.NoRetention())}, mode: storeoptions.RetentionDelete},
		{name: "positive count", opts: []eventstore.Option{eventstore.WithRetentionCount(count), eventstore.WithTTLGraceSeconds(9)}, count: &positiveCount, mode: storeoptions.RetentionDelete, grace: 9},
		{name: "options in order", opts: []eventstore.Option{eventstore.WithRetentionCount(count), eventstore.WithRetentionCount(eventstore.NoRetention()), eventstore.WithTTLGraceSeconds(1), eventstore.WithTTLGraceSeconds(7)}, mode: storeoptions.RetentionDelete, grace: 7},
		{name: "mode ignored without history", opts: []eventstore.Option{eventstore.WithRetentionMode(99)}, mode: 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore(tc.opts...)

			require.NoError(t, err)
			require.NotNil(t, store)
			assert.Equal(t, tc.count, store.settings.RetentionCount)
			assert.Equal(t, tc.mode, store.settings.RetentionMode)
			assert.Equal(t, tc.grace, store.settings.TTLGraceSeconds)
			head, journal := store.observe("Account-new")
			assert.Nil(t, head)
			assert.Empty(t, journal)
		})
	}
}

func TestNewStoreRejectsConfiguration(t *testing.T) {
	count, err := eventstore.KeepLatest(2)
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		opts []eventstore.Option
	}{
		{"zero value retention", []eventstore.Option{eventstore.WithRetentionCount(eventstore.RetentionCount{})}},
		{"numeric zero retention", []eventstore.Option{func(o *storeoptions.Options) error { n := 0; o.RetentionCount = &n; return nil }}},
		{"negative retention", []eventstore.Option{func(o *storeoptions.Options) error { n := -1; o.RetentionCount = &n; return nil }}},
		{"TTL without history", []eventstore.Option{eventstore.WithRetentionMode(eventstore.RetentionTTL)}},
		{"TTL with no retention", []eventstore.Option{eventstore.WithRetentionCount(eventstore.NoRetention()), eventstore.WithRetentionMode(eventstore.RetentionTTL)}},
		{"TTL with history", []eventstore.Option{eventstore.WithRetentionCount(count), eventstore.WithRetentionMode(eventstore.RetentionTTL)}},
		{"unknown mode with history", []eventstore.Option{eventstore.WithRetentionCount(count), eventstore.WithRetentionMode(99)}},
		{"negative grace", []eventstore.Option{eventstore.WithTTLGraceSeconds(-1)}},
		{"nil option", []eventstore.Option{nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore(tc.opts...)

			require.Nil(t, store)
			requireKind(t, err, eventstore.KindConfiguration)
			var configuration *eventstore.ConfigurationError
			require.ErrorAs(t, err, &configuration)
			require.NotNil(t, configuration.Unwrap())
			assert.ErrorIs(t, err, configuration.Unwrap())
			t.Log("Configuration=1 store=nil")
		})
	}
}

func TestNewStorePreservesOptionErrorAndStops(t *testing.T) {
	cause := errors.New("option rejected")
	optionError := fmt.Errorf("option: %w", &eventstore.ConfigurationError{Cause: cause})
	laterCalls := 0

	store, err := NewStore(
		func(*storeoptions.Options) error { return optionError },
		func(*storeoptions.Options) error { laterCalls++; return nil },
	)

	require.Nil(t, store)
	requireKind(t, err, eventstore.KindConfiguration)
	require.ErrorIs(t, err, cause)
	var configuration *eventstore.ConfigurationError
	require.ErrorAs(t, err, &configuration)
	assert.ErrorIs(t, configuration.Unwrap(), cause)
	assert.Zero(t, laterCalls)
}

func TestNewStoreOwnsSettingsAcrossCalls(t *testing.T) {
	count := 3
	var applied *storeoptions.Options
	optionCalls, originalHandlerCalls, replacementHandlerCalls := 0, 0, 0
	store, err := NewStore(func(o *storeoptions.Options) error {
		optionCalls++
		applied = o
		o.RetentionCount = &count
		o.TTLGraceSeconds = 9
		o.RetentionFailureHandler = func(context.Context, error) { originalHandlerCalls++ }
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, originalHandlerCalls)
	first := newTestEvent(t, "settings", 1, []byte("first"))
	require.NoError(t, store.persistEvent(context.Background(), first))
	headBefore, journalBefore := store.observe(first.AggregateID())

	count = 0
	zeroCount := 0
	applied.RetentionCount = &zeroCount
	applied.RetentionMode = storeoptions.RetentionTTL
	applied.TTLGraceSeconds = -1
	applied.RetentionFailureHandler = func(context.Context, error) { replacementHandlerCalls++ }
	second := newTestEvent(t, "settings", 2, []byte("second"))
	require.NoError(t, store.persistEvent(context.Background(), second))

	require.NotNil(t, store.settings.RetentionCount)
	assert.Equal(t, 3, *store.settings.RetentionCount)
	assert.Equal(t, storeoptions.RetentionDelete, store.settings.RetentionMode)
	assert.Equal(t, int64(9), store.settings.TTLGraceSeconds)
	assert.Equal(t, 1, optionCalls)
	store.settings.RetentionFailureHandler(context.Background(), errors.New("configuration observation"))
	assert.Equal(t, 1, originalHandlerCalls)
	assert.Zero(t, replacementHandlerCalls)
	head, journal := store.observe(first.AggregateID())
	require.NotNil(t, head)
	require.Len(t, journal, 2)
	assert.Equal(t, eventstore.SeqNr(2), head.seqNr)
	assert.Equal(t, *headBefore, journal[0])
	assert.Equal(t, journalBefore[0], journal[0])
	t.Log("option applications=1 success=2 head=2 journal=2; settings unchanged after caller mutation")
}

func TestNewStoreSharingAndIsolation(t *testing.T) {
	firstStore, err := NewStore()
	require.NoError(t, err)
	secondStore, err := NewStore()
	require.NoError(t, err)
	firstCaller, secondCaller := firstStore, firstStore
	first := newTestEvent(t, "shared", 1, []byte("first"))
	second := newTestEvent(t, "shared", 2, []byte("second"))

	require.NoError(t, firstCaller.persistEvent(context.Background(), first))
	require.NoError(t, secondCaller.persistEvent(context.Background(), second))
	headBefore, journalBefore := firstStore.observe(first.AggregateID())
	isolated := newTestEvent(t, "shared", 1, []byte("isolated"))
	require.NoError(t, secondStore.persistEvent(context.Background(), isolated))

	head, journal := firstStore.observe(first.AggregateID())
	require.NotNil(t, head)
	assert.Equal(t, eventstore.SeqNr(2), head.seqNr)
	require.Len(t, journal, 2)
	assert.Equal(t, headBefore, head)
	assert.Equal(t, journalBefore, journal)
	otherHead, otherJournal := secondStore.observe(isolated.AggregateID())
	require.NotNil(t, otherHead)
	assert.Equal(t, eventstore.SeqNr(1), otherHead.seqNr)
	assert.Equal(t, []byte("isolated"), otherHead.payload)
	assert.Len(t, otherJournal, 1)
	t.Log("shared Store: success=2 head=2 journal=2; separate Store: success=1 head=1 journal=1")
}

func TestStoredEventCloneProtectsPayloadAndMetadata(t *testing.T) {
	original := storedEvent{aggregateID: "Account-clone", seqNr: 1, occurredAt: time.Unix(1, 123456789), manifest: "opaque", payload: []byte{0, 255}}

	cloned := original.clone()
	cloned.payload[0] = 99
	cloned.aggregateID = "Account-other"
	cloned.seqNr = 2
	cloned.occurredAt = time.Unix(2, 0)
	cloned.manifest = "changed"

	assert.Equal(t, storedEvent{aggregateID: "Account-clone", seqNr: 1, occurredAt: time.Unix(1, 123456789), manifest: "opaque", payload: []byte{0, 255}}, original)
}
