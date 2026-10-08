package eventstore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventEnvelopeAccessors(t *testing.T) {
	// A domain value needs no Event interface or library-specific methods.
	payload := struct{ Name string }{Name: "created"}
	at := time.Date(2026, time.October, 8, 12, 34, 56, 123456789, time.FixedZone("domain", 9*60*60))
	e, err := NewEventEnvelope(userID{"Order", "item-1"}, 7, at, payload)
	require.NoError(t, err)
	require.NoError(t, e.Validate())
	assert.Equal(t, "Order-item-1", e.AggregateID())
	assert.Equal(t, SeqNr(7), e.SeqNr())
	assert.Equal(t, at, e.OccurredAt())
	assert.Empty(t, e.Manifest())
	assert.Equal(t, payload, e.Payload())
}

func TestEventEnvelopeManifest(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []EnvelopeOption
		want string
	}{
		{"omitted", nil, ""},
		{"empty", []EnvelopeOption{WithManifest("")}, ""},
		{"uninterpreted", []EnvelopeOption{WithManifest("  arbitrary/型-v999\x00\n ")}, "  arbitrary/型-v999\x00\n "},
		{"last", []EnvelopeOption{WithManifest("first"), WithManifest("last")}, "last"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := NewEventEnvelope(userID{"Order", "1"}, 1, time.Unix(0, 0), "domain", tc.opts...)
			require.NoError(t, err)
			assert.Equal(t, tc.want, e.Manifest())
			assert.Equal(t, "domain", e.Payload())
		})
	}
}

func TestEventEnvelopeMissingAndExplicitValues(t *testing.T) {
	var zero EventEnvelope[any]
	violation := requireContractViolation(t, zero.Validate(), "T-2")
	assert.Nil(t, violation.SeqNr, "an unconstructed envelope has no supplied number")
	assert.NotContains(t, violation.Error(), "seq_nr")
	for _, payload := range []any{nil, (*string)(nil), json.RawMessage("null"), 0, false, ""} {
		t.Run(fmt.Sprintf("%T/%v", payload, payload), func(t *testing.T) {
			e, err := NewEventEnvelope(userID{"Order", "1"}, 1, time.Unix(0, 0), payload)
			require.NoError(t, err)
			require.NoError(t, e.Validate())
			assert.Equal(t, payload, e.Payload())
		})
	}
}

func TestEventEnvelopeNilIDs(t *testing.T) {
	for _, id := range []AggregateID{nil, (*nilPointerID)(nil), nilMapID(nil), nilSliceID(nil), nilFuncID(nil), nilChanID(nil)} {
		t.Run(fmt.Sprintf("%T", id), func(t *testing.T) {
			e, err := NewEventEnvelope(id, 7, time.Unix(0, 0), "payload")
			assert.Empty(t, e.AggregateID())
			v := requireContractViolation(t, err, "T-2")
			require.NotNil(t, v.SeqNr)
			assert.Equal(t, SeqNr(7), *v.SeqNr)
			assert.Contains(t, err.Error(), "seq_nr=7")
		})
	}
}

func TestEventEnvelopeIDValidation(t *testing.T) {
	for _, tc := range []struct{ typeName, value, want, rule string }{
		{"", "値", "-値", ""}, {"型", "", "型-", ""}, {"", "", "-", ""},
		{"order-item", "1", "", "T-11"},
	} {
		t.Run(tc.typeName+"/"+tc.value, func(t *testing.T) {
			e, err := NewEventEnvelope(userID{tc.typeName, tc.value}, 7, time.Unix(0, 0), "payload")
			if tc.rule == "" {
				require.NoError(t, err)
				assert.Equal(t, tc.want, e.AggregateID())
				return
			}
			v := requireContractViolation(t, err, tc.rule)
			require.NotNil(t, v.SeqNr)
			assert.Equal(t, SeqNr(7), *v.SeqNr)
			assert.Contains(t, err.Error(), "seq_nr=7")
		})
	}
}

func TestEventEnvelopeIDByteBoundaries(t *testing.T) {
	for _, multibyte := range []bool{false, true} {
		for _, size := range []int{1023, 1024, 1025} {
			t.Run(fmt.Sprintf("multibyte=%t/bytes=%d", multibyte, size), func(t *testing.T) {
				typeName, value := "T", strings.Repeat("a", size-2)
				if multibyte {
					typeName = "型"
					n := size - len(typeName) - 1
					value = strings.Repeat("あ", n/3) + strings.Repeat("a", n%3)
				}
				want := typeName + "-" + value
				require.Len(t, want, size)
				e, err := NewEventEnvelope(userID{typeName, value}, 7, time.Unix(0, 0), "payload")
				if size <= 1024 {
					require.NoError(t, err)
					assert.Equal(t, want, e.AggregateID())
					return
				}
				v := requireContractViolation(t, err, "T-12")
				require.NotNil(t, v.SeqNr)
				assert.Equal(t, SeqNr(7), *v.SeqNr)
				assert.Contains(t, err.Error(), "seq_nr=7")
			})
		}
	}
}

func TestEventEnvelopeSequenceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		n    SeqNr
		rule string
	}{
		{1, ""}, {MaxSeqNr, ""}, {0, "W-6"}, {-1, "T-9"}, {MaxSeqNr + 1, "T-9"},
	} {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			e, err := NewEventEnvelope(userID{"Order", "1"}, tc.n, time.Unix(0, 0), "payload")
			if tc.rule == "" {
				require.NoError(t, err)
				require.NoError(t, e.Validate())
				assert.Equal(t, tc.n, e.SeqNr())
				return
			}
			v := requireContractViolation(t, err, tc.rule)
			require.NotNil(t, v.SeqNr)
			assert.Equal(t, tc.n, *v.SeqNr)
			assert.Contains(t, err.Error(), fmt.Sprintf("seq_nr=%d", tc.n))
		})
	}
}

func TestEventEnvelopeTimeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		iso   string
		valid bool
	}{
		{"1677-09-21T00:12:43.145224192Z", true},
		{"2262-04-11T23:47:16.854775807Z", true},
		{"1677-09-21T00:12:43.145224191Z", false},
		{"2262-04-11T23:47:16.854775808Z", false},
		{"0001-01-01T00:00:00Z", false},
		{"1970-01-01T00:00:00Z", true},
	} {
		t.Run(tc.iso, func(t *testing.T) {
			at, err := time.Parse(time.RFC3339Nano, tc.iso)
			require.NoError(t, err)
			e, err := NewEventEnvelope(userID{"Order", "1"}, 7, at, "payload")
			if tc.valid {
				require.NoError(t, err)
				assert.Equal(t, at, e.OccurredAt())
				return
			}
			v := requireContractViolation(t, err, "T-13")
			require.NotNil(t, v.SeqNr)
			assert.Equal(t, SeqNr(7), *v.SeqNr)
			assert.Contains(t, err.Error(), "seq_nr=7")
		})
	}
}

func TestEnvelopeFieldsArePrivate(t *testing.T) {
	// Non-exported fields are an explicit part of the public envelope contract.
	for _, typ := range []reflect.Type{reflect.TypeOf(EventEnvelope[any]{}), reflect.TypeOf(SnapshotEnvelope[any]{})} {
		for i := 0; i < typ.NumField(); i++ {
			assert.False(t, typ.Field(i).IsExported(), "%s.%s", typ.Name(), typ.Field(i).Name)
		}
	}
}
