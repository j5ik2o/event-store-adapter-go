package conformance

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"testing"

	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunCoreValueCases(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	count := 0
	for _, c := range d.Values {
		if c.Operation != "buildAid" && c.Operation != "validateSeqNr" {
			continue
		}
		count++
		t.Run(c.ID, func(t *testing.T) {
			res := runValueCase(c)
			assert.Equal(t, StatusSuccess, res.Status, res.Reason)
			assert.Empty(t, res.Backend)
			assert.Equal(t, c.Rules, res.Rules)
			require.NotNil(t, res.Actual)
			if expected, ok := c.Expect["value"]; ok {
				assert.True(t, jsonEqual(expected, res.Actual.(map[string]any)["value"]))
			} else {
				actual := res.Actual.(map[string]any)["error"].(map[string]any)
				assert.Equal(t, "contract-violation", actual["category"])
				assert.NotEmpty(t, actual["message"])
			}
		})
	}
	assert.Equal(t, 15, count)
}

func TestRunValueCaseRejectsWrongExpectations(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	byID := map[string]ValueCase{}
	for _, c := range d.Values {
		byID[c.ID] = c
	}
	for _, tc := range []struct {
		id     string
		expect map[string]any
	}{
		{"aid-library-format", map[string]any{"value": "custom-display-value"}},
		{"seq-max-value", map[string]any{"value": json.Number("9007199254740992")}},
		{"seq-zero-value", map[string]any{"error": map[string]any{"category": "contract-violation"}}},
		{"seq-zero-event", map[string]any{"value": json.Number("0")}},
		{"seq-negative-value", map[string]any{"error": map[string]any{"category": "optimistic-lock"}}},
		{"aid-hyphen-type", map[string]any{"error": map[string]any{"category": "contract-violation", "rule": "T-12"}}},
		{"seq-above-max-value", map[string]any{"error": map[string]any{"category": "contract-violation", "message": map[string]any{
			"must_contain": []any{"9007199254740993"}, "must_not_contain": []any{},
		}}}},
		{"seq-negative-value", map[string]any{"error": map[string]any{"category": "contract-violation", "message": map[string]any{
			"must_contain": []any{}, "must_not_contain": []any{"-1"},
		}}}},
	} {
		t.Run(tc.id, func(t *testing.T) {
			c := byID[tc.id]
			c.Expect = tc.expect
			res := runValueCase(c)
			assert.Equal(t, StatusFailure, res.Status)
			assert.NotEmpty(t, res.Reason)
			assert.NotEqual(t, res.Expected, res.Actual, "actual must not be copied from expected")
		})
	}
}

func TestRunEventSequenceValues(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		rule string
	}{
		{1, ""}, {9007199254740991, ""}, {0, "W-6"}, {-1, "T-9"}, {9007199254740992, "T-9"},
	} {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			expect := map[string]any{"value": json.Number(fmt.Sprint(tc.n))}
			if tc.rule != "" {
				expect = map[string]any{"error": map[string]any{
					"category": "contract-violation", "rule": tc.rule,
					"message": map[string]any{"must_contain": []any{fmt.Sprintf("seq_nr=%d", tc.n)}, "must_not_contain": []any{}},
				}}
			}
			c := ValueCase{ID: "event-sequence", Operation: "validateSeqNr",
				Input: ValueInput{SeqNr: big.NewInt(tc.n), Raw: map[string]any{"context": "event"}}, Expect: expect}
			res := runValueCase(c)
			assert.Equal(t, StatusSuccess, res.Status, res.Reason)
			actual := res.Actual.(map[string]any)
			if tc.rule == "" {
				assert.Equal(t, json.Number(fmt.Sprint(tc.n)), actual["value"])
			} else {
				actualError := actual["error"].(map[string]any)
				assert.Equal(t, "contract-violation", actualError["category"])
				assert.Contains(t, actualError["message"], tc.rule)
			}
		})
	}
}

func TestCoreOperationErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		err      error
		category string
	}{
		{&eventstore.OptimisticLockError{}, "optimistic-lock"},
		{&eventstore.ContractViolationError{Rule: "T-9"}, "contract-violation"},
		{&eventstore.SerializationError{}, "serialization"},
		{&eventstore.ConfigurationError{}, "configuration"},
		{&eventstore.StorageError{}, "storage"},
	} {
		t.Run(tc.category, func(t *testing.T) {
			err := fmt.Errorf("outer: %w", tc.err)
			actual := coreOperationError(err)
			var operationError *OperationError
			require.ErrorAs(t, actual, &operationError)
			assert.Equal(t, tc.category, operationError.Category)
			assert.Equal(t, err.Error(), operationError.Message)
		})
	}
	assert.Nil(t, coreOperationError(nil))
	err := errors.New("contract-violation: T-9 seq_nr=-1")
	assert.Same(t, err, coreOperationError(err))
	expect := map[string]any{"error": map[string]any{"category": "contract-violation", "rule": "T-9"}}
	res := compareValueResult(CaseResult{Expected: expect}, expect, nil, err)
	assert.Equal(t, StatusFailure, res.Status)
	assert.Contains(t, res.Reason, "分類のないエラー")
}

func TestRunValueCaseInputFailures(t *testing.T) {
	for _, c := range []ValueCase{
		{Operation: "buildAid", Input: ValueInput{Raw: map[string]any{"aggregate_id": "invalid"}}},
		{Operation: "validateSeqNr"},
		{Operation: "validateSeqNr", Input: ValueInput{SeqNr: big.NewInt(0), Raw: map[string]any{"context": "unknown"}}},
		{Operation: "unknown"},
	} {
		res := runValueCase(c)
		assert.Equal(t, StatusFailure, res.Status)
		assert.NotEmpty(t, res.Reason)
	}
	res := compareValueResult(CaseResult{}, map[string]any{"error": "invalid"}, nil, errors.New("error"))
	assert.Equal(t, StatusFailure, res.Status)
	assert.Contains(t, res.Reason, "エラー期待値が不正")
}

func TestRunValueCaseUnrepresentableSequence(t *testing.T) {
	for _, decimal := range []string{"9223372036854775808", "-9223372036854775809", "18446744073709551617"} {
		t.Run(decimal, func(t *testing.T) {
			n, ok := new(big.Int).SetString(decimal, 10)
			require.True(t, ok)
			c := ValueCase{Operation: "validateSeqNr", Input: ValueInput{SeqNr: n, Raw: map[string]any{"context": "value"}},
				Expect: map[string]any{"value": json.Number("1")}}
			res := runValueCase(c)
			assert.Equal(t, StatusUnrepresentable, res.Status)
			assert.Contains(t, res.Reason, decimal)
			assert.Equal(t, json.Number(decimal), res.Actual.(map[string]any)["seq_nr"])
		})
	}
}

func TestRunFNV1a64ValueCases(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	var cases []ValueCase
	for _, c := range d.Values {
		if c.Operation == "fnv1a64" {
			cases = append(cases, c)
		}
	}
	require.Len(t, cases, 4)

	t.Run("the four reference values of hash.md are computed from the UTF-8 input", func(t *testing.T) {
		for _, c := range cases {
			res := runValueCase(c)
			assert.Equal(t, StatusSuccess, res.Status, "%s: %s", c.ID, res.Reason)
			assert.Equal(t, c.Rules, res.Rules)
		}
	})

	t.Run("a wrong expected value is a failure and the actual value is not copied from it", func(t *testing.T) {
		for _, c := range cases {
			c.Expect = map[string]any{"value": "0x0000000000000001"}
			res := runValueCase(c)
			assert.Equal(t, StatusFailure, res.Status, c.ID)
			assert.NotEmpty(t, res.Reason)
			assert.NotEqual(t, res.Expected, res.Actual, c.ID)
		}
	})

	t.Run("an expected value that is not 0x and sixteen hexadecimal digits is a failure, not a pass", func(t *testing.T) {
		for _, bad := range []string{"0xaf63dc4c8601ec8", "af63dc4c8601ec8c", "0XAF63DC4C8601EC8C", "-5808590958014384884", "0xaf63dc4c8601ec8cc"} {
			c := cases[0]
			require.Equal(t, "0xaf63dc4c8601ec8c", c.Expect["value"], "the first case is the one-letter input")
			c.Expect = map[string]any{"value": bad}
			res := runValueCase(c)
			assert.Equal(t, StatusFailure, res.Status, bad)
		}
	})

	t.Run("a hash with the top bit set is compared as an unsigned value", func(t *testing.T) {
		c := cases[0]
		c.Expect = map[string]any{"value": "0x" + fmt.Sprintf("%016x", uint64(0xaf63dc4c8601ec8c)^1<<63)}
		assert.Equal(t, StatusFailure, runValueCase(c).Status, "the sign-flipped value must not equal the reference")
	})
}

func TestRunBackendRunsTheHashTableOnlyForStoragesThatUseTheHash(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	hashIDs := map[string]bool{}
	for _, c := range d.Values {
		if c.Operation == "fnv1a64" {
			hashIDs[c.ID] = true
		}
	}
	require.Len(t, hashIDs, 4)

	for _, name := range []string{"memory", "dynamodb", "sqlite"} {
		results := RunBackend(t.Context(), d, &fakeBackend{name: name})
		seen := 0
		for _, r := range results {
			if hashIDs[r.ID] {
				seen++
				assert.Equal(t, StatusNotApplicable, r.Status, "%s %s", name, r.ID)
				assert.Contains(t, r.Reason, "Spanner", "the reason says which storages use the hash")
			}
		}
		assert.Equal(t, 4, seen, name)
	}

	results := RunBackend(t.Context(), d, &fakeBackend{name: "spanner"})
	seen := 0
	for _, r := range results {
		if hashIDs[r.ID] {
			seen++
			assert.Equal(t, StatusSuccess, r.Status, "%s: %s", r.ID, r.Reason)
			assert.Equal(t, "spanner", r.Backend)
		}
	}
	assert.Equal(t, 4, seen)
	assert.Len(t, ApplicableCases(d, "spanner"), 79)
	for _, id := range ApplicableCases(d, "spanner") {
		assert.NotEqual(t, "", id)
	}
}
