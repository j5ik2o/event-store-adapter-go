package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadData(t *testing.T) {
	t.Run("reads every case and every value case of the real data", func(t *testing.T) {
		d, err := LoadData(dataRoot())
		require.NoError(t, err)
		assert.Equal(t, 169, len(d.Values)+len(d.Scenarios)+len(d.Layouts))
		assert.Len(t, d.Values, 40)
	})

	t.Run("operation is limited to four kinds", func(t *testing.T) {
		d, err := LoadData(dataRoot())
		require.NoError(t, err)
		allowed := map[string]bool{"buildAid": true, "validateOccurredAt": true, "validateSeqNr": true, "fnv1a64": true}
		outOfScope := map[string]bool{"encodeBigtableEventEnvelope": true, "encodeBigtableSnapshotEnvelope": true, "decodeBigtableEnvelope": true}
		for _, v := range d.Values {
			if outOfScope[v.Operation] {
				assert.True(t, v.OutOfScope, v.ID)
				continue
			}
			assert.True(t, allowed[v.Operation], v.ID)
			assert.False(t, v.OutOfScope, v.ID)
		}
	})

	t.Run("unknown operation is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"operation": "fnv1a64"`, `"operation": "unknownOp"`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("wrong format is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"format": "values"`, `"format": "scenarios"`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("wrong version is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"version": "1.3.0"`, `"version": "2.0.0"`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("json file without a known format mapping is rejected", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.WriteFile(filepath.Join(root, "extra.json"), []byte(`{"format":"values","version":"1.3.0"}`), 0o644))
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("broken schema json is rejected", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.WriteFile(filepath.Join(root, "schema", "values.schema.json"), []byte(`{`), 0o644))
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("duplicate key inside data is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "coverage.json"), `"format": "coverage",`, `"format": "coverage", "format": "coverage",`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})
}

func TestLoadData_ValueInputsKeepPrecision(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	byID := map[string]ValueCase{}
	for _, v := range d.Values {
		byID[v.ID] = v
	}
	require.Contains(t, byID, "seq-above-max-value")
	require.Contains(t, byID, "seq-negative-value")
	require.Contains(t, byID, "occurred-at-below-min")
	require.Contains(t, byID, "occurred-at-above-max")

	require.NotNil(t, byID["seq-above-max-value"].Input.SeqNr)
	assert.Equal(t, "9007199254740992", byID["seq-above-max-value"].Input.SeqNr.String())
	require.NotNil(t, byID["seq-negative-value"].Input.SeqNr)
	assert.Equal(t, "-1", byID["seq-negative-value"].Input.SeqNr.String())
	require.NotNil(t, byID["occurred-at-below-min"].Input.EpochNanoseconds)
	assert.Equal(t, "-9223372036854775809", byID["occurred-at-below-min"].Input.EpochNanoseconds.String())
	require.NotNil(t, byID["occurred-at-above-max"].Input.EpochNanoseconds)
	assert.Equal(t, "9223372036854775808", byID["occurred-at-above-max"].Input.EpochNanoseconds.String())
}

func TestLoadData_DuplicateID(t *testing.T) {
	t.Run("real data has unique ids", func(t *testing.T) {
		d, err := LoadData(dataRoot())
		require.NoError(t, err)
		assert.Equal(t, 169, len(d.Values)+len(d.Scenarios)+len(d.Layouts))
	})

	t.Run("duplicate id across files is rejected and named", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"id": "hash-fnv1a64-1"`, `"id": "seq-zero-value"`)
		_, err := LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "seq-zero-value")
	})
}

func TestLoadData_SchemaValidation(t *testing.T) {
	t.Run("a scenario without description is rejected by the scenarios schema", func(t *testing.T) {
		root := copyTree(t)
		files, err := filepath.Glob(filepath.Join(root, "scenarios", "core", "*.json"))
		require.NoError(t, err)
		require.NotEmpty(t, files)
		b, err := os.ReadFile(files[0])
		require.NoError(t, err)
		var doc map[string]any
		require.NoError(t, json.Unmarshal(b, &doc))
		delete(doc["cases"].([]any)[0].(map[string]any), "description")
		out, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(files[0], out, 0o644))
		_, err = LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema validation")
	})

	t.Run("an unknown property in a value case is rejected", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "values", "hash.json"), `"id": "hash-fnv1a64-1"`, `"extra": true, "id": "hash-fnv1a64-1"`)
		_, err := LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema validation")
	})

	t.Run("an invalid generator is caught by the schema before expansion", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "dynamodb", "read.json"), `"byte_length": 320000`, `"byte_length": 0`)
		_, err := LoadData(root)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema validation")
	})

	t.Run("a missing schema file is an error", func(t *testing.T) {
		root := copyTree(t)
		require.NoError(t, os.Remove(filepath.Join(root, "schema", "layout.schema.json")))
		_, err := LoadData(root)
		assert.Error(t, err)
	})
}

func TestLoadData_Exclusions(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)
	byRule := map[string]RuleExclusion{}
	for _, e := range d.Exclusions {
		byRule[e.Rule] = e
	}
	require.Contains(t, byRule, "W-5")
	require.Contains(t, byRule, "R-7")
	assert.Equal(t, "deleted", byRule["W-5"].Status)
	assert.Equal(t, "caller-obligation", byRule["R-7"].Status)
	assert.NotEmpty(t, byRule["W-5"].Reason)
	assert.NotEmpty(t, byRule["R-7"].Reason)
}

func TestLoadData_ValueInputShape(t *testing.T) {
	t.Run("the real data has an input of the right shape for every operation", func(t *testing.T) {
		_, err := LoadData(dataRoot())
		require.NoError(t, err)
	})

	cases := []struct {
		name, file, from, to string
	}{
		{"a utf8 input on buildAid", "hash.json", `"operation": "fnv1a64"`, `"operation": "buildAid"`},
		{"an aggregate_id input on validateSeqNr", "aid.json", `"operation": "buildAid"`, `"operation": "validateSeqNr"`},
		{"an aggregate_id input on validateOccurredAt", "aid.json", `"operation": "buildAid"`, `"operation": "validateOccurredAt"`},
		{"an aggregate_id input on fnv1a64", "aid.json", `"operation": "buildAid"`, `"operation": "fnv1a64"`},
		{"a seq_nr input on validateOccurredAt", "seq-nr.json", `"operation": "validateSeqNr"`, `"operation": "validateOccurredAt"`},
		{"a seq_nr input on buildAid", "seq-nr.json", `"operation": "validateSeqNr"`, `"operation": "buildAid"`},
		{"an occurred-at input on validateSeqNr", "occurred-at.json", `"operation": "validateOccurredAt"`, `"operation": "validateSeqNr"`},
		{"an occurred-at input on fnv1a64", "occurred-at.json", `"operation": "validateOccurredAt"`, `"operation": "fnv1a64"`},
	}
	for _, c := range cases {
		t.Run("rejects "+c.name, func(t *testing.T) {
			root := copyTree(t)
			replaceInFile(t, filepath.Join(root, "values", c.file), c.from, c.to)
			_, err := LoadData(root)
			assert.Error(t, err)
		})
	}
}

func TestParseScenario(t *testing.T) {
	t.Run("every real scenario of a provided storage has a parsed plan", func(t *testing.T) {
		d, err := LoadData(dataRoot())
		require.NoError(t, err)
		require.NotEmpty(t, d.Scenarios)
		for _, c := range d.Scenarios {
			if c.OutOfScope {
				continue
			}
			assert.NotNil(t, c.Plan, c.ID)
		}
	})

	t.Run("a step that names an unknown fixture is a data error", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "scenarios", "core", "write-read.json"), `"event": "e2"`, `"event": "no-such-fixture"`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})

	t.Run("a fault whose operation is beyond the steps is a data error", func(t *testing.T) {
		root := copyTree(t)
		replaceInFile(t, filepath.Join(root, "scenarios", "core", "retention-errors.json"), `"operation": 1,`, `"operation": 99,`)
		_, err := LoadData(root)
		assert.Error(t, err)
	})
}

const outOfScopeScenario = `{"cases":[{
  "id":"x-1","rules":["SQ-1"],"description":"d","backends":["%s"],
  "store":{"retention_count":null,"retention_mode":"delete"},
  %s
  "fixtures":{"events":{},"snapshots":{}},
  "steps":[{"op":"holdWriteLock","arguments":{"hold":"a"},"expect":{"result":"success"}}]
}]}`

func decodeObject(t *testing.T, body string) map[string]any {
	t.Helper()
	v, err := decodeStrictJSON([]byte(body))
	require.NoError(t, err)
	return v.(map[string]any)
}

func TestLoadCases_OutOfScope(t *testing.T) {
	t.Run("a scenario that targets only Spanner is read without its steps being interpreted", func(t *testing.T) {
		d := &Data{}
		err := loadCases(d, "x.json", "scenarios", decodeObject(t, fmt.Sprintf(outOfScopeScenario, "spanner", `"seed":{"events":{},"snapshots":{}},`)), map[string]string{})
		require.NoError(t, err)
		require.Len(t, d.Scenarios, 1)
		assert.True(t, d.Scenarios[0].OutOfScope)
		assert.Nil(t, d.Scenarios[0].Plan)
		assert.Equal(t, []string{"spanner"}, d.Scenarios[0].Backends)
	})

	t.Run("a scenario that targets memory keeps rejecting an unknown operation", func(t *testing.T) {
		d := &Data{}
		err := loadCases(d, "x.json", "scenarios", decodeObject(t, fmt.Sprintf(outOfScopeScenario, "memory", "")), map[string]string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `unsupported op "holdWriteLock"`)
		assert.Empty(t, d.Scenarios)
	})

	t.Run("a scenario that targets dynamodb keeps rejecting an unknown operation", func(t *testing.T) {
		d := &Data{}
		err := loadCases(d, "x.json", "scenarios", decodeObject(t, fmt.Sprintf(outOfScopeScenario, "dynamodb", "")), map[string]string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `unsupported op "holdWriteLock"`)
	})

	t.Run("a value case with a Bigtable envelope operation is read without its input being checked", func(t *testing.T) {
		d := &Data{}
		body := `{"cases":[{"id":"v-1","rules":["BT-D6a"],"description":"d","operation":"decodeBigtableEnvelope",
		  "input":{"kind":"event","bytes_hex":"00"},"expect":{"result":"error"}}]}`
		err := loadCases(d, "x.json", "values", decodeObject(t, body), map[string]string{})
		require.NoError(t, err)
		require.Len(t, d.Values, 1)
		assert.True(t, d.Values[0].OutOfScope)
		assert.Equal(t, "decodeBigtableEnvelope", d.Values[0].Operation)
	})

	t.Run("a value case with an operation outside the list is still rejected", func(t *testing.T) {
		d := &Data{}
		body := `{"cases":[{"id":"v-1","rules":["BT-D6a"],"description":"d","operation":"decodeUnknownEnvelope",
		  "input":{"kind":"event","bytes_hex":"00"},"expect":{"result":"error"}}]}`
		err := loadCases(d, "x.json", "values", decodeObject(t, body), map[string]string{})
		assert.Error(t, err)
	})
}

func TestLoadData_OutOfScopeFiles(t *testing.T) {
	d, err := LoadData(dataRoot())
	require.NoError(t, err)

	t.Run("the SQLite lock scenario that uses holdWriteLock is read as a scenario in scope", func(t *testing.T) {
		var found int
		for _, c := range d.Scenarios {
			if c.File == "sqlite/write-lock.json" {
				found++
				assert.False(t, c.OutOfScope, c.ID)
				require.NotNil(t, c.Plan, c.ID)
				require.NotNil(t, c.Plan.Store.LockWaitMilliseconds, c.ID)
			}
		}
		assert.Positive(t, found)
	})

	t.Run("the SQLite configuration scenarios keep their seeded configuration in the plan", func(t *testing.T) {
		var seeded int
		for _, c := range d.Scenarios {
			if c.File == "sqlite/configuration.json" {
				assert.False(t, c.OutOfScope, c.ID)
				require.NotNil(t, c.Plan, c.ID)
				if c.Plan.SeedConfiguration != nil {
					seeded++
				}
			}
		}
		assert.Equal(t, 4, seeded)
	})

	t.Run("only the scenarios without memory, dynamodb and sqlite are out of scope", func(t *testing.T) {
		for _, c := range d.Scenarios {
			provided := contains(c.Backends, "memory") || contains(c.Backends, "dynamodb") || contains(c.Backends, "sqlite")
			assert.Equal(t, !provided, c.OutOfScope, c.ID)
			assert.Equal(t, provided, c.Plan != nil, c.ID)
		}
	})

	t.Run("the new memory case is read as a scenario in scope", func(t *testing.T) {
		for _, c := range d.Scenarios {
			if c.ID == "memory-store-shared-configuration" {
				assert.False(t, c.OutOfScope)
				assert.Equal(t, []string{"MEM-2"}, c.Rules)
				return
			}
		}
		t.Fatal("memory-store-shared-configuration is not in the data")
	})

	t.Run("every Bigtable envelope value case is out of scope", func(t *testing.T) {
		var found int
		for _, v := range d.Values {
			if v.File == "values/bigtable-envelope.json" {
				found++
				assert.True(t, v.OutOfScope, v.ID)
			}
		}
		assert.Equal(t, 10, found)
	})
}

const storeScenario = `{"cases":[{
  "id":"s-1","rules":["W-1"],"description":"d","backends":%s,
  "store":%s,
  "fixtures":{"events":{},"snapshots":{}},
  "steps":[]
}]}`

func TestLoadCases_StoreSettings(t *testing.T) {
	load := func(t *testing.T, backends, store string) (*Data, error) {
		t.Helper()
		d := &Data{}
		err := loadCases(d, "x.json", "scenarios", decodeObject(t, fmt.Sprintf(storeScenario, backends, store)), map[string]string{})
		return d, err
	}
	unread := map[string]string{
		"lock_wait_milliseconds":  "50",
		"transaction_retry_limit": "3",
		"ttl_grace_days":          "7",
		"envelope_limit_bytes":    "1024",
	}

	for key, value := range unread {
		key, value := key, value
		store := `{"retention_count":null,"retention_mode":"delete","` + key + `":` + value + `}`

		t.Run("a scenario that targets only Spanner is read with the setting "+key, func(t *testing.T) {
			d, err := load(t, `["spanner"]`, store)
			require.NoError(t, err)
			require.Len(t, d.Scenarios, 1)
			assert.True(t, d.Scenarios[0].OutOfScope)
			assert.Nil(t, d.Scenarios[0].Plan)
		})

		if key == "lock_wait_milliseconds" {
			t.Run("a scenario that targets only SQLite interprets the setting "+key, func(t *testing.T) {
				d, err := load(t, `["sqlite"]`, store)
				require.NoError(t, err)
				require.Len(t, d.Scenarios, 1)
				require.NotNil(t, d.Scenarios[0].Plan)
				require.NotNil(t, d.Scenarios[0].Plan.Store.LockWaitMilliseconds)
				assert.Equal(t, int64(50), *d.Scenarios[0].Plan.Store.LockWaitMilliseconds)
			})
		} else {
			t.Run("a scenario that targets only SQLite rejects the setting "+key, func(t *testing.T) {
				d, err := load(t, `["sqlite"]`, store)
				require.Error(t, err)
				assert.Contains(t, err.Error(), key)
				assert.Empty(t, d.Scenarios)
			})
		}

		for _, backends := range []string{`["memory"]`, `["dynamodb"]`, `["memory","dynamodb","sqlite"]`} {
			backends := backends
			t.Run("a scenario that targets "+backends+" rejects the setting "+key, func(t *testing.T) {
				d, err := load(t, backends, store)
				require.Error(t, err)
				assert.Contains(t, err.Error(), key)
				assert.Empty(t, d.Scenarios)
			})
		}
	}

	t.Run("a scenario that targets memory is read with the settings the runner interprets", func(t *testing.T) {
		d, err := load(t, `["memory"]`, `{"retention_count":1,"retention_mode":"delete"}`)
		require.NoError(t, err)
		require.Len(t, d.Scenarios, 1)
		require.NotNil(t, d.Scenarios[0].Plan)
		require.NotNil(t, d.Scenarios[0].Plan.Store.RetentionCount)
		assert.Equal(t, int64(1), *d.Scenarios[0].Plan.Store.RetentionCount)
	})
}

// sqliteMisuseCase builds a scenario that targets the given backends. The lock steps are the accepted form of
// conformance/README.md; each rejection test below changes exactly one thing in it.
func sqliteMisuseCase(backends, rules, store, seed string, steps ...string) string {
	return fmt.Sprintf(`{"cases":[{
  "id":"w-1","rules":%s,"description":"d","backends":%s,
  "store":%s,
  %s
  "fixtures":{"events":{
    "e1":{"aggregate_id":{"type_name":"Order","value":"9"},"seq_nr":1,"occurred_at":"2026-10-05T00:00:00.123456789Z","payload":{"number":1},"manifest":""},
    "e2":{"aggregate_id":{"type_name":"Order","value":"9"},"seq_nr":2,"occurred_at":"2026-10-05T00:00:00.123456789Z","payload":{"number":2},"manifest":""}},
    "snapshots":{}},
  "steps":[%s]
}]}`, rules, backends, store, seed, strings.Join(steps, ","))
}

const (
	lockStore    = `{"retention_count":null,"retention_mode":"delete","lock_wait_milliseconds":100}`
	plainStore   = `{"retention_count":null,"retention_mode":"delete"}`
	stepPersist1 = `{"op":"persistEvent","arguments":{"event":"e1"},"expect":{"result":"success"}}`
	stepHold     = `{"op":"holdWriteLock","arguments":{},"expect":{"result":"success"}}`
	stepFail     = `{"op":"persistEvent","arguments":{"event":"e2"},"expect":{"error":{"category":"storage"}}}`
	stepFailLock = `{"op":"persistEvent","arguments":{"event":"e2"},"expect":{"error":{"category":"optimistic-lock"}}}`
	stepRelease  = `{"op":"releaseWriteLock","arguments":{},"expect":{"result":"success"}}`
	stepPersist2 = `{"op":"persistEvent","arguments":{"event":"e2"},"expect":{"result":"success"}}`
	seedConfig   = `"seed":{"configuration":{"store_id":"5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11","layout_version":1,"keep_snapshot_count":null,"retention_mode":"delete","change_feed_enabled":0}},`
)

func TestLoadCases_SQLiteOnlyMisuse(t *testing.T) {
	load := func(body string) (*Data, error) {
		d := &Data{}
		err := loadCases(d, "x.json", "scenarios", decodeObject(t, body), map[string]string{})
		return d, err
	}
	lockSteps := []string{stepPersist1, stepHold, stepFail, stepRelease, stepPersist2}

	t.Run("accepted forms", func(t *testing.T) {
		d, err := load(sqliteMisuseCase(`["sqlite"]`, `["SQ-7","E-1"]`, lockStore, "", lockSteps...))
		require.NoError(t, err)
		require.Len(t, d.Scenarios, 1)
		assert.NotNil(t, d.Scenarios[0].Plan)

		d, err = load(sqliteMisuseCase(`["sqlite"]`, `["SQ-2"]`, plainStore, seedConfig, stepPersist1))
		require.NoError(t, err)
		require.Len(t, d.Scenarios, 1)
		assert.NotNil(t, d.Scenarios[0].Plan.SeedConfiguration)
	})

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"seed.configuration outside a scenario that targets only sqlite",
			sqliteMisuseCase(`["memory","sqlite"]`, `["SQ-2"]`, plainStore, seedConfig, stepPersist1),
			"seed.configuration is only for"},
		{"seed with both items and configuration",
			sqliteMisuseCase(`["sqlite"]`, `["SQ-2"]`, plainStore, strings.Replace(seedConfig, `"seed":{`, `"seed":{"items":[],`, 1), stepPersist1),
			"exactly one of items and configuration"},
		{"holdWriteLock while the lock is held",
			sqliteMisuseCase(`["sqlite"]`, `["SQ-7"]`, lockStore, "", stepPersist1, stepHold, stepHold, stepFail, stepRelease, stepPersist2),
			"already held"},
		{"releaseWriteLock without a held lock",
			sqliteMisuseCase(`["sqlite"]`, `["SQ-7"]`, lockStore, "", stepPersist1, stepFail, stepRelease, stepPersist2),
			"without a held lock"},
		{"lock scenario without the rule SQ-7",
			sqliteMisuseCase(`["sqlite"]`, `["E-1"]`, lockStore, "", lockSteps...),
			"needs the rule SQ-7"},
		{"lock scenario without lock_wait_milliseconds",
			sqliteMisuseCase(`["sqlite"]`, `["SQ-7"]`, plainStore, "", lockSteps...),
			"needs store.lock_wait_milliseconds"},
		{"the scenario ends while the lock is held",
			sqliteMisuseCase(`["sqlite"]`, `["SQ-7"]`, lockStore, "", stepPersist1, stepHold, stepFail),
			"ends while the write lock is held"},
		{"no persist expects a storage error while the lock is held",
			sqliteMisuseCase(`["sqlite"]`, `["SQ-7"]`, lockStore, "", stepPersist1, stepHold, stepFailLock, stepRelease, stepPersist2),
			"no persist expects a storage error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := load(tc.body)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Empty(t, d.Scenarios)
		})
	}
}
