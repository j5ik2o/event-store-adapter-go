package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/dynamodbtest"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/storeoptions"
	"github.com/stretchr/testify/require"
)

type configurationResults struct {
	mu           sync.Mutex
	reads        []*awsdynamodb.BatchGetItemOutput
	createErrors []error
}

// This observes returned SDK results; inputs remain owned by the existing Recorder.
func (r *configurationResults) apiOption(stack *middleware.Stack) error {
	return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("observe-configuration-result", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
		out, metadata, err := next.HandleInitialize(ctx, in)
		r.mu.Lock()
		defer r.mu.Unlock()
		switch in.Parameters.(type) {
		case *awsdynamodb.BatchGetItemInput:
			result, _ := out.Result.(*awsdynamodb.BatchGetItemOutput)
			r.reads = append(r.reads, result)
		case *awsdynamodb.TransactWriteItemsInput:
			r.createErrors = append(r.createErrors, err)
		}
		return out, metadata, err
	}), middleware.Before)
}

func configurationEnvironment(t *testing.T) *dynamodbtest.Environment {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	e, err := dynamodbtest.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, e.Close(ctx))
	})
	return e
}

func configurationTables(t *testing.T, e *dynamodbtest.Environment, ttl bool) (*dynamodbtest.Tables, Config) {
	t.Helper()
	tables, err := e.CreateTables(context.Background(), ttl)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, tables.Close(ctx))
	})
	names := make(map[string]string, 3)
	for _, table := range []string{"journal", "snapshot", "head"} {
		names[table], err = tables.TableName(dynamodbtest.Table(table))
		require.NoError(t, err)
	}
	layout, err := tables.Describe(context.Background())
	require.NoError(t, err)
	require.Len(t, layout["snapshot"].Description.GlobalSecondaryIndexes, 1)
	return tables, Config{JournalTableName: names["journal"], SnapshotTableName: names["snapshot"], HeadTableName: names["head"], SnapshotHistoryIndexName: aws.ToString(layout["snapshot"].Description.GlobalSecondaryIndexes[0].IndexName)}
}

func configurationObservationKey(table string) map[string]types.AttributeValue {
	key := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "__config__"}}
	if table == "journal" {
		key["seq_nr"] = &types.AttributeValueMemberN{Value: "0"}
	}
	if table == "snapshot" {
		key["skey"] = &types.AttributeValueMemberN{Value: "0"}
	}
	return key
}

func observedConfiguration(t *testing.T, tables *dynamodbtest.Tables) map[string]map[string]types.AttributeValue {
	t.Helper()
	items := make(map[string]map[string]types.AttributeValue)
	for _, table := range []string{"journal", "snapshot", "head"} {
		item, err := tables.GetItem(context.Background(), dynamodbtest.Table(table), configurationObservationKey(table))
		require.NoError(t, err)
		if len(item) > 0 {
			items[table] = item
		}
	}
	return items
}

// These are explicit setup inputs, independent of the conformance expectations.
func configurationSeed(storeID string) []map[string]any {
	var fixtures []map[string]any
	for _, table := range []string{"journal", "snapshot", "head"} {
		attributes := map[string]any{"aid": "S", "store_id": "S", "layout_version": "N"}
		values := map[string]any{"aid": "__config__", "store_id": storeID, "layout_version": "1"}
		if table == "journal" {
			attributes["seq_nr"] = "N"
			values["seq_nr"] = "0"
		}
		if table == "snapshot" {
			attributes["skey"] = "N"
			values["skey"] = "0"
		}
		fixtures = append(fixtures, map[string]any{"table": table, "attributes": attributes, "values": values})
	}
	return fixtures
}

func configurationOptions(t *testing.T, cfg conformance.StoreConfig) []eventstore.Option {
	t.Helper()
	count := eventstore.NoRetention()
	if cfg.RetentionCount != nil {
		var err error
		count, err = eventstore.KeepLatest(int(*cfg.RetentionCount))
		require.NoError(t, err)
	}
	opts := []eventstore.Option{eventstore.WithRetentionCount(count)}
	mode := eventstore.RetentionDelete
	if cfg.RetentionMode == "ttl" {
		mode = eventstore.RetentionTTL
	}
	opts = append(opts, eventstore.WithRetentionMode(mode))
	if cfg.TTLGraceSeconds != nil {
		opts = append(opts, eventstore.WithTTLGraceSeconds(*cfg.TTLGraceSeconds))
	}
	return opts
}

func configurationAttribute(value types.AttributeValue) (kind, text string) {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		return "S", value.Value
	case *types.AttributeValueMemberN:
		return "N", value.Value
	case *types.AttributeValueMemberBOOL:
		return "BOOL", fmt.Sprint(value.Value)
	default:
		return fmt.Sprintf("%T", value), ""
	}
}

func compareConfigurationItems(t *testing.T, expected []any, items map[string]map[string]types.AttributeValue, bindings map[string]string) {
	t.Helper()
	for _, entry := range expected {
		fixture := entry.(map[string]any)
		item := items[fixture["table"].(string)]
		attributes := fixture["attributes"].(map[string]any)
		require.Len(t, item, len(attributes))
		for name, expectedType := range attributes {
			kind, _ := configurationAttribute(item[name])
			require.Equal(t, expectedType, kind, name)
		}
		for name, expectedValue := range fixture["values"].(map[string]any) {
			kind, value := configurationAttribute(item[name])
			if kind == "N" {
				require.True(t, configurationNumberEquals(value, expectedValue.(string)), name)
			} else {
				require.Equal(t, expectedValue, value, name)
			}
		}
		if raw, ok := fixture["bindings"].(map[string]any); ok {
			for name, rawBinding := range raw {
				_, value := configurationAttribute(item[name])
				binding := rawBinding.(string)
				if bound, exists := bindings[binding]; exists {
					require.Equal(t, bound, value)
				} else {
					require.NotEmpty(t, value)
					bindings[binding] = value
				}
			}
		}
	}
}

func configurationTableAliases(cfg Config) map[string]string {
	return map[string]string{cfg.JournalTableName: "journal", cfg.SnapshotTableName: "snapshot", cfg.HeadTableName: "head"}
}

func configurationRequestKeys(t *testing.T, cfg Config, request map[string]types.KeysAndAttributes, requireConsistent bool) []string {
	t.Helper()
	var names []string
	aliases := configurationTableAliases(cfg)
	for table, row := range request {
		alias, ok := aliases[table]
		require.True(t, ok, "unknown table %s", table)
		if requireConsistent {
			require.True(t, aws.ToBool(row.ConsistentRead))
		}
		for _, key := range row.Keys {
			require.Equal(t, configurationObservationKey(alias), key)
			name := alias + ":__config__"
			if alias != "head" {
				name += ":0"
			}
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func compareConfigurationRequests(t *testing.T, cfg Config, observe map[string]any, requests []dynamodbtest.Request, results *configurationResults, waits []time.Duration) {
	t.Helper()
	counts := map[string]int{}
	readIndex := 0
	var wantWaits []time.Duration
	delay := 50 * time.Millisecond
	for _, request := range requests {
		switch input := request.Input.(type) {
		case *awsdynamodb.BatchGetItemInput:
			counts["configuration-read"]++
			keys := configurationRequestKeys(t, cfg, input.RequestItems, true)
			if readIndex > 0 && results.reads[readIndex-1] != nil && len(results.reads[readIndex-1].UnprocessedKeys) > 0 {
				require.Equal(t, configurationRequestKeys(t, cfg, results.reads[readIndex-1].UnprocessedKeys, false), keys)
				wantWaits = append(wantWaits, delay)
				delay = min(2*delay, time.Second)
			} else {
				require.Equal(t, []string{"head:__config__", "journal:__config__:0", "snapshot:__config__:0"}, keys)
				delay = 50 * time.Millisecond
			}
			readIndex++
		case *awsdynamodb.TransactWriteItemsInput:
			counts["configuration-create"]++
			require.Len(t, input.TransactItems, 3)
			var tables []string
			var id string
			for _, action := range input.TransactItems {
				require.NotNil(t, action.Put)
				put := action.Put
				table := configurationTableAliases(cfg)[aws.ToString(put.TableName)]
				require.NotEmpty(t, table)
				tables = append(tables, table)
				require.Equal(t, "attribute_not_exists(aid)", strings.Join(strings.Fields(aws.ToString(put.ConditionExpression)), ""))
				item := put.Item
				require.Len(t, item, len(configurationObservationKey(table))+2)
				for key, value := range configurationObservationKey(table) {
					require.Equal(t, value, item[key])
				}
				require.Equal(t, &types.AttributeValueMemberN{Value: "1"}, item["layout_version"])
				kind, value := configurationAttribute(item["store_id"])
				require.Equal(t, "S", kind)
				require.NotEmpty(t, value)
				if id == "" {
					id = value
				} else {
					require.Equal(t, id, value)
				}
			}
			require.ElementsMatch(t, []string{"journal", "snapshot", "head"}, tables)
		default:
			t.Fatalf("unexpected configuration request %s", request.API)
		}
	}
	require.Equal(t, wantWaits, waits, "backoff observations")
	if expected, ok := observe["requests"].([]any); ok {
		require.Len(t, requests, len(expected))
		for i, raw := range expected {
			row := raw.(map[string]any)
			require.Equal(t, row["api"], requests[i].API)
			constraints := row["constraints"].(map[string]any)
			if rawKeys, ok := constraints["keys"].([]any); ok {
				var expectedKeys []string
				for _, key := range rawKeys {
					expectedKeys = append(expectedKeys, key.(string))
				}
				require.ElementsMatch(t, expectedKeys, configurationRequestKeys(t, cfg, requests[i].Input.(*awsdynamodb.BatchGetItemInput).RequestItems, true))
			}
			if phase := row["phase"].(string); phase == "configuration-read" {
				require.Equal(t, "BatchGetItem", requests[i].API)
			} else {
				require.Equal(t, "configuration-create", phase)
				require.Equal(t, "TransactWriteItems", requests[i].API)
			}
		}
	}
	if expected, ok := observe["request_count"].(map[string]any); ok {
		for phase, count := range expected {
			require.Equal(t, count.(json.Number).String(), fmt.Sprint(counts[phase]))
		}
	}
	if expected, ok := observe["no_requests_in_phases"].([]any); ok {
		for _, phase := range expected {
			require.Zero(t, counts[phase.(string)], phase)
		}
	}
}

func logConfiguration(t *testing.T, cfg Config, requests []dynamodbtest.Request, items map[string]map[string]types.AttributeValue, results *configurationResults, waits []time.Duration, injection conformance.Injection) {
	t.Helper()
	attributes := func(item map[string]types.AttributeValue) map[string]any {
		out := make(map[string]any)
		for name, value := range item {
			kind, text := configurationAttribute(value)
			out[name] = map[string]string{"type": kind, "value": text}
		}
		return out
	}
	physical := make(map[string]any)
	for table, item := range items {
		physical[table] = attributes(item)
	}
	var requestRows []any
	for _, request := range requests {
		row := map[string]any{"api": request.API}
		switch input := request.Input.(type) {
		case *awsdynamodb.BatchGetItemInput:
			row["keys"] = configurationRequestKeys(t, cfg, input.RequestItems, true)
			row["consistent_read_all_tables"] = true
		case *awsdynamodb.TransactWriteItemsInput:
			var puts []any
			for _, action := range input.TransactItems {
				puts = append(puts, map[string]any{"table": configurationTableAliases(cfg)[aws.ToString(action.Put.TableName)], "condition": aws.ToString(action.Put.ConditionExpression), "attributes": attributes(action.Put.Item)})
			}
			row["puts"] = puts
		}
		requestRows = append(requestRows, row)
	}
	var responses []any
	for _, out := range results.reads {
		if out == nil {
			responses = append(responses, nil)
			continue
		}
		processed := make(map[string]any)
		for table, items := range out.Responses {
			var rows []any
			for _, item := range items {
				rows = append(rows, attributes(item))
			}
			processed[configurationTableAliases(cfg)[table]] = rows
		}
		responses = append(responses, map[string]any{"responses": processed, "unprocessed_keys": configurationRequestKeys(t, cfg, out.UnprocessedKeys, false)})
	}
	var fired []int
	for _, fault := range injection.Faults {
		fired = append(fired, fault.Fired())
	}
	var causes []string
	for _, err := range results.createErrors {
		if err == nil {
			causes = append(causes, "success")
		} else {
			causes = append(causes, fmt.Sprintf("%T: %v", err, err))
		}
	}
	raw, err := json.Marshal(map[string]any{"requests": requestRows, "sdk_responses": responses, "items": physical, "waits": waits, "fault_fired": fired, "create_results": causes})
	require.NoError(t, err)
	t.Log(string(raw))
}

func TestConfigurationLocal(t *testing.T) {
	e := configurationEnvironment(t)
	data, err := conformance.LoadData("../conformance")
	require.NoError(t, err)
	matched := 0
	for _, scenario := range data.Scenarios {
		if scenario.File != "dynamodb/configuration.json" {
			continue
		}
		matched++
		t.Run(scenario.ID, func(t *testing.T) {
			tables, cfg := configurationTables(t, e, false)
			plan := scenario.Plan
			require.NoError(t, tables.PutConfigurationItems(context.Background(), plan.Seed))
			before := observedConfiguration(t, tables)
			if plan.Store.RetryLimit != nil {
				cfg.ConfigurationReadRetryLimit = aws.Int(int(*plan.Store.RetryLimit))
			}
			injection, finish := conformance.NewInitializationInjection(plan.Faults)
			var waits []time.Duration
			injection.Hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
			recorder := dynamodbtest.NewRecorder()
			results := &configurationResults{}
			client := e.NewClient(recorder.APIOption, tables.ConfigurationAPIOption(injection), results.apiOption)
			state, openErr := open(dynamodbtest.WithOperation(context.Background(), 0), client, cfg, injection.Hooks, configurationOptions(t, plan.Store)...)
			items := observedConfiguration(t, tables)
			requests := recorder.Requests(0)
			logConfiguration(t, cfg, requests, items, results, waits, injection)
			require.NoError(t, finish())
			if plan.Init.Error == nil {
				require.NoError(t, openErr)
				require.NotNil(t, state)
				require.Len(t, items, 3)
				for _, item := range items {
					require.Equal(t, &types.AttributeValueMemberS{Value: state.storeID}, item["store_id"])
				}
				if len(before) == 3 {
					require.Equal(t, before, items)
				}
			} else {
				require.Nil(t, state)
				kind := eventstore.KindConfiguration
				if plan.Init.Error.Category == "storage" {
					kind = eventstore.KindStorage
				}
				requireKind(t, openErr, kind)
				for _, text := range plan.Init.Error.MustContain {
					require.Contains(t, openErr.Error(), text)
				}
				for _, text := range plan.Init.Error.MustNotContain {
					require.NotContains(t, openErr.Error(), text)
				}
				require.Equal(t, before, items)
			}
			initialization := scenario.Materialized["initialization"].(map[string]any)
			observe := initialization["observe"].(map[string]any)
			compareConfigurationRequests(t, cfg, observe, requests, results, waits)
			if expected, ok := observe["items"].([]any); ok {
				bindings := map[string]string{}
				compareConfigurationItems(t, expected, items, bindings)
				if id, ok := bindings["generated-store-id"]; ok {
					require.Equal(t, state.storeID, id)
				}
			}
		})
	}
	require.Equal(t, 14, matched)
}

func configurationReadFault(keys []string, count int) conformance.FaultSpec {
	var selectors []any
	for _, key := range keys {
		selectors = append(selectors, key)
	}
	return conformance.FaultSpec{Operation: 0, Phase: "configuration-read", Kind: "sdk-response", Repeat: "count", Count: count, Injection: "replace-response", Details: map[string]any{"unprocessed_keys": selectors}}
}

func configurationCreateFault(code, table string, install []map[string]any) conformance.FaultSpec {
	details := map[string]any{"code": "TransactionCanceledException", "cancellation_reasons": []any{map[string]any{"target": "configuration:" + table, "code": code}}}
	if install != nil {
		var fixtures []any
		for _, item := range install {
			fixtures = append(fixtures, item)
		}
		details["install_items"] = fixtures
	}
	return conformance.FaultSpec{Operation: 0, Phase: "configuration-create", Kind: "sdk-error", Repeat: "count", Count: 1, Injection: "replace-request", Details: details}
}

// The original Faults still own application counts. Only the SDK wiring selects
// which declared read faults are eligible before or after the create attempt.
func configurationRereadFaults(tables *dynamodbtest.Tables, injection conformance.Injection, initialReadFaults int) func(*middleware.Stack) error {
	var created atomic.Bool
	return func(stack *middleware.Stack) error {
		selected := injection
		selected.Faults = nil
		for i, fault := range injection.Faults {
			if fault.Spec.Phase == "configuration-create" || (i <= initialReadFaults) != created.Load() {
				selected.Faults = append(selected.Faults, fault)
			}
		}
		if err := tables.ConfigurationAPIOption(selected)(stack); err != nil {
			return err
		}
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("configuration-create-attempt", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			if _, ok := in.Parameters.(*awsdynamodb.TransactWriteItemsInput); ok {
				created.Store(true)
			}
			return next.HandleInitialize(ctx, in)
		}), middleware.Before)
	}
}

func TestConfigurationLocalRetries(t *testing.T) {
	e := configurationEnvironment(t)
	allKeys := []string{"journal:__config__:0", "snapshot:__config__:0", "head:__config__"}
	for _, tc := range []struct {
		name     string
		limit    *int
		requests int
	}{
		{"default ten retries", nil, 11}, {"zero retries", aws.Int(0), 1}, {"explicit two retries", aws.Int(2), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, cfg := configurationTables(t, e, false)
			cfg.ConfigurationReadRetryLimit = tc.limit
			fault := configurationReadFault(allKeys, 0)
			fault.Repeat = "until-operation-finishes"
			injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{fault})
			var waits []time.Duration
			injection.Hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
			recorder := dynamodbtest.NewRecorder()
			results := &configurationResults{}
			state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption, tables.ConfigurationAPIOption(injection), results.apiOption), cfg, injection.Hooks)
			require.Nil(t, state)
			requireKind(t, err, eventstore.KindStorage)
			require.Contains(t, errors.Unwrap(err).Error(), "retry limit")
			require.Len(t, recorder.Requests(0), tc.requests)
			require.Equal(t, tc.requests, injection.Faults[0].Fired())
			require.Empty(t, observedConfiguration(t, tables))
			compareConfigurationRequests(t, cfg, nil, recorder.Requests(0), results, waits)
			logConfiguration(t, cfg, recorder.Requests(0), observedConfiguration(t, tables), results, waits, injection)
			require.NoError(t, finish())
		})
	}
	t.Run("multiple partial responses accumulate", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		require.NoError(t, tables.PutConfigurationItems(context.Background(), configurationSeed("accumulated-id")))
		injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{
			configurationReadFault([]string{"snapshot:__config__:0", "head:__config__"}, 1),
			configurationReadFault([]string{"head:__config__"}, 1),
		})
		var waits []time.Duration
		injection.Hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
		recorder := dynamodbtest.NewRecorder()
		results := &configurationResults{}
		state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption, tables.ConfigurationAPIOption(injection), results.apiOption), cfg, injection.Hooks)
		require.NoError(t, err)
		require.Equal(t, "accumulated-id", state.storeID)
		require.Len(t, recorder.Requests(0), 3)
		require.Len(t, results.reads[0].Responses[cfg.JournalTableName], 1)
		require.Empty(t, results.reads[0].Responses[cfg.SnapshotTableName])
		require.Empty(t, results.reads[0].Responses[cfg.HeadTableName])
		require.Len(t, results.reads[1].Responses[cfg.SnapshotTableName], 1)
		require.Len(t, results.reads[2].Responses[cfg.HeadTableName], 1)
		compareConfigurationRequests(t, cfg, nil, recorder.Requests(0), results, waits)
		logConfiguration(t, cfg, recorder.Requests(0), observedConfiguration(t, tables), results, waits, injection)
		require.NoError(t, finish())
	})
	t.Run("absence is decided only after all keys are processed", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{configurationReadFault(allKeys, 1)})
		var waits []time.Duration
		injection.Hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
		recorder := dynamodbtest.NewRecorder()
		results := &configurationResults{}
		state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption, tables.ConfigurationAPIOption(injection), results.apiOption), cfg, injection.Hooks)
		require.NoError(t, err)
		require.NotEmpty(t, state.storeID)
		require.Len(t, recorder.Requests(0), 3)
		require.Equal(t, "TransactWriteItems", recorder.Requests(0)[2].API)
		compareConfigurationRequests(t, cfg, nil, recorder.Requests(0), results, waits)
		logConfiguration(t, cfg, recorder.Requests(0), observedConfiguration(t, tables), results, waits, injection)
		require.NoError(t, finish())
	})
}

func TestConfigurationLocalReread(t *testing.T) {
	e := configurationEnvironment(t)
	allKeys := []string{"journal:__config__:0", "snapshot:__config__:0", "head:__config__"}
	for _, tc := range []struct {
		name, code                       string
		install                          []map[string]any
		initialCount, rereadCount, limit int
		kind                             eventstore.Kind
	}{
		{name: "TransactionConflict finds winner with a fresh retry budget", code: "TransactionConflict", install: configurationSeed("winner"), initialCount: 2, rereadCount: 2, limit: 2},
		{name: "TransactionConflict reread reaches limit", code: "TransactionConflict", install: configurationSeed("winner"), rereadCount: 2, limit: 1, kind: eventstore.KindStorage},
		{name: "TransactionConflict reread all absent", code: "TransactionConflict", limit: 2, kind: eventstore.KindStorage},
		{name: "condition failure reread all absent", code: "ConditionalCheckFailed", limit: 2, kind: eventstore.KindStorage},
		{name: "condition failure reread partial", code: "ConditionalCheckFailed", install: configurationSeed("winner")[:1], limit: 2, kind: eventstore.KindConfiguration},
		{name: "condition failure reread zero retries", code: "ConditionalCheckFailed", install: configurationSeed("winner"), rereadCount: 1, limit: 0, kind: eventstore.KindStorage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, cfg := configurationTables(t, e, false)
			cfg.ConfigurationReadRetryLimit = aws.Int(tc.limit)
			specs := []conformance.FaultSpec{configurationCreateFault(tc.code, "head", tc.install)}
			initialFaults := 0
			if tc.initialCount > 0 {
				specs = append(specs, configurationReadFault(allKeys, tc.initialCount))
				initialFaults++
			}
			if tc.rereadCount > 0 {
				specs = append(specs, configurationReadFault(allKeys, tc.rereadCount))
			}
			injection, finish := conformance.NewInitializationInjection(specs)
			var waits []time.Duration
			injection.Hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
			recorder := dynamodbtest.NewRecorder()
			results := &configurationResults{}
			state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption, configurationRereadFaults(tables, injection, initialFaults), results.apiOption), cfg, injection.Hooks)
			if tc.kind == 0 {
				require.NoError(t, err)
				require.Equal(t, "winner", state.storeID)
				require.Equal(t, []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}, waits)
			} else {
				require.Nil(t, state)
				requireKind(t, err, tc.kind)
				if tc.install == nil {
					var canceled *types.TransactionCanceledException
					require.ErrorAs(t, err, &canceled)
					require.ErrorIs(t, err, results.createErrors[0])
				}
			}
			require.Len(t, results.createErrors, 1, "never recreate after a race")
			var canceled *types.TransactionCanceledException
			require.ErrorAs(t, results.createErrors[0], &canceled)
			for _, request := range recorder.Requests(0) {
				if tx, ok := request.Input.(*awsdynamodb.TransactWriteItemsInput); ok {
					for i, action := range tx.TransactItems {
						if aws.ToString(action.Put.TableName) == cfg.HeadTableName {
							require.Equal(t, tc.code, aws.ToString(canceled.CancellationReasons[i].Code))
						}
					}
				}
			}
			compareConfigurationRequests(t, cfg, nil, recorder.Requests(0), results, waits)
			logConfiguration(t, cfg, recorder.Requests(0), observedConfiguration(t, tables), results, waits, injection)
			require.NoError(t, finish())
		})
	}
}

func TestConfigurationLocalFreezesValidatedInputs(t *testing.T) {
	e := configurationEnvironment(t)
	tables, cfg := configurationTables(t, e, true)
	retry, count := 2, 2
	cfg.ConfigurationReadRetryLimit = &retry
	original := cfg
	var optionInput *storeoptions.Options
	optionCalls, oldHandlerCalls, newHandlerCalls := 0, 0, 0
	option := eventstore.Option(func(o *storeoptions.Options) error {
		optionCalls++
		o.RetentionCount = &count
		o.RetentionMode = storeoptions.RetentionTTL
		o.TTLGraceSeconds = 7
		o.RetentionFailureHandler = func(context.Context, error) { oldHandlerCalls++ }
		optionInput = o
		return nil
	})
	injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{
		configurationCreateFault("TransactionConflict", "snapshot", configurationSeed("fixed-winner")),
		configurationReadFault([]string{"journal:__config__:0", "snapshot:__config__:0", "head:__config__"}, 2),
		configurationReadFault([]string{"head:__config__"}, 1),
	})
	var waits []time.Duration
	injection.Hooks.SetSleeper(func(d time.Duration) { waits = append(waits, d) })
	mutated := false
	mutate := func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("mutate-validated-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			if !mutated {
				mutated = true
				cfg.JournalTableName, cfg.SnapshotTableName, cfg.HeadTableName, cfg.SnapshotHistoryIndexName = "", "", "", ""
				retry, count = -1, 0
				optionInput.RetentionMode, optionInput.TTLGraceSeconds = 99, -1
				optionInput.RetentionFailureHandler = func(context.Context, error) { newHandlerCalls++ }
			}
			return next.HandleInitialize(ctx, in)
		}), middleware.Before)
	}
	recorder := dynamodbtest.NewRecorder()
	results := &configurationResults{}
	client := e.NewClient(recorder.APIOption, configurationRereadFaults(tables, injection, 1), results.apiOption, mutate)
	state, err := open(dynamodbtest.WithOperation(context.Background(), 0), client, cfg, injection.Hooks, option)
	require.NoError(t, err)
	require.True(t, mutated)
	require.Equal(t, 1, optionCalls)
	require.Same(t, client, state.client)
	require.Equal(t, "fixed-winner", state.storeID)
	require.Equal(t, original.JournalTableName, state.settings.journalTableName)
	require.Equal(t, original.SnapshotTableName, state.settings.snapshotTableName)
	require.Equal(t, original.HeadTableName, state.settings.headTableName)
	require.Equal(t, original.SnapshotHistoryIndexName, state.settings.snapshotHistoryIndexName)
	require.Equal(t, 2, state.settings.configurationReadRetryLimit)
	require.Equal(t, 2, *state.settings.common.RetentionCount)
	require.Equal(t, storeoptions.RetentionTTL, state.settings.common.RetentionMode)
	require.Equal(t, int64(7), state.settings.common.TTLGraceSeconds)
	require.Zero(t, oldHandlerCalls)
	require.Zero(t, newHandlerCalls)
	state.settings.common.RetentionFailureHandler(context.Background(), nil)
	require.Equal(t, 1, oldHandlerCalls)
	require.Zero(t, newHandlerCalls)
	compareConfigurationRequests(t, original, nil, recorder.Requests(0), results, waits)
	logConfiguration(t, original, recorder.Requests(0), observedConfiguration(t, tables), results, waits, injection)
	require.NoError(t, finish())
}

func TestConfigurationLocalStoredMetadata(t *testing.T) {
	e := configurationEnvironment(t)
	for _, table := range []string{"journal", "snapshot", "head"} {
		for _, tc := range []struct {
			name, attribute string
			value           types.AttributeValue
		}{
			{"store id type", "store_id", &types.AttributeValueMemberN{Value: "1"}},
			{"missing store id", "store_id", nil},
			{"version type", "layout_version", &types.AttributeValueMemberS{Value: "1"}},
			{"missing version", "layout_version", nil},
			{"fractional version", "layout_version", &types.AttributeValueMemberN{Value: "1.5"}},
		} {
			t.Run(table+"/"+tc.name, func(t *testing.T) {
				tables, cfg := configurationTables(t, e, false)
				require.NoError(t, tables.PutConfigurationItems(context.Background(), configurationSeed("typed-seed")))
				item := observedConfiguration(t, tables)[table]
				if tc.value == nil {
					delete(item, tc.attribute)
				} else {
					item[tc.attribute] = tc.value
				}
				name, err := tables.TableName(dynamodbtest.Table(table))
				require.NoError(t, err)
				_, err = e.NewClient().PutItem(context.Background(), &awsdynamodb.PutItemInput{TableName: aws.String(name), Item: item})
				require.NoError(t, err)
				before := observedConfiguration(t, tables)
				recorder := dynamodbtest.NewRecorder()
				results := &configurationResults{}
				state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption, results.apiOption), cfg, nil)
				require.Nil(t, state)
				requireKind(t, err, eventstore.KindConfiguration)
				require.Len(t, recorder.Requests(0), 1)
				require.Equal(t, before, observedConfiguration(t, tables))
				compareConfigurationRequests(t, cfg, nil, recorder.Requests(0), results, nil)
				logConfiguration(t, cfg, recorder.Requests(0), before, results, nil, conformance.Injection{})
			})
		}
	}
}

func TestConfigurationLocalSDKCauses(t *testing.T) {
	e := configurationEnvironment(t)
	t.Run("actual read service failure", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		cfg.JournalTableName = "missing-" + cfg.JournalTableName
		recorder := dynamodbtest.NewRecorder()
		state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption), cfg, nil)
		require.Nil(t, state)
		requireKind(t, err, eventstore.KindStorage)
		var missing *types.ResourceNotFoundException
		var operation *smithy.OperationError
		require.ErrorAs(t, err, &missing)
		require.ErrorAs(t, err, &operation)
		require.Equal(t, "BatchGetItem", operation.OperationName)
		require.ErrorIs(t, err, missing)
		require.Len(t, recorder.Requests(0), 1)
		require.Empty(t, observedConfiguration(t, tables))
		t.Logf("actual SDK cause: operation=%s code=%s type=%T", operation.OperationName, missing.ErrorCode(), missing)
	})
	t.Run("actual create service failure", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		removeHead := func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("remove-table-before-create", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				if _, ok := in.Parameters.(*awsdynamodb.TransactWriteItemsInput); ok {
					if _, err := e.NewClient().DeleteTable(ctx, &awsdynamodb.DeleteTableInput{TableName: aws.String(cfg.HeadTableName)}); err != nil {
						return middleware.InitializeOutput{}, middleware.Metadata{}, err
					}
				}
				return next.HandleInitialize(ctx, in)
			}), middleware.Before)
		}
		recorder := dynamodbtest.NewRecorder()
		state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption, removeHead), cfg, nil)
		require.Nil(t, state)
		requireKind(t, err, eventstore.KindStorage)
		var missing *types.ResourceNotFoundException
		var operation *smithy.OperationError
		require.ErrorAs(t, err, &missing)
		require.ErrorAs(t, err, &operation)
		require.Equal(t, "TransactWriteItems", operation.OperationName)
		require.ErrorIs(t, err, missing)
		require.Len(t, recorder.Requests(0), 2)
		for _, table := range []dynamodbtest.Table{"journal", "snapshot"} {
			item, err := tables.GetItem(context.Background(), table, configurationObservationKey(string(table)))
			require.NoError(t, err)
			require.Empty(t, item, "failed transaction must not create a partial configuration")
		}
		t.Logf("actual SDK cause: operation=%s code=%s type=%T", operation.OperationName, missing.ErrorCode(), missing)
	})
	t.Run("read re-request preserves actual SDK failure", func(t *testing.T) {
		tables, cfg := configurationTables(t, e, false)
		injection, finish := conformance.NewInitializationInjection([]conformance.FaultSpec{configurationReadFault([]string{"journal:__config__:0", "snapshot:__config__:0", "head:__config__"}, 1)})
		injection.Hooks.SetSleeper(func(time.Duration) {
			_, err := e.NewClient().DeleteTable(context.Background(), &awsdynamodb.DeleteTableInput{TableName: aws.String(cfg.JournalTableName)})
			require.NoError(t, err)
		})
		recorder := dynamodbtest.NewRecorder()
		state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption, tables.ConfigurationAPIOption(injection)), cfg, injection.Hooks)
		require.Nil(t, state)
		requireKind(t, err, eventstore.KindStorage)
		var missing *types.ResourceNotFoundException
		require.ErrorAs(t, err, &missing)
		require.ErrorIs(t, err, missing)
		require.Len(t, recorder.Requests(0), 2)
		require.NoError(t, finish())
		t.Logf("actual SDK re-request cause: code=%s type=%T", missing.ErrorCode(), missing)
	})
}

func TestConfigurationLocalConcurrentOpenAndReopen(t *testing.T) {
	e := configurationEnvironment(t)
	tables, cfg := configurationTables(t, e, false)
	ctx, cancel := context.WithTimeout(dynamodbtest.WithOperation(context.Background(), 0), time.Minute)
	defer cancel()
	t.Cleanup(cancel)
	type readyResult struct {
		index  int
		absent bool
	}
	ready := make(chan readyResult, 2)
	type openResult struct {
		state *opened
		err   error
	}
	completed := [2]chan openResult{make(chan openResult, 1), make(chan openResult, 1)}
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	recorders := [2]*dynamodbtest.Recorder{dynamodbtest.NewRecorder(), dynamodbtest.NewRecorder()}
	results := [2]*configurationResults{{}, {}}
	for index := 0; index < 2; index++ {
		firstRead := true
		gate := func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("hold-first-empty-read", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				out, metadata, err := next.HandleInitialize(ctx, in)
				if _, ok := in.Parameters.(*awsdynamodb.BatchGetItemInput); ok && firstRead {
					firstRead = false
					actual, ok := out.Result.(*awsdynamodb.BatchGetItemOutput)
					absent := err == nil && ok && len(actual.UnprocessedKeys) == 0
					if ok {
						for _, items := range actual.Responses {
							absent = absent && len(items) == 0
						}
					}
					ready <- readyResult{index: index, absent: absent}
					select {
					case <-release[index]:
					case <-ctx.Done():
						return out, metadata, ctx.Err()
					}
				}
				return out, metadata, err
			}), middleware.Before)
		}
		client := e.NewClient(recorders[index].APIOption, results[index].apiOption, gate)
		go func() { state, err := open(ctx, client, cfg, nil); completed[index] <- openResult{state, err} }()
	}
	seen := map[int]bool{}
	for i := 0; i < 2; i++ {
		select {
		case result := <-ready:
			require.True(t, result.absent)
			seen[result.index] = true
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.Len(t, seen, 2)
	for _, recorder := range recorders {
		require.Len(t, recorder.Requests(0), 1)
		require.Len(t, configurationRequestKeys(t, cfg, recorder.Requests(0)[0].Input.(*awsdynamodb.BatchGetItemInput).RequestItems, true), 3)
	}
	close(release[0])
	var winner, loser openResult
	select {
	case winner = <-completed[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, winner.err)
	close(release[1])
	select {
	case loser = <-completed[1]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, loser.err)
	require.Equal(t, winner.state.storeID, loser.state.storeID)
	require.Len(t, recorders[0].Requests(0), 2)
	require.Len(t, recorders[1].Requests(0), 3)
	var canceled *types.TransactionCanceledException
	require.Len(t, results[1].createErrors, 1)
	require.ErrorAs(t, results[1].createErrors[0], &canceled, "observe the real Local cancellation")
	conditionFailed := false
	for _, reason := range canceled.CancellationReasons {
		conditionFailed = conditionFailed || aws.ToString(reason.Code) == "ConditionalCheckFailed"
	}
	require.True(t, conditionFailed)
	loserPut := recorders[1].Requests(0)[1].Input.(*awsdynamodb.TransactWriteItemsInput).TransactItems[0].Put
	require.NotEqual(t, winner.state.storeID, loserPut.Item["store_id"].(*types.AttributeValueMemberS).Value)
	before := observedConfiguration(t, tables)
	for _, item := range before {
		require.Equal(t, &types.AttributeValueMemberS{Value: winner.state.storeID}, item["store_id"])
	}
	for i := 0; i < 2; i++ {
		compareConfigurationRequests(t, cfg, nil, recorders[i].Requests(0), results[i], nil)
		logConfiguration(t, cfg, recorders[i].Requests(0), before, results[i], nil, conformance.Injection{})
	}
	recorder := dynamodbtest.NewRecorder()
	state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption), cfg, nil)
	require.NoError(t, err)
	require.Equal(t, winner.state.storeID, state.storeID)
	require.Len(t, recorder.Requests(0), 1)
	require.Equal(t, "BatchGetItem", recorder.Requests(0)[0].API)
	require.Equal(t, before, observedConfiguration(t, tables))
}

func TestConfigurationLocalLayoutConfigurationParts(t *testing.T) {
	e := configurationEnvironment(t)
	data, err := conformance.LoadData("../conformance")
	require.NoError(t, err)
	require.Len(t, data.Layouts, 1)
	layoutCase := data.Layouts[0]
	for _, ttl := range []bool{false, true} {
		t.Run(fmt.Sprintf("ttl=%t", ttl), func(t *testing.T) {
			tables, cfg := configurationTables(t, e, ttl)
			require.NoError(t, tables.PutConfigurationItems(context.Background(), configurationSeed("store-a")))
			recorder := dynamodbtest.NewRecorder()
			state, err := open(dynamodbtest.WithOperation(context.Background(), 0), e.NewClient(recorder.APIOption), cfg, nil)
			require.NoError(t, err)
			require.Equal(t, "store-a", state.storeID)
			layout, err := tables.Describe(context.Background())
			require.NoError(t, err)
			for _, raw := range layoutCase.Raw["tables"].([]any) {
				expected := raw.(map[string]any)
				table := expected["name"].(string)
				actual := layout[dynamodbtest.Table(table)]
				wantKeys := []types.KeySchemaElement{{AttributeName: aws.String("aid"), KeyType: types.KeyTypeHash}}
				wantDefinitions := map[string]types.ScalarAttributeType{"aid": types.ScalarAttributeTypeS}
				if sortKey, ok := expected["sort_key"].(map[string]any); ok {
					wantKeys = append(wantKeys, types.KeySchemaElement{AttributeName: aws.String(sortKey["name"].(string)), KeyType: types.KeyTypeRange})
					wantDefinitions[sortKey["name"].(string)] = types.ScalarAttributeTypeN
				}
				require.ElementsMatch(t, wantKeys, actual.Description.KeySchema)
				require.Equal(t, table, configurationTableAliases(cfg)[aws.ToString(actual.Description.TableName)])
				require.Len(t, actual.Description.GlobalSecondaryIndexes, len(expected["gsi"].([]any)))
				for _, rawIndex := range expected["gsi"].([]any) {
					index := rawIndex.(map[string]any)
					gsi := actual.Description.GlobalSecondaryIndexes[0]
					require.Equal(t, cfg.SnapshotHistoryIndexName, aws.ToString(gsi.IndexName))
					require.Equal(t, index["projection"], string(gsi.Projection.ProjectionType))
					require.Empty(t, gsi.Projection.NonKeyAttributes)
					sortName := index["sort_key"].(map[string]any)["name"].(string)
					require.ElementsMatch(t, []types.KeySchemaElement{{AttributeName: aws.String("aid"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String(sortName), KeyType: types.KeyTypeRange}}, gsi.KeySchema)
					wantDefinitions[sortName] = types.ScalarAttributeTypeN
				}
				definitions := make(map[string]types.ScalarAttributeType)
				for _, definition := range actual.Description.AttributeDefinitions {
					definitions[aws.ToString(definition.AttributeName)] = definition.AttributeType
				}
				require.Equal(t, wantDefinitions, definitions)
				streams := expected["streams"].(map[string]any)
				if streams["enabled"].(bool) {
					require.True(t, aws.ToBool(actual.Description.StreamSpecification.StreamEnabled))
					require.Equal(t, streams["view_type"], string(actual.Description.StreamSpecification.StreamViewType))
				} else if actual.Description.StreamSpecification != nil {
					require.False(t, aws.ToBool(actual.Description.StreamSpecification.StreamEnabled))
				}
				if table == "snapshot" && ttl {
					require.Equal(t, types.TimeToLiveStatusEnabled, actual.TTL.TimeToLiveStatus)
					require.Equal(t, "ttl", aws.ToString(actual.TTL.AttributeName))
				} else {
					require.Equal(t, types.TimeToLiveStatusDisabled, actual.TTL.TimeToLiveStatus)
				}
			}
			var expectedItems []any
			for _, raw := range layoutCase.Raw["items"].([]any) {
				if raw.(map[string]any)["values"].(map[string]any)["aid"] == "__config__" {
					expectedItems = append(expectedItems, raw)
				}
			}
			require.Len(t, expectedItems, 3)
			compareConfigurationItems(t, expectedItems, observedConfiguration(t, tables), nil)
			actual, err := json.Marshal(layout)
			require.NoError(t, err)
			t.Logf("layout tables and configuration items observed: %s; product item shapes remain unverified", actual)
		})
	}
}
