package dynamodbtest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/require"
)

func tableNames(t *testing.T, client *dynamodb.Client) []string {
	t.Helper()
	p := dynamodb.NewListTablesPaginator(client, &dynamodb.ListTablesInput{})
	var names []string
	for p.HasMorePages() {
		out, err := p.NextPage(context.Background())
		require.NoError(t, err)
		names = append(names, out.TableNames...)
	}
	return names
}

func createTables(t *testing.T, e *Environment, ttl bool) (*Tables, map[string]string) {
	t.Helper()
	client := e.NewClient()
	before := tableNames(t, client)
	tables, err := e.CreateTables(context.Background(), ttl)
	require.NoError(t, err)
	names := map[string]string{}
	for _, name := range tableNames(t, client) {
		found := false
		for _, old := range before {
			if name == old {
				found = true
			}
		}
		if found {
			continue
		}
		out, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		require.NoError(t, err)
		kind := "head"
		for _, key := range out.Table.KeySchema {
			if key.KeyType == types.KeyTypeRange {
				if *key.AttributeName == "seq_nr" {
					kind = "journal"
				} else {
					kind = "snapshot"
				}
			}
		}
		require.NotContains(t, names, kind)
		names[kind] = name
	}
	require.Len(t, names, 3)
	t.Cleanup(func() {
		remaining := tableNames(t, client)
		for _, name := range names {
			for _, current := range remaining {
				if name == current {
					require.NoError(t, tables.Close(context.Background()))
					return
				}
			}
		}
	})
	return tables, names
}

func assertLayout(t *testing.T, e *Environment, names map[string]string, ttl bool) {
	t.Helper()
	for kind, name := range names {
		out, err := e.NewClient().DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		require.NoError(t, err)
		keys := map[string]types.KeyType{}
		attrs := map[string]types.ScalarAttributeType{}
		for _, k := range out.Table.KeySchema {
			keys[*k.AttributeName] = k.KeyType
		}
		for _, a := range out.Table.AttributeDefinitions {
			attrs[*a.AttributeName] = a.AttributeType
		}
		expectedKeys := map[string]types.KeyType{"aid": types.KeyTypeHash}
		expectedAttrs := map[string]types.ScalarAttributeType{"aid": types.ScalarAttributeTypeS}
		if kind == "journal" {
			expectedKeys["seq_nr"] = types.KeyTypeRange
			expectedAttrs["seq_nr"] = types.ScalarAttributeTypeN
		}
		if kind == "snapshot" {
			expectedKeys["skey"] = types.KeyTypeRange
			expectedAttrs["skey"] = types.ScalarAttributeTypeN
			expectedAttrs["active_history_seq_nr"] = types.ScalarAttributeTypeN
		}
		require.Equal(t, expectedKeys, keys)
		require.Equal(t, expectedAttrs, attrs)
		if kind == "snapshot" {
			require.Len(t, out.Table.GlobalSecondaryIndexes, 1)
			gsi := out.Table.GlobalSecondaryIndexes[0]
			require.Equal(t, types.ProjectionTypeKeysOnly, gsi.Projection.ProjectionType)
			require.ElementsMatch(t, []types.KeySchemaElement{{AttributeName: aws.String("aid"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("active_history_seq_nr"), KeyType: types.KeyTypeRange}}, gsi.KeySchema)
		} else {
			require.Empty(t, out.Table.GlobalSecondaryIndexes)
		}
		if kind == "head" {
			require.NotNil(t, out.Table.StreamSpecification)
			require.True(t, aws.ToBool(out.Table.StreamSpecification.StreamEnabled))
			require.Equal(t, types.StreamViewTypeNewImage, out.Table.StreamSpecification.StreamViewType)
		} else if out.Table.StreamSpecification != nil {
			require.False(t, aws.ToBool(out.Table.StreamSpecification.StreamEnabled))
		}
		ttlOut, err := e.NewClient().DescribeTimeToLive(context.Background(), &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(name)})
		require.NoError(t, err)
		if kind == "snapshot" && ttl {
			require.Equal(t, types.TimeToLiveStatusEnabled, ttlOut.TimeToLiveDescription.TimeToLiveStatus)
			require.Equal(t, "ttl", aws.ToString(ttlOut.TimeToLiveDescription.AttributeName))
		} else {
			require.Equal(t, types.TimeToLiveStatusDisabled, ttlOut.TimeToLiveDescription.TimeToLiveStatus)
		}
	}
}

func TestTableLayout(t *testing.T) {
	// Given TTL retention, When tables are created, Then the SDK describes the shared layout.
	e := startEnvironment(t)
	tables, names := createTables(t, e, true)
	assertLayout(t, e, names, true)
	layout, err := tables.Describe(context.Background())
	require.NoError(t, err)
	require.Len(t, layout, 3)
	for kind, name := range names {
		observed, ok := layout[Table(kind)]
		require.True(t, ok)
		description, err := e.NewClient().DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		require.NoError(t, err)
		ttl, err := e.NewClient().DescribeTimeToLive(context.Background(), &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(name)})
		require.NoError(t, err)
		require.Equal(t, description.Table, observed.Description)
		require.Equal(t, ttl.TimeToLiveDescription, observed.TTL)
	}
	// Only unmarked history is projected into the sparse index.
	for _, row := range []struct{ aid, skey, active, ttl string }{
		{"Order-1", "0", "", ""},
		{"Order-1", "3", "3", ""},
		{"Order-1", "2", "", "4102444860"},
		{"__config__", "0", "", ""},
	} {
		item := map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: row.aid}, "skey": &types.AttributeValueMemberN{Value: row.skey}, "payload": &types.AttributeValueMemberB{Value: []byte("not projected")}}
		if row.active != "" {
			item["active_history_seq_nr"] = &types.AttributeValueMemberN{Value: row.active}
		}
		if row.ttl != "" {
			item["ttl"] = &types.AttributeValueMemberN{Value: row.ttl}
		}
		_, err := e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(names["snapshot"]), Item: item})
		require.NoError(t, err)
	}
	index := layout[Table("snapshot")].Description.GlobalSecondaryIndexes[0].IndexName
	query := &dynamodb.QueryInput{TableName: aws.String(names["snapshot"]), IndexName: index, KeyConditionExpression: aws.String("aid = :aid"), ExpressionAttributeValues: map[string]types.AttributeValue{":aid": &types.AttributeValueMemberS{Value: "Order-1"}}}
	require.Eventually(t, func() bool {
		out, err := e.NewClient().Query(context.Background(), query)
		return err == nil && len(out.Items) == 1
	}, 5*time.Second, 20*time.Millisecond)
	out, err := e.NewClient().Query(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, []map[string]types.AttributeValue{{"aid": &types.AttributeValueMemberS{Value: "Order-1"}, "skey": &types.AttributeValueMemberN{Value: "3"}, "active_history_seq_nr": &types.AttributeValueMemberN{Value: "3"}}}, out.Items)
	query.ExpressionAttributeValues[":aid"] = &types.AttributeValueMemberS{Value: "__config__"}
	out, err = e.NewClient().Query(context.Background(), query)
	require.NoError(t, err)
	require.Empty(t, out.Items)
}

func TestTableLayoutDoesNotEnableUnrequestedTTL(t *testing.T) {
	// Given deletion retention, When tables are created, Then TTL and unrelated Streams remain disabled.
	e := startEnvironment(t)
	_, names := createTables(t, e, false)
	assertLayout(t, e, names, false)
}

func journalItem(payload string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"aid": &types.AttributeValueMemberS{Value: "Order-1"}, "seq_nr": &types.AttributeValueMemberN{Value: "1"}, "payload": &types.AttributeValueMemberB{Value: []byte(payload)}}
}

func putJournal(t *testing.T, e *Environment, name, payload string) {
	t.Helper()
	_, err := e.NewClient().PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: journalItem(payload)})
	require.NoError(t, err)
}

func readJournal(t *testing.T, e *Environment, name string) map[string]types.AttributeValue {
	t.Helper()
	key := journalItem("")
	delete(key, "payload")
	out, err := e.NewClient().GetItem(context.Background(), &dynamodb.GetItemInput{TableName: aws.String(name), Key: key, ConsistentRead: aws.Bool(true)})
	require.NoError(t, err)
	return out.Item
}

func TestResourceIsolationAndCleanup(t *testing.T) {
	// Given identical aggregate keys, When two groups are created concurrently, Then their writes remain separate.
	e := startEnvironment(t)
	var groups [2]*Tables
	var errs [2]error
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		go func(i int) { defer wg.Done(); groups[i], errs[i] = e.CreateTables(context.Background(), false) }(i)
	}
	wg.Wait()
	for i := range groups {
		require.NoError(t, errs[i])
		index := i
		t.Cleanup(func() {
			if groups[index] != nil {
				require.NoError(t, groups[index].Close(context.Background()))
			}
		})
	}
	key := journalItem("")
	delete(key, "payload")
	for _, name := range tableNames(t, e.NewClient()) {
		out, err := e.NewClient().DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		require.NoError(t, err)
		for _, k := range out.Table.KeySchema {
			if aws.ToString(k.AttributeName) == "seq_nr" {
				putJournal(t, e, name, name)
			}
		}
	}
	var journals [2]string
	for i := range groups {
		item, err := groups[i].GetItem(context.Background(), Table("journal"), key)
		require.NoError(t, err)
		payload, ok := item["payload"].(*types.AttributeValueMemberB)
		require.True(t, ok)
		journals[i] = string(payload.Value)
	}
	require.NotEqual(t, journals[0], journals[1])
	for i, payload := range []string{"A", "B"} {
		putJournal(t, e, journals[i], payload)
	}
	for i, payload := range []string{"A", "B"} {
		item, err := groups[i].GetItem(context.Background(), Table("journal"), key)
		require.NoError(t, err)
		require.Equal(t, journalItem(payload), item)
	}
	// Each group owns exactly three distinct resources.
	require.Len(t, tableNames(t, e.NewClient()), 6)
	require.NoError(t, groups[0].Close(context.Background()))
	groups[0] = nil
	require.Len(t, tableNames(t, e.NewClient()), 3)
}

func TestCleanupPreservesExistingAndOtherScenarioTables(t *testing.T) {
	// Given an existing journal and scenario A, When scenario B closes, Then both existing resources remain writable.
	e := startEnvironment(t)
	_, err := e.NewClient().CreateTable(context.Background(), &dynamodb.CreateTableInput{TableName: aws.String("journal"), BillingMode: types.BillingModePayPerRequest, AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("aid"), AttributeType: types.ScalarAttributeTypeS}, {AttributeName: aws.String("seq_nr"), AttributeType: types.ScalarAttributeTypeN}}, KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("aid"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("seq_nr"), KeyType: types.KeyTypeRange}}})
	require.NoError(t, err)
	_, a := createTables(t, e, false)
	putJournal(t, e, "journal", "existing")
	putJournal(t, e, a["journal"], "A")
	b, _ := createTables(t, e, false)
	require.NoError(t, b.Close(context.Background()))
	for name, payload := range map[string]string{"journal": "existing", a["journal"]: "A"} {
		require.Equal(t, journalItem(payload), readJournal(t, e, name))
		putJournal(t, e, name, payload+"-updated")
		require.Equal(t, journalItem(payload+"-updated"), readJournal(t, e, name))
	}
}

func TestTableCreationFailureCleansPartialResources(t *testing.T) {
	// Given a real first table, When the next creation fails, Then only preexisting tables remain.
	e := startEnvironment(t)
	_, existing := createTables(t, e, false)
	before := tableNames(t, e.NewClient())
	injected := errors.New("injected second CreateTable failure")
	creates := 0
	client := e.NewClient(func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("fail-second-create", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			if _, ok := in.Parameters.(*dynamodb.CreateTableInput); ok {
				creates++
				if creates == 2 {
					return middleware.InitializeOutput{}, middleware.Metadata{}, injected
				}
			}
			return next.HandleInitialize(ctx, in)
		}), middleware.Before)
	})
	_, err := e.createTables(context.Background(), client, false)
	require.ErrorIs(t, err, injected)
	require.Equal(t, 2, creates)
	require.ElementsMatch(t, before, tableNames(t, e.NewClient()))
	putJournal(t, e, existing["journal"], "survived")
	require.Equal(t, journalItem("survived"), readJournal(t, e, existing["journal"]))
}

func TestTableCreationResponseFailureCleansCreatedResources(t *testing.T) {
	e := startEnvironment(t)
	_, existing := createTables(t, e, false)
	before := tableNames(t, e.NewClient())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	injected := errors.New("lost successful CreateTable response")
	creates, deletes := 0, 0
	var createdName string
	client := e.NewClient(func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("lose-create-response", func(callCtx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			switch input := in.Parameters.(type) {
			case *dynamodb.CreateTableInput:
				creates++
				out, metadata, err := next.HandleInitialize(callCtx, in)
				if err == nil && creates == 2 {
					createdName = aws.ToString(input.TableName)
					_, inspectErr := e.NewClient().DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: input.TableName})
					require.NoError(t, inspectErr)
					cancel()
					return middleware.InitializeOutput{}, metadata, injected
				}
				return out, metadata, err
			case *dynamodb.DeleteTableInput:
				deletes++
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.NoError(t, callCtx.Err())
			}
			return next.HandleInitialize(callCtx, in)
		}), middleware.Before)
	})
	tables, err := e.createTables(ctx, client, false)
	require.Nil(t, tables)
	require.ErrorIs(t, err, injected)
	require.Equal(t, 2, creates)
	require.NotEmpty(t, createdName)
	require.NotContains(t, tableNames(t, e.NewClient()), createdName, "failed creation must remove the table whose response was lost")
	require.Equal(t, 2, deletes)
	require.ElementsMatch(t, before, tableNames(t, e.NewClient()))
	putJournal(t, e, existing["journal"], "survived")
	require.Equal(t, journalItem("survived"), readJournal(t, e, existing["journal"]))
}

func TestTableCreationConflictPreservesOtherOwnerTableAndData(t *testing.T) {
	e := startEnvironment(t)
	_, existing := createTables(t, e, false)
	before := tableNames(t, e.NewClient())
	other := e.NewClient()
	var otherName string
	client := e.NewClient(func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("create-before-request", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			if input, ok := in.Parameters.(*dynamodb.CreateTableInput); ok && otherName == "" {
				otherName = aws.ToString(input.TableName)
				_, err := other.CreateTable(context.Background(), input)
				require.NoError(t, err)
				putJournal(t, e, otherName, "other-owner")
			}
			return next.HandleInitialize(ctx, in)
		}), middleware.Before)
	})
	tables, err := e.createTables(context.Background(), client, false)
	var conflict *types.ResourceInUseException
	require.Nil(t, tables)
	require.ErrorAs(t, err, &conflict)
	require.NotEmpty(t, otherName)
	require.ElementsMatch(t, append(before, otherName), tableNames(t, other))
	require.Equal(t, journalItem("other-owner"), readJournal(t, e, otherName))
	putJournal(t, e, existing["journal"], "survived")
	require.Equal(t, journalItem("survived"), readJournal(t, e, existing["journal"]))
}

func TestTableCreationCancellationCleansPartialResources(t *testing.T) {
	// Given a first created table, When creation is canceled, Then cleanup uses an independent context.
	e := startEnvironment(t)
	before := tableNames(t, e.NewClient())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	creates := 0
	client := e.NewClient(func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("cancel-second-create", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			if _, ok := in.Parameters.(*dynamodb.CreateTableInput); ok {
				creates++
				if creates == 2 {
					cancel()
					return middleware.InitializeOutput{}, middleware.Metadata{}, context.Canceled
				}
			}
			return next.HandleInitialize(ctx, in)
		}), middleware.Before)
	})
	_, err := e.createTables(ctx, client, false)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, creates)
	require.ElementsMatch(t, before, tableNames(t, e.NewClient()))
}

func TestTableCreationCleanupFailureIsReportedAndContinues(t *testing.T) {
	// Given two created tables, When creation and one deletion fail, Then both errors return and other deletion proceeds.
	e := startEnvironment(t)
	creationError := errors.New("injected third CreateTable failure")
	deletionError := errors.New("injected DeleteTable failure")
	var created, deleted []string
	failedDelete := ""
	client := e.NewClient(func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("fail-create-and-delete", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			switch input := in.Parameters.(type) {
			case *dynamodb.CreateTableInput:
				if len(created) == 2 {
					return middleware.InitializeOutput{}, middleware.Metadata{}, creationError
				}
				out, metadata, err := next.HandleInitialize(ctx, in)
				if err == nil {
					created = append(created, aws.ToString(input.TableName))
				}
				return out, metadata, err
			case *dynamodb.DeleteTableInput:
				name := aws.ToString(input.TableName)
				deleted = append(deleted, name)
				if failedDelete == "" {
					failedDelete = name
				}
				if name == failedDelete {
					return middleware.InitializeOutput{}, middleware.Metadata{}, deletionError
				}
			}
			return next.HandleInitialize(ctx, in)
		}), middleware.Before)
	})
	_, err := e.createTables(context.Background(), client, false)
	require.ErrorIs(t, err, creationError)
	require.ErrorIs(t, err, deletionError)
	require.Len(t, created, 2)
	require.Contains(t, deleted, created[0])
	require.Contains(t, deleted, created[1])
	require.Equal(t, []string{failedDelete}, tableNames(t, e.NewClient()))
	_, err = e.NewClient().DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(failedDelete)})
	require.NoError(t, err)
}
