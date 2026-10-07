package dynamodbtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Table string

// Tables owns only the resources successfully created for one scenario.
type Tables struct {
	client *dynamodb.Client
	names  map[Table]string
	index  string
	mu     sync.Mutex
	owned  []string
}

func (e *Environment) CreateTables(ctx context.Context, ttl bool) (*Tables, error) {
	return e.createTables(ctx, e.NewClient(), ttl)
}

func (e *Environment) createTables(ctx context.Context, client *dynamodb.Client, ttl bool) (*Tables, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	prefix := "conformance-" + hex.EncodeToString(id[:])
	t := &Tables{client: client, names: map[Table]string{}, index: prefix + "-history"}
	readyCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	fail := func(err error) (*Tables, error) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		return nil, errors.Join(err, t.Close(cleanupCtx))
	}
	for _, table := range []Table{"journal", "snapshot", "head"} {
		name := prefix + "-" + string(table)
		t.names[table] = name
		input := &dynamodb.CreateTableInput{TableName: aws.String(name), BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("aid"), AttributeType: types.ScalarAttributeTypeS}},
			KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("aid"), KeyType: types.KeyTypeHash}}}
		var sortKey string
		switch table {
		case "journal":
			sortKey = "seq_nr"
		case "snapshot":
			sortKey = "skey"
		case "head":
			input.StreamSpecification = &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewImage}
		}
		if sortKey != "" {
			input.AttributeDefinitions = append(input.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String(sortKey), AttributeType: types.ScalarAttributeTypeN})
			input.KeySchema = append(input.KeySchema, types.KeySchemaElement{AttributeName: aws.String(sortKey), KeyType: types.KeyTypeRange})
		}
		if table == "snapshot" {
			input.AttributeDefinitions = append(input.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String("active_history_seq_nr"), AttributeType: types.ScalarAttributeTypeN})
			input.GlobalSecondaryIndexes = []types.GlobalSecondaryIndex{{IndexName: aws.String(t.index), KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("aid"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("active_history_seq_nr"), KeyType: types.KeyTypeRange}}, Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly}}}
		}
		if _, err := client.CreateTable(readyCtx, input); err != nil {
			return fail(err)
		}
		t.owned = append(t.owned, name)
		if err := poll(readyCtx, func() (bool, error) {
			out, err := client.DescribeTable(readyCtx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
			if err != nil {
				return false, err
			}
			if out.Table.TableStatus != types.TableStatusActive {
				return false, nil
			}
			for _, index := range out.Table.GlobalSecondaryIndexes {
				if index.IndexStatus != types.IndexStatusActive {
					return false, nil
				}
			}
			return true, nil
		}); err != nil {
			return fail(err)
		}
		if table == "snapshot" && ttl {
			if _, err := client.UpdateTimeToLive(readyCtx, &dynamodb.UpdateTimeToLiveInput{TableName: aws.String(name), TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("ttl"), Enabled: aws.Bool(true)}}); err != nil {
				return fail(err)
			}
		}
		if err := poll(readyCtx, func() (bool, error) {
			out, err := client.DescribeTimeToLive(readyCtx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(name)})
			if err != nil {
				return false, err
			}
			expected := types.TimeToLiveStatusDisabled
			if table == "snapshot" && ttl {
				expected = types.TimeToLiveStatusEnabled
			}
			return out.TimeToLiveDescription.TimeToLiveStatus == expected, nil
		}); err != nil {
			return fail(err)
		}
	}
	return t, nil
}

func poll(ctx context.Context, check func() (bool, error)) error {
	for {
		ready, err := check()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (t *Tables) TableName(table Table) (string, error) {
	name, ok := t.names[table]
	if !ok {
		return "", fmt.Errorf("unknown table %q", table)
	}
	return name, nil
}

func (t *Tables) Close(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var failures []error
	var remaining []string
	for _, name := range t.owned {
		_, err := t.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)})
		var missing *types.ResourceNotFoundException
		if errors.As(err, &missing) {
			continue
		}
		if err == nil {
			err = poll(ctx, func() (bool, error) {
				_, err := t.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
				if errors.As(err, &missing) {
					return true, nil
				}
				return false, err
			})
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("delete %s: %w", name, err))
			remaining = append(remaining, name)
		}
	}
	t.owned = remaining
	return errors.Join(failures...)
}
