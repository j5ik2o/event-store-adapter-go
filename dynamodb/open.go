package dynamodb

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v2"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/testhook"
)

// opened is the confirmed configuration, not a four-operation EventStore.
// The product operations and public New are connected in the integration work.
type opened struct {
	client   *awsdynamodb.Client
	settings settings
	storeID  string
}

func open(ctx context.Context, client *awsdynamodb.Client, cfg Config, hooks *testhook.Hooks, opts ...eventstore.Option) (*opened, error) {
	validated, err := validateConfig(client, cfg, opts...)
	if err != nil {
		return nil, err
	}
	items, err := readConfiguration(ctx, client, validated, hooks)
	if err != nil {
		return nil, err
	}
	storeID, exists, err := matchConfiguration(validated, items)
	if err != nil {
		return nil, err
	}
	if !exists {
		var randomID [16]byte
		if _, err := rand.Read(randomID[:]); err != nil {
			return nil, &eventstore.StorageError{Cause: err}
		}
		storeID = hex.EncodeToString(randomID[:])
		_, createErr := client.TransactWriteItems(ctx, configurationWrite(validated, storeID))
		if createErr != nil {
			if !configurationCreateRace(createErr) {
				return nil, &eventstore.StorageError{Cause: createErr}
			}
			// Start a new accumulation and retry budget for all three keys.
			items, err = readConfiguration(ctx, client, validated, hooks)
			if err != nil {
				return nil, err
			}
			storeID, exists, err = matchConfiguration(validated, items)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, &eventstore.StorageError{Cause: createErr}
			}
		}
	}
	return &opened{client: client, settings: validated, storeID: storeID}, nil
}
