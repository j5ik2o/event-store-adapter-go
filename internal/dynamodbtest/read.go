package dynamodbtest

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
)

// ReadObservation separates the actual SDK response from the delivered response.
// Requests are recorded independently by Recorder, and Tables observes storage
// with a separate client. OriginalError retains the real SDK cause.
type ReadObservation struct {
	Operation     int
	API           string
	Original      any
	Received      any
	OriginalError error
	ReceivedError error
}

// ReadObserver runs only for matched BatchGetItem and Query calls. Before runs
// before the real SDK request; After receives a copy of the actual SDK response.
// Neither callback receives expectations. Results returns independent copies.
type ReadObserver struct {
	Match  func(any) bool
	Before func(context.Context, any) error
	After  func(context.Context, any, any) (any, error)
	mu     sync.Mutex
	reads  []ReadObservation
}

func (r *ReadObserver) APIOption(stack *middleware.Stack) error {
	return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("observe-dynamodb-read", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
		switch in.Parameters.(type) {
		case *dynamodb.BatchGetItemInput, *dynamodb.QueryInput:
		default:
			return next.HandleInitialize(ctx, in)
		}
		if r.Match != nil && !r.Match(in.Parameters) {
			return next.HandleInitialize(ctx, in)
		}
		input := cloneValue(reflect.ValueOf(in.Parameters)).Interface()
		if r.Before != nil {
			if err := r.Before(ctx, input); err != nil {
				return middleware.InitializeOutput{}, middleware.Metadata{}, err
			}
		}
		out, metadata, err := next.HandleInitialize(ctx, in)
		observation := ReadObservation{API: middleware.GetOperationName(ctx), OriginalError: err}
		observation.Operation, _ = ctx.Value(operationKey{}).(int)
		if out.Result != nil {
			observation.Original = copyReadResult(out.Result, metadata)
		}
		if err == nil && r.After != nil {
			if out.Result == nil {
				err = fmt.Errorf("read observer received no SDK result")
			} else {
				out.Result, err = r.After(ctx, input, copyReadResult(out.Result, metadata))
			}
		}
		if out.Result != nil {
			observation.Received = copyReadResult(out.Result, metadata)
		}
		observation.ReceivedError = err
		r.mu.Lock()
		r.reads = append(r.reads, observation)
		r.mu.Unlock()
		return out, metadata, err
	}), middleware.Before)
}

// SDK methods attach ResultMetadata after the middleware stack returns. Keep
// that same real metadata with the copies captured inside the stack as well.
func copyReadResult(result any, metadata middleware.Metadata) any {
	copy := cloneValue(reflect.ValueOf(result)).Interface()
	switch copy := copy.(type) {
	case *dynamodb.BatchGetItemOutput:
		copy.ResultMetadata = metadata.Clone()
	case *dynamodb.QueryOutput:
		copy.ResultMetadata = metadata.Clone()
	}
	return copy
}

func (r *ReadObserver) Results(operation int) []ReadObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	var results []ReadObservation
	for _, read := range r.reads {
		if read.Operation == operation {
			copy := read
			if read.Original != nil {
				copy.Original = cloneValue(reflect.ValueOf(read.Original)).Interface()
				switch out := copy.Original.(type) {
				case *dynamodb.BatchGetItemOutput:
					out.ResultMetadata = out.ResultMetadata.Clone()
				case *dynamodb.QueryOutput:
					out.ResultMetadata = out.ResultMetadata.Clone()
				}
			}
			if read.Received != nil {
				copy.Received = cloneValue(reflect.ValueOf(read.Received)).Interface()
				switch out := copy.Received.(type) {
				case *dynamodb.BatchGetItemOutput:
					out.ResultMetadata = out.ResultMetadata.Clone()
				case *dynamodb.QueryOutput:
					out.ResultMetadata = out.ResultMetadata.Clone()
				}
			}
			results = append(results, copy)
		}
	}
	return results
}

// DeferReadKeys withholds real response items and marks their real requested
// keys as unprocessed. It preserves unrelated responses, original unprocessed
// keys and SDK result metadata, and never modifies either input.
func DeferReadKeys(input *dynamodb.BatchGetItemInput, actual *dynamodb.BatchGetItemOutput, tables ...string) (*dynamodb.BatchGetItemOutput, error) {
	out := copyReadResult(actual, actual.ResultMetadata).(*dynamodb.BatchGetItemOutput)
	if out.UnprocessedKeys == nil {
		out.UnprocessedKeys = map[string]types.KeysAndAttributes{}
	}
	for _, table := range tables {
		request, ok := input.RequestItems[table]
		if !ok || len(request.Keys) == 0 {
			return nil, fmt.Errorf("deferred table %q has no requested keys", table)
		}
		pending, exists := out.UnprocessedKeys[table]
		if !exists {
			pending = cloneValue(reflect.ValueOf(request)).Interface().(types.KeysAndAttributes)
		} else {
			for _, key := range request.Keys {
				duplicate := false
				for _, existing := range pending.Keys {
					duplicate = duplicate || reflect.DeepEqual(existing, key)
				}
				if !duplicate {
					pending.Keys = append(pending.Keys, cloneValue(reflect.ValueOf(key)).Interface().(map[string]types.AttributeValue))
				}
			}
		}
		out.UnprocessedKeys[table] = pending
		delete(out.Responses, table)
	}
	return out, nil
}
