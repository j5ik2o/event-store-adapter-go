package dynamodbtest

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/j5ik2o/event-store-adapter-go/v2/internal/conformance"
)

type readInputKey struct{}

// ReadFailureAPIOption records requests before replacing declared failed reads.
// Successful response plans use ReadObserver and real response items instead.
func (t *Tables) ReadFailureAPIOption(injection conformance.Injection) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("read-failure-input", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			return next.HandleInitialize(context.WithValue(ctx, readInputKey{}, in.Parameters), in)
		}), middleware.Before); err != nil {
			return err
		}
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("read-failure", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
			phase := t.ReadPhase(ctx.Value(readInputKey{}))
			for _, fault := range injection.Faults {
				if phase == "" || fault.Spec.Phase != phase || fault.Spec.Kind != "storage-error" || !fault.CanApply() {
					continue
				}
				if fault.Spec.Injection != "replace-request" {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("unsupported read failure injection")
				}
				if fault.TryApply() {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, errors.New(fmt.Sprint(fault.Spec.Details["message"]))
				}
			}
			return next.HandleFinalize(ctx, in)
		}), middleware.Before)
	}
}

// ReadPhase identifies aggregate reads and excludes configuration reads.
func (t *Tables) ReadPhase(input any) string {
	switch in := input.(type) {
	case *dynamodb.QueryInput:
		if in.TableName != nil && *in.TableName == t.names["journal"] {
			return "read-events"
		}
	case *dynamodb.BatchGetItemInput:
		for _, request := range in.RequestItems {
			for _, key := range request.Keys {
				if aid, ok := key["aid"].(*types.AttributeValueMemberS); ok && aid.Value != "__config__" {
					return "read-snapshot"
				}
			}
		}
	}
	return ""
}
