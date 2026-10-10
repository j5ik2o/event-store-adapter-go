package dynamodbtest

import (
	"context"
	"errors"
	"reflect"
	"sync"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
)

// Response records the delivered result separately from the sent SDK input.
type Response struct {
	Operation int
	API       string
	Input     any
	Result    any
	Error     string
	// Cause preserves the real error identity in memory. SDK HTTP error wrappers
	// contain request functions; ServiceError is the serializable service cause.
	Cause        error `json:"-"`
	ServiceError any
	RequestID    string
	Original     *SDKResponse
}

// SDKResponse is captured inside fault middleware, only if the real SDK ran.
// A request replacement has no Original response.
type SDKResponse struct {
	Result       any
	Error        string
	Cause        error `json:"-"`
	ServiceError any
	RequestID    string
}

type responseCaptureKey struct{}
type responseCapture struct{ original *SDKResponse }

type ResponseRecorder struct {
	mu        sync.Mutex
	responses []Response
}

func (r *ResponseRecorder) APIOption(stack *middleware.Stack) error {
	if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc("record-dynamodb-response", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
		input := cloneValue(reflect.ValueOf(in.Parameters)).Interface()
		capture := &responseCapture{}
		ctx = context.WithValue(ctx, responseCaptureKey{}, capture)
		out, metadata, err := next.HandleInitialize(ctx, in)
		if operation, tagged := ctx.Value(operationKey{}).(int); tagged {
			response := Response{Operation: operation, API: middleware.GetOperationName(ctx), Input: input, Cause: err, ServiceError: serviceError(err), Original: capture.original}
			response.RequestID, _ = awsmiddleware.GetRequestIDMetadata(metadata)
			if out.Result != nil {
				response.Result = copyResponseResult(out.Result, metadata)
			}
			if err != nil {
				response.Error = err.Error()
			}
			r.mu.Lock()
			r.responses = append(r.responses, response)
			r.mu.Unlock()
		}
		return out, metadata, err
	}), middleware.Before); err != nil {
		return err
	}
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("record-original-dynamodb-response", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		out, metadata, err := next.HandleFinalize(ctx, in)
		capture := ctx.Value(responseCaptureKey{}).(*responseCapture)
		original := &SDKResponse{Cause: err, ServiceError: serviceError(err)}
		original.RequestID, _ = awsmiddleware.GetRequestIDMetadata(metadata)
		if out.Result != nil {
			original.Result = copyResponseResult(out.Result, metadata)
		}
		if err != nil {
			original.Error = err.Error()
		}
		capture.original = original
		return out, metadata, err
	}), middleware.After)
}

func (r *ResponseRecorder) Results(operation int) []Response {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []Response{}
	for _, response := range r.responses {
		if response.Operation == operation {
			copy := cloneValue(reflect.ValueOf(response)).Interface().(Response)
			copy.Cause = response.Cause
			if response.Original != nil {
				copy.Original.Cause = response.Original.Cause
			}
			if response.Result != nil {
				metadata := reflect.ValueOf(response.Result).Elem().FieldByName("ResultMetadata").Interface().(middleware.Metadata)
				copy.Result = copyResponseResult(response.Result, metadata)
			}
			if response.Original != nil && response.Original.Result != nil {
				metadata := reflect.ValueOf(response.Original.Result).Elem().FieldByName("ResultMetadata").Interface().(middleware.Metadata)
				copy.Original.Result = copyResponseResult(response.Original.Result, metadata)
			}
			out = append(out, copy)
		}
	}
	return out
}

func serviceError(err error) any {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api
	}
	return nil
}

func copyResponseResult(result any, metadata middleware.Metadata) any {
	copy := cloneValue(reflect.ValueOf(result))
	copy.Elem().FieldByName("ResultMetadata").Set(reflect.ValueOf(metadata.Clone()))
	return copy.Interface()
}
