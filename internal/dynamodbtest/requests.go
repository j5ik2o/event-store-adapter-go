package dynamodbtest

import (
	"context"
	"reflect"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/middleware"
)

type operationKey struct{}

func WithOperation(ctx context.Context, operation int) context.Context {
	return context.WithValue(ctx, operationKey{}, operation)
}

type Request struct {
	Operation int
	API       string
	Targets   []RequestTarget
	Input     any
}
type RequestTarget struct {
	TableName string
	IndexName string
}

// Recorder captures SDK inputs before execution, including failed requests.
type Recorder struct {
	mu       sync.Mutex
	requests []Request
}

func NewRecorder() *Recorder { return &Recorder{} }

func (r *Recorder) APIOption(stack *middleware.Stack) error {
	return stack.Serialize.Add(middleware.SerializeMiddlewareFunc("record-dynamodb-request", func(ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler) (middleware.SerializeOutput, middleware.Metadata, error) {
		if operation, ok := ctx.Value(operationKey{}).(int); ok {
			input := cloneValue(reflect.ValueOf(in.Parameters)).Interface()
			request := Request{Operation: operation, API: middleware.GetOperationName(ctx), Targets: requestTargets(input), Input: input}
			r.mu.Lock()
			r.requests = append(r.requests, request)
			r.mu.Unlock()
		}
		return next.HandleSerialize(ctx, in)
	}), middleware.Before)
}

func (r *Recorder) Requests(operation int) []Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []Request
	for _, request := range r.requests {
		if request.Operation == operation {
			result = append(result, cloneValue(reflect.ValueOf(request)).Interface().(Request))
		}
	}
	return result
}

// cloneValue retains SDK concrete types and copies all exported mutable fields.
func cloneValue(value reflect.Value) reflect.Value {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		copy := reflect.New(value.Type().Elem())
		copy.Elem().Set(cloneValue(value.Elem()))
		return copy
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		copy := reflect.New(value.Type()).Elem()
		copy.Set(cloneValue(value.Elem()))
		return copy
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		copy := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			copy.SetMapIndex(iter.Key(), cloneValue(iter.Value()))
		}
		return copy
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		copy := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			copy.Index(i).Set(cloneValue(value.Index(i)))
		}
		return copy
	case reflect.Struct:
		copy := reflect.New(value.Type()).Elem()
		copy.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).IsExported() {
				copy.Field(i).Set(cloneValue(value.Field(i)))
			}
		}
		return copy
	default:
		return value
	}
}

func requestTargets(input any) []RequestTarget {
	var targets []RequestTarget
	// Batch table names are map keys rather than TableName fields.
	switch input := input.(type) {
	case *dynamodb.BatchGetItemInput:
		for name := range input.RequestItems {
			targets = append(targets, RequestTarget{TableName: name})
		}
	case *dynamodb.BatchWriteItemInput:
		for name := range input.RequestItems {
			targets = append(targets, RequestTarget{TableName: name})
		}
	default:
		targets = collectTargets(reflect.ValueOf(input))
		return targets
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].TableName < targets[j].TableName })
	return targets
}

func collectTargets(value reflect.Value) []RequestTarget {
	if value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return collectTargets(value.Elem())
	}
	var targets []RequestTarget
	switch value.Kind() {
	case reflect.Struct:
		table := value.FieldByName("TableName")
		if table.IsValid() && table.Type() == reflect.TypeOf((*string)(nil)) {
			target := RequestTarget{TableName: aws.ToString(table.Interface().(*string))}
			index := value.FieldByName("IndexName")
			if index.IsValid() && index.Type() == table.Type() {
				target.IndexName = aws.ToString(index.Interface().(*string))
			}
			return []RequestTarget{target}
		}
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).IsExported() {
				targets = append(targets, collectTargets(value.Field(i))...)
			}
		}
	case reflect.Slice:
		for i := 0; i < value.Len(); i++ {
			targets = append(targets, collectTargets(value.Index(i))...)
		}
	}
	return targets
}
