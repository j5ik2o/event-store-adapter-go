package dynamodbtest

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/middleware"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const localImage = "amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab"

// Environment owns one DynamoDB Local container and its explicit SDK endpoint.
type Environment struct {
	container testcontainers.Container
	endpoint  string
	mu        sync.Mutex
	closed    bool
}

// Start waits for a successful SDK request before returning the environment.
func Start(ctx context.Context) (*Environment, error) {
	readyCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(readyCtx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        localImage,
			ExposedPorts: []string{"8000/tcp"},
			Cmd:          []string{"-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-disableTelemetry"},
			WaitingFor:   wait.ForListeningPort("8000/tcp"),
		},
		Started: true,
	})
	e := &Environment{container: container}
	fail := func(cause error) (*Environment, error) {
		if container == nil {
			return nil, cause
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		return nil, errors.Join(cause, e.Close(cleanupCtx))
	}
	if err != nil {
		return fail(err)
	}
	host, err := container.Host(readyCtx)
	if err != nil {
		return fail(err)
	}
	port, err := container.MappedPort(readyCtx, "8000/tcp")
	if err != nil {
		return fail(err)
	}
	e.endpoint = "http://" + net.JoinHostPort(host, port.Port())
	client := e.NewClient()
	for {
		_, err = client.ListTables(readyCtx, &dynamodb.ListTablesInput{})
		if err == nil {
			return e, nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-readyCtx.Done():
			timer.Stop()
			return fail(errors.Join(readyCtx.Err(), err))
		case <-timer.C:
		}
	}
}

// NewClient never loads ambient AWS configuration.
func (e *Environment) NewClient(apiOptions ...func(*middleware.Stack) error) *dynamodb.Client {
	return dynamodb.New(dynamodb.Options{
		Region:           "us-east-1",
		Credentials:      credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		BaseEndpoint:     aws.String(e.endpoint),
		RetryMaxAttempts: 1,
		APIOptions:       apiOptions,
	})
}

// Close terminates only the container owned by this environment.
func (e *Environment) Close(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	if err := e.container.Terminate(ctx); err != nil {
		return err
	}
	e.closed = true
	return nil
}
