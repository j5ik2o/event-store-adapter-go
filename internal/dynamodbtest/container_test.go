package dynamodbtest

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/containerd/errdefs"
	dockerclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func startEnvironment(t *testing.T) *Environment {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	e, err := Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close(context.Background())) })
	return e
}

func TestContainerLifecycle(t *testing.T) {
	// Given a fresh Local environment, When an SDK request is sent, Then it is ready.
	e, err := Start(context.Background())
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, e.Close(context.Background()))
		}
	})
	client := e.NewClient()
	options := client.Options()
	require.Equal(t, "us-east-1", options.Region)
	require.NotNil(t, options.BaseEndpoint)
	endpoint, err := url.Parse(*options.BaseEndpoint)
	require.NoError(t, err)
	port, err := strconv.Atoi(endpoint.Port())
	require.NoError(t, err)
	docker, err := testcontainers.NewDockerClientWithOpts(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, docker.Close()) })
	containers, err := docker.ContainerList(context.Background(), dockerclient.ContainerListOptions{})
	require.NoError(t, err)
	var ids []string
	for _, container := range containers.Items {
		for _, binding := range container.Ports {
			if int(binding.PublicPort) == port && binding.PrivatePort == 8000 {
				ids = append(ids, container.ID)
				break
			}
		}
	}
	require.Len(t, ids, 1)
	inspection, err := docker.ContainerInspect(context.Background(), ids[0], dockerclient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.Equal(t, "amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab", inspection.Container.Config.Image)
	require.Equal(t, []string{"-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-disableTelemetry"}, inspection.Container.Config.Cmd)
	credentials, err := options.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, credentials.AccessKeyID)
	require.NotEmpty(t, credentials.SecretAccessKey)
	_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	require.NoError(t, err)
	require.NoError(t, e.Close(context.Background()))
	closed = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.ListTables(ctx, &dynamodb.ListTablesInput{})
	require.Error(t, err)
}

func TestStartCancellationCleansAcquiredContainer(t *testing.T) {
	// Given an acquired container, When Start's parent context is canceled, Then Start releases it and returns the cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, docker.Close()) })
	original := createContainer
	t.Cleanup(func() { createContainer = original })
	var containerID string
	var parentErr, cleanupErr error
	terminateCalls := 0
	createContainer = func(readyCtx context.Context, request testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		request.LifecycleHooks = append(request.LifecycleHooks, testcontainers.ContainerLifecycleHooks{
			PreTerminates: []testcontainers.ContainerHook{func(cleanupCtx context.Context, _ testcontainers.Container) error {
				terminateCalls++
				parentErr = ctx.Err()
				cleanupErr = cleanupCtx.Err()
				return nil
			}},
		})
		container, err := testcontainers.GenericContainer(readyCtx, request)
		if container != nil {
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				_, inspectErr := docker.ContainerInspect(cleanupCtx, container.GetContainerID(), dockerclient.ContainerInspectOptions{})
				if errdefs.IsNotFound(inspectErr) {
					return
				}
				require.NoError(t, inspectErr)
				require.NoError(t, container.Terminate(cleanupCtx))
			})
		}
		require.NoError(t, err)
		require.NotNil(t, container)
		containerID = container.GetContainerID()
		inspection, inspectErr := docker.ContainerInspect(readyCtx, containerID, dockerclient.ContainerInspectOptions{})
		require.NoError(t, inspectErr)
		require.Equal(t, containerID, inspection.Container.ID)
		require.True(t, inspection.Container.State.Running)
		require.NoError(t, readyCtx.Err())
		cancel()
		return container, err
	}

	e, err := Start(ctx)

	require.Nil(t, e)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, terminateCalls)
	require.ErrorIs(t, parentErr, context.Canceled)
	require.NoError(t, cleanupErr)
	_, err = docker.ContainerInspect(context.Background(), containerID, dockerclient.ContainerInspectOptions{})
	require.ErrorIs(t, err, errdefs.ErrNotFound, "Start must remove the acquired container before test cleanup")
}

func TestContainerIgnoresAmbientAWSConfiguration(t *testing.T) {
	// Given unusable real-service configuration, When two environments start, Then both use independent Local endpoints.
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	t.Setenv("AWS_REGION", "invalid-region")
	t.Setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:1")
	a, err := Start(context.Background())
	require.NoError(t, err)
	aClosed := false
	t.Cleanup(func() {
		if !aClosed {
			require.NoError(t, a.Close(context.Background()))
		}
	})
	b := startEnvironment(t)
	ac, bc := a.NewClient(), b.NewClient()
	require.NotEqual(t, *ac.Options().BaseEndpoint, *bc.Options().BaseEndpoint)
	for _, client := range []*dynamodb.Client{ac, bc} {
		credentials, err := client.Options().Credentials.Retrieve(context.Background())
		require.NoError(t, err)
		require.NotEqual(t, "ambient-access", credentials.AccessKeyID)
		require.NotEqual(t, "ambient-secret", credentials.SecretAccessKey)
		_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
		require.NoError(t, err)
	}
	require.NoError(t, a.Close(context.Background()))
	aClosed = true
	_, err = bc.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	require.NoError(t, err, "closing one environment must preserve the other container")
}
