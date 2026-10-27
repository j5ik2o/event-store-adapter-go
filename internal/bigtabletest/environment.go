// Package bigtabletest starts a real Cloud Bigtable emulator for tests and provisions the two tables of the
// storage layout on it. Nothing here reaches a real service: every connection is an explicit plain-text
// connection to the emulator, and BIGTABLE_EMULATOR_HOST is never set (the SDK would replace the connection
// that a test passes in).
package bigtabletest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	gbigtable "cloud.google.com/go/bigtable"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// emulatorImage is pinned by digest and is the image that the Rust and Java implementations use.
const emulatorImage = "gcr.io/google.com/cloudsdktool/google-cloud-cli:587.0.0-emulators@sha256:8d8573471c489a409f03891d2c24d177fa531276184f7002b9a2d853d1b3ed54"

const (
	// Project and Instance name the emulator's project and instance; the emulator accepts any name.
	Project  = "esa-test"
	Instance = "esa-test"

	emulatorPort = "8086/tcp"
	// maxReceiveBytes lets a read return the largest allowed envelope (8 MiB) plus its row.
	maxReceiveBytes = 256 * 1024 * 1024
)

var createContainer = testcontainers.GenericContainer

// Environment owns one emulator container and the connections it handed out.
type Environment struct {
	container testcontainers.Container
	host      string

	mu     sync.Mutex
	closed bool
	conns  []*grpc.ClientConn
}

// Start runs the emulator and waits until the table administration API answers. A failure is returned to the
// caller; nothing is skipped.
func Start(ctx context.Context) (*Environment, error) {
	readyCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	container, err := createContainer(readyCtx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        emulatorImage,
			ExposedPorts: []string{emulatorPort},
			Cmd:          []string{"gcloud", "beta", "emulators", "bigtable", "start", "--host-port=0.0.0.0:8086"},
			WaitingFor:   wait.ForListeningPort(emulatorPort),
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
	port, err := container.MappedPort(readyCtx, emulatorPort)
	if err != nil {
		return fail(err)
	}
	e.host = net.JoinHostPort(host, port.Port())
	if err := e.waitReady(readyCtx); err != nil {
		return fail(err)
	}
	return e, nil
}

func (e *Environment) waitReady(ctx context.Context) error {
	admin, err := e.NewAdminClient(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	var last error
	for {
		listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, last = admin.Tables(listCtx)
		cancel()
		if last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the Bigtable emulator did not become ready: %w", errors.Join(ctx.Err(), last))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Close closes the connections that the environment created and stops the container. Clients belong to the caller,
// who closes them first.
func (e *Environment) Close(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	var errs []error
	for _, c := range e.conns {
		// A client that was given the connection may already have closed it.
		if err := c.Close(); err != nil && status.Code(err) != codes.Canceled {
			errs = append(errs, err)
		}
	}
	if e.container != nil {
		errs = append(errs, e.container.Terminate(ctx))
	}
	return errors.Join(errs...)
}

// Dial opens a plain-text connection to the emulator. The caller closes it. Extra dial options (per-request
// credentials, interceptors) are applied after the defaults.
func (e *Environment) Dial(_ context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	all := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxReceiveBytes)),
	}, opts...)
	return grpc.NewClient(e.host, all...)
}

// connection returns the client options: the caller's own options, or a connection owned by the environment.
func (e *Environment) connection(ctx context.Context, opts []option.ClientOption) ([]option.ClientOption, error) {
	if len(opts) > 0 {
		return opts, nil
	}
	conn, err := e.Dial(ctx)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.conns = append(e.conns, conn)
	e.mu.Unlock()
	return []option.ClientOption{option.WithGRPCConn(conn)}, nil
}

// NewClient creates a data client. Without options it connects to the emulator; with options (typically
// option.WithGRPCConn of a connection from Dial) it uses exactly those.
func (e *Environment) NewClient(ctx context.Context, opts ...option.ClientOption) (*gbigtable.Client, error) {
	opts, err := e.connection(ctx, opts)
	if err != nil {
		return nil, err
	}
	client, err := gbigtable.NewClientWithConfig(ctx, Project, Instance, gbigtable.ClientConfig{MetricsProvider: gbigtable.NoopMetricsProvider{}}, opts...)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// NewAdminClient creates a table administration client with the same connection rule as NewClient.
func (e *Environment) NewAdminClient(ctx context.Context, opts ...option.ClientOption) (*gbigtable.AdminClient, error) {
	opts, err := e.connection(ctx, opts)
	if err != nil {
		return nil, err
	}
	admin, err := gbigtable.NewAdminClient(ctx, Project, Instance, opts...)
	if err != nil {
		return nil, err
	}
	return admin, nil
}
