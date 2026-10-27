// Package spannertest starts a real Cloud Spanner emulator for tests and provisions databases on it.
package spannertest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

const emulatorImage = "gcr.io/cloud-spanner-emulator/emulator:1.5.58@sha256:c6f3402f2599684f295a0fdefb6fbbbfb18a0e43e309ff5456ccb452a4570a79"

const (
	projectID  = "esa-test"
	instanceID = "esa-test"
)

var createContainer = testcontainers.GenericContainer

var databaseCount atomic.Int64

// Environment owns one emulator container and one instance inside it.
type Environment struct {
	container testcontainers.Container
	host      string

	previousHost    string
	hadPreviousHost bool

	mu            sync.Mutex
	closed        bool
	instanceReady bool
	databaseAdmin *database.DatabaseAdminClient
	databases     []*Database
}

// Start runs the emulator and sets SPANNER_EMULATOR_HOST so that every client reaches it and never a real service.
// A failure is returned to the caller; nothing is skipped.
func Start(ctx context.Context) (*Environment, error) {
	readyCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	container, err := createContainer(readyCtx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        emulatorImage,
			ExposedPorts: []string{"9010/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("9010/tcp"),
				wait.ForLog("Cloud Spanner emulator running"),
			),
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
	port, err := container.MappedPort(readyCtx, "9010/tcp")
	if err != nil {
		return fail(err)
	}
	e.host = net.JoinHostPort(host, port.Port())
	e.previousHost, e.hadPreviousHost = os.LookupEnv("SPANNER_EMULATOR_HOST")
	if err := os.Setenv("SPANNER_EMULATOR_HOST", e.host); err != nil {
		return fail(err)
	}
	return e, nil
}

// Close stops the container, closes the clients it created and restores SPANNER_EMULATOR_HOST.
func (e *Environment) Close(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	var errs []error
	for _, d := range e.databases {
		d.closeClient()
	}
	if e.databaseAdmin != nil {
		errs = append(errs, e.databaseAdmin.Close())
	}
	if e.host != "" {
		if e.hadPreviousHost {
			errs = append(errs, os.Setenv("SPANNER_EMULATOR_HOST", e.previousHost))
		} else {
			errs = append(errs, os.Unsetenv("SPANNER_EMULATOR_HOST"))
		}
	}
	if e.container != nil {
		errs = append(errs, e.container.Terminate(ctx))
	}
	return errors.Join(errs...)
}

func (e *Environment) ensureInstance(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.instanceReady {
		return nil
	}
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdmin.Close()
	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + projectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      "projects/" + projectID + "/instanceConfigs/emulator-config",
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}
	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	e.databaseAdmin = admin
	e.instanceReady = true
	return nil
}

// Database is an empty database inside the emulator's instance.
type Database struct {
	env  *Environment
	name string

	mu     sync.Mutex
	client *spanner.Client
}

// CreateDatabase creates a new, empty database.
func (e *Environment) CreateDatabase(ctx context.Context) (*Database, error) {
	if err := e.ensureInstance(ctx); err != nil {
		return nil, err
	}
	id := fmt.Sprintf("esa_db_%d", databaseCount.Add(1))
	op, err := e.databaseAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/" + projectID + "/instances/" + instanceID,
		CreateStatement: "CREATE DATABASE " + id,
	})
	if err != nil {
		return nil, err
	}
	if _, err := op.Wait(ctx); err != nil {
		return nil, err
	}
	d := &Database{env: e, name: "projects/" + projectID + "/instances/" + instanceID + "/databases/" + id}
	e.mu.Lock()
	e.databases = append(e.databases, d)
	e.mu.Unlock()
	return d, nil
}

// ApplyDDL runs the statements and waits until they finished.
func (d *Database) ApplyDDL(ctx context.Context, statements ...string) error {
	op, err := d.env.databaseAdmin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: d.name, Statements: statements})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

// NewClient returns a new client of the database. The caller closes it.
func (d *Database) NewClient(ctx context.Context, opts ...option.ClientOption) (*spanner.Client, error) {
	return spanner.NewClientWithConfig(ctx, d.name, spanner.ClientConfig{DisableNativeMetrics: true}, opts...)
}

func (d *Database) adminClient(ctx context.Context) (*spanner.Client, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		client, err := d.NewClient(ctx)
		if err != nil {
			return nil, err
		}
		d.client = client
	}
	return d.client, nil
}

func (d *Database) closeClient() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client != nil {
		d.client.Close()
		d.client = nil
	}
}
