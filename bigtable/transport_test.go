package bigtable_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"github.com/j5ik2o/event-store-adapter-go/v5/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// requestRecorder records every gRPC method that a client sends. The clients below connect explicitly (never through
// BIGTABLE_EMULATOR_HOST) so that interceptors and per-request credentials are the ones the SDK uses.
type requestRecorder struct {
	mu      sync.Mutex
	methods []string
}

func (r *requestRecorder) add(method string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.methods = append(r.methods, method)
}

func (r *requestRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.methods)
}

func (r *requestRecorder) countOf(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, m := range r.methods {
		if strings.HasSuffix(m, "/"+name) {
			n++
		}
	}
	return n
}

type recorded[C any] struct {
	client   C
	recorder *requestRecorder
}

// interceptors returns dial options that record the method of every unary and streaming call.
func interceptors(r *requestRecorder, unary grpc.UnaryClientInterceptor, stream grpc.StreamClientInterceptor) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			r.add(method)
			if unary != nil {
				return unary(ctx, method, req, reply, cc, invoker, opts...)
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			r.add(method)
			if stream != nil {
				return stream(ctx, desc, cc, method, streamer, opts...)
			}
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}
}

func connect(t *testing.T, dial ...grpc.DialOption) option.ClientOption {
	t.Helper()
	conn, err := emulator.Dial(t.Context(), dial...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return option.WithGRPCConn(conn)
}

func recordingClients(t *testing.T) (recorded[*gbigtable.Client], recorded[*gbigtable.AdminClient]) {
	t.Helper()
	dataRecorder, adminRecorder := &requestRecorder{}, &requestRecorder{}
	data := newDataClient(t, connect(t, interceptors(dataRecorder, nil, nil)...))
	admin, err := emulator.NewAdminClient(t.Context(), connect(t, interceptors(adminRecorder, nil, nil)...))
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	return recorded[*gbigtable.Client]{data, dataRecorder}, recorded[*gbigtable.AdminClient]{admin, adminRecorder}
}

// tokenCredentials is a caller-owned per-request credential (the authentication plug-in). It records the URI of
// every request it is asked to sign and optionally fails.
type tokenCredentials struct {
	mu   sync.Mutex
	uris []string
	fail error
}

func (c *tokenCredentials) GetRequestMetadata(_ context.Context, uri ...string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uris = append(c.uris, uri...)
	if c.fail != nil {
		return nil, c.fail
	}
	return map[string]string{"authorization": "Bearer test-token"}, nil
}

// RequireTransportSecurity is false because the emulator speaks plain text.
func (c *tokenCredentials) RequireTransportSecurity() bool { return false }

func (c *tokenCredentials) seen(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, u := range c.uris {
		if strings.HasSuffix(u, "/"+method) {
			n++
		}
	}
	return n
}

// TestCredentials shows that credentials supplied by the caller are asked for on every data request and on the
// administrative request, and that a failing credential reaches the caller as a storage error before any write.
func TestCredentials(t *testing.T) {
	t.Run("data and administrative requests use the caller's credentials", func(t *testing.T) {
		l := provision(t, layoutSpec{})
		dataCreds, adminCreds := &tokenCredentials{}, &tokenCredentials{}
		data := newDataClient(t, connect(t, grpc.WithPerRPCCredentials(dataCreds)))
		admin, err := emulator.NewAdminClient(t.Context(), connect(t, grpc.WithPerRPCCredentials(adminCreds)))
		require.NoError(t, err)
		t.Cleanup(func() { _ = admin.Close() })
		es, ss := serializers()
		entry, err := bigtable.New(t.Context(), data, admin, l.config(), es, ss)
		require.NoError(t, err)
		id := aggregate(t, "Account", "creds")

		require.NoError(t, entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one")))
		_, err = entry.GetEventsByIDSinceSeqNr(t.Context(), id, 1)
		require.NoError(t, err)

		// gRPC hands the credentials the service URL (not the method) once per request.
		assert.GreaterOrEqual(t, adminCreds.seen("google.bigtable.admin.v2.BigtableTableAdmin"), 2, "the administrative request asks for credentials: one GetTable per table")
		assert.GreaterOrEqual(t, dataCreds.seen("google.bigtable.v2.Bigtable"), 4, "config reads, the head read, the commit and the journal read each ask for credentials")
	})

	t.Run("a failing credential on the data API writes nothing and is a storage error", func(t *testing.T) {
		l := provision(t, layoutSpec{})
		failing := &tokenCredentials{fail: errors.New("token refused")}
		data := newDataClient(t, connect(t, grpc.WithPerRPCCredentials(failing)))
		es, ss := serializers()
		entry, err := bigtable.New(t.Context(), data, l.admin, l.config(), es, ss)
		if err != nil {
			requireKind(t, err, eventstore.KindStorage)
			return
		}

		err = entry.PersistEvent(t.Context(), eventAt(t, aggregate(t, "Account", "denied"), 1, "one"))

		requireKind(t, err, eventstore.KindStorage)
		assert.Equal(t, []string{"\x00"}, rowKeys(t, l.data, l.head), "nothing is written")
		assert.Equal(t, []string{"\x00"}, l.journalKeys(t))
	})

	t.Run("a failing credential on the administrative API is a storage error", func(t *testing.T) {
		l := provision(t, layoutSpec{})
		failing := &tokenCredentials{fail: errors.New("token refused")}
		admin, err := emulator.NewAdminClient(t.Context(), connect(t, grpc.WithPerRPCCredentials(failing)))
		require.NoError(t, err)
		t.Cleanup(func() { _ = admin.Close() })
		es, ss := serializers()

		entry, err := bigtable.New(t.Context(), l.data, admin, l.config(), es, ss)

		requireKind(t, err, eventstore.KindStorage)
		assert.Nil(t, entry)
	})
}

// TestLostConditionalUpdate loses the response of the commit-point write after the destination applied it. The
// result is a storage error that does not claim failure, and the write is neither retried nor repeated (BT-5).
func TestLostConditionalUpdate(t *testing.T) {
	l := provision(t, layoutSpec{})
	recorder := &requestRecorder{}
	var armed sync.Once
	lose := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil && strings.HasSuffix(method, "/CheckAndMutateRow") {
			lost := false
			armed.Do(func() { lost = true })
			if lost {
				return status.Error(codes.Unavailable, "response lost")
			}
		}
		return err
	}
	data := newDataClient(t, connect(t, interceptors(recorder, lose, nil)...))
	es, ss := serializers()
	entry, err := bigtable.New(t.Context(), data, l.admin, l.config(), es, ss)
	require.NoError(t, err)
	id := aggregate(t, "Account", "lost")

	err = entry.PersistEvent(t.Context(), eventAt(t, id, 1, "one"))

	requireKind(t, err, eventstore.KindStorage)
	var lostLock *eventstore.OptimisticLockError
	assert.False(t, errors.As(err, &lostLock), "an unconfirmed write must not be reported as a lost race")
	assert.Equal(t, 1, recorder.countOf("CheckAndMutateRow"), "the conditional update is sent exactly once")
	got := cells(t, l.data, l.head, string(rowPrefix(aidOf(t, id))))
	require.NotNil(t, got, "the destination did apply the write")
	assert.Equal(t, "00000000000000000001", got["head:seq_nr"])
}

// TestInterruptedReadRows cuts the ReadRows stream after the first message. The store resumes after the last
// completed row and still returns every event once, in order (BT-10).
func TestInterruptedReadRows(t *testing.T) {
	l := provision(t, layoutSpec{})
	writer := l.newEntry(t)
	id := aggregate(t, "Account", "resume")
	big := strings.Repeat("x", 200_000) // row sizes force several response messages
	for seq := eventstore.SeqNr(1); seq <= 12; seq++ {
		require.NoError(t, writer.PersistEvent(t.Context(), eventAt(t, id, seq, big+string(rune('a'+seq)))))
	}
	recorder := &requestRecorder{}
	var streams atomic.Int64
	cutStream := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !strings.HasSuffix(method, "/ReadRows") {
			return stream, err
		}
		if streams.Add(1) != 2 { // the first ReadRows is the head read; the second is the journal range
			return stream, nil
		}
		return &cuttingStream{ClientStream: stream, remaining: 1}, nil
	}
	data := newDataClient(t, connect(t, interceptors(recorder, nil, cutStream)...))
	es, ss := serializers()
	reader, err := bigtable.New(t.Context(), data, l.admin, l.config(), es, ss)
	require.NoError(t, err)

	events, err := reader.GetEventsByIDSinceSeqNr(t.Context(), id, 1)

	require.NoError(t, err)
	require.Len(t, events, 12)
	for i, e := range events {
		assert.Equal(t, eventstore.SeqNr(i+1), e.SeqNr(), "no event is lost or repeated across the resumption")
	}
	assert.GreaterOrEqual(t, recorder.countOf("ReadRows"), 3, "head read, the interrupted journal read, and the resumed read")
}

type cuttingStream struct {
	grpc.ClientStream
	remaining int
}

func (s *cuttingStream) RecvMsg(m any) error {
	if s.remaining == 0 {
		return status.Error(codes.Unavailable, "stream cut")
	}
	s.remaining--
	return s.ClientStream.RecvMsg(m)
}
