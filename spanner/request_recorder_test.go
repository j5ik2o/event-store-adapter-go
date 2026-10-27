package spanner_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	gspanner "cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// rpcRecorder keeps every request that a client sends to the emulator, so that tests can prove which transaction
// options the library really put on the wire. It observes the real gRPC traffic; it does not replace the emulator.
type rpcRecorder struct {
	mu       sync.Mutex
	requests []recordedRequest
}

type recordedRequest struct {
	method  string
	message proto.Message
}

func (r *rpcRecorder) record(method string, message any) {
	// Session management (CreateSession, BatchCreateSessions, DeleteSession) is background traffic of the client
	// library, not a request about the stored data: it is not part of what the tests observe.
	if strings.Contains(method, "Session") {
		return
	}
	if m, ok := message.(proto.Message); ok {
		r.mu.Lock()
		r.requests = append(r.requests, recordedRequest{method: method[strings.LastIndex(method, "/")+1:], message: m})
		r.mu.Unlock()
	}
}

func (r *rpcRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = nil
}

func (r *rpcRecorder) all() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedRequest(nil), r.requests...)
}

func (r *rpcRecorder) methods() []string {
	var out []string
	for _, request := range r.all() {
		out = append(out, request.method)
	}
	return out
}

type recordingStream struct {
	grpc.ClientStream
	method   string
	recorder *rpcRecorder
}

func (s *recordingStream) SendMsg(m any) error {
	s.recorder.record(s.method, m)
	return s.ClientStream.SendMsg(m)
}

func (r *rpcRecorder) options() []option.ClientOption {
	unary := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		r.record(method, req)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	stream := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &recordingStream{ClientStream: s, method: method, recorder: r}, nil
	}
	return []option.ClientOption{option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(unary)), option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(stream))}
}

func newRecordingClient(t *testing.T) (*gspanner.Client, *rpcRecorder) {
	t.Helper()
	recorder := &rpcRecorder{}
	return newClient(t, recorder.options()...), recorder
}

// transactionSelectors returns the TransactionSelector of every statement or read request, in order.
func (r *rpcRecorder) transactionSelectors() []*sppb.TransactionSelector {
	var out []*sppb.TransactionSelector
	for _, request := range r.all() {
		switch m := request.message.(type) {
		case *sppb.ExecuteSqlRequest:
			out = append(out, m.GetTransaction())
		case *sppb.ReadRequest:
			out = append(out, m.GetTransaction())
		case *sppb.ExecuteBatchDmlRequest:
			out = append(out, m.GetTransaction())
		}
	}
	return out
}

// beginOptions returns every TransactionOptions the client asked the server to begin, explicitly or inline.
func (r *rpcRecorder) beginOptions() []*sppb.TransactionOptions {
	var out []*sppb.TransactionOptions
	for _, request := range r.all() {
		if m, ok := request.message.(*sppb.BeginTransactionRequest); ok {
			out = append(out, m.GetOptions())
		}
	}
	for _, selector := range r.transactionSelectors() {
		if begin := selector.GetBegin(); begin != nil {
			out = append(out, begin)
		}
	}
	return out
}
