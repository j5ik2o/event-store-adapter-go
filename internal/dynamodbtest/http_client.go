package dynamodbtest

import (
	"io"
	"net/http"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// httpClient is the SDK's default HTTP client with one difference: the request
// body is exposed to net/http through Read and Close only.
//
// The SDK wraps the request body so that it returns io.EOF after Close, and it
// closes the body as soon as the response headers arrive. DynamoDB Local can
// answer before net/http's write loop has finished with the request. When the
// wrapped body offers WriteTo, the write loop drains the remaining body with
// WriteTo, which returns io.EOF unchanged once the body is closed. net/http
// treats that as a failed request write and closes the connection while the
// response body is still being read, which the SDK reports as
// "use of closed network connection". Read turns the same closed body into an
// ordinary end of stream, so the write loop finishes without error.
type httpClient struct {
	inner *awshttp.BuildableClient
}

func newHTTPClient() httpClient {
	return httpClient{inner: awshttp.NewBuildableClient()}
}

func (c httpClient) Do(r *http.Request) (*http.Response, error) {
	if r.Body != nil && r.Body != http.NoBody {
		r = r.Clone(r.Context())
		r.Body = readCloserOnly{r.Body}
	}
	return c.inner.Do(r)
}

// readCloserOnly hides every optional interface of the wrapped body, notably io.WriterTo.
type readCloserOnly struct {
	io.ReadCloser
}
