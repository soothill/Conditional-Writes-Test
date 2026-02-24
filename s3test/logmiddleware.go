package s3test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	awsmw "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithy "github.com/aws/smithy-go"
	smithymw "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// testingKey is the context key used to carry a testing.TB through the AWS SDK
// request context. The s3responseLogger middleware reads it to log HTTP
// responses against the correct test.
type testingKey struct{}

// injectTesting returns ctx with t stored under testingKey. Call this in
// testContext so every S3 operation made within a test is automatically logged.
func injectTesting(ctx context.Context, t testing.TB) context.Context {
	return context.WithValue(ctx, testingKey{}, t)
}

// s3responseLogger is an AWS SDK Deserialize middleware. Added at the Before
// position it wraps the entire Deserialize chain, so after calling next it has
// access to both the raw HTTP response (in DeserializeOutput.RawResponse) and
// the fully-parsed SDK error (if any). It logs a single
// "[S3] Operation → HTTP STATUS Text (ErrorCode)" line per call using the
// testing.TB found in the request context.
//
// Contexts that do not carry a testingKey (e.g. background contexts used for
// cleanup) are silently skipped, so cleanup calls never pollute the per-test
// [S3] log.
type s3responseLogger struct{}

func (s3responseLogger) ID() string { return "S3ResponseLogger" }

func (s3responseLogger) HandleDeserialize(
	ctx context.Context,
	in smithymw.DeserializeInput,
	next smithymw.DeserializeHandler,
) (out smithymw.DeserializeOutput, meta smithymw.Metadata, err error) {
	out, meta, err = next.HandleDeserialize(ctx, in)

	t, ok := ctx.Value(testingKey{}).(testing.TB)
	if !ok {
		return
	}

	op := awsmw.GetOperationName(ctx)
	if op == "" {
		op = "S3"
	}

	if err != nil {
		// Error path: extract HTTP status and optional AWS error code.
		var respErr *awshttp.ResponseError
		if errors.As(err, &respErr) {
			status := respErr.HTTPStatusCode()
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() != "" {
				t.Logf("[S3] %s → HTTP %d %s (%s)",
					op, status, http.StatusText(status), apiErr.ErrorCode())
			} else {
				t.Logf("[S3] %s → HTTP %d %s", op, status, http.StatusText(status))
			}
		}
		return
	}

	// Success path: log the HTTP status from the raw HTTP response.
	// DeserializeOutput.RawResponse holds the *smithyhttp.Response that the
	// SDK received before converting it to the typed output struct.
	if out.RawResponse != nil {
		if resp, ok2 := out.RawResponse.(*smithyhttp.Response); ok2 {
			t.Logf("[S3] %s → HTTP %d %s", op, resp.StatusCode, http.StatusText(resp.StatusCode))
		}
	}
	return
}
