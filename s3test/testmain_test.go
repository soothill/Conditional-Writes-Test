//go:build integration

package s3test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	testClient *s3.Client
	testBucket string
	testCfg    Config
)

func TestMain(m *testing.M) {
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	client, err := BuildClient(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create S3 client: %v\n", err)
		os.Exit(1)
	}

	// Preflight: verify the bucket is reachable before running any tests.
	// A failure here means every subsequent test would fail with the same
	// connectivity or credential error, so we exit immediately with a clear
	// message rather than letting hundreds of test cases all report failures.
	//
	// Special case: if the provider returns a 301 Moved Permanently the
	// configured endpoint points at the wrong region. The AWS SDK does not
	// follow S3 redirects automatically (re-signing is required), so the 301
	// surfaces as an error. We extract the corrected endpoint from the
	// Location header, rebuild the client, and retry — tests can then run
	// without the user having to fix their config first.
	preCtx, preCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer preCancel()

	if _, headErr := client.HeadBucket(preCtx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	}); headErr != nil {
		if correctedEndpoint, correctedRegion, ok := endpointFromRedirect(headErr); ok {
			hint := fmt.Sprintf("S3_ENDPOINT=%s", correctedEndpoint)
			if correctedRegion != "" && correctedRegion != cfg.Region {
				hint += fmt.Sprintf("  AWS_REGION=%s", correctedRegion)
			}
			fmt.Fprintf(os.Stderr,
				"Preflight: %s redirected to %s (wrong region) — retrying with corrected config.\n"+
					"  Update %s in your .env to remove this delay.\n",
				cfg.Endpoint, correctedEndpoint, hint)

			cfg.Endpoint = correctedEndpoint
			if correctedRegion != "" {
				cfg.Region = correctedRegion
			}
			client, err = BuildClient(cfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to rebuild S3 client: %v\n", err)
				os.Exit(1)
			}

			retryCtx, retryCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer retryCancel()
			if _, retryErr := client.HeadBucket(retryCtx, &s3.HeadBucketInput{
				Bucket: aws.String(cfg.Bucket),
			}); retryErr != nil {
				fmt.Fprintf(os.Stderr, "Preflight: bucket %q still unreachable at %s: %v\n",
					cfg.Bucket, correctedEndpoint, retryErr)
				fmt.Fprintf(os.Stderr, "Check that AWS_REGION matches the bucket's actual region.\n")
				os.Exit(1)
			}
			// Successfully connected via corrected endpoint — fall through.
		} else {
			fmt.Fprintf(os.Stderr, "Preflight: cannot access bucket %q: %v\n", cfg.Bucket, headErr)
			if strings.Contains(headErr.Error(), "301") || strings.Contains(headErr.Error(), "MovedPermanently") {
				fmt.Fprintf(os.Stderr, "Hint: 301 MovedPermanently means S3_ENDPOINT is for the wrong region.\n")
				fmt.Fprintf(os.Stderr, "      Check your bucket's region and update S3_ENDPOINT and AWS_REGION.\n")
			} else {
				fmt.Fprintf(os.Stderr, "Check S3_BUCKET, S3_ENDPOINT, and credentials in your .env file.\n")
			}
			os.Exit(1)
		}
	}

	testCfg = cfg
	testClient = client
	testBucket = cfg.Bucket

	os.Exit(m.Run())
}

// endpointFromRedirect checks whether err is an HTTP 301 response carrying a
// Location header. If so it returns the scheme+host of the redirect target as
// the corrected endpoint, and attempts to infer the region from the hostname
// (e.g. "ap-northeast-1" from "s3.ap-northeast-1.wasabisys.com").
//
// The AWS SDK does not follow S3 redirects automatically because requests must
// be re-signed for the new endpoint. When an S3-compatible provider (e.g.
// Wasabi) returns 301 for a wrong-region request the raw HTTP response is
// surfaced as an error, including its headers. We extract the Location from
// that response so the caller can rebuild the client with the right endpoint
// and region.
func endpointFromRedirect(err error) (endpoint, region string, ok bool) {
	var respErr *awshttp.ResponseError
	if !errors.As(err, &respErr) || respErr.HTTPStatusCode() != 301 {
		return "", "", false
	}
	// respErr.Response is *smithyhttp.Response (embedded via *smithyhttp.ResponseError).
	// smithyhttp.Response embeds *http.Response, so Header is promoted directly.
	if respErr.Response == nil {
		return "", "", false
	}
	location := respErr.Response.Header.Get("Location")
	if location == "" {
		return "", "", false
	}
	u, parseErr := url.Parse(location)
	if parseErr != nil || u.Host == "" {
		return "", "", false
	}
	endpoint = u.Scheme + "://" + u.Host
	region = regionFromHost(u.Host)
	return endpoint, region, true
}

// regionFromHost tries to extract an AWS/S3-compatible region name from a
// hostname. It understands two common patterns:
//
//   - s3.<region>.<domain>   (AWS, Wasabi, Backblaze B2, etc.)
//   - <region>.<domain>      (DigitalOcean Spaces, etc.)
//
// Returns an empty string if no region-like segment can be identified.
func regionFromHost(host string) string {
	// Strip port if present.
	if i := strings.LastIndex(host, ":"); i > strings.LastIndex(host, "]") {
		host = host[:i]
	}
	parts := strings.Split(host, ".")
	// Pattern: s3.<region>.<rest...>
	if len(parts) >= 3 && parts[0] == "s3" {
		if looksLikeRegion(parts[1]) {
			return parts[1]
		}
	}
	// Pattern: <region>.<rest...>
	if len(parts) >= 2 && looksLikeRegion(parts[0]) {
		return parts[0]
	}
	return ""
}

// looksLikeRegion returns true if s looks like a region slug: two or more
// lowercase words joined by hyphens (e.g. "us-east-1", "ap-northeast-1").
func looksLikeRegion(s string) bool {
	return len(s) >= 4 &&
		strings.Contains(s, "-") &&
		strings.IndexFunc(s, func(r rune) bool {
			return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-')
		}) == -1
}
