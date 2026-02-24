//go:build integration

package s3test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	preCtx, preCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer preCancel()
	if _, err := client.HeadBucket(preCtx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "Preflight: cannot access bucket %q: %v\n", cfg.Bucket, err)
		fmt.Fprintf(os.Stderr, "Check S3_BUCKET, S3_ENDPOINT, and credentials in your .env file.\n")
		os.Exit(1)
	}

	testCfg = cfg
	testClient = client
	testBucket = cfg.Bucket

	os.Exit(m.Run())
}
