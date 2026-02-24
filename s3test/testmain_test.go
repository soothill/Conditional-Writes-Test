//go:build integration

package s3test

import (
	"fmt"
	"os"
	"testing"

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

	testCfg = cfg
	testClient = client
	testBucket = cfg.Bucket

	os.Exit(m.Run())
}
