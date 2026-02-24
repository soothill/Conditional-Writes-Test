package s3test

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// Config holds all S3 test configuration.
type Config struct {
	Endpoint        string // S3_ENDPOINT - custom endpoint for MinIO/LocalStack (optional)
	Region          string // AWS_REGION - AWS region (default: "us-east-1")
	Bucket          string // S3_BUCKET - bucket to use for tests (required)
	AccessKeyID     string // AWS_ACCESS_KEY_ID
	SecretAccessKey string // AWS_SECRET_ACCESS_KEY
	SessionToken    string // AWS_SESSION_TOKEN (optional)
	PathStyle       bool   // S3_PATH_STYLE - force path-style addressing (default: true if endpoint set)
}

// LoadConfigFromEnv reads configuration from environment variables.
// Returns an error if required variables are missing.
func LoadConfigFromEnv() (Config, error) {
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return Config{}, fmt.Errorf("S3_BUCKET environment variable is required")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	endpoint := os.Getenv("S3_ENDPOINT")

	// Default path-style to true when a custom endpoint is set (MinIO/LocalStack need it).
	pathStyle := endpoint != ""
	if v := os.Getenv("S3_PATH_STYLE"); v != "" {
		pathStyle = strings.EqualFold(v, "true") || v == "1"
	}

	return Config{
		Endpoint:        endpoint,
		Region:          region,
		Bucket:          bucket,
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		PathStyle:       pathStyle,
	}, nil
}

// LoadConfig reads configuration from environment variables and fails the test
// if required variables are missing.
func LoadConfig(t testing.TB) Config {
	t.Helper()
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
