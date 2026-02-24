package s3test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// BuildClient creates an S3 client from the given Config.
// Used by TestMain where testing.TB is not available.
func BuildClient(cfg Config) (*s3.Client, error) {
	var optFns []func(*awsconfig.LoadOptions) error

	optFns = append(optFns, awsconfig.WithRegion(cfg.Region))

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		optFns = append(optFns, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				cfg.SessionToken,
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
	})

	return client, nil
}

// NewS3Client creates an S3 client from the given Config.
// It fails the test if the client cannot be created.
func NewS3Client(t testing.TB, cfg Config) *s3.Client {
	t.Helper()
	client, err := BuildClient(cfg)
	if err != nil {
		t.Fatalf("failed to create S3 client: %v", err)
	}
	return client
}
