package s3test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTempEnv creates a temporary .env file containing contents and returns
// its path. The file is automatically removed when the test ends.
func writeTempEnv(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

// ---------------------------------------------------------------------------
// validateBucketName
// ---------------------------------------------------------------------------

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name    string
		bucket  string
		wantErr string // non-empty substring expected in the error message
	}{
		// --- valid names ---
		{name: "simple lowercase", bucket: "mybucket"},
		{name: "with hyphens", bucket: "my-test-bucket"},
		{name: "with dots", bucket: "my.test.bucket"},
		{name: "min length (3)", bucket: "abc"},
		{name: "max length (63)", bucket: strings.Repeat("a", 63)},
		{name: "digits only", bucket: "123"},
		{name: "starts with digit", bucket: "1bucket"},

		// --- too short / too long ---
		{name: "too short (2)", bucket: "ab", wantErr: "3–63"},
		{name: "too long (64)", bucket: strings.Repeat("a", 64), wantErr: "3–63"},

		// --- invalid characters ---
		{name: "uppercase letter", bucket: "MyBucket", wantErr: "invalid character"},
		{name: "underscore", bucket: "my_bucket", wantErr: "invalid character"},
		{name: "space", bucket: "my bucket", wantErr: "invalid character"},
		{name: "at sign", bucket: "my@bucket", wantErr: "invalid character"},

		// --- start / end rules ---
		{name: "starts with hyphen", bucket: "-bucket", wantErr: "start"},
		{name: "starts with dot", bucket: ".bucket", wantErr: "start"},
		{name: "ends with hyphen", bucket: "bucket-", wantErr: "end"},
		{name: "ends with dot", bucket: "bucket.", wantErr: "end"},

		// --- consecutive dots ---
		{name: "consecutive dots", bucket: "my..bucket", wantErr: "consecutive dots"},

		// --- IP address ---
		{name: "IPv4 address", bucket: "192.168.1.1", wantErr: "IP address"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBucketName(tc.bucket)
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// validateHTTPURL
// ---------------------------------------------------------------------------

func TestValidateHTTPURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		// --- valid ---
		{name: "http with port", url: "http://localhost:9000"},
		{name: "https AWS", url: "https://s3.amazonaws.com"},
		{name: "http with path", url: "http://localhost:4566/"},
		{name: "https with IP", url: "https://192.168.1.10:9000"},

		// --- wrong scheme ---
		{name: "no scheme", url: "localhost:9000", wantErr: "scheme"},
		{name: "ftp scheme", url: "ftp://localhost:9000", wantErr: "scheme"},
		{name: "s3 scheme", url: "s3://my-bucket", wantErr: "scheme"},

		// --- missing host ---
		{name: "scheme only", url: "http://", wantErr: "missing host"},

		// --- unparseable ---
		{name: "invalid URL", url: "://bad url", wantErr: "not a valid URL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateHTTPURL(tc.url)
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// validateRegion
// ---------------------------------------------------------------------------

func TestValidateRegion(t *testing.T) {
	tests := []struct {
		name    string
		region  string
		wantErr string
	}{
		// --- valid ---
		{name: "us-east-1", region: "us-east-1"},
		{name: "eu-west-2", region: "eu-west-2"},
		{name: "ap-southeast-1", region: "ap-southeast-1"},
		{name: "us-gov-west-1", region: "us-gov-west-1"},
		{name: "two chars", region: "us"},

		// --- too short ---
		{name: "single char", region: "a", wantErr: "2–30"},

		// --- too long ---
		{name: "31 chars", region: strings.Repeat("a", 31), wantErr: "2–30"},

		// --- invalid characters ---
		{name: "uppercase", region: "US-EAST-1", wantErr: "invalid character"},
		{name: "underscore", region: "us_east_1", wantErr: "invalid character"},
		{name: "space", region: "us east", wantErr: "invalid character"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRegion(tc.region)
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Config.validate — credential pairing and combined checks
// ---------------------------------------------------------------------------

func TestConfigValidate(t *testing.T) {
	validBase := Config{
		Bucket: "my-bucket",
		Region: "us-east-1",
	}

	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "fully valid with credentials",
			cfg: Config{
				Bucket:          "my-bucket",
				Region:          "us-east-1",
				Endpoint:        "http://localhost:9000",
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "secret",
			},
		},
		{
			name: "valid – no credentials (uses SDK chain)",
			cfg:  validBase,
		},
		// endpoint validation
		{
			name:    "bad endpoint scheme",
			cfg:     Config{Bucket: "b", Region: "us-east-1", Endpoint: "ftp://localhost"},
			wantErr: "S3_ENDPOINT",
		},
		{
			name:    "endpoint missing host",
			cfg:     Config{Bucket: "b", Region: "us-east-1", Endpoint: "http://"},
			wantErr: "S3_ENDPOINT",
		},
		// bucket validation
		{
			name:    "bucket too short",
			cfg:     Config{Bucket: "ab", Region: "us-east-1"},
			wantErr: "S3_BUCKET",
		},
		{
			name:    "bucket uppercase",
			cfg:     Config{Bucket: "MyBucket", Region: "us-east-1"},
			wantErr: "S3_BUCKET",
		},
		// region validation
		{
			name:    "region with uppercase",
			cfg:     Config{Bucket: "b", Region: "US-EAST-1"},
			wantErr: "AWS_REGION",
		},
		// credential pairing
		{
			name:    "key id without secret",
			cfg:     Config{Bucket: "b", Region: "us-east-1", AccessKeyID: "AKID"},
			wantErr: "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
		},
		{
			name:    "secret without key id",
			cfg:     Config{Bucket: "b", Region: "us-east-1", SecretAccessKey: "secret"},
			wantErr: "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
		},
		{
			name: "session token without credentials",
			cfg: Config{
				Bucket:       "b",
				Region:       "us-east-1",
				SessionToken: "token",
			},
			wantErr: "AWS_SESSION_TOKEN",
		},
		// multiple errors reported together
		{
			name: "multiple errors at once",
			cfg: Config{
				Bucket:   "AB",           // bad: uppercase + too short
				Region:   "US-EAST-1",    // bad: uppercase
				Endpoint: "ftp://host",   // bad: scheme
			},
			wantErr: "invalid configuration",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// LoadConfigFromEnv — S3_PATH_STYLE parsing
// ---------------------------------------------------------------------------

func TestLoadConfigFromEnv_PathStyle(t *testing.T) {
	tests := []struct {
		name          string
		rawValue      string
		endpoint      string
		wantPathStyle bool
		wantErr       string
	}{
		{name: "true literal", rawValue: "true", wantPathStyle: true},
		{name: "TRUE uppercase", rawValue: "TRUE", wantPathStyle: true},
		{name: "1", rawValue: "1", wantPathStyle: true},
		{name: "false literal", rawValue: "false", endpoint: "http://localhost:9000", wantPathStyle: false},
		{name: "FALSE uppercase", rawValue: "FALSE", endpoint: "http://localhost:9000", wantPathStyle: false},
		{name: "0", rawValue: "0", endpoint: "http://localhost:9000", wantPathStyle: false},
		{name: "invalid value", rawValue: "yes", wantErr: "S3_PATH_STYLE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Stamp every var LoadConfigFromEnv reads so the test is fully
			// isolated from the host environment and any .env file on disk.
			// Point S3_CONFIG_FILE at an empty temp file so the file-loading
			// path is exercised but doesn't inject unexpected values.
			emptyEnv := filepath.Join(t.TempDir(), "empty.env")
			require.NoError(t, os.WriteFile(emptyEnv, nil, 0o600))
			t.Setenv("S3_CONFIG_FILE", emptyEnv)
			t.Setenv("S3_BUCKET", "my-bucket")
			t.Setenv("S3_PATH_STYLE", tc.rawValue)
			t.Setenv("S3_ENDPOINT", tc.endpoint) // always set; empty clears it
			t.Setenv("AWS_REGION", "us-east-1")
			t.Setenv("AWS_ACCESS_KEY_ID", "")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "")
			t.Setenv("AWS_SESSION_TOKEN", "")

			cfg, err := LoadConfigFromEnv()
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantPathStyle, cfg.PathStyle)
		})
	}
}

// ---------------------------------------------------------------------------
// parseDotEnv
// ---------------------------------------------------------------------------

func TestParseDotEnv(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		want     map[string]string
		wantErr  bool
	}{
		{
			name: "simple key=value pairs",
			contents: "S3_BUCKET=my-bucket\nAWS_REGION=us-east-1\n",
			want: map[string]string{
				"S3_BUCKET":  "my-bucket",
				"AWS_REGION": "us-east-1",
			},
		},
		{
			name:     "comments and blank lines are ignored",
			contents: "# This is a comment\n\nS3_BUCKET=my-bucket\n\n# another comment\n",
			want:     map[string]string{"S3_BUCKET": "my-bucket"},
		},
		{
			name:     "export prefix is stripped",
			contents: "export S3_BUCKET=my-bucket\n",
			want:     map[string]string{"S3_BUCKET": "my-bucket"},
		},
		{
			name:     "double-quoted value stripped",
			contents: `S3_BUCKET="my-bucket"` + "\n",
			want:     map[string]string{"S3_BUCKET": "my-bucket"},
		},
		{
			name:     "single-quoted value stripped",
			contents: "S3_BUCKET='my-bucket'\n",
			want:     map[string]string{"S3_BUCKET": "my-bucket"},
		},
		{
			name:     "unquoted value preserved",
			contents: "S3_BUCKET=my-bucket\n",
			want:     map[string]string{"S3_BUCKET": "my-bucket"},
		},
		{
			name:     "line without equals is skipped",
			contents: "NO_EQUALS\nS3_BUCKET=ok\n",
			want:     map[string]string{"S3_BUCKET": "ok"},
		},
		{
			name:     "empty value",
			contents: "S3_ENDPOINT=\n",
			want:     map[string]string{"S3_ENDPOINT": ""},
		},
		{
			name:     "value with embedded equals sign",
			contents: "TOKEN=abc=def\n",
			want:     map[string]string{"TOKEN": "abc=def"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempEnv(t, tc.contents)
			got, err := parseDotEnv(path)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("non-existent file returns error", func(t *testing.T) {
		_, err := parseDotEnv("/does/not/exist.env")
		require.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// envOrFile
// ---------------------------------------------------------------------------

func TestEnvOrFile(t *testing.T) {
	fileVars := map[string]string{
		"MY_KEY": "from-file",
	}

	t.Run("env var set returns env value", func(t *testing.T) {
		t.Setenv("MY_KEY", "from-env")
		assert.Equal(t, "from-env", envOrFile("MY_KEY", fileVars))
	})

	t.Run("env var explicitly empty returns empty string", func(t *testing.T) {
		t.Setenv("MY_KEY", "")
		// LookupEnv("MY_KEY") returns ("", true) — env wins, file value ignored.
		assert.Equal(t, "", envOrFile("MY_KEY", fileVars))
	})

	t.Run("env var unset falls back to file value", func(t *testing.T) {
		t.Setenv("MY_KEY", "")
		// Unset it after Setenv so LookupEnv returns ("", false).
		os.Unsetenv("MY_KEY")
		assert.Equal(t, "from-file", envOrFile("MY_KEY", fileVars))
	})

	t.Run("key absent in both returns empty string", func(t *testing.T) {
		os.Unsetenv("ABSENT_KEY")
		assert.Equal(t, "", envOrFile("ABSENT_KEY", fileVars))
	})
}

// ---------------------------------------------------------------------------
// findConfigFile
// ---------------------------------------------------------------------------

func TestFindConfigFile(t *testing.T) {
	t.Run("S3_CONFIG_FILE env var wins", func(t *testing.T) {
		customPath := writeTempEnv(t, "")
		t.Setenv("S3_CONFIG_FILE", customPath)
		got := findConfigFile()
		assert.Equal(t, customPath, got)
	})

	t.Run("returns empty string when no config file exists and env unset", func(t *testing.T) {
		// Work in a fresh temp dir that has no .env file.
		dir := t.TempDir()
		orig, err := os.Getwd()
		require.NoError(t, err)
		require.NoError(t, os.Chdir(dir))
		t.Cleanup(func() { _ = os.Chdir(orig) })

		os.Unsetenv("S3_CONFIG_FILE")
		got := findConfigFile()
		assert.Equal(t, "", got)
	})
}
