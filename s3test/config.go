package s3test

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
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

// findConfigFile returns the path to a .env config file to load.
// It checks (in order):
//  1. The path set in S3_CONFIG_FILE (custom override)
//  2. .env in the current directory
//  3. .env one directory up (project root when tests run from s3test/)
func findConfigFile() string {
	if p := os.Getenv("S3_CONFIG_FILE"); p != "" {
		return p
	}
	for _, p := range []string{".env", "../.env"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// parseDotEnv reads a .env-style file and returns a map of key→value pairs.
// Blank lines and lines starting with # are ignored.
// Values may be optionally quoted with single or double quotes.
// The "export " prefix is accepted but not required.
func parseDotEnv(filename string) (map[string]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	vars := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Strip optional "export " prefix.
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		// Strip surrounding single or double quotes.
		if len(v) >= 2 && ((v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'')) {
			v = v[1 : len(v)-1]
		}
		vars[k] = v
	}
	return vars, scanner.Err()
}

// envOrFile returns the value of the environment variable key, falling back
// to the value from fileVars only when the environment variable is not set at
// all. An explicitly-set-but-empty env var (e.g. S3_ENDPOINT="") takes
// precedence over the file, keeping "env vars always win" semantics intact.
func envOrFile(key string, fileVars map[string]string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fileVars[key]
}

// LoadConfigFromEnv reads configuration from environment variables, optionally
// supplemented by a .env config file. Environment variables always take
// precedence over file values. All parameters are validated before returning.
//
// Config file lookup order (first found wins):
//  1. Path specified by S3_CONFIG_FILE environment variable
//  2. .env in the current directory
//  3. .env one directory up (project root when running go test ./s3test/)
//
// Returns an error if required variables are missing or any value is invalid.
func LoadConfigFromEnv() (Config, error) {
	fileVars := map[string]string{}
	if path := findConfigFile(); path != "" {
		var err error
		fileVars, err = parseDotEnv(path)
		if err != nil {
			return Config{}, fmt.Errorf("loading config file %s: %w", path, err)
		}
	}

	bucket := envOrFile("S3_BUCKET", fileVars)
	if bucket == "" {
		return Config{}, fmt.Errorf("S3_BUCKET is required (set it in the environment or in a .env file)")
	}

	region := envOrFile("AWS_REGION", fileVars)
	if region == "" {
		region = "us-east-1"
	}

	endpoint := envOrFile("S3_ENDPOINT", fileVars)

	// Default path-style to true when a custom endpoint is set (MinIO/LocalStack need it).
	pathStyle := endpoint != ""
	if v := envOrFile("S3_PATH_STYLE", fileVars); v != "" {
		switch strings.ToLower(v) {
		case "true", "1":
			pathStyle = true
		case "false", "0":
			pathStyle = false
		default:
			return Config{}, fmt.Errorf("S3_PATH_STYLE: invalid value %q (accepted: true, false, 1, 0)", v)
		}
	}

	cfg := Config{
		Endpoint:        endpoint,
		Region:          region,
		Bucket:          bucket,
		AccessKeyID:     envOrFile("AWS_ACCESS_KEY_ID", fileVars),
		SecretAccessKey: envOrFile("AWS_SECRET_ACCESS_KEY", fileVars),
		SessionToken:    envOrFile("AWS_SESSION_TOKEN", fileVars),
		PathStyle:       pathStyle,
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate checks all Config fields for correctness, collecting every problem
// into a single error so the caller sees everything at once.
func (c Config) validate() error {
	var errs []string

	if err := validateBucketName(c.Bucket); err != nil {
		errs = append(errs, "S3_BUCKET: "+err.Error())
	}
	if c.Endpoint != "" {
		if err := validateHTTPURL(c.Endpoint); err != nil {
			errs = append(errs, "S3_ENDPOINT: "+err.Error())
		}
	}
	if err := validateRegion(c.Region); err != nil {
		errs = append(errs, "AWS_REGION: "+err.Error())
	}
	// Credentials must be supplied as a pair.
	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") {
		errs = append(errs, "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must both be set or both be empty")
	}
	// A session token is only meaningful when static credentials are present.
	if c.SessionToken != "" && (c.AccessKeyID == "" || c.SecretAccessKey == "") {
		errs = append(errs, "AWS_SESSION_TOKEN requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// validateBucketName checks that name follows S3 bucket naming rules:
//   - 3–63 characters
//   - Only lowercase letters, digits, hyphens, and dots
//   - Must start and end with a letter or digit
//   - No consecutive dots
//   - Not formatted as an IP address
func validateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("must be 3–63 characters long (got %d)", len(name))
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '.':
		default:
			return fmt.Errorf("invalid character %q at position %d "+
				"(only lowercase letters, digits, hyphens, and dots allowed)", r, i)
		}
	}
	if first := rune(name[0]); first == '-' || first == '.' {
		return fmt.Errorf("must start with a letter or digit (starts with %q)", first)
	}
	if last := rune(name[len(name)-1]); last == '-' || last == '.' {
		return fmt.Errorf("must end with a letter or digit (ends with %q)", last)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("must not contain consecutive dots")
	}
	if net.ParseIP(name) != nil {
		return fmt.Errorf("must not be formatted as an IP address")
	}
	return nil
}

// validateHTTPURL checks that rawURL is a syntactically valid HTTP or HTTPS
// URL with a non-empty host.
func validateHTTPURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("not a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https (got %q)", u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("missing host")
	}
	return nil
}

// validateRegion checks that region contains only lowercase letters, digits,
// and hyphens — the characters used in all AWS and S3-compatible region names.
func validateRegion(region string) error {
	if len(region) < 2 || len(region) > 30 {
		return fmt.Errorf("must be 2–30 characters long (got %d)", len(region))
	}
	for i, r := range region {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-':
		default:
			return fmt.Errorf("invalid character %q at position %d "+
				"(only lowercase letters, digits, and hyphens allowed)", r, i)
		}
	}
	return nil
}

// LoadConfig reads configuration from environment variables (and an optional
// .env file) and fails the test if required variables are missing or invalid.
func LoadConfig(t testing.TB) Config {
	t.Helper()
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
