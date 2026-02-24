package s3test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "quoted etag",
			input:    `"d41d8cd98f00b204e9800998ecf8427e"`,
			expected: "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:     "unquoted etag",
			input:    "d41d8cd98f00b204e9800998ecf8427e",
			expected: "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "just quotes",
			input:    `""`,
			expected: "",
		},
		{
			name:     "leading quote only",
			input:    `"abc`,
			expected: "abc",
		},
		{
			name:     "trailing quote only",
			input:    `abc"`,
			expected: "abc",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, stripQuotes(tc.input))
		})
	}
}
