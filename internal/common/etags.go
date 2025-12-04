package common

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"
)

// Generate creates an ETag based on timestamp
// Azure Table Storage ETags are typically W/"datetime'<timestamp>'" format
func Generate(t time.Time) string {
	hash := md5.Sum([]byte(t.Format(time.RFC3339Nano)))
	return fmt.Sprintf(`W/"%s"`, fmt.Sprintf("%x", hash))
}

// Validate checks if an ETag matches
func Validate(etag string, t time.Time) bool {
	expected := Generate(t)
	return etag == expected || etag == "*"
}

// GenerateETag generates an ETag for the given data
func GenerateETag(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf(`"%s"`, base64.StdEncoding.EncodeToString(hash[:]))
}

// GenerateETagFromTime generates an ETag based on timestamp
// Useful for metadata-only updates
func GenerateETagFromTime(t time.Time) string {
	data := []byte(t.Format(time.RFC3339Nano))
	return GenerateETag(data)
}

// ValidateETag checks if the provided ETag matches the expected ETag
func ValidateETag(provided, expected string) bool {
	if provided == "" || provided == "*" {
		return true
	}
	return provided == expected
}
