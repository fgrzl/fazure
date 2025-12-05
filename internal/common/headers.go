package common

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SetResponseHeaders adds standard Azure Storage response headers
func SetResponseHeaders(w http.ResponseWriter, eTag string) {
	w.Header().Set("x-ms-version", "2023-11-03")
	w.Header().Set("x-ms-request-id", GenerateRequestID())
	w.Header().Set("Date", time.Now().UTC().Format(http.TimeFormat))
	if eTag != "" {
		w.Header().Set("ETag", eTag)
	}
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
}

// SetBlobHeaders adds blob-specific headers
func SetBlobHeaders(w http.ResponseWriter, contentType string, contentLength int64) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLength))
	w.Header().Set("Accept-Ranges", "bytes")
}

// WriteErrorResponse writes an Azure Storage error response
func WriteErrorResponse(w http.ResponseWriter, statusCode int, errorCode, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>%s</Code>
  <Message>%s</Message>
  <RequestId>%s</RequestId>
</Error>`, errorCode, message, GenerateRequestID())
}

// GenerateRequestID creates a unique request ID
func GenerateRequestID() string {
	return uuid.NewString()
}

// ValidateSASToken validates a SAS token signature
func ValidateSASToken(r *http.Request, accountKey string) bool {
	sig := r.URL.Query().Get("sig")
	if sig == "" {
		return false
	}

	// Build signature string (simplified - real Azure uses specific format)
	method := r.Method
	path := r.URL.Path
	query := r.URL.RawQuery
	// Remove sig from query for verification
	query = strings.ReplaceAll(query, "&sig="+sig, "")
	query = strings.ReplaceAll(query, "sig="+sig+"&", "")

	signatureString := fmt.Sprintf("%s\n%s\n%s", method, path, query)

	// Verify HMAC-SHA256
	decodedKey, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return false
	}

	h := hmac.New(sha256.New, decodedKey)
	h.Write([]byte(signatureString))
	expectedSig := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return hmac.Equal([]byte(sig), []byte(expectedSig))
}

// ValidateSharedKey validates Shared Key authentication
func ValidateSharedKey(r *http.Request, accountKey string) bool {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}

	// Format: SharedKey accountName:signature
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || parts[0] != "SharedKey" {
		return false
	}

	sig := strings.Split(parts[1], ":")
	if len(sig) != 2 {
		return false
	}

	// Build canonical string for signing
	canonicalString := buildCanonicalString(r)

	// Verify HMAC-SHA256
	decodedKey, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return false
	}

	h := hmac.New(sha256.New, decodedKey)
	h.Write([]byte(canonicalString))
	expectedSig := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return hmac.Equal([]byte(sig[1]), []byte(expectedSig))
}

// buildCanonicalString builds the canonical string for SharedKey signing (simplified)
func buildCanonicalString(r *http.Request) string {
	// Simplified version - real Azure has specific header ordering and rules
	headers := []string{
		r.Method,
		r.Header.Get("Content-Encoding"),
		r.Header.Get("Content-Language"),
		r.Header.Get("Content-Length"),
		r.Header.Get("Content-MD5"),
		r.Header.Get("Content-Type"),
		r.Header.Get("Date"),
		r.Header.Get("If-Modified-Since"),
		r.Header.Get("If-Match"),
		r.Header.Get("If-None-Match"),
		r.Header.Get("If-Unmodified-Since"),
		r.Header.Get("Range"),
	}

	return strings.Join(headers, "\n") + "\n" + r.URL.Path
}
