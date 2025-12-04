package common

import (
	"net/http"
	"time"
)

// CheckConditionalHeaders validates conditional request headers
// Returns (shouldProceed bool, statusCode int)
func CheckConditionalHeaders(r *http.Request, currentETag string, lastModified time.Time) (bool, int) {
	// If-Match: proceed only if ETag matches
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if ifMatch != "*" && ifMatch != currentETag {
			return false, http.StatusPreconditionFailed
		}
	}

	// If-None-Match: proceed only if ETag doesn't match
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if ifNoneMatch == "*" || ifNoneMatch == currentETag {
			if r.Method == http.MethodGet || r.Method == http.MethodHead {
				return false, http.StatusNotModified
			}
			return false, http.StatusPreconditionFailed
		}
	}

	// If-Modified-Since: proceed only if modified since date
	if ifModSince := r.Header.Get("If-Modified-Since"); ifModSince != "" {
		t, err := time.Parse(http.TimeFormat, ifModSince)
		if err == nil && lastModified.Before(t) {
			return false, http.StatusNotModified
		}
	}

	// If-Unmodified-Since: proceed only if not modified since date
	if ifUnmodSince := r.Header.Get("If-Unmodified-Since"); ifUnmodSince != "" {
		t, err := time.Parse(http.TimeFormat, ifUnmodSince)
		if err == nil && lastModified.After(t) {
			return false, http.StatusPreconditionFailed
		}
	}

	return true, http.StatusOK
}
