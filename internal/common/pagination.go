package common

import (
	"encoding/base64"
	"fmt"
	"net/url"
)

// ContinuationToken represents pagination state
type ContinuationToken struct {
	Offset int
	Marker string
}

// EncodeContinuationToken encodes a token for the next page
func EncodeContinuationToken(offset int) string {
	data := fmt.Sprintf("offset=%d", offset)
	return base64.StdEncoding.EncodeToString([]byte(data))
}

// DecodeContinuationToken decodes a continuation token
func DecodeContinuationToken(token string) (int, error) {
	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, err
	}

	var offset int
	_, err = fmt.Sscanf(string(data), "offset=%d", &offset)
	return offset, err
}

// GetPaginationParams extracts pagination parameters from query string
func GetPaginationParams(query url.Values, defaultPageSize int) (pageSize int, continuationToken string) {
	pageSize = defaultPageSize

	// Azure uses different param names for different services
	if top := query.Get("$top"); top != "" {
		fmt.Sscanf(top, "%d", &pageSize)
	}
	if maxResults := query.Get("maxresults"); maxResults != "" {
		fmt.Sscanf(maxResults, "%d", &pageSize)
	}

	// Continuation token can be named differently
	if marker := query.Get("marker"); marker != "" {
		continuationToken = marker
	}
	if nextMarker := query.Get("next-marker"); nextMarker != "" {
		continuationToken = nextMarker
	}

	return pageSize, continuationToken
}

// AppendContinuationHeader adds next continuation token to response header
func AppendContinuationHeader(values url.Values, nextToken string) string {
	if nextToken == "" {
		return ""
	}

	q := url.Values{}
	for k, v := range values {
		q[k] = v
	}
	q.Set("marker", nextToken)
	return q.Encode()
}
