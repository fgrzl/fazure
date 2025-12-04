package tables

import (
	"io"
	"mime"
	"mime/multipart"
	"net/http"
)

// BatchRequest represents a batch request
type BatchRequest struct {
	Operations []BatchOperation
}

// BatchOperation represents a single operation in a batch
type BatchOperation struct {
	Method       string
	URL          string
	Headers      http.Header
	Body         []byte
	PartitionKey string
	RowKey       string
}

// BatchResponse represents a batch response
type BatchResponse struct {
	Responses []BatchOperationResponse
}

// BatchOperationResponse represents a response for a single batch operation
type BatchOperationResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

// ParseBatchRequest parses a multipart batch request
func ParseBatchRequest(r *http.Request) (*BatchRequest, error) {
	contentType := r.Header.Get("Content-Type")

	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, err
	}

	boundary := params["boundary"]
	if boundary == "" {
		return nil, ErrInvalidBatchRequest
	}

	reader := multipart.NewReader(r.Body, boundary)

	batchReq := &BatchRequest{
		Operations: make([]BatchOperation, 0),
	}

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// TODO: Parse individual operations from multipart
		// Each part contains an HTTP request

		part.Close()
	}

	return batchReq, nil
}

// ValidateBatchRequest validates that all operations target the same partition
func ValidateBatchRequest(req *BatchRequest) error {
	if len(req.Operations) == 0 {
		return ErrInvalidBatchRequest
	}

	if len(req.Operations) > 100 {
		return ErrInvalidBatchRequest
	}

	firstPartition := req.Operations[0].PartitionKey
	for _, op := range req.Operations[1:] {
		if op.PartitionKey != firstPartition {
			return ErrBatchPartitionMismatch
		}
	}

	return nil
}

// ExecuteBatch executes a batch of operations atomically
func ExecuteBatch(store *TableStore, req *BatchRequest) (*BatchResponse, error) {
	if err := ValidateBatchRequest(req); err != nil {
		return nil, err
	}

	// TODO: Implement atomic batch execution using Pebble batch
	// All operations must succeed or all fail
	_ = store

	response := &BatchResponse{
		Responses: make([]BatchOperationResponse, len(req.Operations)),
	}

	return response, nil
}
