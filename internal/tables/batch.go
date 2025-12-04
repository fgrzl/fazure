package tables

import (
	"bufio"
	"bytes"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
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

		// Read part content type to check if it's a changeset
		partContentType := part.Header.Get("Content-Type")
		if strings.Contains(partContentType, "multipart/mixed") {
			// This is a changeset, parse its inner operations
			_, changesetParams, err := mime.ParseMediaType(partContentType)
			if err != nil {
				part.Close()
				continue
			}
			changesetBoundary := changesetParams["boundary"]
			if changesetBoundary == "" {
				part.Close()
				continue
			}

			// Read the part content into a buffer
			partContent, err := io.ReadAll(part)
			if err != nil {
				part.Close()
				continue
			}

			// Parse the changeset as multipart
			changesetReader := multipart.NewReader(bytes.NewReader(partContent), changesetBoundary)
			for {
				opPart, err := changesetReader.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					continue
				}

				// Each operation part contains an HTTP request
				opContent, err := io.ReadAll(opPart)
				if err != nil {
					opPart.Close()
					continue
				}

				// Parse the HTTP request from the content
				httpReq, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(opContent)))
				if err != nil {
					opPart.Close()
					continue
				}

				body, _ := io.ReadAll(httpReq.Body)
				httpReq.Body.Close()

				op := BatchOperation{
					Method:  httpReq.Method,
					URL:     httpReq.URL.String(),
					Headers: httpReq.Header,
					Body:    body,
				}
				batchReq.Operations = append(batchReq.Operations, op)

				opPart.Close()
			}
		}

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
