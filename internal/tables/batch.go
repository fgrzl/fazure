package tables

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"regexp"
	"strings"
)

// BatchRequest represents a batch request
type BatchRequest struct {
	Operations []BatchOperation
	TableName  string
}

// BatchOperation represents a single operation in a batch
type BatchOperation struct {
	Method       string
	URL          string
	Headers      http.Header
	Body         []byte
	PartitionKey string
	RowKey       string
	TableName    string
	EntityData   map[string]interface{}
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

				// Parse body to extract PartitionKey and RowKey
				var entityData map[string]interface{}
				var pk, rk string
				if len(body) > 0 {
					if err := json.Unmarshal(body, &entityData); err == nil {
						if p, ok := entityData["PartitionKey"].(string); ok {
							pk = p
						}
						if r, ok := entityData["RowKey"].(string); ok {
							rk = r
						}
					}
				}

				// Extract table name from URL path
				// URL looks like: /devstoreaccount1/tablename or
				// /devstoreaccount1/tablename(PartitionKey='pk',RowKey='rk')
				urlPath := strings.Trim(httpReq.URL.Path, "/")
				urlParts := strings.Split(urlPath, "/")
				var tableName string
				if len(urlParts) >= 2 {
					tablePart := urlParts[1]
					// Check if the URL contains entity keys
					// Format: tablename(PartitionKey='pk',RowKey='rk')
					entityPattern := regexp.MustCompile(`^([^(]+)\(PartitionKey='([^']+)',RowKey='([^']+)'\)$`)
					if matches := entityPattern.FindStringSubmatch(tablePart); len(matches) == 4 {
						tableName = matches[1]
						// Extract PK/RK from URL if not in body
						if pk == "" {
							pk = matches[2]
						}
						if rk == "" {
							rk = matches[3]
						}
					} else {
						tableName = tablePart
					}
				}

				// Set table name on the batch request if not already set
				if batchReq.TableName == "" && tableName != "" {
					batchReq.TableName = tableName
				}

				op := BatchOperation{
					Method:       httpReq.Method,
					URL:          httpReq.URL.String(),
					Headers:      httpReq.Header,
					Body:         body,
					PartitionKey: pk,
					RowKey:       rk,
					TableName:    tableName,
					EntityData:   entityData,
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

	// Validate total batch size (4MB maximum)
	totalSize := 0
	for _, op := range req.Operations {
		totalSize += len(op.Body)
	}
	if totalSize > 4*1024*1024 {
		return ErrInvalidBatchRequest
	}

	firstTable := req.Operations[0].TableName
	if firstTable == "" {
		return ErrInvalidBatchRequest
	}
	req.TableName = firstTable

	firstPartition := req.Operations[0].PartitionKey
	for _, op := range req.Operations[1:] {
		if op.TableName != firstTable {
			return ErrInvalidBatchRequest
		}
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
