package tables

import "errors"

var (
	ErrTableNotFound      = errors.New("table not found")
	ErrTableExists        = errors.New("table already exists")
	ErrEntityNotFound     = errors.New("entity not found")
	ErrEntityExists       = errors.New("entity already exists")
	ErrInvalidEntity      = errors.New("invalid entity")
	ErrPreconditionFailed = errors.New("precondition failed")

	// Batch operation errors
	ErrInvalidBatchRequest    = errors.New("invalid batch request")
	ErrBatchPartitionMismatch = errors.New("all operations in a batch must target the same partition")
)
