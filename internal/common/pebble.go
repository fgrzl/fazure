package common

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

// Store provides a shared Pebble instance for all emulators
type Store struct {
	db *pebble.DB
	mu sync.RWMutex
}

// NewStore creates a new shared storage instance
func NewStore(datadir string) (*Store, error) {
	opts := &pebble.Options{
		MaxConcurrentCompactions: func() int { return 2 },
	}

	db, err := pebble.Open(datadir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &Store{db: db}, nil
}

// DB returns the underlying Pebble database
func (s *Store) DB() *pebble.DB {
	return s.db
}

// Close closes the Pebble database
func (s *Store) Close() error {
	return s.db.Close()
}
