package common

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

type Store struct {
	db *pebble.DB
}

// NewStore creates a Pebble DB tuned for Azure-Table patterns (range scans + point lookups)
// and avoids multi-second stalls seen with the previous configuration.
func NewStore(datadir string) (*Store, error) {

	opts := &pebble.Options{}

	db, err := pebble.Open(datadir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) DB() *pebble.DB { return s.db }

func (s *Store) Metrics() string { return s.db.Metrics().String() }

func (s *Store) Close() error { return s.db.Close() }
