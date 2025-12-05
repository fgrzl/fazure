package common

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// Store provides a shared Pebble instance for all emulators
type Store struct {
	db        *pebble.DB
	writeOpts *pebble.WriteOptions
	syncOpts  *pebble.WriteOptions
}

// WriteOptions returns the default write options (async for better performance)
func (s *Store) WriteOptions() *pebble.WriteOptions {
	return s.writeOpts
}

// SyncWriteOptions returns synchronous write options for critical operations
func (s *Store) SyncWriteOptions() *pebble.WriteOptions {
	return s.syncOpts
}

// NewStore creates a new shared storage instance
func NewStore(datadir string) (*Store, error) {
	opts := &pebble.Options{
		MaxConcurrentCompactions: func() int { return 4 },
		// Use bloom filters for faster point lookups
		Levels: []pebble.LevelOptions{
			{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
		},
		// Increase memtable size for better write performance
		MemTableSize: 64 * 1024 * 1024, // 64MB
		// Cache for faster reads
		Cache: pebble.NewCache(128 * 1024 * 1024), // 128MB cache
	}

	db, err := pebble.Open(datadir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &Store{
		db:        db,
		writeOpts: &pebble.WriteOptions{Sync: false}, // Async writes for performance
		syncOpts:  &pebble.WriteOptions{Sync: true},  // Sync writes when durability is critical
	}, nil
}

// DB returns the underlying Pebble database
func (s *Store) DB() *pebble.DB {
	return s.db
}

// Close closes the Pebble database
func (s *Store) Close() error {
	return s.db.Close()
}
