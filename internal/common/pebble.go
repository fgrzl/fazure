package common

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

// snappyCompression returns a Snappy compression profile for level options
func snappyCompression() *block.CompressionProfile {
	return block.SnappyCompression
}

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
		// Control concurrent compactions (min 4, max 4)
		CompactionConcurrencyRange: func() (int, int) { return 4, 4 },
		// Use bloom filters for faster point lookups
		Levels: [7]pebble.LevelOptions{
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
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
