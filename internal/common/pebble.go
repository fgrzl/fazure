package common

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

func snappyCompression() *block.CompressionProfile {
	return block.SnappyCompression
}

type Store struct {
	db        *pebble.DB
	writeOpts *pebble.WriteOptions
	syncOpts  *pebble.WriteOptions
}

func (s *Store) WriteOptions() *pebble.WriteOptions     { return s.writeOpts }
func (s *Store) SyncWriteOptions() *pebble.WriteOptions { return s.syncOpts }

// NewStore creates a Pebble DB tuned for Azure-Table patterns (range scans + point lookups)
// and avoids multi-second stalls seen with the previous configuration.
func NewStore(datadir string) (*Store, error) {

	opts := &pebble.Options{
		// Allow Pebble to scale compactions naturally.
		// 1–2 threads is ideal for stable latency on dev hardware.
		CompactionConcurrencyRange: func() (int, int) { return 1, 2 },

		// Major cause of your stalls was 64MB memtables producing huge L0 files.
		// 8–16MB is a sweet spot for scan-heavy workloads.
		MemTableSize: 16 << 20, // 16 MB

		// Larger cache smooths read latency during prefix scans & JSON decode.
		Cache: pebble.NewCache(256 << 20), // 256 MB

		// Level options tuned for mixed point + range queries.
		Levels: [7]pebble.LevelOptions{
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
			{FilterPolicy: bloom.FilterPolicy(10), Compression: snappyCompression},
		},

		// These two *do* exist and control L0 pressure.
		// They prevent runaway L0 buildup and stop the long read stalls you observed.
		L0CompactionThreshold: 4,
		L0StopWritesThreshold: 12,

		// Recommended for modern Pebble engines.
		FormatMajorVersion: pebble.FormatNewest,
	}

	db, err := pebble.Open(datadir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &Store{
		db:        db,
		writeOpts: &pebble.WriteOptions{Sync: false},
		syncOpts:  &pebble.WriteOptions{Sync: true},
	}, nil
}

func (s *Store) DB() *pebble.DB { return s.db }

func (s *Store) Metrics() string { return s.db.Metrics().String() }

func (s *Store) Close() error { return s.db.Close() }
