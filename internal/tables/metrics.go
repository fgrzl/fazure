package tables

import (
	"fmt"
	"sync/atomic"
	"time"
)

// slowQueryThreshold flags queries that take noticeably longer than typical emulator latency.
const slowQueryThreshold = 200 * time.Millisecond

// QueryMetric captures a single query observation for metrics accounting.
type QueryMetric struct {
	Duration     time.Duration
	Returned     int
	Scanned      int
	FullScan     bool
	Err          bool
	FilterError  bool
	Continuation bool
}

// Metrics holds lightweight counters and timers for table operations.
type Metrics struct {
	queriesTotal         uint64
	queriesErrors        uint64
	queriesFullScans     uint64
	queriesFilterErrors  uint64
	queriesDurationMs    uint64
	queriesMaxDurationMs uint64
	queriesSlow          uint64
	queriesReturned      uint64
	queriesScanned       uint64
	queriesContinuations uint64
}

// NewMetrics creates an empty metrics sink.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// ObserveQuery records a query execution snapshot.
func (m *Metrics) ObserveQuery(q QueryMetric) {
	if m == nil {
		return
	}

	atomic.AddUint64(&m.queriesTotal, 1)
	atomic.AddUint64(&m.queriesReturned, uint64(q.Returned))
	atomic.AddUint64(&m.queriesScanned, uint64(q.Scanned))

	durMs := uint64(q.Duration.Milliseconds())
	atomic.AddUint64(&m.queriesDurationMs, durMs)
	for {
		prev := atomic.LoadUint64(&m.queriesMaxDurationMs)
		if durMs <= prev || atomic.CompareAndSwapUint64(&m.queriesMaxDurationMs, prev, durMs) {
			break
		}
	}

	if q.Err {
		atomic.AddUint64(&m.queriesErrors, 1)
	}
	if q.FullScan {
		atomic.AddUint64(&m.queriesFullScans, 1)
	}
	if q.FilterError {
		atomic.AddUint64(&m.queriesFilterErrors, 1)
	}
	if q.Duration >= slowQueryThreshold {
		atomic.AddUint64(&m.queriesSlow, 1)
	}
	if q.Continuation {
		atomic.AddUint64(&m.queriesContinuations, 1)
	}
}

// MetricsSnapshot is a human-friendly snapshot of current counters.
type MetricsSnapshot struct {
	QueriesTotal         uint64
	QueriesErrors        uint64
	QueriesFullScans     uint64
	QueriesFilterErrors  uint64
	QueriesDurationMsSum uint64
	QueriesDurationMsMax uint64
	QueriesSlow          uint64
	QueriesReturned      uint64
	QueriesScanned       uint64
	QueriesContinuations uint64
	QueriesDurationMsAvg uint64
}

// Snapshot captures a consistent view of current metrics.
func (m *Metrics) Snapshot() MetricsSnapshot {
	total := atomic.LoadUint64(&m.queriesTotal)
	duration := atomic.LoadUint64(&m.queriesDurationMs)

	avg := uint64(0)
	if total > 0 {
		avg = duration / total
	}

	return MetricsSnapshot{
		QueriesTotal:         total,
		QueriesErrors:        atomic.LoadUint64(&m.queriesErrors),
		QueriesFullScans:     atomic.LoadUint64(&m.queriesFullScans),
		QueriesFilterErrors:  atomic.LoadUint64(&m.queriesFilterErrors),
		QueriesDurationMsSum: duration,
		QueriesDurationMsMax: atomic.LoadUint64(&m.queriesMaxDurationMs),
		QueriesSlow:          atomic.LoadUint64(&m.queriesSlow),
		QueriesReturned:      atomic.LoadUint64(&m.queriesReturned),
		QueriesScanned:       atomic.LoadUint64(&m.queriesScanned),
		QueriesContinuations: atomic.LoadUint64(&m.queriesContinuations),
		QueriesDurationMsAvg: avg,
	}
}

// String renders the metrics snapshot as a text block for /debug endpoints.
func (m *Metrics) String() string {
	s := m.Snapshot()
	return fmt.Sprintf(
		"queries_total=%d\nqueries_errors=%d\nqueries_full_scans=%d\nqueries_filter_errors=%d\nqueries_slow=%d\nqueries_duration_ms_sum=%d\nqueries_duration_ms_avg=%d\nqueries_duration_ms_max=%d\nqueries_returned=%d\nqueries_scanned=%d\nqueries_continuations=%d\n",
		s.QueriesTotal,
		s.QueriesErrors,
		s.QueriesFullScans,
		s.QueriesFilterErrors,
		s.QueriesSlow,
		s.QueriesDurationMsSum,
		s.QueriesDurationMsAvg,
		s.QueriesDurationMsMax,
		s.QueriesReturned,
		s.QueriesScanned,
		s.QueriesContinuations,
	)
}
