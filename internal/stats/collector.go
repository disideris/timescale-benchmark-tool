package stats

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/tdigest"
)

// Collector collects and aggregates benchmark statistics and is thread-safe
type Collector struct {
	mu sync.Mutex

	queryCount  atomic.Int64 // Successful queries
	errorCount  atomic.Int64 // Failed queries
	totalTimeNs atomic.Int64 // Sum of all query times
	minTimeNs   atomic.Int64 // Minimum query time
	maxTimeNs   atomic.Int64 // Maximum query time

	digest *tdigest.TDigest // For percentile calculation (mutex-protected)

	startTime time.Time // Benchmark start time
	endTime   time.Time // Benchmark end time
}

// NewCollector creates a new statistics collector
func NewCollector() *Collector {
	c := &Collector{
		digest: tdigest.NewWithCompression(100),
	}
	c.minTimeNs.Store(math.MaxInt64)
	c.maxTimeNs.Store(0)
	return c
}

// Start marks the start of the benchmark
func (c *Collector) Start() {
	c.startTime = time.Now()
}

// End marks the end of the benchmark
func (c *Collector) End() {
	c.endTime = time.Now()
}

// RecordQuery records a single query duration
func (c *Collector) RecordQuery(duration time.Duration) {
	ns := duration.Nanoseconds()

	c.queryCount.Add(1)
	c.totalTimeNs.Add(ns)
	c.updateMin(ns)
	c.updateMax(ns)

	c.mu.Lock()
	c.digest.Add(float64(ns), 1)
	c.mu.Unlock()
}

// RecordError records a query error
func (c *Collector) RecordError() {
	c.errorCount.Add(1)
}

// QueryCount returns the number of successful queries
func (c *Collector) QueryCount() int64 {
	return c.queryCount.Load()
}

// ErrorCount returns the number of failed queries
func (c *Collector) ErrorCount() int64 {
	return c.errorCount.Load()
}

// Summary contains the final benchmark statistics
type Summary struct {
	TotalQueries     int64
	TotalErrors      int64
	TotalTime        time.Duration // Sum of all query times
	WallClockTime    time.Duration // Actual elapsed time
	MinQueryTime     time.Duration
	MaxQueryTime     time.Duration
	AvgQueryTime     time.Duration
	MedianQueryTime  time.Duration
	P95QueryTime     time.Duration
	P99QueryTime     time.Duration
	QueriesPerSecond float64
}

// GetSummary returns the final benchmark statistics
func (c *Collector) GetSummary() *Summary {
	count := c.queryCount.Load()
	totalNs := c.totalTimeNs.Load()

	summary := &Summary{
		TotalQueries:  count,
		TotalErrors:   c.errorCount.Load(),
		TotalTime:     time.Duration(totalNs),
		WallClockTime: c.endTime.Sub(c.startTime),
	}

	if count == 0 {
		return summary
	}

	summary.MinQueryTime = time.Duration(c.minTimeNs.Load())
	summary.MaxQueryTime = time.Duration(c.maxTimeNs.Load())
	summary.AvgQueryTime = time.Duration(totalNs / count)

	c.calculatePercentiles(summary)

	if summary.WallClockTime > 0 {
		summary.QueriesPerSecond = float64(count) / summary.WallClockTime.Seconds()
	}

	return summary
}

// String returns a formatted string representation of the summary
func (s *Summary) String() string {
	return fmt.Sprintf(`
Benchmark Results
=================
Queries processed:    %d
Queries failed:       %d
Total processing time: %v
Wall clock time:      %v
Queries per second:   %.2f

Query Time Statistics
---------------------
Minimum:  %v
Average:  %v
Median:   %v
P95:      %v
P99:      %v
Maximum:  %v`,
		s.TotalQueries,
		s.TotalErrors,
		s.TotalTime,
		s.WallClockTime,
		s.QueriesPerSecond,
		s.MinQueryTime,
		s.AvgQueryTime,
		s.MedianQueryTime,
		s.P95QueryTime,
		s.P99QueryTime,
		s.MaxQueryTime,
	)
}

// JSON returns the summary as a map for JSON encoding
func (s *Summary) JSON() map[string]interface{} {
	return map[string]interface{}{
		"total_queries":        s.TotalQueries,
		"total_errors":         s.TotalErrors,
		"total_time_ms":        s.TotalTime.Milliseconds(),
		"wall_clock_time_ms":   s.WallClockTime.Milliseconds(),
		"queries_per_second":   s.QueriesPerSecond,
		"min_query_time_ms":    s.MinQueryTime.Milliseconds(),
		"avg_query_time_ms":    s.AvgQueryTime.Milliseconds(),
		"median_query_time_ms": s.MedianQueryTime.Milliseconds(),
		"p95_query_time_ms":    s.P95QueryTime.Milliseconds(),
		"p99_query_time_ms":    s.P99QueryTime.Milliseconds(),
		"max_query_time_ms":    s.MaxQueryTime.Milliseconds(),
	}
}

// updateMin atomically updates the minimum value using CAS
func (c *Collector) updateMin(ns int64) {
	for {
		old := c.minTimeNs.Load()
		if ns >= old {
			break
		}
		if c.minTimeNs.CompareAndSwap(old, ns) {
			break
		}
	}
}

// updateMax atomically updates the maximum value using CAS
func (c *Collector) updateMax(ns int64) {
	for {
		old := c.maxTimeNs.Load()
		if ns <= old {
			break
		}
		if c.maxTimeNs.CompareAndSwap(old, ns) {
			break
		}
	}
}

// calculatePercentiles computes percentile statistics using t-digest
func (c *Collector) calculatePercentiles(summary *Summary) {
	// Mutex ensures memory visibility of digest state after concurrent updates.
	// While called after workers finish, this is defensive against
	// cache effects and ensures proper happens-before guarantees of Go memory model.
	c.mu.Lock()
	defer c.mu.Unlock()

	summary.MedianQueryTime = time.Duration(c.digest.Quantile(0.5))
	summary.P95QueryTime = time.Duration(c.digest.Quantile(0.95))
	summary.P99QueryTime = time.Duration(c.digest.Quantile(0.99))
}
