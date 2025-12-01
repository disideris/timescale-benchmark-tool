package stats

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCollector_BasicStats verifies core statistics calculation including
// min, max, and average query times.
func TestCollector_BasicStats(t *testing.T) {
	c := NewCollector()
	c.Start()

	c.RecordQuery(100 * time.Millisecond)
	c.RecordQuery(200 * time.Millisecond)
	c.RecordQuery(300 * time.Millisecond)

	c.End()

	summary := c.GetSummary()

	assert.Equal(t, int64(3), summary.TotalQueries)
	assert.Equal(t, int64(0), summary.TotalErrors)
	assert.Equal(t, 100*time.Millisecond, summary.MinQueryTime)
	assert.Equal(t, 300*time.Millisecond, summary.MaxQueryTime)
	assert.Equal(t, 200*time.Millisecond, summary.AvgQueryTime)
}

// TestCollector_Median validates median calculation using t-digest
// approximation for odd number of samples.
func TestCollector_Median(t *testing.T) {
	c := NewCollector()
	c.Start()

	durations := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}

	for _, d := range durations {
		c.RecordQuery(d)
	}

	c.End()

	summary := c.GetSummary()

	assert.Equal(t, int64(5), summary.TotalQueries)
	assert.Equal(t, 10*time.Millisecond, summary.MinQueryTime)
	assert.Equal(t, 50*time.Millisecond, summary.MaxQueryTime)
	// T-digest approximation, allow some delta
	assert.InDelta(t, 30*time.Millisecond, summary.MedianQueryTime, float64(5*time.Millisecond))
}

// TestCollector_Errors ensures error recording is tracked separately
// from successful queries.
func TestCollector_Errors(t *testing.T) {
	c := NewCollector()
	c.Start()

	c.RecordQuery(100 * time.Millisecond)
	c.RecordError()
	c.RecordError()
	c.RecordQuery(200 * time.Millisecond)

	c.End()

	summary := c.GetSummary()

	assert.Equal(t, int64(2), summary.TotalQueries)
	assert.Equal(t, int64(2), summary.TotalErrors)
}

// TestCollector_ConcurrentRecording validates thread-safety with multiple
// goroutines recording queries simultaneously.
func TestCollector_ConcurrentRecording(t *testing.T) {
	c := NewCollector()
	c.Start()

	var wg sync.WaitGroup
	numGoroutines := 10
	queriesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < queriesPerGoroutine; j++ {
				c.RecordQuery(time.Duration(j+1) * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	c.End()

	summary := c.GetSummary()

	expectedCount := int64(numGoroutines * queriesPerGoroutine)
	assert.Equal(t, expectedCount, summary.TotalQueries)
	assert.Equal(t, 1*time.Millisecond, summary.MinQueryTime)
	assert.Equal(t, 100*time.Millisecond, summary.MaxQueryTime)
}

// TestCollector_Empty verifies collector handles zero queries gracefully
// without panics or division-by-zero errors.
func TestCollector_Empty(t *testing.T) {
	c := NewCollector()
	c.Start()
	c.End()

	summary := c.GetSummary()

	assert.Equal(t, int64(0), summary.TotalQueries)
	assert.Equal(t, int64(0), summary.TotalErrors)
}

// TestCollector_QueryCount checks that query counter increments
// correctly and can be read during execution.
func TestCollector_QueryCount(t *testing.T) {
	c := NewCollector()

	c.RecordQuery(10 * time.Millisecond)
	assert.Equal(t, int64(1), c.QueryCount())

	c.RecordQuery(20 * time.Millisecond)
	assert.Equal(t, int64(2), c.QueryCount())
}

// TestCollector_ErrorCount validates error counter increments
// correctly and can be queried mid-execution.
func TestCollector_ErrorCount(t *testing.T) {
	c := NewCollector()

	c.RecordError()
	assert.Equal(t, int64(1), c.ErrorCount())

	c.RecordError()
	assert.Equal(t, int64(2), c.ErrorCount())
}

// TestSummary_String ensures formatted text output contains
// all key statistics in human-readable form.
func TestSummary_String(t *testing.T) {
	summary := &Summary{
		TotalQueries:     100,
		TotalErrors:      5,
		TotalTime:        10 * time.Second,
		WallClockTime:    2 * time.Second,
		MinQueryTime:     10 * time.Millisecond,
		MaxQueryTime:     500 * time.Millisecond,
		AvgQueryTime:     100 * time.Millisecond,
		MedianQueryTime:  80 * time.Millisecond,
		P95QueryTime:     300 * time.Millisecond,
		P99QueryTime:     450 * time.Millisecond,
		QueriesPerSecond: 50.0,
	}

	str := summary.String()

	assert.Contains(t, str, "100")
	assert.Contains(t, str, "5")
	assert.Contains(t, str, "Minimum")
	assert.Contains(t, str, "Median")
}

// TestSummary_JSON verifies JSON serialization produces correct
// field names and converts durations to milliseconds.
func TestSummary_JSON(t *testing.T) {
	summary := &Summary{
		TotalQueries:     100,
		TotalErrors:      5,
		TotalTime:        10 * time.Second,
		WallClockTime:    2 * time.Second,
		QueriesPerSecond: 50.0,
	}

	json := summary.JSON()

	assert.Equal(t, int64(100), json["total_queries"])
	assert.Equal(t, int64(5), json["total_errors"])
	assert.Equal(t, int64(10000), json["total_time_ms"])
	assert.Equal(t, float64(50.0), json["queries_per_second"])
}

// TestCollector_Percentiles validates P95 and P99 percentile calculations
// using t-digest with 100 sequential samples.
func TestCollector_Percentiles(t *testing.T) {
	c := NewCollector()
	c.Start()

	for i := 1; i <= 100; i++ {
		c.RecordQuery(time.Duration(i) * time.Millisecond)
	}

	c.End()

	summary := c.GetSummary()

	// T-digest approximation - P95 should be around 95ms (allow larger delta)
	assert.InDelta(t, 95*time.Millisecond, summary.P95QueryTime, float64(5*time.Millisecond))

	// T-digest approximation - P99 should be around 99ms (allow larger delta)
	assert.InDelta(t, 99*time.Millisecond, summary.P99QueryTime, float64(5*time.Millisecond))
}
