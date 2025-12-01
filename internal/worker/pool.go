package worker

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tsdb-bench/internal/csv"
	"github.com/tsdb-bench/internal/db"
	"github.com/tsdb-bench/internal/hasher"
	"github.com/tsdb-bench/internal/stats"
	"go.uber.org/zap"
)

const (
	defaultQueueDepth = 1000
)

// Worker represents a single worker that processes queries.
type Worker struct {
	id      int
	dbPool  *db.Pool // Shared connection pool (acquire per query)
	tasks   chan *csv.Query
	ctx     context.Context
	timeout time.Duration
	log     *zap.Logger

	// Backpressure monitoring
	backpressureEvents atomic.Int64
}

// Pool manages a pool of workers for concurrent query execution.
type Pool struct {
	workers    []*Worker
	hasher     *hasher.ConsistentHasher
	collector  *stats.Collector
	dbPool     *db.Pool
	numWorkers int
	queueDepth int
	timeout    time.Duration
	log        *zap.Logger
	workerWg   sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc

	// Tracking
	processed         atomic.Int64
	submitted         atomic.Int64
	totalBackpressure atomic.Int64 // Total times we hit backpressure
	maxQueueDepthSeen atomic.Int64 // Maximum queue depth observed

	// Shutdown state
	shutdownOnce sync.Once
}

// PoolConfig holds configuration for the worker pool.
type PoolConfig struct {
	NumWorkers   int
	QueueDepth   int
	QueryTimeout time.Duration
	Logger       *zap.Logger
}

// NewPool creates a new worker pool
func NewPool(ctx context.Context, dbPool *db.Pool, collector *stats.Collector, cfg PoolConfig) (*Pool, error) {
	cfg = normalizeConfig(cfg)
	ctx, cancel := context.WithCancel(ctx)

	p := &Pool{
		workers:    make([]*Worker, cfg.NumWorkers),
		hasher:     hasher.New(cfg.NumWorkers),
		collector:  collector,
		dbPool:     dbPool,
		numWorkers: cfg.NumWorkers,
		queueDepth: cfg.QueueDepth,
		timeout:    cfg.QueryTimeout,
		log:        cfg.Logger,
		ctx:        ctx,
		cancel:     cancel,
	}

	if err := p.initWorkers(ctx, dbPool, cfg); err != nil {
		p.Shutdown()
		return nil, err
	}

	p.log.Info("worker pool created",
		zap.Int("workers", cfg.NumWorkers),
		zap.Int("queue_depth", cfg.QueueDepth),
		zap.Duration("query_timeout", cfg.QueryTimeout))

	return p, nil
}

// Start begins processing with all workers
func (p *Pool) Start() {
	for _, w := range p.workers {
		p.workerWg.Add(1)
		go p.runWorker(w)
	}
}

// Submit submits a single query to the appropriate worker.
// Uses blocking backpressure, so it will wait if queue is full.
func (p *Pool) Submit(query *csv.Query) error {
	workerID := p.hasher.GetWorkerID(query.Hostname)
	return p.submitToWorker(workerID, query)
}

// CloseQueues signals workers to stop accepting new work
func (p *Pool) CloseQueues() {
	for _, w := range p.workers {
		close(w.tasks)
	}
}

// Wait waits for all workers to complete
func (p *Pool) Wait() {
	p.workerWg.Wait()

	// Silently track backpressure
	p.log.Debug("processing complete",
		zap.Int64("processed", p.processed.Load()),
		zap.Int64("backpressure_events", p.totalBackpressure.Load()),
		zap.Int64("max_queue_depth", p.maxQueueDepthSeen.Load()))
}

// Shutdown gracefully shuts down the pool (safe to call multiple times)
func (p *Pool) Shutdown() {
	p.shutdownOnce.Do(func() {
		p.cancel()
		p.workerWg.Wait()
		// No need to release connections - workers release after each query
		// The db.Pool will be closed by the caller
	})
}

// Processed returns processed count
func (p *Pool) Processed() int64 {
	return p.processed.Load()
}

// Total returns total submitted
func (p *Pool) Total() int64 {
	return p.submitted.Load()
}

// GetWorkerQueueLengths returns queue lengths for monitoring
func (p *Pool) GetWorkerQueueLengths() []int {
	lengths := make([]int, len(p.workers))
	for i, w := range p.workers {
		lengths[i] = len(w.tasks)
	}
	return lengths
}

// runWorker processes queries from a worker queue
func (p *Pool) runWorker(w *Worker) {
	defer p.workerWg.Done()

	for query := range w.tasks {
		duration, err := w.executeQuery(query)

		if err != nil {
			p.collector.RecordError()
			w.log.Debug("query failed",
				zap.Int("worker_id", w.id),
				zap.String("hostname", query.Hostname),
				zap.Error(err))
		} else {
			p.collector.RecordQuery(duration)
		}

		p.processed.Add(1)
	}
}

// executeQuery executes a single query and measures full network round-trip time.
// Connection acquisition happens BEFORE timing starts, so we only measure query execution.
// This allows workers to share a connection pool without skewing benchmark results.
func (w *Worker) executeQuery(query *csv.Query) (time.Duration, error) {
	// Acquire connection from pool (NOT TIMED)
	// This allows N workers to share M connections where N >> M
	conn, err := w.dbPool.AcquireConn(w.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Return to pool after query completes

	// Create timeout context for query execution
	ctx, cancel := context.WithTimeout(w.ctx, w.timeout)
	defer cancel()

	// Measure only query execution time (TIMED)
	// Connection acquisition time is explicitly excluded
	start := time.Now()
	err = conn.ExecuteQuery(ctx, query.Hostname, query.StartTime, query.EndTime)
	duration := time.Since(start)

	return duration, err
}

// submitToWorker sends a query to a specific worker using blocking backpressure.
//
// Backpressure Strategy:
// 1. Try non-blocking send first (fast path)
// 2. If queue full, SILENTLY BLOCK until space available
// 3. All queries are guaranteed to be processed (no drops)
//
// This is the correct approach for benchmarking because:
// - We must process ALL queries from the CSV file
// - Dropping queries would give inaccurate benchmark results
// - Blocking naturally throttles the CSV reader to match worker capacity
// - Memory is bounded (queueDepth × numWorkers)
//
// The blocking reveals the actual system throughput limit, which is
// exactly what we want to measure in a benchmark tool.
//
// Backpressure is tracked silently and reported only in final statistics.
func (p *Pool) submitToWorker(workerID int, query *csv.Query) error {
	worker := p.workers[workerID]

	// Fast path: try non-blocking send first
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case worker.tasks <- query:
		p.submitted.Add(1)
		// Track queue depth for monitoring
		p.updateMaxQueueDepth(len(worker.tasks))
		return nil
	default:
		// Queue is full - will block below
	}

	// Backpressure detected - track silently (no logging during execution)
	p.totalBackpressure.Add(1)
	worker.backpressureEvents.Add(1)

	// Blocking send. Here we wait until queue has space.
	// Ee don't drop queries.
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case worker.tasks <- query:
		p.submitted.Add(1)
		p.updateMaxQueueDepth(len(worker.tasks))
		return nil
	}
}

// updateMaxQueueDepth atomically updates the maximum queue depth seen
func (p *Pool) updateMaxQueueDepth(currentDepth int) {
	for {
		old := p.maxQueueDepthSeen.Load()
		if int64(currentDepth) <= old {
			break
		}
		if p.maxQueueDepthSeen.CompareAndSwap(old, int64(currentDepth)) {
			break
		}
	}
}

// normalizeConfig ensures all config values are valid
func normalizeConfig(cfg PoolConfig) PoolConfig {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = runtime.NumCPU()
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = defaultQueueDepth
	}
	if cfg.Logger == nil {
		cfg.Logger, _ = zap.NewDevelopment()
	}
	return cfg
}

// initWorkers creates and initializes all workers.
// Workers share the connection pool and acquire connections per-query,
// allowing N workers to operate with M connections where N >> M.
func (p *Pool) initWorkers(ctx context.Context, dbPool *db.Pool, cfg PoolConfig) error {
	for i := 0; i < cfg.NumWorkers; i++ {
		p.workers[i] = &Worker{
			id:      i,
			dbPool:  dbPool, // Share the pool, acquire per query
			tasks:   make(chan *csv.Query, cfg.QueueDepth),
			ctx:     ctx,
			timeout: cfg.QueryTimeout,
			log:     cfg.Logger,
		}
	}
	return nil
}

