package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/tsdb-bench/internal/config"
	csvpkg "github.com/tsdb-bench/internal/csv"
	"github.com/tsdb-bench/internal/db"
	"github.com/tsdb-bench/internal/stats"
	"github.com/tsdb-bench/internal/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	if err := run(); err != nil {
		logger.Fatal("application failed", zap.Error(err))
	}
}

// run contains all application logic
func run() error {
	cfg, err := config.ParseFlags()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	// Start CPU profiling if requested
	if cfg.CPUProfile != "" {
		f, err := os.Create(cfg.CPUProfile)
		if err != nil {
			return fmt.Errorf("could not create CPU profile: %w", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return fmt.Errorf("could not start CPU profile: %w", err)
		}
		defer pprof.StopCPUProfile()
	}

	logger := initLogger(cfg.Verbose)
	defer logger.Sync()

	ctx, cancel := setupGracefulShutdown(logger)
	defer cancel()

	pool, err := initDatabasePool(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer pool.Close()

	collector := stats.NewCollector()

	workerPool, err := initWorkerPool(ctx, pool, collector, cfg, logger)
	if err != nil {
		return err
	}
	defer workerPool.Shutdown()

	workerPool.Start()
	collector.Start()

	logger.Info("starting benchmark")

	totalQueries, parseErrors, err := processQueries(ctx, cfg, workerPool, logger)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("processing completed with errors", zap.Error(err))
	}

	summary := finalizeBenchmark(workerPool, collector, totalQueries, logger)
	printResults(summary, parseErrors)

	// Write memory profile if requested
	if cfg.MemProfile != "" {
		f, err := os.Create(cfg.MemProfile)
		if err != nil {
			return fmt.Errorf("could not create memory profile: %w", err)
		}
		defer f.Close()
		runtime.GC() // Get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			return fmt.Errorf("could not write memory profile: %w", err)
		}
	}

	return nil
}

// initLogger creates and configures the application logger
func initLogger(verbose bool) *zap.Logger {
	zapCfg := zap.NewDevelopmentConfig()
	if verbose {
		zapCfg.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	} else {
		zapCfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
	logger, err := zapCfg.Build()
	if err != nil {
		// Fallback to production logger if development config fails
		logger, _ = zap.NewProduction()
	}
	return logger
}

// setupGracefulShutdown configures signal handling for graceful shutdown
func setupGracefulShutdown(logger *zap.Logger) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
	}()

	return ctx, cancel
}

// initDatabasePool creates and validates the database connection pool.
// Uses intelligent pool sizing to respect PostgreSQL connection limits while
// allowing many workers to share a smaller pool of connections.
func initDatabasePool(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*db.Pool, error) {
	logger.Info("connecting to database",
		zap.String("host", cfg.DBHost),
		zap.Int("port", cfg.DBPort),
		zap.String("database", cfg.DBName))

	retryConfig := db.RetryConfig{
		MaxRetries:    cfg.MaxRetries,
		RetryInterval: cfg.RetryInterval,
	}

	// Determine db connection pool size
	poolSize := determinePoolSize(ctx, cfg, logger)

	// Create new db connection pool
	pool, err := db.NewPool(ctx, cfg.DSN(), poolSize, retryConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create database pool: %w", err)
	}

	if err := pool.CheckTableExists(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database validation failed: %w", err)
	}

	logger.Info("database connection established",
		zap.Int("pool_size", poolSize),
		zap.Int("workers", cfg.NumWorkers))

	// Warn if workers significantly exceed connections
	if cfg.NumWorkers > poolSize*5 {
		logger.Warn("workers significantly exceed database connections",
			zap.Int("workers", cfg.NumWorkers),
			zap.Int("connections", poolSize),
			zap.String("note", "workers will share connections - this is normal but may impact throughput"))
	}

	return pool, nil
}

// determinePoolSize calculates the connection pool size.
// If user specifies --max-db-connections, it is used.
// Otherwise, query PostgreSQL max_connections and use 80%
// and cap at number of workers.
func determinePoolSize(ctx context.Context, cfg *config.Config, logger *zap.Logger) int {
	// User explicitly set pool size
	if cfg.MaxDBConnections > 0 {
		logger.Debug("using user-specified connection pool size",
			zap.Int("pool_size", cfg.MaxDBConnections))
		return cfg.MaxDBConnections
	}

	// detect from database
	retryConfig := db.RetryConfig{
		MaxRetries:    cfg.MaxRetries,
		RetryInterval: cfg.RetryInterval,
	}

	// Create temporary pool with 1 connection just to query max_connections
	tempPool, err := db.NewPool(ctx, cfg.DSN(), 1, retryConfig)
	if err != nil {
		logger.Warn("failed to auto-detect max_connections, using default",
			zap.Int("default_pool_size", cfg.NumWorkers),
			zap.Error(err))
		return cfg.NumWorkers
	}
	defer tempPool.Close()

	maxConns := tempPool.GetMaxConnections(ctx)
	if maxConns == 0 {
		logger.Debug("could not query max_connections, using workers as pool size",
			zap.Int("pool_size", cfg.NumWorkers))
		return cfg.NumWorkers
	}

	// Use 80% of max_connections
	safePoolSize := int(float64(maxConns) * 0.8)

	// Cap at number of workers (no point having more connections than workers)
	poolSize := min(safePoolSize, cfg.NumWorkers)

	// Ensure at least 1 connection
	if poolSize < 1 {
		poolSize = 1
	}

	logger.Debug("auto-detected connection pool size",
		zap.Int("db_max_connections", maxConns),
		zap.Int("safe_pool_size", safePoolSize),
		zap.Int("workers", cfg.NumWorkers),
		zap.Int("final_pool_size", poolSize))

	return poolSize
}

// initWorkerPool creates and configures the worker pool
func initWorkerPool(ctx context.Context, pool *db.Pool, collector *stats.Collector, cfg *config.Config, logger *zap.Logger) (*worker.Pool, error) {
	logger.Info("creating worker pool",
		zap.Int("workers", cfg.NumWorkers),
		zap.Int("queue_depth", cfg.QueueDepth))

	workerPool, err := worker.NewPool(ctx, pool, collector, worker.PoolConfig{
		NumWorkers:   cfg.NumWorkers,
		QueueDepth:   cfg.QueueDepth,
		QueryTimeout: cfg.QueryTimeout,
		Logger:       logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create worker pool: %w", err)
	}

	return workerPool, nil
}

// processQueries reads and submits queries from the configured input source
func processQueries(ctx context.Context, cfg *config.Config, pool *worker.Pool, logger *zap.Logger) (int64, int64, error) {
	if cfg.UseStdin {
		logger.Info("reading from stdin")
		return processReader(ctx, os.Stdin, pool, logger)
	}

	logger.Info("reading from file", zap.String("file", cfg.InputFile))
	return processFile(ctx, cfg.InputFile, pool, logger)
}

// finalizeBenchmark waits for completion and returns the summary
func finalizeBenchmark(pool *worker.Pool, collector *stats.Collector, totalQueries int64, logger *zap.Logger) *stats.Summary {
	logger.Info("waiting for workers", zap.Int64("submitted", totalQueries))
	pool.CloseQueues()
	pool.Wait()
	collector.End()
	return collector.GetSummary()
}

// printResults outputs benchmark results to stdout
func printResults(summary *stats.Summary, parseErrors int64) {
	fmt.Println(summary.String())

	if parseErrors > 0 {
		fmt.Printf("\nParse Errors: %d\n", parseErrors)
	}
	if summary.TotalErrors > 0 {
		fmt.Printf("Query Errors: %d\n", summary.TotalErrors)
	}
}

// processFile reads queries from a file and submits them to the worker pool
func processFile(ctx context.Context, path string, pool *worker.Pool, logger *zap.Logger) (int64, int64, error) {
	reader, err := csvpkg.NewReaderFromFile(path)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer reader.Close()

	return processWithReader(ctx, reader, pool, logger)
}

// processReader reads queries from an io.Reader and submits them to the worker pool
func processReader(ctx context.Context, r io.Reader, pool *worker.Pool, logger *zap.Logger) (int64, int64, error) {
	reader := csvpkg.NewReader(r)
	return processWithReader(ctx, reader, pool, logger)
}

// processWithReader reads CSV line by line and dispatches each query to its hashed worker
func processWithReader(ctx context.Context, reader *csvpkg.Reader, pool *worker.Pool, logger *zap.Logger) (int64, int64, error) {
	var totalQueries int64
	var parseErrors int64

	// Use a goroutine to make reading cancellable
	type readResult struct {
		query *csvpkg.Query
		err   error
	}
	readChan := make(chan readResult, 1)

	// Start read loop in goroutine
	go func() {
		for {
			query, err := reader.Read()
			select {
			case readChan <- readResult{query, err}:
				// Only stop on EOF or fatal errors
				if err == io.EOF {
					return
				}
				// For parse errors, continue reading
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return totalQueries, parseErrors, ctx.Err()

		case result := <-readChan:
			if result.err == io.EOF {
				return totalQueries, parseErrors, nil
			}
			if result.err != nil {
				parseErrors++
				var parseErr *csvpkg.ParseError
				if errors.As(result.err, &parseErr) {
					logger.Error("CSV parse error",
						zap.Int("line", parseErr.LineNum),
						zap.String("error", parseErr.Message))
				} else {
					logger.Error("parse error", zap.Error(result.err))
				}
				continue
			}

			// Submit to appropriate worker based on hostname hash
			if err := pool.Submit(result.query); err != nil {
				if errors.Is(err, context.Canceled) {
					return totalQueries, parseErrors, err
				}
				logger.Warn("submit failed", zap.Error(err))
				continue
			}

			totalQueries++
		}
	}
}
