package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	benchQueryName = "bench_query" // Prepared statement name
)

var (
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
	ErrConnectionFailed   = errors.New("database connection failed")
)

// RetryConfig holds retry configuration.
type RetryConfig struct {
	MaxRetries    int
	RetryInterval time.Duration
}

// DefaultRetryConfig returns retry defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:    3,
		RetryInterval: 100 * time.Millisecond,
	}
}

// Pool wraps a pgx connection pool with additional functionality.
type Pool struct {
	pool        *pgxpool.Pool
	retryConfig RetryConfig
}

// NewPool creates a new database connection pool
func NewPool(ctx context.Context, dsn string, maxConns int, retryConfig RetryConfig) (*Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	configurePool(config, maxConns)

	pool, err := connectWithRetry(ctx, config, retryConfig)
	if err != nil {
		return nil, err
	}

	return &Pool{pool: pool, retryConfig: retryConfig}, nil
}

// Close closes all connections in the pool.
func (p *Pool) Close() {
	p.pool.Close()
}

// Conn represents a single database connection for a worker.
type Conn struct {
	conn        *pgxpool.Conn
	retryConfig RetryConfig
}

// AcquireConn acquires a connection from the pool for a worker
func (p *Pool) AcquireConn(ctx context.Context) (*Conn, error) {
	conn, err := retryOperation(p.retryConfig, func() (*pgxpool.Conn, error) {
		return p.pool.Acquire(ctx)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	return &Conn{
		conn:        conn,
		retryConfig: p.retryConfig,
	}, nil
}

// Release returns the connection to the pool
func (c *Conn) Release() {
	c.conn.Release()
}

// ExecuteQuery executes the benchmark query using prepared statement.
// This measures the full network round-trip time for benchmarking purposes.
func (c *Conn) ExecuteQuery(ctx context.Context, hostname, startTime, endTime string) error {
	var lastErr error

	for attempt := 0; attempt <= c.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			sleepWithBackoff(c.retryConfig.RetryInterval, attempt)
		}

		err := c.executeQueryOnce(ctx, hostname, startTime, endTime)
		if err == nil {
			return nil
		}

		if isRetryableError(err) {
			lastErr = err
			continue
		}
		return err
	}

	return fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}

// CheckTableExists verifies that the cpu_usage table exists
func (p *Pool) CheckTableExists(ctx context.Context) error {
	var exists bool
	err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'cpu_usage'
		)
	`).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("table 'cpu_usage' does not exist")
	}
	return nil
}

// GetMaxConnections queries the database for its max_connections setting.
// Returns 0 if the query fails (caller should use a safe default).
func (p *Pool) GetMaxConnections(ctx context.Context) int {
	var maxConns int
	err := p.pool.QueryRow(ctx, "SHOW max_connections").Scan(&maxConns)
	if err != nil {
		// Return 0 to signal caller to use default
		return 0
	}
	return maxConns
}

// configurePool sets connection pool configuration
func configurePool(config *pgxpool.Config, maxConns int) {
	config.MaxConns = int32(maxConns)
	config.MinConns = int32(max(1, maxConns/2))
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second

	// Prepare the benchmark query on each connection
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Prepare(ctx, benchQueryName, `
			SELECT
				time_bucket('1 minute', ts) AS minute,
				MAX(usage) AS max_usage,
				MIN(usage) AS min_usage
			FROM cpu_usage
			WHERE host = $1 AND ts >= $2 AND ts <= $3
			GROUP BY minute
			ORDER BY minute
		`)
		return err
	}
}

// connectWithRetry establishes a database connection with exponential backoff
func connectWithRetry(ctx context.Context, config *pgxpool.Config, retryConfig RetryConfig) (*pgxpool.Pool, error) {
	var lastErr error

	for attempt := 0; attempt <= retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			sleepWithBackoff(retryConfig.RetryInterval, attempt-1)
		}

		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			lastErr = err
			continue
		}

		if err = pool.Ping(ctx); err != nil {
			pool.Close()
			lastErr = err
			continue
		}

		return pool, nil
	}

	return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, lastErr)
}

// retryOperation executes an operation with retry logic
func retryOperation[T any](retryConfig RetryConfig, operation func() (T, error)) (T, error) {
	var result T
	var lastErr error

	for attempt := 0; attempt <= retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			sleepWithBackoff(retryConfig.RetryInterval, attempt-1)
		}

		result, err := operation()
		if err == nil {
			return result, nil
		}
		lastErr = err
	}

	return result, lastErr
}

// sleepWithBackoff implements exponential backoff
func sleepWithBackoff(baseInterval time.Duration, attempt int) {
	time.Sleep(baseInterval * time.Duration(1<<attempt))
}

// isRetryableError determines if an error should trigger a retry.
// Handles PostgreSQL-specific error codes and network errors that are transient.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for PostgreSQL error codes (SQLSTATE codes from PostgreSQL documentation)
	// Reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		// Class 40 — Transaction Rollback
		case "40001": // serialization_failure - concurrent transaction conflicts
		case "40P01": // deadlock_detected - circular lock dependencies
			return true

		// Class 57 — Operator Intervention (database shutdown/restart scenarios)
		case "57P01": // admin_shutdown - database administrator commanded shutdown
		case "57P02": // crash_shutdown - database crashed and is restarting
		case "57P03": // cannot_connect_now - database is starting up
			return true

		// Class 08 — Connection Exception
		case "08000": // connection_exception - generic connection error
		case "08003": // connection_does_not_exist - connection was closed
		case "08006": // connection_failure - connection failed during operation
			return true
		}
	}

	// Check for other errors
	errStr := err.Error()
	return strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "timeout")
}

// executeQueryOnce executes a single query attempt without retry.
// Results are consumed but not stored - we only care about timing/errors for benchmarking.
// This ensures we measure the full network round-trip time.
func (c *Conn) executeQueryOnce(ctx context.Context, hostname, startTime, endTime string) error {
	rows, err := c.conn.Query(ctx, benchQueryName, hostname, startTime, endTime)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Reused variables to avoid allocations per row
	var minute time.Time
	var maxUsage, minUsage float64

	// Consume all rows without storing - must scan to complete network read
	for rows.Next() {
		if err := rows.Scan(&minute, &maxUsage, &minUsage); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	return nil
}
