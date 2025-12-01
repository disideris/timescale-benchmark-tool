package config

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

// Config holds all configuration options for the benchmark tool.
type Config struct {
	// Database connection settings
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string

	// Worker settings
	NumWorkers        int
	QueueDepth        int
	MaxDBConnections  int // Max database connections in pool (0 = auto-detect)

	// Retry settings
	MaxRetries    int
	RetryInterval time.Duration

	// Input settings
	InputFile string
	UseStdin  bool

	// Query settings
	QueryTimeout time.Duration

	// Output settings
	Verbose bool

	// Profiling settings
	CPUProfile string
	MemProfile string
}

// DefaultConfig returns a Config with defaults.
func DefaultConfig() *Config {
	return &Config{
		DBHost:        getEnv("PGHOST", "localhost"),
		DBPort:        getEnvInt("PGPORT", 5432),
		DBUser:        getEnv("PGUSER", "postgres"),
		DBPassword:    getEnv("PGPASSWORD", "password"),
		DBName:           getEnv("PGDATABASE", "homework"),
		DBSSLMode:        getEnv("PGSSLMODE", "disable"),
		NumWorkers:       runtime.NumCPU(),
		QueueDepth:       1000,
		MaxDBConnections: 0, // 0 = auto-detect from database
		MaxRetries:    3,
		RetryInterval: 100 * time.Millisecond,
		QueryTimeout:  30 * time.Second,
		Verbose:       false,
	}
}

// ParseFlags parses command line flags and returns the configuration.
func ParseFlags() (*Config, error) {
	cfg := DefaultConfig()

	// Custom usage function for better help output
	flag.Usage = printUsage

	// Input/Output
	flag.StringVar(&cfg.InputFile, "file", "", "CSV file with query parameters (reads from stdin if not specified)")

	// Performance
	flag.IntVar(&cfg.NumWorkers, "workers", cfg.NumWorkers, "Number of concurrent workers (default: number of CPUs)")

	// Database connection
	flag.StringVar(&cfg.DBHost, "db-host", cfg.DBHost, "Database host (env: PGHOST)")
	flag.IntVar(&cfg.DBPort, "db-port", cfg.DBPort, "Database port (env: PGPORT)")
	flag.StringVar(&cfg.DBUser, "db-user", cfg.DBUser, "Database user (env: PGUSER)")
	flag.StringVar(&cfg.DBPassword, "db-password", cfg.DBPassword, "Database password (env: PGPASSWORD)")
	flag.StringVar(&cfg.DBName, "db-name", cfg.DBName, "Database name (env: PGDATABASE)")
	flag.StringVar(&cfg.DBSSLMode, "db-sslmode", cfg.DBSSLMode, "SSL mode: disable, require, verify-ca, verify-full (env: PGSSLMODE)")

	// Advanced options
	flag.IntVar(&cfg.QueueDepth, "queue-depth", cfg.QueueDepth, "Queue depth per worker (memory vs backpressure tradeoff)")
	flag.IntVar(&cfg.MaxDBConnections, "max-db-connections", cfg.MaxDBConnections, "Max database connections (0 = auto-detect, default: min(workers, 80% of max_connections))")
	flag.DurationVar(&cfg.QueryTimeout, "timeout", cfg.QueryTimeout, "Query timeout")
	flag.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries, "Maximum retries for failed queries")
	flag.DurationVar(&cfg.RetryInterval, "retry-interval", cfg.RetryInterval, "Interval between retries")

	// Debug
	flag.BoolVar(&cfg.Verbose, "verbose", cfg.Verbose, "Enable debug logging")

	// Profiling
	flag.StringVar(&cfg.CPUProfile, "cpuprofile", "", "Write CPU profile to file")
	flag.StringVar(&cfg.MemProfile, "memprofile", "", "Write memory profile to file")

	flag.Parse()

	cfg.UseStdin = cfg.InputFile == ""

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// printUsage prints a user-friendly help message
func printUsage() {
	fmt.Fprintf(os.Stderr, `TimescaleDB Benchmark Tool
==========================

Benchmark SELECT query performance against TimescaleDB with concurrent workers.
Reads query parameters from CSV and measures query latency statistics.

USAGE
    benchmark [options]

    Read from file:
        benchmark --file query_params.csv --workers 10

    Read from stdin:
        cat query_params.csv | benchmark --workers 10

    With custom database:
        benchmark --file query_params.csv --db-host mydb.example.com --db-name production

REQUIRED CSV FORMAT
    hostname,start_time,end_time
    host_000001,2017-01-01 08:59:22,2017-01-01 09:59:22
    host_000002,2017-01-02 13:02:02,2017-01-02 14:02:02

OPTIONS
    Input/Output:
      -file string
            CSV file with query parameters (reads from stdin if not specified)

      -verbose
            Enable debug logging

    Performance:
      -workers int
            Number of concurrent workers (default: %d)
            Recommended: 2-4x number of CPU cores for I/O-bound workloads

    Database Connection (uses standard PostgreSQL environment variables):
      -db-host string
            Database host (default: %s, env: PGHOST)

      -db-port int
            Database port (default: %d, env: PGPORT)

      -db-user string
            Database user (default: %s, env: PGUSER)

      -db-password string
            Database password (default: %s, env: PGPASSWORD)

      -db-name string
            Database name (default: %s, env: PGDATABASE)

      -db-sslmode string
            SSL mode: disable, require, verify-ca, verify-full
            (default: %s, env: PGSSLMODE)

    Advanced:
      -queue-depth int
            Queue depth per worker (default: %d)
            Higher values handle traffic bursts better but use more memory

      -max-db-connections int
            Maximum database connections in pool (default: 0 = auto-detect)
            Auto-detect uses min(workers, 80%% of PostgreSQL max_connections)
            Workers share connections, so this can be much less than --workers

      -timeout duration
            Query timeout (default: %s)

      -max-retries int
            Maximum retries for failed queries (default: %d)

      -retry-interval duration
            Interval between retries (default: %s)

EXAMPLES
    Basic benchmark:
        benchmark --file data/query_params.csv

    Benchmark with 32 workers:
        benchmark --file data/query_params.csv --workers 32

    Benchmark from stdin:
        cat data/query_params.csv | benchmark --workers 10

    Connect to remote database:
        benchmark --file queries.csv \
          --db-host prod-db.example.com \
          --db-name timescaledb \
          --db-sslmode require

    Using environment variables:
        export PGHOST=localhost
        export PGDATABASE=homework
        benchmark --file queries.csv --workers 16

ENVIRONMENT VARIABLES
    PGHOST          Database host (default: localhost)
    PGPORT          Database port (default: 5432)
    PGUSER          Database user (default: postgres)
    PGPASSWORD      Database password (default: password)
    PGDATABASE      Database name (default: homework)
    PGSSLMODE       SSL mode (default: disable)

OUTPUT
    The tool outputs statistics after processing all queries:
    - Number of queries processed
    - Total processing time (sum of all query times)
    - Wall clock time (actual elapsed time with parallelism)
    - Queries per second (throughput)
    - Query latency statistics (min, avg, median, p95, p99, max)

DOCKER USAGE
    Using docker-compose.yml in the project:

    1. Start database and load data:
       docker-compose up -d timescaledb
       docker-compose up dataloader

    2. Run benchmark:
       docker-compose run --rm benchmark --file /data/query_params.csv --workers 10

    Clean up:
       docker-compose down -v
`,
		runtime.NumCPU(),                 // workers default
		getEnv("PGHOST", "localhost"),    // db-host
		getEnvInt("PGPORT", 5432),        // db-port
		getEnv("PGUSER", "postgres"),     // db-user
		getEnv("PGPASSWORD", "password"), // db-password
		getEnv("PGDATABASE", "homework"), // db-name
		getEnv("PGSSLMODE", "disable"),   // db-sslmode
		1000,                             // queue-depth
		30*time.Second,                   // timeout
		3,                                // max-retries
		100*time.Millisecond,             // retry-interval
	)
}

// Validate checks the configuration for errors
func (c *Config) Validate() error {
	if err := validateWorkerConfig(c.NumWorkers, c.QueueDepth); err != nil {
		return err
	}
	if err := validateRetryConfig(c.MaxRetries, c.QueryTimeout); err != nil {
		return err
	}
	if err := validateInputFile(c.UseStdin, c.InputFile); err != nil {
		return err
	}
	return nil
}

// DSN returns the PostgreSQL connection string
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBName, c.DBSSLMode,
	)
}

// validateWorkerConfig validates worker pool configuration
func validateWorkerConfig(numWorkers, queueDepth int) error {
	if numWorkers < 1 || numWorkers > 100 {
		return fmt.Errorf("number of workers must be between 1 and 100, got %d", numWorkers)
	}
	if queueDepth < 1 {
		return fmt.Errorf("queue depth must be at least 1, got %d", queueDepth)
	}
	return nil
}

// validateRetryConfig validates retry and timeout configuration
func validateRetryConfig(maxRetries int, queryTimeout time.Duration) error {
	if maxRetries < 0 {
		return fmt.Errorf("max retries must be non-negative, got %d", maxRetries)
	}
	if queryTimeout < time.Second {
		return fmt.Errorf("query timeout must be at least 1 second, got %v", queryTimeout)
	}

	// Ensure retry budget is reasonable
	maxRetryTime := queryTimeout * time.Duration(maxRetries+1)
	if maxRetryTime > 5*time.Minute {
		return fmt.Errorf("retry budget (%v) exceeds 5 minutes (timeout %v × %d retries)", maxRetryTime, queryTimeout, maxRetries)
	}

	return nil
}

// validateInputFile validates input file configuration
func validateInputFile(useStdin bool, inputFile string) error {
	if !useStdin && inputFile != "" {
		if _, err := os.Stat(inputFile); os.IsNotExist(err) {
			return fmt.Errorf("input file does not exist: %s", inputFile)
		}
	}
	return nil
}

// getEnv returns environment variable or default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt returns environment variable as int or default value
func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	result, err := strconv.Atoi(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: invalid integer for %s=%q, using default %d\n", key, value, defaultValue)
		return defaultValue
	}
	return result
}
