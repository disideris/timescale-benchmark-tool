# TimescaleDB Benchmark Tool

A command-line tool for benchmarking TimescaleDB SELECT query performance with concurrent workers.

## Quick Start

### Option 1: Using Make (Recommended)

```bash
# 1. Start database and build (all-in-one)
make

# 2. Load sample data
psql -h localhost -U postgres -d homework -c "\COPY cpu_usage FROM 'data/cpu_usage.csv' CSV HEADER"

# 3. Run benchmark
./benchmark --file data/query_params.csv --workers 10

# See all make commands
make help
```

### Option 2: Manual Setup

```bash
# 1. Start TimescaleDB
docker-compose up -d timescaledb

# 2. Build the tool
go build -o benchmark ./cmd/benchmark

# 3. Load sample data
psql -h localhost -U postgres -d homework -c "\COPY cpu_usage FROM 'data/cpu_usage.csv' CSV HEADER"

# 4. Run benchmark
./benchmark --file data/query_params.csv --workers 10
```

### Option 3: Existing PostgreSQL

```bash
# 1. Create schema and hypertable
psql -h your-db.example.com -U your-user -d your-db -f migrations/001_init.sql

# 2. Load your data
psql -h your-db.example.com -U your-user -d your-db -c "\COPY cpu_usage FROM 'your_data.csv' CSV HEADER"

# 3. Build and run
go build -o benchmark ./cmd/benchmark
./benchmark --file queries.csv --db-host your-db.example.com --db-name your-db
```

## Usage

### Reading from a file
```bash
./benchmark --file query_params.csv --workers 10
```

### Reading from stdin
```bash
cat query_params.csv | ./benchmark --workers 10
```

### Common Examples

```bash
# Basic benchmark
./benchmark --file query_params.csv

# With 32 concurrent workers
./benchmark --file query_params.csv --workers 32

# Remote database
./benchmark --file queries.csv \
  --db-host prod.example.com \
  --db-name mydb

# Using environment variables
export PGHOST=localhost
export PGDATABASE=homework
./benchmark --file queries.csv

# Stdin with custom settings
cat queries.csv | ./benchmark --workers 32 --timeout 60s
```

### Profiling

Profile CPU and memory usage to find performance bottlenecks:

**Prerequisites:**

Install Graphviz for web UI (required for visualization)

**Run profiling:**
```bash
# Run with profiling enabled
./benchmark --file query_params.csv --workers 10 \
  --cpuprofile cpu.prof \
  --memprofile mem.prof

# Analyze CPU profile (interactive web UI)
go tool pprof -http=:8080 cpu.prof

# Analyze memory profile
go tool pprof -http=:8080 mem.prof

# Or use command-line interface (no graphviz needed)
go tool pprof cpu.prof
```

**What to look for:**
- **CPU profile**: Functions consuming most CPU time, hot paths
- **Memory profile**: Large allocations, potential memory leaks

Run with realistic data and worker counts to get meaningful profiles.

### Input Format

CSV file with query parameters:
```csv
hostname,start_time,end_time
host_000001,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000002,2017-01-02 13:02:02,2017-01-02 14:02:02
```

### Output

```
Benchmark Results
=================
Queries processed:    100000
Queries failed:       0
Total processing time: 1m20.544s
Wall clock time:      4.121s
Queries per second:   24264.78

Query Time Statistics
---------------------
Minimum:  100.881µs
Average:  805.466µs
Median:   792.128µs
P95:      1.033ms
P99:      1.242ms
Maximum:  7.765ms
```

## Configuration

### Command-Line Options

Run `./benchmark --help` to see all available options.

**Input/Output:**
- `--file` - CSV file path (reads from stdin if not specified)
- `--verbose` - Enable debug logging

**Performance:**
- `--workers` - Number of concurrent workers (default: number of CPUs)

**Database Connection:**
- `--db-host` - Database host (default: localhost)
- `--db-port` - Database port (default: 5432)
- `--db-user` - Database user (default: postgres)
- `--db-password` - Database password (default: password)
- `--db-name` - Database name (default: homework)
- `--db-sslmode` - SSL mode: disable, require, verify-ca, verify-full (default: disable)

**Advanced:**
- `--queue-depth` - Queue depth per worker (default: 1000)
- `--timeout` - Query timeout (default: 30s)
- `--max-retries` - Maximum retries for failed queries (default: 3)
- `--retry-interval` - Interval between retries (default: 100ms)

**Profiling:**
- `--cpuprofile` - Write CPU profile to file (e.g., `--cpuprofile cpu.prof`)
- `--memprofile` - Write memory profile to file (e.g., `--memprofile mem.prof`)

### Environment Variables

The tool uses standard PostgreSQL environment variables:

```bash
PGHOST          Database host (default: localhost)
PGPORT          Database port (default: 5432)
PGUSER          Database user (default: postgres)
PGPASSWORD      Database password (default: password)
PGDATABASE      Database name (default: homework)
PGSSLMODE       SSL mode (default: disable)
```

## Development

### Make Commands

**Setup:**
```bash
make              # Start database and build (same as 'make all')
make db           # Start TimescaleDB only
make build        # Build the binary only
```

**Testing:**
```bash
make test         # Run unit tests
make test-e2e     # Run end-to-end tests (requires Docker)
make test-all     # Run all tests (unit + e2e)
make test-coverage # Run tests with coverage report
make test-race    # Run tests with race detector
```


**Cleanup:**
```bash
make clean        # Remove build artifacts
make help         # Show all available commands
```
