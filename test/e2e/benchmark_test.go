package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestBenchmark_EndToEnd runs the actual compiled binary as a user would
func TestBenchmark_EndToEnd(t *testing.T) {
	ctx := context.Background()

	// Build the binary
	binPath := buildBinary(t)
	defer os.Remove(binPath)

	// Start TimescaleDB container
	pgContainer, host, port := setupTimescaleDB(t, ctx)
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	// Load test data
	loadTestData(t, ctx, host, port)

	// Create test CSV file
	csvFile := createTestCSV(t, `hostname,start_time,end_time
host_000001,2017-01-01 00:00:00,2017-01-01 01:00:00
host_000001,2017-01-01 02:00:00,2017-01-01 03:00:00
host_000002,2017-01-01 00:00:00,2017-01-01 01:00:00
host_000003,2017-01-01 01:00:00,2017-01-01 02:00:00
host_000001,2017-01-01 04:00:00,2017-01-01 05:00:00`)
	defer os.Remove(csvFile)

	// Run the benchmark binary
	cmd := exec.Command(binPath,
		"--file", csvFile,
		"--workers", "4",
		"--db-host", host,
		"--db-port", port,
		"--db-user", "postgres",
		"--db-password", "password",
		"--db-name", "homework",
		"--db-sslmode", "disable",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	require.NoError(t, err, "benchmark command failed: %s", stderr.String())

	output := stdout.String()
	t.Logf("Benchmark output:\n%s", output)

	// Verify output contains expected metrics
	assert.Contains(t, output, "Benchmark Results")
	assert.Contains(t, output, "Queries processed:")
	assert.Contains(t, output, "5") // Should process 5 queries
	assert.Contains(t, output, "Queries failed:")
	assert.Contains(t, output, "0") // Should have 0 errors
	assert.Contains(t, output, "Queries per second:")
	assert.Contains(t, output, "Minimum:")
	assert.Contains(t, output, "Average:")
	assert.Contains(t, output, "Median:")
	assert.Contains(t, output, "P95:")
	assert.Contains(t, output, "P99:")
	assert.Contains(t, output, "Maximum:")
}

// TestBenchmark_StdinInput tests reading from stdin
func TestBenchmark_StdinInput(t *testing.T) {
	ctx := context.Background()

	binPath := buildBinary(t)
	defer os.Remove(binPath)

	pgContainer, host, port := setupTimescaleDB(t, ctx)
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	loadTestData(t, ctx, host, port)

	// Prepare CSV content for stdin
	csvContent := `hostname,start_time,end_time
host_000001,2017-01-01 00:00:00,2017-01-01 01:00:00
host_000002,2017-01-01 00:00:00,2017-01-01 01:00:00`

	// Run benchmark with stdin
	cmd := exec.Command(binPath,
		"--workers", "2",
		"--db-host", host,
		"--db-port", port,
		"--db-user", "postgres",
		"--db-password", "password",
		"--db-name", "homework",
		"--db-sslmode", "disable",
	)

	cmd.Stdin = strings.NewReader(csvContent)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	require.NoError(t, err, "benchmark command failed: %s", stderr.String())

	output := stdout.String()
	assert.Contains(t, output, "Queries processed:")
	assert.Contains(t, output, "2") // Should process 2 queries
}

// TestBenchmark_LargeDataset tests with a larger number of queries
func TestBenchmark_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	ctx := context.Background()

	binPath := buildBinary(t)
	defer os.Remove(binPath)

	pgContainer, host, port := setupTimescaleDB(t, ctx)
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	loadTestData(t, ctx, host, port)

	// Generate 100 queries
	var csvBuffer bytes.Buffer
	csvBuffer.WriteString("hostname,start_time,end_time\n")
	for i := 0; i < 100; i++ {
		hostname := fmt.Sprintf("host_%06d", (i%10)+1)
		startTime := fmt.Sprintf("2017-01-01 %02d:00:00", i%24)
		endTime := fmt.Sprintf("2017-01-01 %02d:00:00", (i%24)+1)
		csvBuffer.WriteString(fmt.Sprintf("%s,%s,%s\n", hostname, startTime, endTime))
	}

	csvFile := createTestCSV(t, csvBuffer.String())
	defer os.Remove(csvFile)

	// Run benchmark
	cmd := exec.Command(binPath,
		"--file", csvFile,
		"--workers", "8",
		"--db-host", host,
		"--db-port", port,
		"--db-user", "postgres",
		"--db-password", "password",
		"--db-name", "homework",
		"--db-sslmode", "disable",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	require.NoError(t, err, "benchmark command failed: %s", stderr.String())

	output := stdout.String()
	assert.Contains(t, output, "Queries processed:")
	assert.Contains(t, output, "100")

	t.Logf("Large dataset benchmark output:\n%s", output)
}

// TestBenchmark_EnvironmentVariables tests using environment variables for database config
func TestBenchmark_EnvironmentVariables(t *testing.T) {
	ctx := context.Background()

	binPath := buildBinary(t)
	defer os.Remove(binPath)

	pgContainer, host, port := setupTimescaleDB(t, ctx)
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	loadTestData(t, ctx, host, port)

	csvFile := createTestCSV(t, `hostname,start_time,end_time
host_000001,2017-01-01 00:00:00,2017-01-01 01:00:00`)
	defer os.Remove(csvFile)

	// Run with environment variables
	cmd := exec.Command(binPath, "--file", csvFile, "--workers", "2")
	cmd.Env = append(os.Environ(),
		"PGHOST="+host,
		"PGPORT="+port,
		"PGUSER=postgres",
		"PGPASSWORD=password",
		"PGDATABASE=homework",
		"PGSSLMODE=disable",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	require.NoError(t, err, "benchmark command failed: %s", stderr.String())

	output := stdout.String()
	assert.Contains(t, output, "Queries processed:")
	assert.Contains(t, output, "1")
}

// TestBenchmark_VerboseOutput tests verbose logging
func TestBenchmark_VerboseOutput(t *testing.T) {
	ctx := context.Background()

	binPath := buildBinary(t)
	defer os.Remove(binPath)

	pgContainer, host, port := setupTimescaleDB(t, ctx)
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	loadTestData(t, ctx, host, port)

	csvFile := createTestCSV(t, `hostname,start_time,end_time
host_000001,2017-01-01 00:00:00,2017-01-01 01:00:00`)
	defer os.Remove(csvFile)

	// Run with verbose flag
	cmd := exec.Command(binPath,
		"--file", csvFile,
		"--workers", "2",
		"--verbose",
		"--db-host", host,
		"--db-port", port,
		"--db-user", "postgres",
		"--db-password", "password",
		"--db-name", "homework",
		"--db-sslmode", "disable",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	require.NoError(t, err, "benchmark command failed: %s", stderr.String())

	combinedOutput := stdout.String() + stderr.String()
	// Verbose mode should show debug information (INFO/DEBUG logs)
	assert.Contains(t, combinedOutput, "starting benchmark")
	assert.Contains(t, combinedOutput, "INFO")
	assert.Contains(t, combinedOutput, "worker pool created")
}

// buildBinary compiles the benchmark tool for testing
func buildBinary(t *testing.T) string {
	t.Helper()

	// Create temp binary path
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "benchmark")

	// Get project root (two levels up from test/e2e)
	projectRoot, err := filepath.Abs("../..")
	require.NoError(t, err, "failed to get project root")

	// Build the binary
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/benchmark")
	cmd.Dir = projectRoot

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err = cmd.Run()
	require.NoError(t, err, "failed to build binary: %s", stderr.String())

	t.Logf("Built benchmark binary at: %s", binPath)
	return binPath
}

// setupTimescaleDB starts a TimescaleDB container and returns connection info
func setupTimescaleDB(t *testing.T, ctx context.Context) (*postgres.PostgresContainer, string, string) {
	t.Helper()

	// Read init script
	initScriptPath, err := filepath.Abs("../../migrations/001_init.sql")
	require.NoError(t, err, "failed to get init script path")

	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg16",
		postgres.WithDatabase("homework"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts(initScriptPath),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err, "failed to start TimescaleDB container")

	// Wait a bit for TimescaleDB extension to be fully ready
	time.Sleep(2 * time.Second)

	host, err := pgContainer.Host(ctx)
	require.NoError(t, err, "failed to get container host")

	mappedPort, err := pgContainer.MappedPort(ctx, "5432")
	require.NoError(t, err, "failed to get mapped port")

	return pgContainer, host, mappedPort.Port()
}

// loadTestData loads sample cpu_usage data into the database
func loadTestData(t *testing.T, ctx context.Context, host, port string) {
	t.Helper()

	connStr := fmt.Sprintf("host=%s port=%s user=postgres password=password dbname=homework sslmode=disable",
		host, port)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "failed to connect to database")
	defer conn.Close(ctx)

	// Insert sample data for testing
	// Generate data for 10 hosts over 24 hours
	for hostNum := 1; hostNum <= 10; hostNum++ {
		hostname := fmt.Sprintf("host_%06d", hostNum)

		for hour := 0; hour < 24; hour++ {
			for minute := 0; minute < 60; minute++ {
				timestamp := fmt.Sprintf("2017-01-01 %02d:%02d:00", hour, minute)
				usage := 10.0 + float64(hostNum)*5.0 + float64(minute)*0.1

				_, err := conn.Exec(ctx,
					"INSERT INTO cpu_usage (ts, host, usage) VALUES ($1, $2, $3)",
					timestamp, hostname, usage)
				require.NoError(t, err, "failed to insert test data")
			}
		}
	}

	t.Logf("Loaded test data for 10 hosts with 1440 rows each")
}

// createTestCSV creates a temporary CSV file with the given content
func createTestCSV(t *testing.T, content string) string {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "benchmark-test-*.csv")
	require.NoError(t, err, "failed to create temp CSV file")

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err, "failed to write CSV content")

	err = tmpFile.Close()
	require.NoError(t, err, "failed to close temp CSV file")

	return tmpFile.Name()
}
