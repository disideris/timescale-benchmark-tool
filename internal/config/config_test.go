package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDefaultConfig verifies that default configuration values are set correctly
// including database connection defaults and worker pool settings.
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, "localhost", cfg.DBHost)
	assert.Equal(t, 5432, cfg.DBPort)
	assert.Equal(t, "postgres", cfg.DBUser)
	assert.Equal(t, "homework", cfg.DBName)
	assert.Greater(t, cfg.NumWorkers, 0)
	assert.Equal(t, 1000, cfg.QueueDepth)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, 30*time.Second, cfg.QueryTimeout)
}

// TestConfig_Validate_ValidConfig ensures that a properly configured
// config passes validation without errors.
func TestConfig_Validate_ValidConfig(t *testing.T) {
	cfg := DefaultConfig()
	err := cfg.Validate()
	assert.NoError(t, err)
}

// TestConfig_Validate_InvalidWorkers checks that worker count validation
// enforces the 1-100 range limit.
func TestConfig_Validate_InvalidWorkers(t *testing.T) {
	cfg := DefaultConfig()

	cfg.NumWorkers = 0
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workers must be between 1 and 100")

	cfg.NumWorkers = 101
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workers must be between 1 and 100")
}

// TestConfig_Validate_InvalidQueueDepth verifies that queue depth
// must be at least 1 to prevent deadlocks.
func TestConfig_Validate_InvalidQueueDepth(t *testing.T) {
	cfg := DefaultConfig()
	cfg.QueueDepth = 0

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue depth")
}

// TestConfig_Validate_InvalidTimeout ensures query timeout is at least 1 second
// to prevent premature timeouts on slow networks.
func TestConfig_Validate_InvalidTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.QueryTimeout = 500 * time.Millisecond

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

// TestConfig_Validate_NonExistentFile checks that validation catches
// non-existent input files before starting the benchmark.
func TestConfig_Validate_NonExistentFile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InputFile = "/nonexistent/file.csv"
	cfg.UseStdin = false

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

// TestConfig_DSN verifies that the PostgreSQL connection string (DSN)
// is correctly formatted with all connection parameters.
func TestConfig_DSN(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DBHost = "myhost"
	cfg.DBPort = 5433
	cfg.DBUser = "myuser"
	cfg.DBPassword = "mypass"
	cfg.DBName = "mydb"
	cfg.DBSSLMode = "require"

	dsn := cfg.DSN()

	assert.Contains(t, dsn, "host=myhost")
	assert.Contains(t, dsn, "port=5433")
	assert.Contains(t, dsn, "user=myuser")
	assert.Contains(t, dsn, "password=mypass")
	assert.Contains(t, dsn, "dbname=mydb")
	assert.Contains(t, dsn, "sslmode=require")
}

// TestConfig_FromEnv validates that configuration correctly reads values
// from PostgreSQL standard environment variables (PGHOST, PGPORT, etc).
func TestConfig_FromEnv(t *testing.T) {
	// Set environment variables
	os.Setenv("PGHOST", "envhost")
	os.Setenv("PGPORT", "5433")
	os.Setenv("PGUSER", "envuser")
	os.Setenv("PGDATABASE", "envdb")
	defer func() {
		os.Unsetenv("PGHOST")
		os.Unsetenv("PGPORT")
		os.Unsetenv("PGUSER")
		os.Unsetenv("PGDATABASE")
	}()

	cfg := DefaultConfig()

	assert.Equal(t, "envhost", cfg.DBHost)
	assert.Equal(t, 5433, cfg.DBPort)
	assert.Equal(t, "envuser", cfg.DBUser)
	assert.Equal(t, "envdb", cfg.DBName)
}
