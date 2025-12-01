package hasher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConsistentHasher_GetWorkerID verifies that the same hostname consistently
// maps to the same worker ID and all IDs are within valid range.
func TestConsistentHasher_GetWorkerID(t *testing.T) {
	h := New(10)

	// Same hostname should always return same worker
	workerID1 := h.GetWorkerID("host_000001")
	workerID2 := h.GetWorkerID("host_000001")
	assert.Equal(t, workerID1, workerID2, "same hostname should map to same worker")

	// Worker ID should be in valid range
	for i := 0; i < 100; i++ {
		hostname := fmt.Sprintf("host_%06d", i)
		workerID := h.GetWorkerID(hostname)
		assert.GreaterOrEqual(t, workerID, 0)
		assert.Less(t, workerID, 10)
	}
}

// TestConsistentHasher_VariousWorkerCounts verifies hashing works correctly
// with different worker counts.
func TestConsistentHasher_VariousWorkerCounts(t *testing.T) {
	testCases := []int{1, 8, 10, 16, 32, 100}

	for _, numWorkers := range testCases {
		h := New(numWorkers)
		assert.Equal(t, numWorkers, h.NumWorkers())

		// Verify all worker IDs are in valid range
		for i := 0; i < 50; i++ {
			hostname := fmt.Sprintf("host_%06d", i)
			workerID := h.GetWorkerID(hostname)
			assert.GreaterOrEqual(t, workerID, 0)
			assert.Less(t, workerID, numWorkers)
		}
	}
}

// TestConsistentHasher_Distribution validates that hash function distributes
// hostnames reasonably evenly across workers (within 50% of expected average).
func TestConsistentHasher_Distribution(t *testing.T) {
	h := New(10)
	distribution := make(map[int]int)

	// Test distribution across 1000 hostnames
	for i := 0; i < 1000; i++ {
		hostname := fmt.Sprintf("host_%06d", i)
		workerID := h.GetWorkerID(hostname)
		distribution[workerID]++
	}

	// Check all workers got some work
	for i := 0; i < 10; i++ {
		assert.Greater(t, distribution[i], 0, "worker %d should have some work", i)
	}

	// Check distribution is reasonably balanced (within 50% of expected)
	expected := 100.0 // 1000 / 10
	for workerID, count := range distribution {
		ratio := float64(count) / expected
		assert.Greater(t, ratio, 0.5, "worker %d is underloaded: %d", workerID, count)
		assert.Less(t, ratio, 1.5, "worker %d is overloaded: %d", workerID, count)
	}
}

// TestConsistentHasher_SingleWorker ensures single-worker configuration
// always maps all hostnames to worker 0.
func TestConsistentHasher_SingleWorker(t *testing.T) {
	h := New(1)

	// All hostnames should map to worker 0
	assert.Equal(t, 0, h.GetWorkerID("host_000001"))
	assert.Equal(t, 0, h.GetWorkerID("host_000002"))
	assert.Equal(t, 0, h.GetWorkerID("any_host"))
}

// TestConsistentHasher_ZeroWorkers verifies that zero workers
// defaults to single worker configuration.
func TestConsistentHasher_ZeroWorkers(t *testing.T) {
	h := New(0) // Should default to 1

	assert.Equal(t, 1, h.NumWorkers())
	assert.Equal(t, 0, h.GetWorkerID("host"))
}

// TestConsistentHasher_NegativeWorkers checks that negative worker count
// is handled gracefully by defaulting to single worker.
func TestConsistentHasher_NegativeWorkers(t *testing.T) {
	h := New(-5) // Should default to 1

	assert.Equal(t, 1, h.NumWorkers())
}

// TestConsistentHasher_Consistency ensures multiple hasher instances
// with same config produce identical mappings for the same hostnames.
func TestConsistentHasher_Consistency(t *testing.T) {
	// Create multiple hashers with same config
	h1 := New(10)
	h2 := New(10)

	hostnames := []string{
		"host_000001",
		"host_000002",
		"server_production_1",
		"db-master-001",
		"cache-node-42",
	}

	for _, hostname := range hostnames {
		assert.Equal(t,
			h1.GetWorkerID(hostname),
			h2.GetWorkerID(hostname),
			"different hasher instances should produce same result for %s", hostname)
	}
}

// TestConsistentHasher_GetWorkerIDBytes validates that string and byte slice
// versions of GetWorkerID produce identical results.
func TestConsistentHasher_GetWorkerIDBytes(t *testing.T) {
	h := New(10)

	hostname := "host_000001"
	hostnameBytes := []byte(hostname)

	// String and bytes version should produce same result
	assert.Equal(t,
		h.GetWorkerID(hostname),
		h.GetWorkerIDBytes(hostnameBytes),
		"string and bytes version should produce same result")
}

// TestHashFNV1a verifies the FNV-1a hash function is deterministic
// and produces different hashes for different inputs.
func TestHashFNV1a(t *testing.T) {
	// Test that hash is deterministic
	hash1 := hashFNV1a("test")
	hash2 := hashFNV1a("test")
	assert.Equal(t, hash1, hash2)

	// Different strings should produce different hashes
	hash3 := hashFNV1a("test2")
	assert.NotEqual(t, hash1, hash3)
}
