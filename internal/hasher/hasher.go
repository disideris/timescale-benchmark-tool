package hasher

const (
	// FNV-1a 64-bit constants
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// ConsistentHasher maps hostnames to worker IDs consistently.
// The same hostname will always map to the same worker.
// Uses FNV-1a for fast hashing.
type ConsistentHasher struct {
	numWorkers int
}

// New creates a new ConsistentHasher with the given number of workers.
func New(numWorkers int) *ConsistentHasher {
	if numWorkers < 1 {
		numWorkers = 1
	}

	return &ConsistentHasher{
		numWorkers: numWorkers,
	}
}

// GetWorkerID returns the worker ID for a given hostname.
// The result is deterministic: the same hostname always returns the same worker ID.
func (h *ConsistentHasher) GetWorkerID(hostname string) int {
	hash := hashFNV1a(hostname)
	return int(hash % uint64(h.numWorkers))
}

// GetWorkerIDBytes returns the worker ID for a hostname as bytes.
// Avoids string allocation when working with []byte directly.
func (h *ConsistentHasher) GetWorkerIDBytes(hostname []byte) int {
	hash := hashFNV1aBytes(hostname)
	return int(hash % uint64(h.numWorkers))
}

// hashFNV1a computes FNV-1a hash of a string.
// FNV-1a: XOR then multiply.
func hashFNV1a(s string) uint64 {
	hash := uint64(offset64)
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}
	return hash
}

// hashFNV1aBytes computes FNV-1a hash of a byte slice. Zero allocation.
func hashFNV1aBytes(b []byte) uint64 {
	hash := uint64(offset64)
	for _, c := range b {
		hash ^= uint64(c)
		hash *= prime64
	}
	return hash
}

// NumWorkers returns the number of workers in the hash ring.
func (h *ConsistentHasher) NumWorkers() int {
	return h.numWorkers
}
