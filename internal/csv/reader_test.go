package csv

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReader_CommaSeparated verifies basic CSV parsing with proper header,
// comma-separated fields, and correct timestamp format.
func TestReader_CommaSeparated(t *testing.T) {
	input := "hostname,start_time,end_time\n" +
		"host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22\n" +
		"host_000001,2017-01-02 13:02:02,2017-01-02 14:02:02\n"

	reader := NewReader(strings.NewReader(input))

	// First line
	query, err := reader.Read()
	require.NoError(t, err)
	require.NotNil(t, query)
	assert.Equal(t, "host_000008", query.Hostname)
	assert.Equal(t, "2017-01-01 08:59:22", query.StartTime)
	assert.Equal(t, "2017-01-01 09:59:22", query.EndTime)

	// Second line
	query, err = reader.Read()
	require.NoError(t, err)
	require.NotNil(t, query)
	assert.Equal(t, "host_000001", query.Hostname)
	assert.Equal(t, "2017-01-02 13:02:02", query.StartTime)
	assert.Equal(t, "2017-01-02 14:02:02", query.EndTime)

	// EOF
	query, err = reader.Read()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, query)
}

// TestReader_NoHeader tests that CSV files without header line
// are correctly parsed (first data row is not skipped).
func TestReader_NoHeader(t *testing.T) {
	input := "host_000001,2017-01-01 08:59:22,2017-01-01 09:59:22\n"

	reader := NewReader(strings.NewReader(input))

	query, err := reader.Read()
	require.NoError(t, err)
	require.NotNil(t, query)
	assert.Equal(t, "host_000001", query.Hostname)
}

// TestReader_InvalidTimestamp ensures malformed timestamps are rejected
// with clear ParseError messages.
func TestReader_InvalidTimestamp(t *testing.T) {
	input := "hostname,start_time,end_time\n" +
		"host_000001,invalid,2017-01-01 09:59:22\n"

	reader := NewReader(strings.NewReader(input))

	_, err := reader.Read()
	require.Error(t, err)

	var parseErr *ParseError
	require.ErrorAs(t, err, &parseErr)
	assert.Contains(t, parseErr.Message, "invalid start_time format")
}

// TestReader_ExtraFields validates that rows with incorrect field count
// (e.g., empty field creating 4 fields instead of 3) are rejected.
func TestReader_ExtraFields(t *testing.T) {
	input := "hostname,start_time,end_time\n" +
		"host_000001,,2017-01-01 08:59:22,2017-01-01 09:59:22\n"

	reader := NewReader(strings.NewReader(input))

	_, err := reader.Read()
	require.Error(t, err)

	var parseErr *ParseError
	require.ErrorAs(t, err, &parseErr)
	assert.Contains(t, parseErr.Message, "expected 3 fields")
	assert.Contains(t, parseErr.Message, "got 4")
}

// TestReader_EmptyHostname checks that empty hostname field
// is properly detected and rejected with descriptive error.
func TestReader_EmptyHostname(t *testing.T) {
	input := "hostname,start_time,end_time\n" +
		",2017-01-01 08:59:22,2017-01-01 09:59:22\n"

	reader := NewReader(strings.NewReader(input))

	_, err := reader.Read()
	require.Error(t, err)

	var parseErr *ParseError
	require.ErrorAs(t, err, &parseErr)
	assert.Contains(t, parseErr.Message, "empty hostname")
}

// TestReader_MultipleLines verifies streaming behavior with 100 queries
// to ensure memory efficiency with large files.
func TestReader_MultipleLines(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("hostname,start_time,end_time\n")
	for i := 0; i < 100; i++ {
		sb.WriteString("host_000001,2017-01-01 08:59:22,2017-01-01 09:59:22\n")
	}
	input := sb.String()

	reader := NewReader(strings.NewReader(input))

	count := 0
	for {
		query, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotNil(t, query)
		count++
	}

	assert.Equal(t, 100, count)
}
