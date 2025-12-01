package csv

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
)

// Query represents a single benchmark query specification
type Query struct {
	Hostname  string // Host to query
	StartTime string // Start timestamp (raw string from CSV)
	EndTime   string // End timestamp (raw string from CSV)
	LineNum   int    // Line number for error reporting
}

// ParseError represents an error encountered while parsing
type ParseError struct {
	LineNum int
	Message string
	Err     error
}

func (e *ParseError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("line %d: %s: %v", e.LineNum, e.Message, e.Err)
	}
	return fmt.Sprintf("line %d: %s", e.LineNum, e.Message)
}

// Reader reads CSV line by line and parses each line individually
type Reader struct {
	file       *os.File
	scanner    *bufio.Scanner
	lineNum    int
	headerRead bool
}

// NewReader creates a new CSV reader
func NewReader(r io.Reader) *Reader {
	scanner := bufio.NewScanner(r)
	// Set a larger buffer for lines (default is 64KB)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024) // Allow up to 1MB lines

	return &Reader{
		scanner: scanner,
	}
}

// NewReaderFromFile creates a reader from a file path
func NewReaderFromFile(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reader := NewReader(f)
	reader.file = f
	return reader, nil
}

// Close closes the underlying file if opened by this reader
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// Read reads and parses the next line, returns io.EOF when done
func (r *Reader) Read() (*Query, error) {
	for r.scanner.Scan() {
		r.lineNum++
		line := r.scanner.Bytes()

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Trim \r if present
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}

		// Skip header
		if !r.headerRead {
			r.headerRead = true
			if isHeaderLine(line) {
				continue
			}
		}

		// Parse the line
		query, err := r.parseLine(line)
		if err != nil {
			return nil, err
		}

		return query, nil
	}

	if err := r.scanner.Err(); err != nil {
		return nil, err
	}

	return nil, io.EOF
}

// parseLine parses a single CSV line with strict validation.
// Expected format: hostname,start_time,end_time
func (r *Reader) parseLine(line []byte) (*Query, error) {
	fields, err := r.splitCSVLine(line)
	if err != nil {
		return nil, err
	}

	hostname := fields[0]
	startTime := fields[1]
	endTime := fields[2]

	if err := r.validateHostname(hostname); err != nil {
		return nil, err
	}

	if err := r.validateTimestamp(startTime, "start_time"); err != nil {
		return nil, err
	}

	if err := r.validateTimestamp(endTime, "end_time"); err != nil {
		return nil, err
	}

	return &Query{
		Hostname:  string(hostname),
		StartTime: string(startTime),
		EndTime:   string(endTime),
		LineNum:   r.lineNum,
	}, nil
}

// splitCSVLine splits a CSV line into exactly 3 fields.
func (r *Reader) splitCSVLine(line []byte) ([][]byte, error) {
	comma1 := bytes.IndexByte(line, ',')
	if comma1 < 0 {
		return nil, r.newParseError("expected 3 fields (hostname,start_time,end_time), got 1")
	}

	rest := line[comma1+1:]
	comma2 := bytes.IndexByte(rest, ',')
	if comma2 < 0 {
		return nil, r.newParseError("expected 3 fields (hostname,start_time,end_time), got 2")
	}

	remainder := rest[comma2+1:]
	if bytes.IndexByte(remainder, ',') >= 0 {
		fieldCount := bytes.Count(line, []byte{','}) + 1
		return nil, r.newParseError(fmt.Sprintf("expected 3 fields (hostname,start_time,end_time), got %d", fieldCount))
	}

	return [][]byte{
		line[:comma1], // hostname
		rest[:comma2], // start_time
		remainder,     // end_time
	}, nil
}

// validateHostname ensures hostname is not empty
func (r *Reader) validateHostname(hostname []byte) error {
	if len(hostname) == 0 {
		return r.newParseError("empty hostname")
	}
	return nil
}

// validateTimestamp validates "YYYY-MM-DD HH:MM:SS" format
func (r *Reader) validateTimestamp(ts []byte, fieldName string) error {
	if !isValidTimestampFormat(ts) {
		return r.newParseError(fmt.Sprintf(
			"invalid %s format: %q (expected YYYY-MM-DD HH:MM:SS)",
			fieldName, string(ts),
		))
	}
	return nil
}

// newParseError creates a ParseError with the current line number
func (r *Reader) newParseError(message string) *ParseError {
	return &ParseError{
		LineNum: r.lineNum,
		Message: message,
	}
}

// isHeaderLine checks if a line is the CSV header
func isHeaderLine(line []byte) bool {
	// Exact match prevents matching data rows that contain these strings
	return bytes.Equal(line, []byte("hostname,start_time,end_time"))
}

// isValidTimestampFormat validates "YYYY-MM-DD HH:MM:SS" format
func isValidTimestampFormat(ts []byte) bool {
	if len(ts) != 19 {
		return false
	}

	// Check structure: YYYY-MM-DD HH:MM:SS
	if ts[4] != '-' || ts[7] != '-' || ts[10] != ' ' || ts[13] != ':' || ts[16] != ':' {
		return false
	}

	// Check all other positions are digits
	for i, c := range ts {
		if i == 4 || i == 7 || i == 10 || i == 13 || i == 16 {
			continue
		}
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}
