package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
)

// WAL represents an append-only, checksummed Write-Ahead Log for crash resilience.
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	seq    atomic.Uint64
}

func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:   f,
		writer: bufio.NewWriterSize(f, 32*1024),
	}, nil
}

// Append synchronously writes an event with sequence number and CRC32 checksum.
func (w *WAL) Append(ev model.LogEvent) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(ev)
	if err != nil {
		return 0, err
	}

	seq := w.seq.Add(1)
	checksum := crc32.ChecksumIEEE(data)

	// Format: [8 bytes seq][4 bytes len][4 bytes crc32][data]
	var header [16]byte
	binary.BigEndian.PutUint64(header[0:8], seq)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))
	binary.BigEndian.PutUint32(header[12:16], checksum)

	if _, err := w.writer.Write(header[:]); err != nil {
		return 0, err
	}
	if _, err := w.writer.Write(data); err != nil {
		return 0, err
	}
	if err := w.writer.Flush(); err != nil {
		return 0, err
	}

	return seq, nil
}

// Recover reads all valid records from the start of the WAL file.
func (w *WAL) Recover() ([]model.LogEvent, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return nil, err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var events []model.LogEvent
	var header [16]byte

	for {
		_, err := io.ReadFull(w.file, header[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return events, err
		}

		seq := binary.BigEndian.Uint64(header[0:8])
		length := binary.BigEndian.Uint32(header[8:12])
		checksum := binary.BigEndian.Uint32(header[12:16])

		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			break
		}

		if crc32.ChecksumIEEE(data) != checksum {
			// Corrupted record detected: stop recovery at safe boundary
			break
		}

		var ev model.LogEvent
		if err := json.Unmarshal(data, &ev); err == nil {
			events = append(events, ev)
		}
		if seq > w.seq.Load() {
			w.seq.Store(seq)
		}
	}

	// Seek back to end for future appends
	_, _ = w.file.Seek(0, io.SeekEnd)
	return events, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.writer.Flush()
	return w.file.Close()
}
