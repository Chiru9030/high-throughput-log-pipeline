package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
)

const (
	maxWALRecordBytes  uint32 = 4 * 1024 * 1024 // 4 MiB safety cap prevents OOM on corrupt headers
	CompactThreshold          = 4 * 1024 * 1024 // Trigger compaction once WAL exceeds 4 MiB and checkpoint advances
)

var (
	ErrRecordTooLarge = errors.New("wal record exceeds maximum safe size")
	ErrWALClosed      = errors.New("wal file is closed")
)

type Record struct {
	Seq   uint64
	Event model.LogEvent
}

// WAL represents an append-only, checksummed Write-Ahead Log with crash recovery and compaction.
type WAL struct {
	mu               sync.Mutex
	path             string
	ckptPath         string
	file             *os.File
	writer           *bufio.Writer
	seq              atomic.Uint64
	committedSeq     atomic.Uint64
	bytesWritten     atomic.Int64
	nextCompactBytes atomic.Int64
}

func Open(path string) (*WAL, error) {
	// Open without O_APPEND so Recover can cleanly truncate any corrupt tail before switching to tail appends.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	info, statErr := f.Stat()
	var size int64
	if statErr == nil {
		size = info.Size()
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	w := &WAL{
		path:     path,
		ckptPath: path + ".ckpt",
		file:     f,
		writer:   bufio.NewWriterSize(f, 32*1024),
	}
	w.bytesWritten.Store(size)
	nextThresh := int64(CompactThreshold)
	if size >= nextThresh {
		nextThresh = size + CompactThreshold
	}
	w.nextCompactBytes.Store(nextThresh)

	if ckpt, err := w.readCheckpoint(); err == nil {
		w.committedSeq.Store(ckpt)
		w.seq.Store(ckpt)
	}
	return w, nil
}

func (w *WAL) readCheckpoint() (uint64, error) {
	data, err := os.ReadFile(w.ckptPath)
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (w *WAL) CommittedSeq() uint64 {
	return w.committedSeq.Load()
}

// MarkCommitted persists the cold-storage checkpoint sequence and compacts oversized flushed segments.
func (w *WAL) MarkCommitted(seq uint64) error {
	if seq <= w.committedSeq.Load() {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq <= w.committedSeq.Load() {
		return nil
	}
	tmp := w.ckptPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(seq, 10)+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.ckptPath); err != nil {
		return err
	}
	w.committedSeq.Store(seq)
	if w.bytesWritten.Load() >= w.nextCompactBytes.Load() {
		_ = w.compactLocked(seq)
	}
	return nil
}

func (w *WAL) CompactIfNeeded(force bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return ErrWALClosed
	}
	if !force && w.bytesWritten.Load() < w.nextCompactBytes.Load() {
		return nil
	}
	return w.compactLocked(w.committedSeq.Load())
}

// compactLocked rewrites the WAL retaining only records with Seq > committedSeq without losing valid data on error.
func (w *WAL) compactLocked(committed uint64) error {
	if committed == 0 || w.file == nil {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		w.writer.Reset(w.file)
		return err
	}
	records, readErr := w.readValidRecordsLocked()
	if readErr != nil {
		return readErr
	}
	var kept []Record
	for _, r := range records {
		if r.Seq > committed {
			kept = append(kept, r)
		}
	}

	tmpPath := w.path + ".compact"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(tmpFile, 32*1024)
	var newSize int64
	for _, r := range kept {
		data, mErr := json.Marshal(r.Event)
		if mErr != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return mErr
		}
		var header [16]byte
		binary.BigEndian.PutUint64(header[0:8], r.Seq)
		binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))
		binary.BigEndian.PutUint32(header[12:16], crc32.ChecksumIEEE(data))
		if _, err := bw.Write(header[:]); err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return err
		}
		if _, err := bw.Write(data); err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return err
		}
		newSize += 16 + int64(len(data))
	}
	if err := bw.Flush(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	_ = tmpFile.Close()

	if err := os.Rename(tmpPath, w.path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	reopened, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = w.file.Close()
		w.file = nil
		return err
	}
	_, _ = reopened.Seek(0, io.SeekEnd)
	_ = w.file.Close()
	w.file = reopened
	w.writer.Reset(reopened)
	w.bytesWritten.Store(newSize)
	nextThresh := int64(CompactThreshold)
	if newSize*2 > nextThresh {
		nextThresh = newSize * 2
	}
	w.nextCompactBytes.Store(nextThresh)
	return nil
}

// Append synchronously writes an event with sequence number and CRC32 checksum.
func (w *WAL) Append(ev model.LogEvent) (uint64, error) {
	return w.AppendWithHook(ev, nil)
}

// AppendWithHook assigns and registers the sequence atomically under the WAL mutex prior to flushing.
func (w *WAL) AppendWithHook(ev model.LogEvent, onSeqReserved func(uint64)) (uint64, error) {
	data, err := json.Marshal(ev)
	if err != nil {
		return 0, err
	}
	if uint64(len(data)) > uint64(maxWALRecordBytes) {
		return 0, ErrRecordTooLarge
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, ErrWALClosed
	}

	seq := w.seq.Add(1)
	if onSeqReserved != nil {
		onSeqReserved(seq)
	}
	checksum := crc32.ChecksumIEEE(data)

	var header [16]byte
	binary.BigEndian.PutUint64(header[0:8], seq)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))
	binary.BigEndian.PutUint32(header[12:16], checksum)

	if _, err := w.writer.Write(header[:]); err != nil {
		w.writer.Reset(w.file)
		return seq, err
	}
	if _, err := w.writer.Write(data); err != nil {
		w.writer.Reset(w.file)
		return seq, err
	}
	if err := w.writer.Flush(); err != nil {
		w.writer.Reset(w.file)
		return seq, err
	}

	w.bytesWritten.Add(16 + int64(len(data)))
	return seq, nil
}

func (w *WAL) readValidRecordsLocked() ([]Record, error) {
	if w.file == nil {
		return nil, ErrWALClosed
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records []Record
	var header [16]byte
	var lastValidOffset int64

	for {
		_, err := io.ReadFull(w.file, header[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return records, err
		}

		seq := binary.BigEndian.Uint64(header[0:8])
		length := binary.BigEndian.Uint32(header[8:12])
		checksum := binary.BigEndian.Uint32(header[12:16])

		if length == 0 || length > maxWALRecordBytes {
			break
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return records, err
		}

		if crc32.ChecksumIEEE(data) != checksum {
			break
		}

		var ev model.LogEvent
		if err := json.Unmarshal(data, &ev); err != nil {
			break
		}
		records = append(records, Record{Seq: seq, Event: ev})
		if seq > w.seq.Load() {
			w.seq.Store(seq)
		}
		lastValidOffset += int64(16) + int64(length)
	}

	if truncErr := w.file.Truncate(lastValidOffset); truncErr != nil {
		return records, truncErr
	}
	if _, seekErr := w.file.Seek(lastValidOffset, io.SeekStart); seekErr != nil {
		return records, seekErr
	}
	w.bytesWritten.Store(lastValidOffset)
	return records, nil
}

// RecoverRecords reads all valid records with their sequence numbers and truncates any corrupt tail.
func (w *WAL) RecoverRecords() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil, ErrWALClosed
	}
	if err := w.writer.Flush(); err != nil {
		w.writer.Reset(w.file)
		return nil, err
	}
	recs, err := w.readValidRecordsLocked()
	w.writer.Reset(w.file)
	return recs, err
}

// Recover returns all decoded LogEvents from valid WAL frames.
func (w *WAL) Recover() ([]model.LogEvent, error) {
	recs, err := w.RecoverRecords()
	events := make([]model.LogEvent, len(recs))
	for i := range recs {
		events[i] = recs[i].Event
	}
	return events, err
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		w.writer.Reset(w.file)
	}
	err := w.file.Close()
	w.file = nil
	return err
}
