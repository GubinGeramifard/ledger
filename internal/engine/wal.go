package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Record is one durable, replayable state change. The log is the source of
// truth: the in-memory state is just a fast projection of the records, and can
// always be rebuilt by replaying them in order.
type Record struct {
	Seq     uint64 `json:"seq"`
	Type    string `json:"type"` // "create" or "transfer"
	Account string `json:"account,omitempty"`
	IdemKey string `json:"idem,omitempty"`
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Amount  Money  `json:"amount,omitempty"`
}

const (
	recCreate   = "create"
	recTransfer = "transfer"
)

// WAL is an append-only write-ahead log stored as JSON lines. Records are
// buffered by append and made durable by sync; the engine's run loop appends a
// group of records and then calls sync once for the whole group (group commit).
type WAL struct {
	mu      sync.Mutex
	f       *os.File
	w       *bufio.Writer
	durable bool
}

// OpenWAL opens (creating if needed) the log at path for appending. When durable
// is true, sync flushes to stable storage with fsync; when false, sync only
// flushes to the OS (fast, but a crash can lose the last records).
func OpenWAL(path string, durable bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{f: f, w: bufio.NewWriter(f), durable: durable}, nil
}

// append buffers one record without flushing. It is called only from the
// engine's single run loop; the mutex only guards against a concurrent Close.
func (w *WAL) append(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = w.w.Write(append(b, '\n'))
	return err
}

// sync flushes buffered records and, when durable, fsyncs them to disk. This is
// the commit point: the engine calls it once per batch, before acknowledging any
// caller in that batch.
func (w *WAL) sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.durable {
		return w.f.Sync()
	}
	return nil
}

// Close flushes and (when durable) fsyncs any remaining records, then closes.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.durable {
		_ = w.f.Sync()
	}
	return w.f.Close()
}

// crashClose releases the file handle WITHOUT flushing the buffer, modelling a
// process that dies abruptly. Records already synced (acknowledged) are durable;
// this is used only to simulate a crash in tests while releasing the OS handle.
func (w *WAL) crashClose() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// ReadWAL reads every record from the log at path, in order. A missing file is
// treated as an empty log so a fresh node starts cleanly.
func ReadWAL(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var out []Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			// A torn final line (partial write during a crash) is expected;
			// stop at the last fully-written record.
			break
		}
		out = append(out, r)
	}
	return out, nil
}
