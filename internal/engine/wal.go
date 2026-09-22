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

// WAL is an append-only write-ahead log stored as JSON lines. Each applied
// command is written (and optionally fsync'd) before it is acknowledged, so a
// crash can never lose an acknowledged transfer.
type WAL struct {
	mu    sync.Mutex
	f     *os.File
	w     *bufio.Writer
	fsync bool
}

// OpenWAL opens (creating if needed) the log at path for appending. When fsync
// is true every append is flushed to stable storage before returning, which is
// the durable-but-slower mode; when false, appends are buffered for throughput.
func OpenWAL(path string, fsync bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{f: f, w: bufio.NewWriter(f), fsync: fsync}, nil
}

// append writes one record. It is called only from the engine's single run
// loop, so the mutex only guards against a concurrent Close.
func (w *WAL) append(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := w.w.Write(append(b, '\n')); err != nil {
		return err
	}
	if w.fsync {
		if err := w.w.Flush(); err != nil {
			return err
		}
		return w.f.Sync()
	}
	return nil
}

// Close flushes any buffered records and closes the file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

// crashClose releases the file handle WITHOUT flushing the buffer, modelling a
// process that dies abruptly. With fsync enabled nothing is buffered, so every
// acknowledged record is already durable; this is used only to simulate a crash
// in tests while still releasing the OS handle.
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
