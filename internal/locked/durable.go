package locked

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"github.com/GubinGeramifard/ledger/internal/engine"
)

// DurableLedger is the mutex ledger made durable the obvious way: every transfer
// writes its record and fsyncs it to disk before releasing the lock. It is
// correct and crash-safe, but the fsync happens inside the critical section, so
// every transfer serializes on disk I/O. That is the cost the engine's group
// commit is built to avoid: this ledger can do at most one fsync per transfer,
// while the engine amortizes one fsync over a whole batch.
type DurableLedger struct {
	mu  sync.Mutex
	bal map[string]engine.Money
	f   *os.File
	w   *bufio.Writer
}

func NewDurable(path string) (*DurableLedger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &DurableLedger{bal: map[string]engine.Money{}, f: f, w: bufio.NewWriter(f)}, nil
}

func (l *DurableLedger) CreateAccount(id string) { l.bal[id] = 0 }
func (l *DurableLedger) Deposit(id string, amount engine.Money) {
	l.bal[id] += amount
}

// Transfer applies the change and durably logs it, all under the lock. The lock
// is held across the fsync, which is what serializes throughput on the disk.
func (l *DurableLedger) Transfer(from, to string, amount engine.Money) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bal[from] < amount {
		return
	}
	l.bal[from] -= amount
	l.bal[to] += amount
	fmt.Fprintf(l.w, "%s %s %d\n", from, to, amount)
	_ = l.w.Flush()
	_ = l.f.Sync() // durability: not acknowledged until on disk
}

func (l *DurableLedger) Total() engine.Money {
	var t engine.Money
	for _, b := range l.bal {
		t += b
	}
	return t
}

func (l *DurableLedger) Close() error {
	_ = l.w.Flush()
	return l.f.Close()
}
