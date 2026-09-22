// Package locked is a correct ledger that serializes every operation behind a
// single global mutex. It is the third implementation in the benchmark, and the
// most important one for an honest comparison.
//
// The naive ledger shows that skipping synchronization loses money. But an
// interviewer will rightly say that is a strawman: obviously you need a lock.
// So this package adds the obvious fix, one big lock, and it is genuinely
// correct: money is conserved and retries are idempotent.
//
// The catch is that the single lock is a global serialization point. Every
// transfer on every core contends for the same mutex, so throughput does not
// scale and tail latency grows under load. Comparing this against the engine is
// the real comparison: not "correct vs broken", but two correct designs and the
// cost of how each one serializes.
package locked

import (
	"sync"

	"github.com/GubinGeramifard/ledger/internal/engine"
)

type Ledger struct {
	mu   sync.Mutex
	bal  map[string]engine.Money
	seen map[string]struct{}
}

func New() *Ledger {
	return &Ledger{bal: map[string]engine.Money{}, seen: map[string]struct{}{}}
}

func (l *Ledger) CreateAccount(id string) {
	l.mu.Lock()
	l.bal[id] = 0
	l.mu.Unlock()
}

func (l *Ledger) Deposit(id string, amount engine.Money) {
	l.mu.Lock()
	l.bal[id] += amount
	l.mu.Unlock()
}

// Transfer moves money under the lock, with idempotency. It reports whether the
// call was a deduplicated retry. The whole critical section, including the
// idempotency check, is serialized, which is what makes it correct and also what
// makes it a bottleneck.
func (l *Ledger) Transfer(idemKey, from, to string, amount engine.Money) (deduped bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if idemKey != "" {
		if _, ok := l.seen[idemKey]; ok {
			return true
		}
		l.seen[idemKey] = struct{}{}
	}
	if l.bal[from] < amount {
		return false
	}
	l.bal[from] -= amount
	l.bal[to] += amount
	return false
}

func (l *Ledger) Balance(id string) engine.Money {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bal[id]
}

func (l *Ledger) Total() engine.Money {
	l.mu.Lock()
	defer l.mu.Unlock()
	var t engine.Money
	for _, b := range l.bal {
		t += b
	}
	return t
}
