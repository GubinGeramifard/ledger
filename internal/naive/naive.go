// Package naive is a deliberately incorrect ledger. It is the "before" picture
// in the benchmark: the way money transfers are often written the first time,
// and why that is dangerous under concurrency.
//
// Two bugs are baked in on purpose:
//
//  1. No synchronization. A balance is read, checked, and written with no lock.
//     Two goroutines transferring from the same account interleave: both read
//     the old balance, both subtract, and one write is lost. Money is created
//     or destroyed.
//
//  2. No idempotency. Every call applies, so a retried request (the norm on any
//     real network) is charged twice.
//
// Balances are kept in a slice rather than a map only so the demonstration can
// run to completion: Go's runtime deliberately crashes on concurrent map access,
// which would mask the subtler and more dangerous failure, silent corruption of
// the numbers. The point is not that this code is bad. It is that it looks
// reasonable and passes every single-threaded test, yet loses real money the
// moment it runs under load. That is exactly what the engine package prevents.
package naive

import "github.com/GubinGeramifard/ledger/internal/engine"

// Ledger is an unsynchronized account store. Do not use it for anything real.
// Account ids are mapped to slice indexes once, before the concurrent phase, so
// the map itself is only ever read concurrently; the races are all on the
// balances.
type Ledger struct {
	idx map[string]int
	bal []engine.Money
}

func New() *Ledger {
	return &Ledger{idx: map[string]int{}}
}

func (l *Ledger) CreateAccount(id string) {
	l.idx[id] = len(l.bal)
	l.bal = append(l.bal, 0)
}

func (l *Ledger) Deposit(id string, amount engine.Money) {
	l.bal[l.idx[id]] += amount
}

// Transfer moves money with no locking and no idempotency. The idemKey argument
// is accepted so the benchmark can call it the same way as the engine, but it
// is ignored, which is the second bug.
func (l *Ledger) Transfer(idemKey, from, to string, amount engine.Money) {
	fi, ti := l.idx[from], l.idx[to]
	// Read.
	fromBal := l.bal[fi]
	if fromBal < amount {
		return
	}
	toBal := l.bal[ti]
	// ... another goroutine can run right here, between read and write ...
	// Write. Any concurrent update to these accounts is now clobbered.
	l.bal[fi] = fromBal - amount
	l.bal[ti] = toBal + amount
}

func (l *Ledger) Balance(id string) engine.Money {
	return l.bal[l.idx[id]]
}

// Total sums every balance. In a correct ledger this never changes as money
// moves around; here it drifts as updates are lost.
func (l *Ledger) Total() engine.Money {
	var t engine.Money
	for _, b := range l.bal {
		t += b
	}
	return t
}
