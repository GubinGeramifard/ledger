// Package engine implements a deterministic double-entry ledger.
//
// Design: all state changes flow through a single goroutine (the run loop),
// which processes commands one at a time. This is the model used by real
// financial databases such as TigerBeetle. It buys three things at once:
//
//   - Correctness: because only one goroutine ever touches the balances, there
//     are no data races and no lost updates, no matter how many clients call
//     concurrently. No per-account locking to get wrong.
//   - Determinism: the same sequence of commands always produces the same
//     state, which is what makes replaying the write-ahead log safe.
//   - Speed: an in-memory state machine with no lock contention still processes
//     hundreds of thousands of transfers per second on a single core.
//
// Every transfer is double-entry: it debits one account and credits another by
// the same amount, so the sum of all balances is invariant. Deposits move money
// in from a special ExternalAccount (which is allowed to go negative and
// represents the world outside the ledger), so the grand total across every
// account, external included, is always exactly zero. That single number is the
// system's conservation law: if it is ever non-zero, money was created or
// destroyed by a bug.
package engine

import "sync"

// ExternalAccount is the counterparty for deposits and withdrawals. It holds
// the negative mirror of all money that has entered user accounts, which keeps
// the ledger-wide balance at exactly zero.
const ExternalAccount = "external"

// Account is a named balance in minor units.
type Account struct {
	ID      string `json:"id"`
	Balance Money  `json:"balance"`
}

// TransferResult reports the outcome of a transfer request.
type TransferResult struct {
	IdemKey string `json:"idem_key,omitempty"`
	Applied bool   `json:"applied"`         // true if it changed state on this call
	Deduped bool   `json:"deduped"`         // true if an idempotency key replayed a prior result
	Err     string `json:"error,omitempty"` // set if the transfer was rejected
}

// Stats is a point-in-time snapshot of ledger-wide counters.
type Stats struct {
	Accounts  int    `json:"accounts"`
	Transfers uint64 `json:"transfers"`
	Total     Money  `json:"total"` // must always be zero
}

// Engine is a single ledger. Its methods are safe to call from any number of
// goroutines; each call is serialized onto the run loop.
type Engine struct {
	ops  chan func()
	quit chan struct{}

	// Everything below is owned exclusively by the run loop.
	accounts map[string]*Account
	seen     map[string]TransferResult // idempotency keys -> first result
	wal      *WAL
	seq      uint64
	count    uint64
}

// New creates an empty engine with no durable log. The run loop starts
// immediately.
func New() *Engine {
	e := &Engine{
		ops:      make(chan func(), 1024),
		quit:     make(chan struct{}),
		accounts: map[string]*Account{ExternalAccount: {ID: ExternalAccount}},
		seen:     map[string]TransferResult{},
	}
	go e.loop()
	return e
}

func (e *Engine) loop() {
	for {
		select {
		case fn := <-e.ops:
			fn()
		case <-e.quit:
			return
		}
	}
}

// donePool recycles the reply channels used by submit so that a hot path of
// millions of calls does not allocate one channel per call. The channels are
// buffered (size 1) and signaled with a send rather than a close, so they can be
// reused instead of thrown away.
var donePool = sync.Pool{New: func() any { return make(chan struct{}, 1) }}

// submit runs fn on the run loop and waits for it to finish, giving callers a
// synchronous API over the serialized state.
func (e *Engine) submit(fn func()) {
	done := donePool.Get().(chan struct{})
	e.ops <- func() {
		fn()
		done <- struct{}{}
	}
	<-done
	donePool.Put(done)
}

// Close stops the run loop and closes the log, if any.
func (e *Engine) Close() error {
	var err error
	e.submit(func() {
		if e.wal != nil {
			err = e.wal.Close()
		}
	})
	close(e.quit)
	return err
}

// simulateCrash models an abrupt process death: it stops the run loop and
// releases the log's file handle without a clean flush. With fsync enabled every
// acknowledged transfer is already on disk, so recovery must reproduce them all.
// Used by the crash-recovery tests.
func (e *Engine) simulateCrash() {
	e.submit(func() {
		if e.wal != nil {
			_ = e.wal.crashClose()
			e.wal = nil
		}
	})
	close(e.quit)
}

// CreateAccount adds a new zero-balance account.
func (e *Engine) CreateAccount(id string) error {
	var err error
	e.submit(func() {
		if _, ok := e.accounts[id]; ok {
			err = ErrAccountExists
			return
		}
		e.accounts[id] = &Account{ID: id}
		e.log(Record{Type: recCreate, Account: id})
	})
	return err
}

// Deposit moves money into an account from the external account. It models
// funds entering the ledger from the outside world.
func (e *Engine) Deposit(id string, amount Money) TransferResult {
	var res TransferResult
	e.submit(func() {
		res = e.apply("", ExternalAccount, id, amount, true)
	})
	return res
}

// Transfer moves money between two accounts. If idemKey is non-empty and has
// been seen before, the original result is returned and no new state change is
// made: retrying a payment is safe and never double-charges.
func (e *Engine) Transfer(idemKey, from, to string, amount Money) TransferResult {
	var res TransferResult
	e.submit(func() {
		res = e.apply(idemKey, from, to, amount, false)
	})
	return res
}

// apply is the heart of the ledger. It runs only on the run loop.
func (e *Engine) apply(idemKey, from, to string, amount Money, allowNegSource bool) TransferResult {
	if idemKey != "" {
		if prev, ok := e.seen[idemKey]; ok {
			prev.Applied = false
			prev.Deduped = true
			return prev
		}
	}

	res := TransferResult{IdemKey: idemKey}
	reject := func(err error) TransferResult {
		res.Err = err.Error()
		e.remember(idemKey, res)
		return res
	}

	src, ok := e.accounts[from]
	if !ok {
		return reject(ErrNoAccount(from))
	}
	dst, ok := e.accounts[to]
	if !ok {
		return reject(ErrNoAccount(to))
	}
	if amount <= 0 {
		return reject(ErrBadAmount)
	}
	if from == to {
		return reject(ErrSameAccount)
	}
	if !allowNegSource && src.Balance < amount {
		return reject(ErrInsufficient)
	}

	// Double-entry posting: the debit and credit are equal, so the ledger-wide
	// total is unchanged.
	src.Balance -= amount
	dst.Balance += amount

	e.seq++
	if from != ExternalAccount {
		e.count++ // deposits are setup, not user transfers
	}
	e.log(Record{
		Seq: e.seq, Type: recTransfer, IdemKey: idemKey,
		From: from, To: to, Amount: amount,
	})

	res.Applied = true
	e.remember(idemKey, res)
	return res
}

func (e *Engine) remember(key string, res TransferResult) {
	if key != "" {
		e.seen[key] = res
	}
}

// log appends a record to the write-ahead log when one is attached. It is a
// no-op during replay (see Recover) and when running without durability.
func (e *Engine) log(r Record) {
	if e.wal != nil {
		_ = e.wal.append(r)
	}
}

// Accounts returns a snapshot of all real (non-external) accounts.
func (e *Engine) Accounts() []Account {
	var out []Account
	e.submit(func() {
		for _, a := range e.accounts {
			if a.ID == ExternalAccount {
				continue
			}
			out = append(out, *a)
		}
	})
	return out
}

// Balance returns the balance of one account.
func (e *Engine) Balance(id string) (Money, bool) {
	var (
		bal Money
		ok  bool
	)
	e.submit(func() {
		a, found := e.accounts[id]
		if found {
			bal, ok = a.Balance, true
		}
	})
	return bal, ok
}

// Stats returns ledger-wide counters, including the conservation total that
// must always be zero.
func (e *Engine) Stats() Stats {
	var s Stats
	e.submit(func() {
		s.Accounts = len(e.accounts) - 1 // exclude the external account
		s.Transfers = e.count
		for _, a := range e.accounts {
			s.Total += a.Balance
		}
	})
	return s
}
