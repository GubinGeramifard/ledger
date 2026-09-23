// Package sharded scales the ledger past a single core. Accounts are partitioned
// across N independent single-writer shards, so transfers whose two accounts land
// on the same shard run fully in parallel with transfers on other shards.
//
// The interesting problem is the transfer whose accounts live on different
// shards: it must be atomic across two independent writers. Tally handles it with
// a two-phase protocol built on per-shard "clearing" accounts, the same idea as
// the nostro/vostro accounts banks use to settle between each other:
//
//	phase 1 (source shard): debit `from`, credit this shard's clearing account
//	phase 2 (dest shard):   debit this shard's clearing account, credit `to`
//
// Each phase is an ordinary balanced double-entry posting applied by one shard's
// single writer, so at every instant every shard still sums to zero and money is
// never in two places at once, it is either in `from`, in a clearing account
// (in transit), or in `to`. The clearing accounts across all shards net to zero.
//
// This variant is in-memory; it demonstrates scaling and cross-shard correctness.
// Making cross-shard transfers crash-safe (a durable outbox on the source shard,
// replayed idempotently on recovery to finish any in-doubt transfer) is described
// in DESIGN.md.
package sharded

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/GubinGeramifard/ledger/internal/engine"
)

// Internal accounts present on every shard. The @ prefix keeps them out of the
// user's namespace. Both are exempt from the overdraft rule.
const (
	external = "@external"
	clearing = "@clearing"
)

// Result reports the outcome of a transfer.
type Result struct {
	Applied bool   `json:"applied"`
	Deduped bool   `json:"deduped"`
	Err     string `json:"error,omitempty"`
}

type shard struct {
	ops  chan func()
	quit chan struct{}
	// Owned exclusively by the shard's run loop.
	bal  map[string]engine.Money
	seen map[string]bool
}

var donePool = sync.Pool{New: func() any { return make(chan struct{}, 1) }}

func newShard() *shard {
	s := &shard{
		ops:  make(chan func(), 1024),
		quit: make(chan struct{}),
		bal:  map[string]engine.Money{external: 0, clearing: 0},
		seen: map[string]bool{},
	}
	go s.loop()
	return s
}

func (s *shard) loop() {
	for {
		select {
		case fn := <-s.ops:
			fn()
		case <-s.quit:
			return
		}
	}
}

func (s *shard) submit(fn func()) {
	d := donePool.Get().(chan struct{})
	s.ops <- func() { fn(); d <- struct{}{} }
	<-d
	donePool.Put(d)
}

// apply is the single-shard posting primitive, run only on a shard's loop.
func apply(s *shard, key, from, to string, amount engine.Money, allowNegSource bool) Result {
	if key != "" && s.seen[key] {
		return Result{Deduped: true}
	}
	if _, ok := s.bal[from]; !ok {
		return Result{Err: fmt.Sprintf("account %q not found", from)}
	}
	if _, ok := s.bal[to]; !ok {
		return Result{Err: fmt.Sprintf("account %q not found", to)}
	}
	if !allowNegSource && s.bal[from] < amount {
		return Result{Err: "insufficient funds"}
	}
	s.bal[from] -= amount
	s.bal[to] += amount
	if key != "" {
		s.seen[key] = true
	}
	return Result{Applied: true}
}

// Engine is a sharded ledger.
type Engine struct {
	shards []*shard
	n      uint32
}

// New creates a sharded engine with n shards (at least 1).
func New(n int) *Engine {
	if n < 1 {
		n = 1
	}
	e := &Engine{shards: make([]*shard, n), n: uint32(n)}
	for i := range e.shards {
		e.shards[i] = newShard()
	}
	return e
}

// Shards returns the shard count.
func (e *Engine) Shards() int { return int(e.n) }

func (e *Engine) shardOf(id string) *shard {
	return e.shards[ShardIndex(int(e.n), id)]
}

// ShardIndex reports which of n shards owns id, matching the engine's routing.
// Exposed so a benchmark can group accounts by shard to control the fraction of
// cross-shard transfers.
func ShardIndex(n int, id string) int {
	if n < 1 {
		n = 1
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(n))
}

// Close stops every shard's run loop.
func (e *Engine) Close() {
	for _, s := range e.shards {
		close(s.quit)
	}
}

// CreateAccount opens a zero-balance account on whichever shard owns its id.
func (e *Engine) CreateAccount(id string) error {
	s := e.shardOf(id)
	var err error
	s.submit(func() {
		if _, ok := s.bal[id]; ok {
			err = fmt.Errorf("account %q already exists", id)
			return
		}
		s.bal[id] = 0
	})
	return err
}

// Deposit moves money into an account from its shard's external account.
func (e *Engine) Deposit(id string, amount engine.Money) {
	s := e.shardOf(id)
	s.submit(func() { _ = apply(s, "", external, id, amount, true) })
}

func (e *Engine) exists(s *shard, id string) bool {
	var ok bool
	s.submit(func() { _, ok = s.bal[id] })
	return ok
}

// Transfer moves money between two accounts, routing to the same-shard fast path
// or the cross-shard two-phase protocol as needed. It is idempotent on key.
func (e *Engine) Transfer(key, from, to string, amount engine.Money) Result {
	if amount <= 0 {
		return Result{Err: "amount must be positive"}
	}
	if from == to {
		return Result{Err: "source and destination are the same account"}
	}
	sa, sb := e.shardOf(from), e.shardOf(to)

	if sa == sb {
		var r Result
		sa.submit(func() { r = apply(sa, key, from, to, amount, false) })
		return r
	}

	// Cross-shard. Validate both accounts exist up front so phase 2 cannot fail
	// after phase 1 has already moved the money into clearing.
	if !e.exists(sb, to) {
		return Result{Err: fmt.Sprintf("account %q not found", to)}
	}

	// Phase 1: on the source shard, move `from` -> clearing.
	var r1 Result
	sa.submit(func() { r1 = apply(sa, key+":out", from, clearing, amount, false) })
	if r1.Err != "" {
		return r1 // e.g. insufficient funds or missing source; nothing has moved
	}

	// Phase 2: on the dest shard, move clearing -> `to` (clearing may go negative).
	var r2 Result
	sb.submit(func() { r2 = apply(sb, key+":in", clearing, to, amount, true) })
	if r2.Err != "" {
		return r2
	}
	return Result{Applied: r1.Applied || r2.Applied, Deduped: r1.Deduped && r2.Deduped}
}

// Balance returns one account's balance.
func (e *Engine) Balance(id string) (engine.Money, bool) {
	s := e.shardOf(id)
	var (
		bal engine.Money
		ok  bool
	)
	s.submit(func() { bal, ok = s.bal[id] })
	return bal, ok
}

// Total sums every balance across every shard. Because each posting is balanced
// within its shard, this is always exactly zero.
func (e *Engine) Total() engine.Money {
	var total engine.Money
	for _, s := range e.shards {
		var sum engine.Money
		s.submit(func() {
			for _, b := range s.bal {
				sum += b
			}
		})
		total += sum
	}
	return total
}

// ShardTotals returns each shard's local sum. Every one must be zero.
func (e *Engine) ShardTotals() []engine.Money {
	out := make([]engine.Money, len(e.shards))
	for i, s := range e.shards {
		var sum engine.Money
		s.submit(func() {
			for _, b := range s.bal {
				sum += b
			}
		})
		out[i] = sum
	}
	return out
}

// MinRealBalance returns the smallest balance among real (non-internal) accounts,
// so tests can assert nothing was overdrawn.
func (e *Engine) MinRealBalance() engine.Money {
	min := engine.Money(0)
	first := true
	for _, s := range e.shards {
		var localMin engine.Money
		var has bool
		s.submit(func() {
			for id, b := range s.bal {
				if id == external || id == clearing {
					continue
				}
				if !has || b < localMin {
					localMin, has = b, true
				}
			}
		})
		if has && (first || localMin < min) {
			min, first = localMin, false
		}
	}
	return min
}
