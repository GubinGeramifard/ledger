// Package bench runs one concurrent transfer workload against three ledger
// implementations and reports what each did to the money and how fast.
//
//   - naive:  no synchronization, no idempotency. Fast, but loses money.
//   - locked: correct, but every operation serializes on one global mutex.
//   - engine: correct, via a single-writer state machine.
//
// The naive run shows why synchronization is not optional. The locked-vs-engine
// comparison is the honest one: two correct designs, measured on throughput and
// tail latency.
//
// The workload isolates correctness under concurrency: every account starts with
// a balance so large that no transfer is ever legitimately rejected, so any
// change in the total is money created or destroyed by a race, not a business
// rule. A fraction of the operations are exact duplicates (simulated retries) to
// exercise idempotency.
package bench

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GubinGeramifard/ledger/internal/engine"
	"github.com/GubinGeramifard/ledger/internal/locked"
	"github.com/GubinGeramifard/ledger/internal/naive"
	"github.com/GubinGeramifard/ledger/internal/sharded"
)

// Params configures a run.
type Params struct {
	Accounts    int          `json:"accounts"`
	Transfers   int          `json:"transfers"`
	Workers     int          `json:"workers"`
	DupFrac     float64      `json:"dup_frac"`
	SeedBalance engine.Money `json:"seed_balance"`
}

// Default returns a workload sized to run quickly while still making the naive
// ledger lose a large, obvious amount of money.
func Default() Params {
	return Params{Accounts: 20, Transfers: 500000, Workers: 16, DupFrac: 0.05, SeedBalance: 100_000_000_00}
}

func (p Params) clamp() Params {
	if p.Accounts < 2 {
		p.Accounts = 2
	}
	if p.Accounts > 1000 {
		p.Accounts = 1000
	}
	if p.Transfers < 1 {
		p.Transfers = 1
	}
	if p.Transfers > 5_000_000 {
		p.Transfers = 5_000_000
	}
	if p.Workers < 1 {
		p.Workers = 1
	}
	if p.Workers > 256 {
		p.Workers = 256
	}
	if p.DupFrac < 0 {
		p.DupFrac = 0
	}
	if p.DupFrac > 1 {
		p.DupFrac = 1
	}
	if p.SeedBalance <= 0 {
		p.SeedBalance = 100_000_000_00
	}
	return p
}

// Result is the outcome for one implementation.
type Result struct {
	Name      string       `json:"name"`
	Correct   bool         `json:"correct"` // did it conserve money and stay non-negative
	Millis    int64        `json:"millis"`
	TPS       int64        `json:"tps"`
	P50Micros float64      `json:"p50_micros"`
	P99Micros float64      `json:"p99_micros"`
	Total     engine.Money `json:"total"`
	Drift     engine.Money `json:"drift"` // final total minus initial; 0 means money was conserved
	Negative  int          `json:"negative_accounts"`
	Deduped   int64        `json:"retries_deduped"`
}

// Comparison is the full report from a run.
type Comparison struct {
	Params       Params       `json:"params"`
	InitialTotal engine.Money `json:"initial_total"`
	Ops          int          `json:"ops"`
	Retries      int          `json:"retries"`
	Naive        Result       `json:"naive"`
	Locked       Result       `json:"locked"`
	Engine       Result       `json:"engine"`
}

type op struct {
	key    string
	from   string
	to     string
	amount engine.Money
}

// Run executes the workload against all three implementations.
func Run(p Params) Comparison {
	p = p.clamp()

	ids := make([]string, p.Accounts)
	for i := range ids {
		ids[i] = fmt.Sprintf("acct-%02d", i)
	}
	ops := buildOps(ids, p.Transfers, p.DupFrac)
	initial := engine.Money(int64(p.Accounts) * int64(p.SeedBalance))

	return Comparison{
		Params:       p,
		InitialTotal: initial,
		Ops:          len(ops),
		Retries:      len(ops) - p.Transfers,
		Naive:        runNaive(ids, p.SeedBalance, ops, p.Workers, initial),
		Locked:       runLocked(ids, p.SeedBalance, ops, p.Workers, initial),
		Engine:       runEngine(ids, p.SeedBalance, ops, p.Workers, initial),
	}
}

func buildOps(ids []string, transfers int, dupFrac float64) []op {
	rng := rand.New(rand.NewSource(1)) // fixed seed: every run sees identical work
	ops := make([]op, 0, transfers+int(float64(transfers)*dupFrac))
	for i := 0; i < transfers; i++ {
		a := rng.Intn(len(ids))
		b := rng.Intn(len(ids) - 1)
		if b >= a {
			b++
		}
		ops = append(ops, op{
			key:    fmt.Sprintf("pay-%d", i),
			from:   ids[a],
			to:     ids[b],
			amount: engine.Money(rng.Int63n(100000) + 1),
		})
	}
	for i := 0; i < int(float64(transfers)*dupFrac); i++ {
		ops = append(ops, ops[rng.Intn(transfers)])
	}
	rng.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })
	return ops
}

func runNaive(ids []string, seed engine.Money, ops []op, workers int, initial engine.Money) Result {
	l := naive.New()
	for _, id := range ids {
		l.CreateAccount(id)
		l.Deposit(id, seed)
	}
	elapsed, lat := drive(len(ops), workers, func(i int) {
		o := ops[i]
		l.Transfer(o.key, o.from, o.to, o.amount)
	})
	total := l.Total()
	neg := 0
	for _, id := range ids {
		if l.Balance(id) < 0 {
			neg++
		}
	}
	return finish("naive", elapsed, lat, total, total-initial, neg, 0)
}

func runLocked(ids []string, seed engine.Money, ops []op, workers int, initial engine.Money) Result {
	l := locked.New()
	for _, id := range ids {
		l.CreateAccount(id)
		l.Deposit(id, seed)
	}
	var deduped int64
	elapsed, lat := drive(len(ops), workers, func(i int) {
		o := ops[i]
		if l.Transfer(o.key, o.from, o.to, o.amount) {
			atomic.AddInt64(&deduped, 1)
		}
	})
	total := l.Total()
	neg := 0
	for _, id := range ids {
		if l.Balance(id) < 0 {
			neg++
		}
	}
	return finish("locked", elapsed, lat, total, total-initial, neg, deduped)
}

func runEngine(ids []string, seed engine.Money, ops []op, workers int, initial engine.Money) Result {
	e := engine.New()
	defer e.Close()
	for _, id := range ids {
		_ = e.CreateAccount(id)
		e.Deposit(id, seed)
	}
	var deduped int64
	elapsed, lat := drive(len(ops), workers, func(i int) {
		o := ops[i]
		if e.Transfer(o.key, o.from, o.to, o.amount).Deduped {
			atomic.AddInt64(&deduped, 1)
		}
	})
	var total engine.Money
	neg := 0
	for _, a := range e.Accounts() {
		total += a.Balance
		if a.Balance < 0 {
			neg++
		}
	}
	return finish("engine", elapsed, lat, total, total-initial, neg, deduped)
}

// drive runs fn(i) for i in [0,n) across the given number of workers, pulling
// indexes from a shared atomic counter, and records each call's latency.
func drive(n, workers int, fn func(i int)) (time.Duration, []int64) {
	lat := make([]int64, n)
	start := time.Now()
	var idx int64 = -1
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&idx, 1)
				if int(i) >= n {
					return
				}
				t0 := time.Now()
				fn(int(i))
				lat[i] = time.Since(t0).Nanoseconds()
			}
		}()
	}
	wg.Wait()
	return time.Since(start), lat
}

func finish(name string, elapsed time.Duration, lat []int64, total, drift engine.Money, neg int, deduped int64) Result {
	var tps int64
	if elapsed > 0 {
		tps = int64(float64(len(lat)) / elapsed.Seconds())
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return Result{
		Name:      name,
		Correct:   drift == 0 && neg == 0,
		Millis:    elapsed.Milliseconds(),
		TPS:       tps,
		P50Micros: float64(pct(lat, 0.50)) / 1000,
		P99Micros: float64(pct(lat, 0.99)) / 1000,
		Total:     total,
		Drift:     drift,
		Negative:  neg,
		Deduped:   deduped,
	}
}

func pct(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

// DurableResult is one implementation's durable-throughput number.
type DurableResult struct {
	Name   string `json:"name"`
	Millis int64  `json:"millis"`
	TPS    int64  `json:"tps"`
}

// DurableComparison contrasts a mutex that fsyncs under its lock with the engine
// that group-commits.
type DurableComparison struct {
	Transfers   int           `json:"transfers"`
	Workers     int           `json:"workers"`
	MutexFsync  DurableResult `json:"mutex_fsync"`
	GroupCommit DurableResult `json:"group_commit"`
	Speedup     float64       `json:"speedup"`
}

// RunDurable measures durable throughput: real transfers, real fsyncs. The mutex
// ledger fsyncs on every transfer while holding its lock; the engine appends to
// its write-ahead log and lets the run loop batch the fsyncs (group commit). It
// uses far fewer transfers than the in-memory benchmark because an fsync-per-op
// design is orders of magnitude slower.
func RunDurable(transfers, workers int) (DurableComparison, error) {
	if transfers < 1 {
		transfers = 10000
	}
	if workers < 1 {
		workers = 16
	}
	const n = 8
	const seed = engine.Money(100_000_000_00)

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("acct-%d", i)
	}
	ops := buildOps(ids, transfers, 0)

	dir, err := os.MkdirTemp("", "tally-durable")
	if err != nil {
		return DurableComparison{}, err
	}
	defer os.RemoveAll(dir)

	// Mutex + fsync-per-transfer.
	dl, err := locked.NewDurable(filepath.Join(dir, "mutex.log"))
	if err != nil {
		return DurableComparison{}, err
	}
	for _, id := range ids {
		dl.CreateAccount(id)
		dl.Deposit(id, seed)
	}
	mElapsed, _ := drive(len(ops), workers, func(i int) {
		o := ops[i]
		dl.Transfer(o.from, o.to, o.amount)
	})
	_ = dl.Close()

	// Engine + group commit (durable WAL).
	e, err := engine.Recover(filepath.Join(dir, "engine.wal"), true)
	if err != nil {
		return DurableComparison{}, err
	}
	for _, id := range ids {
		_ = e.CreateAccount(id)
		e.Deposit(id, seed)
	}
	eElapsed, _ := drive(len(ops), workers, func(i int) {
		o := ops[i]
		e.Transfer(o.key, o.from, o.to, o.amount)
	})
	_ = e.Close()

	m := DurableResult{Name: "mutex-fsync", Millis: mElapsed.Milliseconds(), TPS: tps(len(ops), mElapsed)}
	g := DurableResult{Name: "group-commit", Millis: eElapsed.Milliseconds(), TPS: tps(len(ops), eElapsed)}
	speedup := 0.0
	if mElapsed > 0 {
		speedup = float64(mElapsed) / float64(eElapsed)
	}
	return DurableComparison{
		Transfers: transfers, Workers: workers,
		MutexFsync: m, GroupCommit: g, Speedup: speedup,
	}, nil
}

func tps(ops int, d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(float64(ops) / d.Seconds())
}

// ShardRow is throughput at one shard count.
type ShardRow struct {
	Shards  int     `json:"shards"`
	TPS     int64   `json:"tps"`
	Millis  int64   `json:"millis"`
	Speedup float64 `json:"speedup"` // vs 1 shard
}

// ShardScaling reports how throughput scales with shard count.
type ShardScaling struct {
	Accounts  int        `json:"accounts"`
	Transfers int        `json:"transfers"`
	Workers   int        `json:"workers"`
	CrossFrac float64    `json:"cross_frac"`
	Rows      []ShardRow `json:"rows"`
}

// RunSharded measures the sharded engine's throughput at each shard count, with a
// controlled fraction of cross-shard transfers, to show how partitioning scales
// the write path across cores.
func RunSharded(shardCounts []int, accounts, transfers, workers int, crossFrac float64) ShardScaling {
	ids := make([]string, accounts)
	for i := range ids {
		ids[i] = fmt.Sprintf("acct-%03d", i)
	}
	out := ShardScaling{Accounts: accounts, Transfers: transfers, Workers: workers, CrossFrac: crossFrac}
	var base int64
	for _, n := range shardCounts {
		groups := make([][]string, n)
		for _, id := range ids {
			g := sharded.ShardIndex(n, id)
			groups[g] = append(groups[g], id)
		}
		var nonempty []int
		for i, g := range groups {
			if len(g) > 0 {
				nonempty = append(nonempty, i)
			}
		}
		ops := buildShardOps(groups, nonempty, transfers, crossFrac)

		e := sharded.New(n)
		for _, id := range ids {
			_ = e.CreateAccount(id)
			e.Deposit(id, 100_000_000_00)
		}
		elapsed, _ := drive(len(ops), workers, func(i int) {
			o := ops[i]
			e.Transfer(o.key, o.from, o.to, o.amount)
		})
		e.Close()

		t := tps(len(ops), elapsed)
		if n == shardCounts[0] {
			base = t
		}
		sp := 1.0
		if base > 0 {
			sp = float64(t) / float64(base)
		}
		out.Rows = append(out.Rows, ShardRow{Shards: n, TPS: t, Millis: elapsed.Milliseconds(), Speedup: sp})
	}
	return out
}

func buildShardOps(groups [][]string, nonempty []int, transfers int, crossFrac float64) []op {
	rng := rand.New(rand.NewSource(1))
	ops := make([]op, 0, transfers)
	for i := 0; i < transfers; i++ {
		sg := nonempty[rng.Intn(len(nonempty))]
		from := groups[sg][rng.Intn(len(groups[sg]))]

		crossPossible := len(nonempty) > 1
		sameShardDistinctPossible := len(groups[sg]) > 1
		cross := crossPossible && (rng.Float64() < crossFrac || !sameShardDistinctPossible)

		var to string
		if cross {
			dg := sg
			for dg == sg {
				dg = nonempty[rng.Intn(len(nonempty))]
			}
			to = groups[dg][rng.Intn(len(groups[dg]))]
		} else {
			to = from
			for to == from {
				to = groups[sg][rng.Intn(len(groups[sg]))]
			}
		}
		ops = append(ops, op{
			key:    fmt.Sprintf("s%d", i),
			from:   from,
			to:     to,
			amount: engine.Money(rng.Int63n(1000) + 1),
		})
	}
	return ops
}
