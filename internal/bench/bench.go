// Package bench runs the same concurrent transfer workload against the naive
// ledger and the engine and reports what each did to the money. It is shared by
// the command-line benchmark and the dashboard's "stress test" button so both
// measure exactly the same thing.
package bench

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GubinGeramifard/ledger/internal/engine"
	"github.com/GubinGeramifard/ledger/internal/naive"
)

// Params configures a run.
type Params struct {
	Accounts    int          `json:"accounts"`
	Transfers   int          `json:"transfers"`
	Workers     int          `json:"workers"`
	DupFrac     float64      `json:"dup_frac"`
	SeedBalance engine.Money `json:"seed_balance"`
}

// Default returns a workload sized to run in well under a second while still
// making the naive ledger lose a large, obvious amount of money.
func Default() Params {
	return Params{Accounts: 20, Transfers: 500000, Workers: 16, DupFrac: 0.05, SeedBalance: 100_000_000_00}
}

// clamp keeps user-supplied params (from the web endpoint) in a safe range.
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
	Millis   int64        `json:"millis"`
	TPS      int64        `json:"tps"`
	Total    engine.Money `json:"total"`
	Drift    engine.Money `json:"drift"` // final total minus initial; 0 means money was conserved
	Negative int          `json:"negative_accounts"`
	Deduped  int64        `json:"retries_deduped"`
	Rejected int64        `json:"rejected"`
}

// Comparison is the full report from a run.
type Comparison struct {
	Params       Params       `json:"params"`
	InitialTotal engine.Money `json:"initial_total"`
	Ops          int          `json:"ops"`
	Retries      int          `json:"retries"`
	Naive        Result       `json:"naive"`
	Engine       Result       `json:"engine"`
}

type op struct {
	key    string
	from   string
	to     string
	amount engine.Money
}

// Run executes the workload against both implementations and returns the report.
func Run(p Params) Comparison {
	p = p.clamp()

	ids := make([]string, p.Accounts)
	for i := range ids {
		ids[i] = fmt.Sprintf("acct-%02d", i)
	}
	ops := buildOps(ids, p.Transfers, p.DupFrac)
	retries := len(ops) - p.Transfers
	initial := engine.Money(int64(p.Accounts) * int64(p.SeedBalance))

	return Comparison{
		Params:       p,
		InitialTotal: initial,
		Ops:          len(ops),
		Retries:      retries,
		Naive:        runNaive(ids, p.SeedBalance, ops, p.Workers, initial),
		Engine:       runEngine(ids, p.SeedBalance, ops, p.Workers, initial),
	}
}

func buildOps(ids []string, transfers int, dupFrac float64) []op {
	rng := rand.New(rand.NewSource(1)) // fixed seed: both runs see identical work
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

	elapsed := drive(len(ops), workers, func(i int) {
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
	return finish(elapsed, len(ops), total, total-initial, neg, 0, 0)
}

func runEngine(ids []string, seed engine.Money, ops []op, workers int, initial engine.Money) Result {
	e := engine.New()
	defer e.Close()
	for _, id := range ids {
		_ = e.CreateAccount(id)
		e.Deposit(id, seed)
	}

	var deduped, rejected int64
	elapsed := drive(len(ops), workers, func(i int) {
		o := ops[i]
		r := e.Transfer(o.key, o.from, o.to, o.amount)
		if r.Deduped {
			atomic.AddInt64(&deduped, 1)
		} else if r.Err != "" {
			atomic.AddInt64(&rejected, 1)
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
	return finish(elapsed, len(ops), total, total-initial, neg, deduped, rejected)
}

// drive runs fn(i) for i in [0,n) across the given number of workers, pulling
// indexes from a shared atomic counter, and returns how long it took.
func drive(n, workers int, fn func(i int)) time.Duration {
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
				fn(int(i))
			}
		}()
	}
	wg.Wait()
	return time.Since(start)
}

func finish(elapsed time.Duration, ops int, total, drift engine.Money, neg int, deduped, rejected int64) Result {
	var tps int64
	if elapsed > 0 {
		tps = int64(float64(ops) / elapsed.Seconds())
	}
	return Result{
		Millis:   elapsed.Milliseconds(),
		TPS:      tps,
		Total:    total,
		Drift:    drift,
		Negative: neg,
		Deduped:  deduped,
		Rejected: rejected,
	}
}
