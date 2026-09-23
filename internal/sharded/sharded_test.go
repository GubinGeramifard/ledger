package sharded

import (
	"fmt"
	"sync"
	"testing"

	"github.com/GubinGeramifard/ledger/internal/engine"
)

func TestBasicTransfers(t *testing.T) {
	e := New(4)
	defer e.Close()
	mustCreate(t, e, "alice", "bob", "carol", "dave")
	e.Deposit("alice", 10000)

	// Some of these are same-shard and some cross-shard depending on the hash;
	// both paths must behave identically to the caller.
	if r := e.Transfer("t1", "alice", "bob", 3000); !r.Applied || r.Err != "" {
		t.Fatalf("t1: %+v", r)
	}
	if r := e.Transfer("t2", "bob", "carol", 1000); !r.Applied || r.Err != "" {
		t.Fatalf("t2: %+v", r)
	}
	assertBal(t, e, "alice", 7000)
	assertBal(t, e, "bob", 2000)
	assertBal(t, e, "carol", 1000)
	assertConserved(t, e)
}

func TestNoOverdraft(t *testing.T) {
	e := New(4)
	defer e.Close()
	mustCreate(t, e, "alice", "bob")
	e.Deposit("alice", 100)
	if r := e.Transfer("t1", "alice", "bob", 500); r.Applied {
		t.Fatal("overdraft allowed")
	}
	assertBal(t, e, "alice", 100)
	assertConserved(t, e)
}

func TestIdempotentCrossShard(t *testing.T) {
	e := New(8)
	defer e.Close()
	mustCreate(t, e, "alice", "bob")
	e.Deposit("alice", 10000)
	first := e.Transfer("pay-1", "alice", "bob", 4000)
	second := e.Transfer("pay-1", "alice", "bob", 4000) // retry
	if !first.Applied {
		t.Fatal("first should apply")
	}
	if !second.Deduped {
		t.Fatalf("retry should dedupe, got %+v", second)
	}
	assertBal(t, e, "bob", 4000)
	assertConserved(t, e)
}

// TestConcurrentMixed hammers the sharded engine with many goroutines doing a mix
// of same-shard and cross-shard transfers, then asserts the global and per-shard
// invariants: money is conserved (every shard sums to zero) and no real account
// is overdrawn.
func TestConcurrentMixed(t *testing.T) {
	e := New(8)
	defer e.Close()

	const nAccounts = 64
	const per = 100000
	var deposited engine.Money
	for i := 0; i < nAccounts; i++ {
		id := fmt.Sprintf("a%d", i)
		if err := e.CreateAccount(id); err != nil {
			t.Fatal(err)
		}
		e.Deposit(id, per)
		deposited += per
	}

	var wg sync.WaitGroup
	const workers = 32
	const perWorker = 2000
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				from := fmt.Sprintf("a%d", (w*7+i)%nAccounts)
				to := fmt.Sprintf("a%d", (w*13+i*3+1)%nAccounts)
				if from == to {
					continue
				}
				e.Transfer(fmt.Sprintf("w%d-%d", w, i), from, to, 1)
			}
		}(w)
	}
	wg.Wait()

	if total := e.Total(); total != 0 {
		t.Errorf("global total = %s, want 0", total)
	}
	for i, s := range e.ShardTotals() {
		if s != 0 {
			t.Errorf("shard %d total = %s, want 0", i, s)
		}
	}
	if min := e.MinRealBalance(); min < 0 {
		t.Errorf("an account was overdrawn: min balance = %s", min)
	}
	// Total money in real accounts must still equal what was deposited: the
	// clearing accounts hold the net in-transit position and are excluded.
	if got := realTotal(e); got != deposited {
		t.Errorf("real-account total = %s, want %s (deposited)", got, deposited)
	}
}

func realTotal(e *Engine) engine.Money {
	// Real total = global total minus internal accounts; since global is 0 and
	// externals mirror deposits, sum the non-internal balances directly.
	var t engine.Money
	for _, s := range e.shards {
		var sum engine.Money
		s.submit(func() {
			for id, b := range s.bal {
				if id == external || id == clearing {
					continue
				}
				sum += b
			}
		})
		t += sum
	}
	return t
}

func mustCreate(t *testing.T, e *Engine, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := e.CreateAccount(id); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
}

func assertBal(t *testing.T, e *Engine, id string, want engine.Money) {
	t.Helper()
	if b, _ := e.Balance(id); b != want {
		t.Errorf("%s = %s, want %s", id, b, want)
	}
}

func assertConserved(t *testing.T, e *Engine) {
	t.Helper()
	if total := e.Total(); total != 0 {
		t.Errorf("global total = %s, want 0", total)
	}
	for i, s := range e.ShardTotals() {
		if s != 0 {
			t.Errorf("shard %d total = %s, want 0", i, s)
		}
	}
}
