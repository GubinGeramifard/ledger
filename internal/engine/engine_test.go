package engine

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestDoubleEntryConserves(t *testing.T) {
	e := New()
	defer e.Close()

	mustCreate(t, e, "alice", "bob")
	e.Deposit("alice", 10000) // $100.00

	r := e.Transfer("t1", "alice", "bob", 2500)
	if !r.Applied || r.Err != "" {
		t.Fatalf("transfer rejected: %+v", r)
	}
	if bal, _ := e.Balance("alice"); bal != 7500 {
		t.Errorf("alice = %s, want 75.00", bal)
	}
	if bal, _ := e.Balance("bob"); bal != 2500 {
		t.Errorf("bob = %s, want 25.00", bal)
	}
	if s := e.Stats(); s.Total != 0 {
		t.Errorf("ledger total = %s, want 0 (money was created or destroyed)", s.Total)
	}
}

func TestNoOverdraft(t *testing.T) {
	e := New()
	defer e.Close()

	mustCreate(t, e, "alice", "bob")
	e.Deposit("alice", 100)

	r := e.Transfer("t1", "alice", "bob", 500) // more than the balance
	if r.Applied {
		t.Fatal("overdraft was allowed")
	}
	if r.Err == "" {
		t.Fatal("expected an insufficient-funds error")
	}
	if bal, _ := e.Balance("alice"); bal != 100 {
		t.Errorf("alice balance changed on a rejected transfer: %s", bal)
	}
}

func TestIdempotency(t *testing.T) {
	e := New()
	defer e.Close()

	mustCreate(t, e, "alice", "bob")
	e.Deposit("alice", 10000)

	first := e.Transfer("pay-42", "alice", "bob", 3000)
	second := e.Transfer("pay-42", "alice", "bob", 3000) // a retry of the same payment

	if !first.Applied {
		t.Fatal("first transfer should apply")
	}
	if second.Applied || !second.Deduped {
		t.Fatalf("retry should be deduped, got %+v", second)
	}
	if bal, _ := e.Balance("bob"); bal != 3000 {
		t.Errorf("bob = %s, want 30.00 (retry double-charged)", bal)
	}
}

// TestConcurrentCorrectness hammers the engine from many goroutines. A correct
// ledger must end with the total conserved (zero) and no account overdrawn, no
// matter how the transfers interleave.
func TestConcurrentCorrectness(t *testing.T) {
	e := New()
	defer e.Close()

	const nAccounts = 50
	const perAccount = 100000 // $1000.00 each
	for i := 0; i < nAccounts; i++ {
		id := fmt.Sprintf("acct-%d", i)
		if err := e.CreateAccount(id); err != nil {
			t.Fatal(err)
		}
		e.Deposit(id, perAccount)
	}

	var wg sync.WaitGroup
	const workers = 32
	const perWorker = 2000
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				from := fmt.Sprintf("acct-%d", (w+i)%nAccounts)
				to := fmt.Sprintf("acct-%d", (w+i+1)%nAccounts)
				key := fmt.Sprintf("w%d-i%d", w, i)
				e.Transfer(key, from, to, 1)
			}
		}(w)
	}
	wg.Wait()

	s := e.Stats()
	if s.Total != 0 {
		t.Errorf("total = %s after concurrent transfers, want 0", s.Total)
	}
	for _, a := range e.Accounts() {
		if a.Balance < 0 {
			t.Errorf("account %s went negative: %s", a.ID, a.Balance)
		}
	}
}

func TestWALRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.wal")

	// Write some history with a durable log, then close.
	e1, err := Recover(path, true)
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, e1, "alice", "bob", "carol")
	e1.Deposit("alice", 50000)
	e1.Transfer("t1", "alice", "bob", 12000)
	e1.Transfer("t2", "bob", "carol", 3000)
	want := map[string]Money{"alice": 38000, "bob": 9000, "carol": 3000}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	// Recover into a brand new engine and confirm the balances match exactly.
	e2, err := Recover(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	for id, w := range want {
		if bal, _ := e2.Balance(id); bal != w {
			t.Errorf("after recovery %s = %s, want %s", id, bal, w)
		}
	}
	if s := e2.Stats(); s.Total != 0 || s.Transfers != 2 {
		t.Errorf("after recovery stats = %+v, want total 0 and 2 transfers", s)
	}

	// The idempotency set must survive recovery too: replaying t1 must not
	// move money a second time.
	retry := e2.Transfer("t1", "alice", "bob", 12000)
	if retry.Applied {
		t.Error("idempotency key was lost across recovery; payment re-applied")
	}
}

func mustCreate(t *testing.T, e *Engine, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := e.CreateAccount(id); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
}
