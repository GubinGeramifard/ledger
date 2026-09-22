package engine

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// TestInvariantUnderRandomOps throws thousands of random transfers (many
// concurrent, some with reused idempotency keys) at the engine and asserts the
// core invariants after every run: money is conserved, no account is overdrawn,
// and the ledger-wide total is exactly zero. It repeats with several seeds so a
// single lucky ordering can't hide a bug.
func TestInvariantUnderRandomOps(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 1000, 99999} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			e := New()
			defer e.Close()

			const n = 12
			var deposited Money
			ids := make([]string, n)
			for i := range ids {
				ids[i] = fmt.Sprintf("a%d", i)
				if err := e.CreateAccount(ids[i]); err != nil {
					t.Fatal(err)
				}
				amt := Money(rng.Int63n(1_000_00) + 1_00)
				e.Deposit(ids[i], amt)
				deposited += amt
			}

			var wg sync.WaitGroup
			for w := 0; w < 8; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed*100 + int64(w)))
					for i := 0; i < 3000; i++ {
						from := ids[r.Intn(n)]
						to := ids[r.Intn(n)]
						amt := Money(r.Int63n(500) + 1)
						// Reuse a key ~10% of the time to exercise idempotency.
						key := ""
						if r.Intn(10) == 0 {
							key = fmt.Sprintf("k%d", r.Intn(200))
						} else {
							key = fmt.Sprintf("w%d-%d", w, i)
						}
						e.Transfer(key, from, to, amt)
					}
				}(w)
			}
			wg.Wait()

			// Invariant 1: the ledger-wide total (including external) is zero.
			if s := e.Stats(); s.Total != 0 {
				t.Fatalf("conservation total = %s, want 0", s.Total)
			}
			// Invariant 2: no real account is negative, and the money in user
			// accounts equals what was deposited (nothing created or destroyed).
			var circulation Money
			for _, a := range e.Accounts() {
				if a.Balance < 0 {
					t.Errorf("account %s overdrawn: %s", a.ID, a.Balance)
				}
				circulation += a.Balance
			}
			if circulation != deposited {
				t.Errorf("money in circulation = %s, want %s (deposited)", circulation, deposited)
			}
		})
	}
}

// FuzzLedger lets the fuzzer generate arbitrary transfer sequences and checks
// that the invariants hold for every one of them. Run with:
//
//	go test -run=x -fuzz=FuzzLedger ./internal/engine
//
// The seed corpus also runs as a normal test under `go test`.
func FuzzLedger(f *testing.F) {
	f.Add([]byte{0, 1, 0, 50, 1, 2, 0, 200, 2, 0, 1, 44})
	f.Add([]byte{3, 3, 255, 255, 0, 3, 1, 1})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		e := New()
		defer e.Close()

		const n = 4
		deposited := Money(0)
		for i := 0; i < n; i++ {
			_ = e.CreateAccount(fmt.Sprintf("a%d", i))
			e.Deposit(fmt.Sprintf("a%d", i), 1_000_00)
			deposited += 1_000_00
		}

		// Each 4 bytes is one transfer: from, to, amount-hi, amount-lo.
		for i := 0; i+3 < len(data); i += 4 {
			from := fmt.Sprintf("a%d", int(data[i])%n)
			to := fmt.Sprintf("a%d", int(data[i+1])%n)
			amount := Money(int(data[i+2])<<8 | int(data[i+3]))
			key := fmt.Sprintf("k%d", i)
			e.Transfer(key, from, to, amount)
		}

		if s := e.Stats(); s.Total != 0 {
			t.Fatalf("conservation broken: total = %s", s.Total)
		}
		var circulation Money
		for _, a := range e.Accounts() {
			if a.Balance < 0 {
				t.Fatalf("account %s overdrawn: %s", a.ID, a.Balance)
			}
			circulation += a.Balance
		}
		if circulation != deposited {
			t.Fatalf("money changed: circulation %s, deposited %s", circulation, deposited)
		}
	})
}
