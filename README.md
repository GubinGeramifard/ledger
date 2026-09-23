# ⚖️ Tally

[![CI](https://github.com/GubinGeramifard/ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/GubinGeramifard/ledger/actions/workflows/ci.yml)

**A double-entry ledger engine that cannot lose money under concurrency.**

Tally moves money between accounts the way a payment system has to: exactly. Every
transfer is a balanced double-entry posting, processed by a single-writer state
machine, made durable by a write-ahead log, and protected against duplicate
requests by idempotency keys. Concurrent transfers and retried payments, the two
things that quietly corrupt naive implementations, cannot break the books.

The project ships with a benchmark that proves it: the same concurrent workload is
run against a naive ledger and against Tally's engine, and the naive one is shown
creating and destroying millions of dollars while Tally stays exact.

**Live demo:** https://tally-ledger-1d83.onrender.com — create accounts, move
money, and press "Run stress test" to see the comparison in the browser. (Hosted
on a free tier, so the first request may take a moment to wake, and throughput is
lower than the laptop numbers below.)

---

## The problem

Writing "move money from A to B" is deceptively easy to get wrong. The obvious
version, read both balances, check funds, write both balances, has two bugs that
never show up in a single-threaded test:

1. **Lost updates.** Two transfers from the same account run at once. Both read the
   old balance, both compute a new one, and one write silently overwrites the
   other. Money is created or destroyed.
2. **Double application.** Networks retry. Without idempotency, a retried request
   is a second, real payment.

Both are invisible until the system is under load, and both move real money.

## The result

Run the benchmark (`go run ./cmd/bench`). It runs the same workload, 20 accounts,
500,000 concurrent transfers across 16 workers, plus 25,000 retries, against three
ledgers. A representative run on a laptop:

| | Naive | Global mutex | Single-writer engine |
|---|---|---|---|
| Correct? | **no** | yes | yes |
| **Money created or destroyed** | **~$3,700,000** | **$0.00** | **$0.00** |
| Throughput | ~18,000,000 /s | ~990,000 /s | ~620,000 /s |
| Retries | re-applied all | 25,000 deduped | 25,000 deduped |

### Why not just a mutex?

The naive ledger is a strawman, and the benchmark says so: a **global mutex** is
also correct, and it is even faster than the engine at this scale. So why the
single-writer design?

Because in memory the raw throughput is not the interesting number. **Turn on
durability** and the picture flips. To be crash-safe, the mutex ledger must
`fsync` to disk while holding its lock, so every transfer serializes on I/O. The
single-writer engine instead appends to its log and lets the run loop **group
commit**: it drains a batch of transfers, does one `fsync` for the whole batch,
and only then acknowledges them all. No transfer is acknowledged before it is on
disk, so durability is never weakened, but one `fsync` now covers many transfers.

Measured with `go run ./cmd/bench -durable` (10,000 transfers, real `fsync`s):

| | Durable throughput |
|---|---|
| Mutex, `fsync` under the lock | ~490 /s |
| Engine, group commit | ~6,300 /s |

**A ~13x speedup, purely from how the design serializes.** That is the payoff of
the single-writer state machine: because it owns the write path, it can batch the
expensive part. The deterministic command log also makes write-ahead-log replay
safe and would let the ledger be sharded across cores without rewriting the
transfer logic. "Correct" is the easy part; "correct once you add durability and
scale" is the point.

See **[DESIGN.md](DESIGN.md)** for the full rationale: the alternatives considered
(per-account locks, one global mutex, single-writer), why each was or wasn't
chosen, what is deliberately out of scope, and how I would productionize it.

### Scaling across cores (sharding)

One writer means one core. To scale, accounts are partitioned across N single-writer
shards (`internal/sharded`); same-shard transfers run fully in parallel. A transfer
across two shards must be atomic across two independent writers, handled with a
two-phase protocol over per-shard **clearing accounts** (like the accounts banks use
to settle between each other): debit `from` into the source shard's clearing, then
credit `to` from the dest shard's clearing. Every phase is a balanced posting, so
**every shard stays summed to zero at every instant**, money is never in two places,
and each phase is idempotent so retries are safe. The concurrency test asserts this
across many goroutines mixing same- and cross-shard transfers, under `-race`.

Measured (`go run ./cmd/bench -shards -workers 64`, 12-core laptop, no cross-shard
traffic):

| Shards | Throughput | Speedup |
|---|---|---|
| 1 | ~830,000 /s | 1.0x |
| 2 | ~1,240,000 /s | 1.5x |
| 4 | ~1,900,000 /s | 2.3x |
| 8 | ~2,400,000 /s | 2.9x |

Sub-linear because each transfer is a channel hand-off, not raw CPU work, and
cross-shard transfers touch two shards; but partitioning clearly moves the write
path off a single core. Making cross-shard transfers crash-safe (a durable outbox,
replayed idempotently on recovery) is the remaining production step, described in
DESIGN.md.

---

## How it works

**Single-writer state machine.** All state changes flow through one goroutine that
processes commands one at a time. This is the model real financial databases such
as [TigerBeetle](https://tigerbeetle.com) use. Because only one goroutine ever
touches a balance, there are no data races and no lost updates, no matter how many
clients call at once, and no per-account locking to get subtly wrong. It is also
deterministic, which is what makes log replay safe.

**Double-entry accounting.** Every transfer debits one account and credits another
by the same amount, so the sum of all balances never changes. Deposits move money
in from a special `external` account, so the grand total across every account is
always exactly **zero**. That single number is the system's conservation law: if it
is ever non-zero, a bug created or destroyed money.

**Money is integer cents.** Never floating point. `0.1 + 0.2 != 0.3` in float64,
and those errors accumulate into real discrepancies.

**Idempotency.** A transfer can carry an idempotency key. The first time it is seen
the transfer applies; every retry with that key returns the original result and
changes nothing, so resending a payment is always safe.

**Durability via a write-ahead log.** Each applied command is appended to a log
(optionally `fsync`'d) before it is acknowledged. On restart the engine replays the
log to rebuild state exactly, idempotency set included. Recovery is covered by a
test that writes history, reopens a fresh engine, and asserts the balances match.

---

## Correctness tests

```bash
go test -race ./...                                    # unit + property + crash tests
go test -run=x -fuzz=FuzzLedger ./internal/engine      # fuzz the invariants
```

Everything runs under the race detector on every push ([CI](.github/workflows/ci.yml)),
and covers:

- **Double-entry conservation, no-overdraft, idempotency** as focused unit tests.
- **Concurrency:** 64,000 transfers across 32 goroutines, asserting the invariant
  holds and no account is overdrawn.
- **Property-based testing:** thousands of randomized transfers across several
  seeds, asserting after every run that money is conserved and the total is zero.
- **Fuzzing:** a `FuzzLedger` target lets the fuzzer generate arbitrary transfer
  sequences and checks the invariants hold for all of them.
- **Crash recovery:** an abrupt "crash" (no clean shutdown) must reproduce every
  balance from the log, and a torn final record (a half-written line from a crash
  mid-`append`) must be ignored while every complete record is applied.

---

## Project structure

```
ledger/
├── cmd/
│   ├── server/          HTTP API + embedded dashboard
│   │   └── web/         single-page dashboard (vanilla JS)
│   └── bench/           command-line naive-vs-engine benchmark
└── internal/
    ├── engine/          the ledger: state machine, money, WAL, recovery, group commit
    ├── sharded/         partitions accounts across single-writer shards (cross-shard 2PC)
    ├── locked/          a global-mutex ledger, for the benchmark contrast
    ├── naive/           the deliberately-wrong ledger, for contrast
    └── bench/           shared workload runner (used by cmd/bench and the API)
```

## API

| Endpoint | Description |
|----------|-------------|
| `GET /api/stats` | Account count, transfers processed, conservation total (always 0) |
| `GET /api/accounts` | All accounts with balances |
| `POST /api/accounts` | Create an account `{ "id": "dave" }` |
| `POST /api/deposit` | Fund an account `{ "id", "amount": "250.00" }` |
| `POST /api/transfers` | Move money `{ "from", "to", "amount", "idempotency_key" }` |
| `POST /api/stress` | Run the naive-vs-engine comparison and return the report |

## Running locally

```bash
# Correctness tests (with the race detector)
go test -race ./...

# The benchmark
go run ./cmd/bench                       # three-way: naive vs mutex vs engine
go run ./cmd/bench -durable              # durable throughput: mutex fsync vs group commit
go run ./cmd/bench -shards -workers 64   # how the sharded engine scales across cores

# The server + dashboard
go run ./cmd/server                # http://localhost:8080
```

State is written to `ledger.wal` in the working directory (override with
`LEDGER_WAL`). Delete it to start fresh.

## Tech

**Go 1.27**, standard library only, no third-party dependencies. Concurrency via
goroutines and channels; persistence via an append-only write-ahead log; a
zero-build vanilla-JavaScript dashboard embedded in the binary with `go:embed`, so
the whole thing ships as a single executable.
