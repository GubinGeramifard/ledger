# ⚖️ Tally

**A double-entry ledger engine that cannot lose money under concurrency.**

Tally moves money between accounts the way a payment system has to: exactly. Every
transfer is a balanced double-entry posting, processed by a single-writer state
machine, made durable by a write-ahead log, and protected against duplicate
requests by idempotency keys. Concurrent transfers and retried payments, the two
things that quietly corrupt naive implementations, cannot break the books.

The project ships with a benchmark that proves it: the same concurrent workload is
run against a naive ledger and against Tally's engine, and the naive one is shown
creating and destroying millions of dollars while Tally stays exact.

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

Run the benchmark (`go run ./cmd/bench`). A representative run on a laptop, 20
accounts, 500,000 concurrent transfers across 16 workers, plus 25,000 retries:

| | Naive ledger | Tally engine |
|---|---|---|
| Throughput | ~21,000,000 /s | ~620,000 /s |
| **Money created or destroyed** | **~$3,900,000** | **$0.00** |
| Accounts left negative | varies | 0 |
| Retries handled | re-applied all of them | 25,000 deduped |

The naive ledger is faster precisely because it skips the work that keeps money
correct. Tally still sustains hundreds of thousands of transfers per second on a
single core while keeping the ledger exact.

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
go test -race ./...
```

The suite runs under the race detector and covers double-entry conservation, the
no-overdraft rule, idempotent retries, 64,000 transfers across 32 goroutines with
the invariant held, and write-ahead-log recovery.

---

## Project structure

```
ledger/
├── cmd/
│   ├── server/          HTTP API + embedded dashboard
│   │   └── web/         single-page dashboard (vanilla JS)
│   └── bench/           command-line naive-vs-engine benchmark
└── internal/
    ├── engine/          the ledger: state machine, money, WAL, recovery
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
go run ./cmd/bench                 # defaults
go run ./cmd/bench -transfers 2000000 -workers 32

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
