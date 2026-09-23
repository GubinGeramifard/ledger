# Design notes

Why Tally is built the way it is, and the tradeoffs behind each decision. This is
the document I would walk an interviewer through.

## The problem it solves

Moving money between accounts must be **exact** and **durable**, even when many
requests arrive at once and clients retry. Two bugs make naive implementations
silently wrong, and neither shows up in a single-threaded test:

- **Lost updates.** Two transfers touch the same account concurrently; both read
  the old balance, both write a new one, and one write is lost. Money is created
  or destroyed.
- **Double application.** A network retry, without idempotency, is a second real
  payment.

Everything below follows from taking those two failures seriously.

## Core decisions

### Money is `int64` cents, never a float

`0.1 + 0.2 != 0.3` in float64, and the error compounds. Amounts are whole minor
units in an `int64`, which is exact up to ~92 quadrillion dollars.

### Double-entry with a conservation invariant

Every transfer debits one account and credits another by the same amount, so the
sum of all balances never changes. Deposits move money in from a special
`external` account, so the total across **every** account is always exactly zero.
That single number is a machine-checkable invariant: if it is ever non-zero, a bug
created or destroyed money. The property tests and fuzzer assert it after every
run.

### A single-writer state machine, not locks

All state changes flow through one goroutine that applies commands one at a time.
This is the model used by real accounting databases such as TigerBeetle.

**Alternatives considered:**

- **Per-account locks.** Correct, and scales across accounts, but you have to get
  lock ordering right to avoid deadlock when a transfer touches two accounts, and
  the locking logic is easy to get subtly wrong.
- **One global mutex.** Also correct, and actually the *fastest* option for
  in-memory transfers at this scale (see the benchmark). But it does not scale
  past one core, its tail latency grows under contention, and, decisively, it
  forces `fsync` to happen inside the lock once you want durability.
- **Single-writer (chosen).** One goroutine owns all state. No data races and no
  lock-ordering to get wrong. It gives up a little raw in-memory throughput, and
  buys three things: determinism, batched durability, and a clean path to
  sharding.

### Why single-writer wins where it matters: group commit

In memory, the global mutex beats the single-writer engine (~990k vs ~620k
transfers/sec on a laptop) because a mutex lock/unlock is cheaper than a channel
hand-off. That is real, and the benchmark reports it honestly.

The picture flips under durability. To be crash-safe, the mutex ledger must
`fsync` while holding its lock, serializing every transfer on disk I/O. The
single-writer engine appends each command to its log and lets the run loop **group
commit**: drain a batch of pending transfers, `fsync` once for the whole batch,
then acknowledge them all. No caller is acknowledged before its record is on disk,
so durability is never weakened; one `fsync` just covers many transfers.

Measured (`go run ./cmd/bench -durable`): ~490 transfers/sec for the mutex versus
~6,300 for group commit, about **13x**. Because the single writer owns the write
path, it can batch the expensive part; a lock-per-transfer design cannot.

### Determinism enables recovery

Because commands are applied in a single, deterministic order, the write-ahead log
is a perfect script of history. Recovery just replays it to rebuild state exactly,
idempotency set included. A torn final record (a crash mid-write) is detected and
ignored; every complete record is applied.

### Idempotency

A transfer may carry an idempotency key. The first time a key is seen the transfer
applies; every later call with that key returns the original result and changes
nothing. Retrying a payment is always safe. Keys are part of the logged state, so
idempotency survives recovery.

## What this is not

Tally is a focused engine, not a production database. Deliberately out of scope:

- **Replication / high availability.** It is single-node. A production system
  would replicate the log (Raft or similar) so a node failure does not lose data.
- **Horizontal scale.** One writer means one core for the write path. The design
  shards cleanly (partition accounts across writers, with a two-phase path for
  cross-shard transfers), but that is not built here.
- **A network protocol, auth, or multi-tenancy.** The HTTP API is a thin demo
  layer over the engine.

Naming these limits is part of the point: the engine is small enough to be
obviously correct, and the correctness properties it does guarantee are tested and
measured rather than assumed.

## How I would productionize it

1. Replicate the write-ahead log across nodes for durability and failover.
2. Snapshot state periodically and compact the log so recovery stays fast.
3. Shard accounts across single-writer cores; route cross-shard transfers through
   a two-phase commit.
4. Add metrics (per-op latency histograms), structured logging, and backpressure
   on the command channel.
