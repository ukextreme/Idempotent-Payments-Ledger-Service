# Idempotent Payments & Double-Entry Ledger Service

A money-transfer API built twice — once the obvious way, once correctly — with a
concurrency harness that drives identical traffic at both and measures what
breaks.

The headline result: under 500 concurrent transfers the naive implementation
**creates money** (15–27 lost updates per run) and applies every retried request
again (**4.0× duplicate charge rate**). The hardened implementation applies each
logical transfer **exactly once** and conserves value across every run.

Built with Python 3.14, FastAPI, PostgreSQL 18, asyncpg.

---

## Why build it twice

An invariant nobody has seen violated is a claim. An invariant measured against an
implementation that *does* violate it is a result. Every number below is a
difference between two implementations running the same workload against the same
database on the same machine.

---

## Results

Measured on WSL2 / Ubuntu, PostgreSQL 18.6, single uvicorn process, asyncpg pool
of 64. Reproduce with `make harness`.

### Lost updates — 500 concurrent transfers out of one account

| | naive | hardened |
|---|---:|---:|
| Requests applied | 500 / 500 | 500 / 500 |
| Money drift | **+2,000** (20 lost updates) | **0** |
| `SUM(entries.delta)` | 0 | 0 |

Across five consecutive runs the naive path drifted by +1,500 to +2,700 — 15 to 27
lost updates every time, always in the direction of creating money. The hardened
path drifted by zero in all five.

### Duplicate submission — 100 distinct transfers, each submitted 5× concurrently

| | naive | hardened |
|---|---:|---:|
| Submissions | 500 | 500 |
| Ledger applications | **500** | **100** |
| Duplicate charge rate | **4.0×** | **0×** |
| Replays served from stored response | 0 | 400 |

100 transfers submitted 500 times must apply exactly 100 times. The naive path
applies all 500.

### Isolation strategy — the measurement that picked the default

Both strategies are correct. They differ entirely in what happens under
contention on a hot account.

| | ordered `FOR UPDATE` | `SERIALIZABLE` (SSI) |
|---|---:|---:|
| Requests served (of 500) | **500** | **122** |
| Serialization retries | **0** | **5,110** |
| Retry budget exhausted | **0** | **378** |
| Money drift | 0 | 0 |

SSI holds the invariant but sheds 76% of traffic when 500 transfers contend on one
row — it detects conflicts optimistically and aborts one side of each. Taking row
locks in a canonical order turns the same workload into a queue: conflicting
transfers wait rather than abort. `READ COMMITTED` + ordered locking is the default
*because of this measurement*, not by assumption.

### Throughput and latency

64 concurrent workers, 3,840 requests, 30% of them retries of an in-flight key:

| Workload | rps | p50 | p95 | p99 | errors |
|---|---:|---:|---:|---:|---:|
| hardened, 30% duplicates | **380.6** | 93.9 ms | 519 ms | 848 ms | 0 |
| hardened, no duplicates | 364.6 | 94.9 ms | 566 ms | 946 ms | 0 |
| naive, 30% duplicates | 307.8 | 93.9 ms | 671 ms | 1189 ms | 0 (drift +160) |

The idempotency barrier is not only a correctness mechanism — it is a throughput
win under retry-heavy traffic, because a replay is answered from the stored
response without touching the ledger at all.

---

## How the guarantees are built

### Exactly-once: the unique constraint decides the race

```sql
INSERT INTO idempotency_keys (key, endpoint, request_hash, state)
VALUES ($1, 'transfer', $2, 'in_flight')
ON CONFLICT (key, endpoint) DO NOTHING
RETURNING key
```

Exactly one concurrent caller gets a row back. Everyone else polls until the
winner stores its response, then returns that response verbatim with
`replayed: true`. If the winner fails, it deletes its claim so a retry can make
progress rather than inheriting a permanently in-flight key.

The stored `request_hash` is what separates a retry from a client bug: the same
key with a different body is rejected with 422, not silently treated as a replay.

### Conservation: one transaction, ordered locks, relative deltas

Both balance updates, both ledger entries and the outbox row commit together.
Two details carry the weight:

- **Ordered locking.** Account rows are locked `FOR UPDATE` sorted by id. Without
  it, `A→B` and a concurrent `B→A` grab the two rows in opposite orders and one
  dies with `40P01`. Sorting costs nothing and removes the whole class.
- **Relative deltas.** `SET balance = balance - $1`, never
  `SET balance = <value computed in Python>`. The naive path's defect is exactly
  the second form: a value derived from a read that is already stale by the time
  it is written.

`SQLSTATE 40001` and `40P01` are retried with exponentially backed-off jitter;
anything else propagates.

### No dual write: the transactional outbox

The event row is inserted in the same transaction as the ledger rows, so the
ledger and the event stream cannot disagree. A separate poller claims unpublished
rows with `FOR UPDATE SKIP LOCKED`, so several drainers can run concurrently
without publishing a row twice.

Publish-then-mark is deliberate: a crash between the two redelivers the event,
which the consumer's own idempotency key absorbs. The opposite order loses it.

---

## Running it

```bash
make install     # venv + dependencies
make db          # initdb, start postgres on :5433, create the database
make run         # uvicorn on :8000
make test        # 26 assertions across 10 tests
make harness     # the naive-vs-hardened differential run
make load        # throughput and latency
```

### Endpoints

| | |
|---|---|
| `POST /transfer` | hardened path — ordered locks, `READ COMMITTED` |
| `POST /transfer/ssi` | same logic under `SERIALIZABLE`, for the comparison |
| `POST /naive/transfer` | the broken baseline |
| `POST /admin/reset` | drop, recreate, seed |
| `GET  /admin/audit` | total balance, entry sum, application count, retry stats |
| `POST /admin/outbox/drain` | publish pending outbox rows |

Pass `Idempotency-Key` as a header. Without one, the transfer still applies
correctly but is not replay-protected.

```bash
curl -XPOST localhost:8000/transfer -H 'content-type: application/json' \
     -H 'idempotency-key: abc123' \
     -d '{"src":"acct0","dst":"acct1","amount":500}'
```

---

## Tests

26 assertions across 10 tests (`make test`), including:

- the double-entry invariant after every operation
- 20 concurrent submissions of one key collapsing to a single application
- key reuse with a mismatched body rejected rather than replayed
- outbox row count equal to ledger application count
- **a test asserting the naive path is genuinely broken** — if the baseline ever
  starts passing, the comparison proves nothing and the suite says so

---

## Layout

```
app/
  schema.sql      accounts, transfers, entries, idempotency_keys, outbox
  db.py           asyncpg pool, reset, audit queries
  naive.py        the broken baseline
  safe.py         idempotency barrier, ordered locking, retry loop
  outbox.py       SKIP LOCKED drainer
  main.py         FastAPI surface
harness/
  concurrency.py  differential correctness harness
  loadtest.py     closed-loop throughput and latency
tests/
  test_ledger.py  unit and property tests
```

## Built with

Python 3.14 · FastAPI · PostgreSQL 18.6 · asyncpg · httpx
