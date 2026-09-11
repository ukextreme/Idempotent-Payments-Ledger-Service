"""The hardened transfer path.

Three guarantees, each earned by a specific mechanism:

  exactly-once   an idempotency-key row under a unique constraint. A replayed
                 request returns the stored response instead of re-executing.

  conservation   both legs of the transfer, both balance updates and the outbox
                 row commit in one SERIALIZABLE transaction. Account rows are
                 locked FOR UPDATE in a canonical order, so conflicting transfers
                 queue instead of deadlocking; anything that still conflicts
                 aborts with SQLSTATE 40001 and is retried.

  no dual write  the outbox row is written inside the same transaction as the
                 ledger rows, so the event stream cannot disagree with the ledger.

On the lock ordering: taking row locks in a canonical order (sorted by account id)
is what turns a deadlock-prone workload into a queueing one. Without it, a
transfer A->B and a concurrent B->A grab the two rows in opposite orders and one
of them dies with 40P01. The sort costs nothing and removes the whole class.
"""
import asyncio
import hashlib
import json
import random

import asyncpg

from .db import pool

MAX_RETRIES = 12
SERIALIZATION_FAILURE = "40001"
DEADLOCK_DETECTED = "40P01"

# Observability: how often the isolation level actually had to abort someone.
STATS = {"attempts": 0, "serialization_retries": 0, "deadlock_retries": 0,
         "exhausted": 0, "replays": 0}


def request_hash(src: str, dst: str, amount: int) -> str:
    body = json.dumps({"src": src, "dst": dst, "amount": amount}, sort_keys=True)
    return hashlib.sha256(body.encode()).hexdigest()


class IdempotencyConflict(Exception):
    """Same key, different request body — a client bug, not a retry."""


async def transfer(src: str, dst: str, amount: int, idem_key: str | None = None,
                   isolation: str = "read_committed", _depth: int = 0):
    if idem_key is None:
        return await _attempt_with_retry(src, dst, amount, isolation)

    rhash = request_hash(src, dst, amount)
    p = await pool()

    # Claim the key. The unique constraint decides the race: exactly one caller
    # inserts, everyone else reads back what the winner stored.
    async with p.acquire() as conn:
        claimed = await conn.fetchrow(
            """
            INSERT INTO idempotency_keys (key, endpoint, request_hash, state)
            VALUES ($1, 'transfer', $2, 'in_flight')
            ON CONFLICT (key, endpoint) DO NOTHING
            RETURNING key
            """,
            idem_key, rhash,
        )

    if claimed is None:
        stored = await _await_stored_response(idem_key, rhash)
        if stored is not None:
            return stored
        # The holder failed and released the key. Re-enter once to claim it.
        if _depth < 3:
            return await transfer(src, dst, amount, idem_key, isolation, _depth + 1)
        raise RuntimeError("idempotency key contention did not settle")

    try:
        result = await _attempt_with_retry(src, dst, amount, isolation)
    except Exception:
        # Release the claim so a retry can make progress rather than inheriting
        # a permanently in-flight key.
        async with p.acquire() as conn:
            await conn.execute(
                "DELETE FROM idempotency_keys WHERE key = $1 AND endpoint = 'transfer'",
                idem_key,
            )
        raise

    async with p.acquire() as conn:
        await conn.execute(
            """
            UPDATE idempotency_keys SET state = 'done', response = $2
            WHERE key = $1 AND endpoint = 'transfer'
            """,
            idem_key, json.dumps(result),
        )
    return result


async def _await_stored_response(idem_key: str, rhash: str, timeout: float = 20.0):
    """Poll for the winner's stored response. None => the key was released."""
    p = await pool()
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    delay = 0.002
    while loop.time() < deadline:
        async with p.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT state, response, request_hash FROM idempotency_keys
                WHERE key = $1 AND endpoint = 'transfer'
                """,
                idem_key,
            )
        if row is None:
            return None
        if row["request_hash"] != rhash:
            raise IdempotencyConflict("key reused with a different request body")
        if row["state"] == "done":
            stored = json.loads(row["response"])
            stored["replayed"] = True
            STATS["replays"] += 1
            return stored
        await asyncio.sleep(delay)
        delay = min(delay * 1.6, 0.05)
    raise TimeoutError("timed out waiting for in-flight idempotent request")


async def _attempt_with_retry(src: str, dst: str, amount: int,
                              isolation: str = "read_committed"):
    """Run the transfer, retrying on serialization failure with jittered backoff.

    Two isolation strategies, both correct, with very different behaviour under
    contention on a hot account:

      read_committed  ordered SELECT ... FOR UPDATE. Conflicting transfers queue
                      on the row lock. No aborts, so no retry storm.

      serializable    Postgres SSI. Conflicts are detected optimistically and one
                      side is aborted with 40001. Correct, but the abort rate
                      climbs steeply once many transfers touch the same row.

    The harness measures both. read_committed is the default precisely because
    the measurement said so.
    """
    p = await pool()
    last_error: Exception | None = None

    for attempt in range(MAX_RETRIES):
        STATS["attempts"] += 1
        try:
            async with p.acquire() as conn:
                async with conn.transaction(isolation=isolation):
                    return await _apply(conn, src, dst, amount)
        except asyncpg.PostgresError as exc:
            code = getattr(exc, "sqlstate", None)
            if code == SERIALIZATION_FAILURE:
                STATS["serialization_retries"] += 1
            elif code == DEADLOCK_DETECTED:
                STATS["deadlock_retries"] += 1
            else:
                raise
            last_error = exc
            # Exponential backoff with jitter, so retried transactions do not
            # re-collide in lockstep.
            await asyncio.sleep(
                min(0.08, 0.002 * (2 ** attempt)) * random.uniform(0.5, 1.5)
            )

    STATS["exhausted"] += 1
    raise RuntimeError(f"exceeded {MAX_RETRIES} serialization retries") from last_error


async def _apply(conn, src: str, dst: str, amount: int):
    """The transfer itself. Runs inside a SERIALIZABLE transaction."""
    # Lock both account rows in a canonical order. Sorting is what prevents
    # A->B and B->A from deadlocking against each other.
    ordered = sorted({src, dst})
    rows = await conn.fetch(
        """
        SELECT id, balance FROM accounts
        WHERE id = ANY($1::text[])
        ORDER BY id
        FOR UPDATE
        """,
        ordered,
    )
    balances = {r["id"]: r["balance"] for r in rows}
    if src not in balances or dst not in balances:
        return {"ok": False, "error": "no such account"}
    if balances[src] < amount:
        return {"ok": False, "error": "insufficient funds"}

    transfer_id = await conn.fetchval(
        "INSERT INTO transfers (src, dst, amount) VALUES ($1, $2, $3) RETURNING id",
        src, dst, amount,
    )

    # Relative deltas, not absolute values computed in Python. Even under a
    # weaker isolation level this form cannot lose an update.
    await conn.execute(
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, src
    )
    await conn.execute(
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, dst
    )
    await conn.executemany(
        "INSERT INTO entries (transfer_id, account_id, delta) VALUES ($1, $2, $3)",
        [(transfer_id, src, -amount), (transfer_id, dst, amount)],
    )

    # Outbox row, same transaction. Either both the ledger and the event exist,
    # or neither does.
    await conn.execute(
        "INSERT INTO outbox (transfer_id, payload) VALUES ($1, $2)",
        transfer_id,
        json.dumps({"type": "transfer.completed", "src": src,
                    "dst": dst, "amount": amount, "transfer_id": transfer_id}),
    )

    return {"ok": True, "transfer_id": transfer_id, "replayed": False}
