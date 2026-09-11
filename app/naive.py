"""The naive transfer path — the baseline this project exists to disprove.

This is the implementation most people write first: read the balance, check it in
application code, write the new balance back. It has two independent defects, and
the harness measures both.

  1. Read-modify-write across a transaction boundary loses updates. Two concurrent
     transfers both read balance B, both compute B - amount, and the second write
     silently discards the first. Money is created.

  2. No idempotency. A retried request — which any HTTP client, load balancer or
     SQS consumer will eventually send — applies the transfer twice.
"""
import asyncio

from .db import pool


async def transfer(src: str, dst: str, amount: int, idem_key: str | None = None):
    """Read balance, decide, write. One statement at a time, no transaction."""
    p = await pool()

    async with p.acquire() as conn:
        # Read the source balance.
        src_balance = await conn.fetchval(
            "SELECT balance FROM accounts WHERE id = $1", src
        )
        if src_balance is None:
            return {"ok": False, "error": "no such account"}

        # Application-level business check, outside any transaction. This is the
        # window: between this check and the write below, any number of other
        # requests can read the same balance and reach the same conclusion.
        if src_balance < amount:
            return {"ok": False, "error": "insufficient funds"}

        # An explicit scheduler yield, standing in for the application logic a
        # real handler would run here — a fraud check, a limit lookup, an RPC.
        # It does not cause the bug; it makes an existing bug reproducible
        # instead of timing-dependent. Over HTTP the network hop supplies the
        # same gap on its own, which is why the harness sees drift without this.
        await asyncio.sleep(0)

        dst_balance = await conn.fetchval(
            "SELECT balance FROM accounts WHERE id = $1", dst
        )

        # Write back an absolute value computed from the stale read.
        await conn.execute(
            "UPDATE accounts SET balance = $1 WHERE id = $2",
            src_balance - amount, src,
        )
        await conn.execute(
            "UPDATE accounts SET balance = $1 WHERE id = $2",
            dst_balance + amount, dst,
        )

        transfer_id = await conn.fetchval(
            "INSERT INTO transfers (src, dst, amount) VALUES ($1, $2, $3) RETURNING id",
            src, dst, amount,
        )
        await conn.execute(
            "INSERT INTO entries (transfer_id, account_id, delta) VALUES ($1, $2, $3)",
            transfer_id, src, -amount,
        )
        await conn.execute(
            "INSERT INTO entries (transfer_id, account_id, delta) VALUES ($1, $2, $3)",
            transfer_id, dst, amount,
        )

    return {"ok": True, "transfer_id": transfer_id, "replayed": False}
