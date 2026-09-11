"""Outbox drainer.

The ledger write and the outbox write commit together. This poller is what turns
the outbox row into a published event. It claims rows with FOR UPDATE SKIP LOCKED
so several drainers can run concurrently without publishing the same row twice.
"""
from .db import pool

BATCH = 100


async def drain_once() -> int:
    p = await pool()
    async with p.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT id, payload FROM outbox
                WHERE published_at IS NULL
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT $1
                """,
                BATCH,
            )
            if not rows:
                return 0
            # In production this is where the broker publish goes. The ordering
            # matters: publish, then mark. A crash in between redelivers the
            # event, which the consumer's own idempotency key absorbs.
            await conn.execute(
                "UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])",
                [r["id"] for r in rows],
            )
            return len(rows)


async def unpublished() -> int:
    p = await pool()
    return await p.fetchval("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL")
