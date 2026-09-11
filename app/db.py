"""Connection pool and schema helpers."""
import os
import asyncpg

DSN = os.environ.get(
    "LEDGER_DSN", "postgresql://uday@127.0.0.1:5433/ledger"
)

_pool: asyncpg.Pool | None = None


async def pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DSN, min_size=8, max_size=64, command_timeout=30
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def reset(accounts: int = 4, opening_balance: int = 1_000_000) -> None:
    """Drop, recreate and seed. Used by the harness between runs."""
    here = os.path.dirname(__file__)
    with open(os.path.join(here, "schema.sql")) as fh:
        ddl = fh.read()
    p = await pool()
    async with p.acquire() as conn:
        await conn.execute(ddl)
        await conn.executemany(
            "INSERT INTO accounts (id, balance) VALUES ($1, $2)",
            [(f"acct{i}", opening_balance) for i in range(accounts)],
        )


async def total_balance() -> int:
    p = await pool()
    return await p.fetchval("SELECT COALESCE(SUM(balance), 0) FROM accounts")


async def entry_sum() -> int:
    """Double-entry invariant: every entry pair sums to zero, so must the table."""
    p = await pool()
    return await p.fetchval("SELECT COALESCE(SUM(delta), 0) FROM entries")


async def transfer_count() -> int:
    p = await pool()
    return await p.fetchval("SELECT COUNT(*) FROM transfers")
