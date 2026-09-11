"""Unit and property tests. Run with: make test

These cover the invariants directly, without going through HTTP. The harness
covers the concurrent behaviour; this covers the logic.
"""
import asyncio
import sys

from app import safe, naive
from app.db import (close_pool, entry_sum, pool, reset, total_balance,
                    transfer_count)
from app.outbox import drain_once, unpublished

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{(' — ' + detail) if detail and not cond else ''}")


async def test_basic_transfer():
    await reset(accounts=2, opening_balance=1000)
    r = await safe.transfer("acct0", "acct1", 300)
    check("transfer succeeds", r["ok"])
    p = await pool()
    a0 = await p.fetchval("SELECT balance FROM accounts WHERE id='acct0'")
    a1 = await p.fetchval("SELECT balance FROM accounts WHERE id='acct1'")
    check("debit applied", a0 == 700, f"got {a0}")
    check("credit applied", a1 == 1300, f"got {a1}")
    check("total conserved", await total_balance() == 2000)
    check("double-entry sums to zero", await entry_sum() == 0)


async def test_insufficient_funds():
    await reset(accounts=2, opening_balance=100)
    r = await safe.transfer("acct0", "acct1", 500)
    check("overdraft rejected", not r["ok"] and r["error"] == "insufficient funds")
    check("no ledger rows written", await transfer_count() == 0)
    check("balances untouched", await total_balance() == 200)


async def test_unknown_account():
    await reset(accounts=2, opening_balance=100)
    r = await safe.transfer("acct0", "ghost", 10)
    check("unknown account rejected", not r["ok"])


async def test_idempotent_replay():
    await reset(accounts=2, opening_balance=1000)
    first = await safe.transfer("acct0", "acct1", 100, "key-a")
    second = await safe.transfer("acct0", "acct1", 100, "key-a")
    check("replay returns same transfer", first["transfer_id"] == second["transfer_id"])
    check("replay is flagged", second["replayed"] is True)
    check("replay applied once", await transfer_count() == 1)
    p = await pool()
    a0 = await p.fetchval("SELECT balance FROM accounts WHERE id='acct0'")
    check("replay did not double-debit", a0 == 900, f"got {a0}")


async def test_concurrent_same_key():
    """The unique constraint must collapse N concurrent submissions to one."""
    await reset(accounts=2, opening_balance=100000)
    results = await asyncio.gather(*[
        safe.transfer("acct0", "acct1", 50, "key-race") for _ in range(20)
    ])
    check("all 20 succeed", all(r["ok"] for r in results))
    check("exactly one application", await transfer_count() == 1)
    ids = {r["transfer_id"] for r in results}
    check("all share one transfer id", len(ids) == 1, f"got {ids}")
    check("total conserved", await total_balance() == 200000)


async def test_key_reuse_with_different_body():
    await reset(accounts=2, opening_balance=1000)
    await safe.transfer("acct0", "acct1", 100, "key-b")
    try:
        await safe.transfer("acct0", "acct1", 999, "key-b")
        check("mismatched body rejected", False, "no exception raised")
    except safe.IdempotencyConflict:
        check("mismatched body rejected", True)


async def test_outbox_written_in_transaction():
    await reset(accounts=2, opening_balance=1000)
    await safe.transfer("acct0", "acct1", 100)
    check("outbox row written with ledger", await unpublished() == 1)
    published = await drain_once()
    check("drain publishes the row", published == 1)
    check("drained row not republished", await drain_once() == 0)


async def test_outbox_matches_ledger():
    await reset(accounts=4, opening_balance=100000)
    for i in range(25):
        await safe.transfer("acct0", f"acct{1 + i % 3}", 10)
    p = await pool()
    ledger = await transfer_count()
    events = await p.fetchval("SELECT COUNT(*) FROM outbox")
    check("one event per transfer", ledger == events == 25, f"{ledger} vs {events}")


async def test_naive_is_actually_broken():
    """The baseline must fail, or the comparison proves nothing."""
    await reset(accounts=2, opening_balance=1000000)
    await asyncio.gather(*[naive.transfer("acct0", "acct1", 100) for _ in range(200)])
    drift = await total_balance() - 2000000
    check("naive loses updates under concurrency", drift != 0,
          f"expected nonzero drift, got {drift}")


async def test_conservation_under_mixed_load():
    await reset(accounts=8, opening_balance=100000)
    before = await total_balance()
    jobs = []
    for i in range(300):
        src, dst = f"acct{i % 8}", f"acct{(i + 3) % 8}"
        key = f"mixed-{i % 120}"       # deliberate duplicate keys
        jobs.append(safe.transfer(src, dst, 25, key))
    await asyncio.gather(*jobs, return_exceptions=True)
    check("total conserved under mixed load", await total_balance() == before)
    check("entries still sum to zero", await entry_sum() == 0)
    check("applications equal distinct keys", await transfer_count() == 120,
          f"got {await transfer_count()}")


async def main():
    tests = [
        test_basic_transfer, test_insufficient_funds, test_unknown_account,
        test_idempotent_replay, test_concurrent_same_key,
        test_key_reuse_with_different_body, test_outbox_written_in_transaction,
        test_outbox_matches_ledger, test_naive_is_actually_broken,
        test_conservation_under_mixed_load,
    ]
    for t in tests:
        print(f"\n{t.__name__}")
        await t()
    await close_pool()
    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed:", ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
