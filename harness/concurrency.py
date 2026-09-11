"""Differential correctness harness.

Drives identical concurrent traffic at the naive and hardened endpoints and
measures three properties on each:

  conservation   does the sum of all balances equal what it started as?
                 Money created or destroyed is a lost update.

  exactly-once   fire the same idempotency key N times concurrently. How many
                 times did the transfer actually apply?

  double-entry   does SUM(entries.delta) equal zero?

The point is not that the hardened version passes. It is that the naive version
fails, measurably, on inputs a production system sees every day.
"""
import argparse
import asyncio
import json
import sys
import time

import httpx

BASE = "http://127.0.0.1:8000"
OPENING = 1_000_000
ACCOUNTS = 4


async def reset(client, accounts=ACCOUNTS, opening=OPENING):
    r = await client.post(f"{BASE}/admin/reset",
                          json={"accounts": accounts, "opening_balance": opening})
    r.raise_for_status()
    await client.post(f"{BASE}/admin/stats/reset")


async def audit(client):
    r = await client.get(f"{BASE}/admin/audit")
    r.raise_for_status()
    return r.json()


async def fire(client, path, src, dst, amount, key=None):
    headers = {"idempotency-key": key} if key else {}
    try:
        r = await client.post(f"{BASE}{path}",
                              json={"src": src, "dst": dst, "amount": amount},
                              headers=headers, timeout=60.0)
        if r.status_code != 200:
            return {"ok": False, "error": f"http {r.status_code}",
                    "detail": r.text[:200]}
        return r.json()
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__, "detail": str(exc)[:200]}


# ---------------------------------------------------------------- scenario 1

async def lost_update_test(client, path, n, amount):
    """N concurrent transfers, all moving money out of the same account.

    Every transfer is independently valid. The only question is whether the
    ledger still balances afterwards.
    """
    await reset(client)
    before = await audit(client)

    t0 = time.perf_counter()
    results = await asyncio.gather(*[
        fire(client, path, "acct0", f"acct{1 + (i % (ACCOUNTS - 1))}", amount)
        for i in range(n)
    ])
    elapsed = time.perf_counter() - t0

    after = await audit(client)
    applied = sum(1 for r in results if r.get("ok"))
    rejected = n - applied
    errors: dict[str, int] = {}
    for r in results:
        if not r.get("ok"):
            e = f"{r.get('error')}: {r.get('detail', '')[:80]}"
            errors[e] = errors.get(e, 0) + 1

    expected_total = before["total_balance"]
    actual_total = after["total_balance"]
    drift = actual_total - expected_total

    return {
        "requests": n,
        "applied": applied,
        "rejected": rejected,
        "expected_total": expected_total,
        "actual_total": actual_total,
        "money_drift": drift,
        "lost_updates": abs(drift) // amount if amount else 0,
        "entry_sum": after["entry_sum"],
        "elapsed_s": round(elapsed, 3),
        "throughput_rps": round(n / elapsed, 1),
        "errors": errors,
        "concurrency": after.get("concurrency", {}),
    }


# ---------------------------------------------------------------- scenario 2

async def duplicate_retry_test(client, path, distinct_keys, copies, amount):
    """Each logical transfer submitted `copies` times concurrently under one key.

    This is what a retrying client, a load balancer replay, or an at-least-once
    queue consumer actually produces.
    """
    await reset(client)
    before = await audit(client)

    jobs = []
    for k in range(distinct_keys):
        key = f"idem-{k}"
        for _ in range(copies):
            jobs.append(fire(client, path, "acct0",
                             f"acct{1 + (k % (ACCOUNTS - 1))}", amount, key))

    t0 = time.perf_counter()
    results = await asyncio.gather(*jobs)
    elapsed = time.perf_counter() - t0

    after = await audit(client)
    submitted = len(jobs)
    ok = sum(1 for r in results if r.get("ok"))
    replayed = sum(1 for r in results if r.get("replayed"))

    moved = before["total_balance"] - 0  # total is conserved; measure applications
    applications = after["transfers"]
    over_applied = applications - distinct_keys

    failed = submitted - ok
    return {
        "distinct_transfers": distinct_keys,
        "submissions": submitted,
        "ok_responses": ok,
        "failed_responses": failed,
        "replayed_responses": replayed,
        "ledger_applications": applications,
        "over_applications": over_applied,
        "duplicate_charge_rate": round(over_applied / distinct_keys, 3) if distinct_keys else 0,
        "money_drift": after["total_balance"] - before["total_balance"],
        "elapsed_s": round(elapsed, 3),
        "throughput_rps": round(submitted / elapsed, 1),
        "concurrency": after.get("concurrency", {}),
    }


# ---------------------------------------------------------------- runner

async def run(n, dup_keys, copies, amount):
    limits = httpx.Limits(max_connections=600, max_keepalive_connections=600)
    async with httpx.AsyncClient(limits=limits, timeout=60.0) as client:
        report = {}
        for label, path in (("naive", "/naive/transfer"),
                            ("hardened_locking", "/transfer"),
                            ("hardened_ssi", "/transfer/ssi")):
            print(f"\n=== {label.upper()} : concurrent transfers (lost update) ===",
                  file=sys.stderr)
            lu = await lost_update_test(client, path, n, amount)
            print(json.dumps(lu, indent=2), file=sys.stderr)

            print(f"\n=== {label.upper()} : duplicate submission (idempotency) ===",
                  file=sys.stderr)
            dr = await duplicate_retry_test(client, path, dup_keys, copies, amount)
            print(json.dumps(dr, indent=2), file=sys.stderr)

            report[label] = {"lost_update": lu, "duplicate_retry": dr}
        return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=500, help="concurrent transfers")
    ap.add_argument("--dup-keys", type=int, default=100, help="distinct idempotent transfers")
    ap.add_argument("--copies", type=int, default=5, help="concurrent submissions per key")
    ap.add_argument("--amount", type=int, default=100)
    ap.add_argument("-o", "--out", default=None)
    args = ap.parse_args()

    report = asyncio.run(run(args.n, args.dup_keys, args.copies, args.amount))
    text = json.dumps(report, indent=2)
    print(text)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(text)


if __name__ == "__main__":
    main()
