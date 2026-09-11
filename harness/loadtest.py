"""Closed-loop load generator. Reports throughput and the latency tail.

Duplicate-heavy by default, because the interesting question for this service is
what the idempotency barrier costs when clients actually retry.
"""
import argparse
import asyncio
import json
import statistics
import time

import httpx

BASE = "http://127.0.0.1:8000"
ERRORS: list[str] = []


async def worker(client, path, n, amount, dup_ratio, key_space, out, worker_id,
                 accounts):
    for i in range(n):
        # A dup_ratio fraction of requests reuse a key from a small hot set.
        # A reused key must carry an identical body — that is what makes it a
        # retry rather than a client bug — so the accounts are derived from the
        # key itself, not from the worker.
        if (i * 2654435761) % 100 < dup_ratio * 100:
            slot = (i + worker_id) % key_space
            key = f"hot-{slot}"
            src = f"acct{slot % accounts}"
            dst = f"acct{(slot + 1) % accounts}"
        else:
            key = f"w{worker_id}-{i}"
            src = f"acct{(worker_id * 7 + i) % accounts}"
            dst = f"acct{(worker_id * 7 + i + 1) % accounts}"

        if src == dst:
            dst = f"acct{(int(src[4:]) + 1) % accounts}"

        headers = {"idempotency-key": key}
        t0 = time.perf_counter()
        try:
            r = await client.post(
                f"{BASE}{path}",
                json={"src": src, "dst": dst, "amount": amount},
                headers=headers, timeout=60.0,
            )
            ok = r.status_code == 200
            if not ok:
                ERRORS.append(f"http {r.status_code}: {r.text[:120]}")
        except Exception as exc:
            ok = False
            ERRORS.append(f"{type(exc).__name__}: {str(exc)[:120]}")
        out.append(((time.perf_counter() - t0) * 1000.0, ok))


def pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[k]


async def run(path, concurrency, per_worker, amount, dup_ratio, key_space, accounts):
    limits = httpx.Limits(max_connections=concurrency + 16,
                          max_keepalive_connections=concurrency + 16)
    async with httpx.AsyncClient(limits=limits, timeout=60.0) as client:
        await client.post(f"{BASE}/admin/reset",
                          json={"accounts": accounts,
                                "opening_balance": 10_000_000_000})
        await client.post(f"{BASE}/admin/stats/reset")
        samples: list = []
        t0 = time.perf_counter()
        await asyncio.gather(*[
            worker(client, path, per_worker, amount, dup_ratio, key_space,
                   samples, w, accounts)
            for w in range(concurrency)
        ])
        elapsed = time.perf_counter() - t0

        lat = [s[0] for s in samples]
        ok = sum(1 for s in samples if s[1])
        audit = (await client.get(f"{BASE}/admin/audit")).json()

        return {
            "path": path,
            "concurrency": concurrency,
            "accounts": accounts,
            "requests": len(samples),
            "successful": ok,
            "error_rate": round(1 - ok / len(samples), 5) if samples else 1.0,
            "duplicate_ratio": dup_ratio,
            "elapsed_s": round(elapsed, 3),
            "throughput_rps": round(len(samples) / elapsed, 1),
            "latency_ms": {
                "mean": round(statistics.fmean(lat), 2),
                "p50": round(pct(lat, 50), 2),
                "p95": round(pct(lat, 95), 2),
                "p99": round(pct(lat, 99), 2),
                "max": round(max(lat), 2),
            },
            "audit": audit,
            "error_sample": list(dict.fromkeys(ERRORS))[:5],
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="/transfer")
    ap.add_argument("-c", "--concurrency", type=int, default=64)
    ap.add_argument("-n", "--per-worker", type=int, default=50)
    ap.add_argument("--amount", type=int, default=10)
    ap.add_argument("--dup-ratio", type=float, default=0.3)
    ap.add_argument("--key-space", type=int, default=64)
    ap.add_argument("--accounts", type=int, default=64,
                    help="account-space width; 2 = single hot pair")
    ap.add_argument("-o", "--out", default=None)
    args = ap.parse_args()

    rep = asyncio.run(run(args.path, args.concurrency, args.per_worker,
                          args.amount, args.dup_ratio, args.key_space,
                          args.accounts))
    text = json.dumps(rep, indent=2)
    print(text)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(text)


if __name__ == "__main__":
    main()
