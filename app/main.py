"""FastAPI surface. Both implementations are mounted so the harness can drive
identical traffic at each and compare outcomes."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from . import naive, safe
from .db import close_pool, entry_sum, pool, reset, total_balance, transfer_count
from .outbox import drain_once


class TransferRequest(BaseModel):
    src: str
    dst: str
    amount: int = Field(gt=0)


class ResetRequest(BaseModel):
    accounts: int = 4
    opening_balance: int = 1_000_000


@asynccontextmanager
async def lifespan(_: FastAPI):
    await pool()
    yield
    await close_pool()


app = FastAPI(title="Ledger Service", lifespan=lifespan)


@app.post("/naive/transfer")
async def naive_transfer(
    req: TransferRequest, idempotency_key: str | None = Header(default=None)
):
    return await naive.transfer(req.src, req.dst, req.amount, idempotency_key)


@app.post("/transfer")
async def safe_transfer(
    req: TransferRequest, idempotency_key: str | None = Header(default=None)
):
    """Production path: ordered row locks under READ COMMITTED."""
    try:
        return await safe.transfer(req.src, req.dst, req.amount,
                                   idempotency_key, "read_committed")
    except safe.IdempotencyConflict as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/transfer/ssi")
async def ssi_transfer(
    req: TransferRequest, idempotency_key: str | None = Header(default=None)
):
    """Same logic under SERIALIZABLE (SSI), for the isolation-cost comparison."""
    try:
        return await safe.transfer(req.src, req.dst, req.amount,
                                   idempotency_key, "serializable")
    except safe.IdempotencyConflict as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/admin/reset")
async def admin_reset(req: ResetRequest):
    await reset(req.accounts, req.opening_balance)
    return {"ok": True, "accounts": req.accounts, "opening_balance": req.opening_balance}


@app.get("/admin/audit")
async def audit():
    return {
        "total_balance": await total_balance(),
        "entry_sum": await entry_sum(),
        "transfers": await transfer_count(),
        "concurrency": dict(safe.STATS),
    }


@app.post("/admin/stats/reset")
async def stats_reset():
    for k in safe.STATS:
        safe.STATS[k] = 0
    return {"ok": True}


@app.post("/admin/outbox/drain")
async def outbox_drain():
    return {"published": await drain_once()}
