"""Batched payout daemon.

Once per `payout_interval_s`, picks every worker whose balance has cleared
their personal `min_payout` (or the configured default), splits them into
batches of up to `batch_size`, and pays each batch in a single
`walletsendct_multi` transaction. The transparent fee is split equally
among the batch's recipients (largest-remainder rounding so the totals
add up exactly), then each leg's payout becomes `balance - fee_share`.

On RPC success: deduct from worker balances, append a `payouts` row and
one `payout_legs` row per recipient, all in one transaction. On RPC
failure: log and retry next cycle (no balance deduction).
"""
from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass
from typing import Callable

from . import db


log = logging.getLogger("pool.payouts")

SATS_PER_PRIC = 100_000_000


@dataclass
class PayoutStats:
    eligible_workers:  int = 0
    payout_txs:        int = 0
    paid_recipients:   int = 0
    paid_total_sats:   int = 0
    failed_batches:    int = 0


def _split_fee(total_fee_sats: int, n_legs: int) -> list[int]:
    """Largest-remainder split of `total_fee_sats` across `n_legs`.
    Returns a list summing to exactly total_fee_sats."""
    if n_legs <= 0:
        return []
    base = total_fee_sats // n_legs
    rem  = total_fee_sats - base * n_legs
    return [base + (1 if i < rem else 0) for i in range(n_legs)]


class PayoutDaemon:
    """Stateless apart from the DB. Call `run_once()` from a scheduler."""

    def __init__(
        self,
        *,
        db_path: str,
        rpc_call: Callable[..., object],
        default_min_payout_sats: int,
        batch_size: int = 50,
        fee_per_payout_sats: int = 10_000,   # 0.0001 PRIC
    ):
        self._db_path = db_path
        self._rpc_call = rpc_call
        self._default_min = default_min_payout_sats
        self._batch_size = batch_size
        self._fee_per_payout = fee_per_payout_sats

    def run_once(self) -> PayoutStats:
        stats = PayoutStats()
        with db.connect(self._db_path) as conn_db:
            workers = self._select_eligible(conn_db)
            stats.eligible_workers = len(workers)
            for batch in _chunked(workers, self._batch_size):
                ok, paid_count, paid_sats = self._pay_batch(conn_db, batch)
                if ok:
                    stats.payout_txs        += 1
                    stats.paid_recipients   += paid_count
                    stats.paid_total_sats   += paid_sats
                else:
                    stats.failed_batches    += 1
        return stats

    # ---------- internals ----------

    def _select_eligible(self, conn_db: sqlite3.Connection) -> list[sqlite3.Row]:
        """Highest-balance first so a small batch always tackles the biggest
        payouts first (in case one batch fails and we run out of time)."""
        return conn_db.execute(
            "SELECT id, stealth_address, balance_sats, "
            "       COALESCE(min_payout_sats, ?) AS effective_min "
            "FROM workers "
            "WHERE balance_sats >= COALESCE(min_payout_sats, ?) "
            "ORDER BY balance_sats DESC",
            (self._default_min, self._default_min),
        ).fetchall()

    def _pay_batch(self, conn_db: sqlite3.Connection,
                   batch: list[sqlite3.Row]) -> tuple[bool, int, int]:
        """Pay one batch. Returns (success, recipient_count, total_paid_sats)."""
        if not batch:
            return True, 0, 0

        # Distribute the flat tx fee across legs. Each leg's payout
        # amount is (balance - fee_share). If that goes <=0 the leg is
        # too small to absorb its share — drop it and recompute.
        legs = list(batch)
        fees = _split_fee(self._fee_per_payout, len(legs))
        amounts = [leg["balance_sats"] - fee for leg, fee in zip(legs, fees)]
        # Drop any legs whose amount went non-positive after fee, then
        # redistribute fee across the survivors.
        kept = [(leg, amt) for leg, amt in zip(legs, amounts) if amt > 0]
        if not kept:
            log.info("payout: batch dropped (all legs too small to absorb fee)")
            return False, 0, 0
        if len(kept) != len(legs):
            legs = [k[0] for k in kept]
            fees = _split_fee(self._fee_per_payout, len(legs))
            amounts = [leg["balance_sats"] - fee for leg, fee in zip(legs, fees)]
            if any(a <= 0 for a in amounts):
                log.info("payout: batch dropped after redistribution (fee too large)")
                return False, 0, 0

        recipients = [
            {"address": leg["stealth_address"], "amount": amt / SATS_PER_PRIC}
            for leg, amt in zip(legs, amounts)
        ]
        fee_pric = self._fee_per_payout / SATS_PER_PRIC

        log.info("payout: sending %d recipients, total=%d sats, fee=%d sats",
                 len(legs), sum(amounts), self._fee_per_payout)
        try:
            res = self._rpc_call("walletsendct_multi", recipients, fee_pric)
        except Exception as e:
            log.warning("payout: walletsendct_multi failed: %s", e)
            return False, 0, 0
        txid = res.get("txid")
        if not txid:
            log.warning("payout: walletsendct_multi returned no txid: %s", res)
            return False, 0, 0

        # Persist: payout row + legs + balance deductions, all in one tx.
        now = int(time.time())
        with conn_db:                                 # implicit BEGIN/COMMIT
            cur = conn_db.execute(
                "INSERT INTO payouts (ts, txid, total_sats, fee_sats, recipient_count) "
                "VALUES (?, ?, ?, ?, ?) RETURNING id",
                (now, txid, sum(amounts), self._fee_per_payout, len(legs)),
            )
            payout_id = cur.fetchone()["id"]
            conn_db.executemany(
                "INSERT INTO payout_legs (payout_id, worker_id, amount_sats) "
                "VALUES (?, ?, ?)",
                [(payout_id, leg["id"], amt) for leg, amt in zip(legs, amounts)],
            )
            # Deduct full (amount + fee_share) from each worker's balance.
            conn_db.executemany(
                "UPDATE workers SET balance_sats = balance_sats - ?, "
                "       paid_total_sats = paid_total_sats + ? WHERE id = ?",
                [(amt + fee, amt, leg["id"])
                 for leg, amt, fee in zip(legs, amounts, fees)],
            )
        return True, len(legs), sum(amounts)


def _chunked(rows: list, size: int) -> list[list]:
    out = []
    for i in range(0, len(rows), size):
        out.append(rows[i:i + size])
    return out
