"""PPLNS share accounting + per-block split.

Window N is the number of most-recent accepted shares (across all
workers) considered for a single block's reward split. Pool-hopping
mitigation: a fresh joiner has to first contribute shares before they
can collect — they can't cherry-pick the moment a block is found.

split_block_reward returns a {worker_id: sats} mapping that sums
**exactly** to (reward - pool_fee). We use largest-remainder (Hamilton)
rounding so no satoshis are lost or over-allocated.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable


def take_window(conn: sqlite3.Connection, n_window: int) -> list[sqlite3.Row]:
    """Last N accepted shares pool-wide, newest first. Returns the rows
    so caller can also see how saturated the window is (len < n_window
    means we haven't accumulated enough shares yet — the early-block
    edge case)."""
    return conn.execute(
        "SELECT worker_id, difficulty FROM shares ORDER BY id DESC LIMIT ?",
        (n_window,),
    ).fetchall()


def split_block_reward(
    rows: Iterable[sqlite3.Row | tuple],
    reward_sats: int,
    fee_pct: float,
) -> tuple[dict[int, int], int]:
    """Split (reward_sats - pool_fee) across rows by their share-difficulty
    contributions. Returns ({worker_id: sats}, pool_fee_sats).

    rows can be sqlite Rows OR plain (worker_id, difficulty) tuples — handy
    for unit tests. fee_pct is in percent (1.0 means 1%).

    Largest-remainder rounding: every recipient gets floor(share). The
    leftover satoshi (if any) is allocated one-by-one to recipients with
    the largest fractional remainders, biggest first. Sum of returned
    values equals reward_sats - pool_fee_sats exactly.
    """
    if reward_sats < 0:
        raise ValueError("reward_sats must be >= 0")
    if not (0.0 <= fee_pct < 100.0):
        raise ValueError("fee_pct must be in [0, 100)")
    pool_fee_sats = (reward_sats * int(round(fee_pct * 1_000_000))) // 100_000_000
    distributable = reward_sats - pool_fee_sats

    # Aggregate per-worker difficulty.
    per_worker: dict[int, float] = {}
    total_diff = 0.0
    for r in rows:
        wid = r["worker_id"] if isinstance(r, sqlite3.Row) else r[0]
        d   = r["difficulty"] if isinstance(r, sqlite3.Row) else r[1]
        per_worker[wid] = per_worker.get(wid, 0.0) + d
        total_diff += d

    if total_diff <= 0 or not per_worker:
        return {}, pool_fee_sats

    # Floor allocation + remainders.
    raw: dict[int, int] = {}
    remainders: list[tuple[float, int]] = []
    allocated = 0
    for wid, d in per_worker.items():
        exact = distributable * d / total_diff
        floor = int(exact)
        raw[wid] = floor
        allocated += floor
        remainders.append((exact - floor, wid))

    leftover = distributable - allocated
    # Largest-remainder: distribute one satoshi at a time to the biggest
    # fractional remainders. ties broken by smaller worker_id (deterministic).
    remainders.sort(key=lambda x: (-x[0], x[1]))
    for _, wid in remainders[:leftover]:
        raw[wid] += 1

    assert sum(raw.values()) == distributable, "split arithmetic broken"
    return raw, pool_fee_sats
