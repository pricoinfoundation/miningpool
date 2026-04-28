"""SQLite schema + connection helpers.

WAL mode + a sane busy_timeout so the stratum hot path never blocks on
the payout reader. Connections are per-call (not pooled) — sqlite3 is
cheap to open against a WAL DB and this is far simpler than threadlocal
pooling at our share rate.
"""
from __future__ import annotations

import os
import sqlite3
import time
from typing import Iterable


SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    id              INTEGER PRIMARY KEY,
    stealth_address TEXT UNIQUE NOT NULL,
    joined_at       INTEGER NOT NULL,
    last_seen       INTEGER NOT NULL,
    paid_total_sats INTEGER NOT NULL DEFAULT 0,
    balance_sats    INTEGER NOT NULL DEFAULT 0,
    min_payout_sats INTEGER                          -- NULL = use config default
);
CREATE INDEX IF NOT EXISTS idx_workers_balance ON workers(balance_sats DESC);

CREATE TABLE IF NOT EXISTS shares (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id    INTEGER NOT NULL REFERENCES workers(id),
    difficulty   REAL    NOT NULL,
    ts           INTEGER NOT NULL,
    height_at    INTEGER NOT NULL
);
-- Window query: ORDER BY id DESC LIMIT N. id is monotonic so no index
-- on ts is needed for the hot path. ts index supports the time-based
-- pruner.
CREATE INDEX IF NOT EXISTS idx_shares_ts     ON shares(ts);
CREATE INDEX IF NOT EXISTS idx_shares_worker ON shares(worker_id);

CREATE TABLE IF NOT EXISTS blocks (
    height       INTEGER PRIMARY KEY,
    hash         TEXT UNIQUE NOT NULL,
    found_at     INTEGER NOT NULL,
    reward_sats  INTEGER NOT NULL,
    pool_fee_sats INTEGER NOT NULL,
    accepted     INTEGER NOT NULL DEFAULT 1,  -- 0 if orphaned later
    credited     INTEGER NOT NULL DEFAULT 0   -- 1 once balances are moved out of pending
);

-- Per-block, per-worker amounts captured from the PPLNS window at the
-- time the block was found. Stays here until coinbase maturity (100
-- confs); on maturity we add them to workers.balance_sats and on
-- orphan we drop them.
CREATE TABLE IF NOT EXISTS pending_credits (
    block_height INTEGER NOT NULL REFERENCES blocks(height),
    worker_id    INTEGER NOT NULL REFERENCES workers(id),
    amount_sats  INTEGER NOT NULL,
    PRIMARY KEY (block_height, worker_id)
);
CREATE INDEX IF NOT EXISTS idx_pending_block ON pending_credits(block_height);
CREATE INDEX IF NOT EXISTS idx_pending_worker ON pending_credits(worker_id);

CREATE TABLE IF NOT EXISTS payouts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              INTEGER NOT NULL,
    txid            TEXT UNIQUE NOT NULL,
    total_sats      INTEGER NOT NULL,
    fee_sats        INTEGER NOT NULL,
    recipient_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS payout_legs (
    payout_id   INTEGER NOT NULL REFERENCES payouts(id),
    worker_id   INTEGER NOT NULL REFERENCES workers(id),
    amount_sats INTEGER NOT NULL,
    PRIMARY KEY (payout_id, worker_id)
);
CREATE INDEX IF NOT EXISTS idx_legs_worker ON payout_legs(worker_id);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def _migrate(conn: sqlite3.Connection) -> None:
    """Apply forward-only schema migrations idempotently."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(blocks)").fetchall()}
    if "credited" not in cols:
        # Existing rows had their balances credited under the old (buggy)
        # immediate-credit model. Mark them credited=1 so the new
        # maturity pass doesn't double-count them. New inserts will
        # explicitly set credited=0 and go through the pending path.
        conn.execute("ALTER TABLE blocks ADD COLUMN credited INTEGER NOT NULL DEFAULT 0")
        conn.execute("UPDATE blocks SET credited = 1")


def connect(path: str, *, write: bool = True) -> sqlite3.Connection:
    """Open a connection. Schema is applied on every write-mode open
    (CREATE TABLE IF NOT EXISTS — cheap when already there); we don't
    rely on file-existence checks because external readers (e.g. the
    web UI smoke test or scripts polling the pool) might have created
    an empty file before the daemon ever opens it."""
    uri = f"file:{path}?mode={'rwc' if write else 'ro'}"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    if write:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.executescript(SCHEMA)
        _migrate(conn)
    return conn


def upsert_worker(conn: sqlite3.Connection, stealth_address: str) -> int:
    """Return the worker id, inserting if new."""
    now = int(time.time())
    cur = conn.execute(
        "INSERT INTO workers (stealth_address, joined_at, last_seen) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(stealth_address) DO UPDATE SET last_seen = excluded.last_seen "
        "RETURNING id",
        (stealth_address, now, now),
    )
    return cur.fetchone()["id"]


def insert_share(conn: sqlite3.Connection, worker_id: int,
                 difficulty: float, height_at: int) -> int:
    cur = conn.execute(
        "INSERT INTO shares (worker_id, difficulty, ts, height_at) "
        "VALUES (?, ?, ?, ?) RETURNING id",
        (worker_id, difficulty, int(time.time()), height_at),
    )
    return cur.fetchone()["id"]


def prune_shares_older_than(conn: sqlite3.Connection, cutoff_ts: int) -> int:
    cur = conn.execute("DELETE FROM shares WHERE ts < ?", (cutoff_ts,))
    return cur.rowcount


def credit_balance(conn: sqlite3.Connection, deltas: Iterable[tuple[int, int]]) -> None:
    """Bulk add satoshi to worker balances. `deltas`: (worker_id, sats)."""
    conn.executemany(
        "UPDATE workers SET balance_sats = balance_sats + ? WHERE id = ?",
        [(sats, wid) for wid, sats in deltas],
    )
