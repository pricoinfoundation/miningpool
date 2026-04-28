"""Pricoin pool — public stats web UI (Flask).

Reads the pool's SQLite database that the stratum daemon writes to.
Read-only on this side, so multiple gunicorn workers are fine. Run
separately from `pool.main`:

    EXPLORER_DB=/var/lib/pricoinpool/pool.sqlite \\
    POOL_FEE_PCT=1.0 \\
    POOL_NAME="Pricoin Pool" \\
    gunicorn --workers 2 --bind 127.0.0.1:5099 'pool.web:app'
"""
from __future__ import annotations

import os
import sqlite3
import time

from flask import Flask, abort, jsonify, render_template, url_for


SATS_PER_PRIC = 100_000_000

DB_PATH    = os.environ.get("POOL_DB", "pool.sqlite")
POOL_NAME  = os.environ.get("POOL_NAME", "Pricoin Pool")
POOL_FEE   = float(os.environ.get("POOL_FEE_PCT", "1.0"))
RECENT_60  = 60
RECENT_24H = 86400
ACTIVE_S   = 300        # worker is "active" if it submitted in the last 5 min


app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False


# ---------- helpers ----------

def _db() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        abort(503, "pool db not present yet — daemon hasn't created it")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _hashrate_in_window(conn, seconds: int, worker_id: int | None = None) -> float:
    cutoff = int(time.time()) - seconds
    if worker_id is None:
        row = conn.execute(
            "SELECT COALESCE(SUM(difficulty), 0) AS s FROM shares WHERE ts >= ?",
            (cutoff,)).fetchone()
    else:
        row = conn.execute(
            "SELECT COALESCE(SUM(difficulty), 0) AS s FROM shares "
            "WHERE worker_id = ? AND ts >= ?",
            (worker_id, cutoff)).fetchone()
    return float(row["s"]) / seconds


def _format_hashrate(h_per_s: float) -> str:
    for unit, scale in (("TH/s", 1e12), ("GH/s", 1e9), ("MH/s", 1e6),
                        ("kH/s", 1e3), ("H/s", 1.0)):
        if h_per_s >= scale:
            return f"{h_per_s / scale:.2f} {unit}"
    return "0 H/s"


def _format_pric(sats: int | None) -> str:
    if sats is None: return "—"
    return f"{sats / SATS_PER_PRIC:.8f}"


def _short(s: str | None, n: int = 12) -> str:
    if not s: return ""
    return s if len(s) <= 2 * n else f"{s[:n]}…{s[-n:]}"


def _format_age(unix: int | None) -> str:
    if unix is None: return ""
    delta = int(time.time()) - int(unix)
    if delta < 0:    return "just now"
    if delta < 60:   return f"{delta}s ago"
    if delta < 3600: return f"{delta // 60}m ago"
    if delta < 86400: return f"{delta // 3600}h ago"
    return f"{delta // 86400}d ago"


@app.context_processor
def _inject_helpers():
    return {
        "fmt_pric":     _format_pric,
        "fmt_age":      _format_age,
        "fmt_short":    _short,
        "fmt_hashrate": _format_hashrate,
        "POOL_NAME":    POOL_NAME,
        "POOL_FEE":     POOL_FEE,
    }


# ---------- pages ----------

@app.route("/")
def home():
    conn = _db()
    blocks = conn.execute(
        "SELECT * FROM blocks ORDER BY height DESC LIMIT 25"
    ).fetchall()
    n_blocks = conn.execute("SELECT COUNT(*) AS n FROM blocks").fetchone()["n"]
    active   = conn.execute(
        "SELECT COUNT(DISTINCT worker_id) AS n FROM shares WHERE ts >= ?",
        (int(time.time()) - ACTIVE_S,)
    ).fetchone()["n"]
    total_paid_sats = conn.execute(
        "SELECT COALESCE(SUM(total_sats), 0) AS s FROM payouts"
    ).fetchone()["s"]
    return render_template(
        "home.html",
        blocks=blocks,
        n_blocks=n_blocks,
        active=active,
        total_paid_sats=total_paid_sats,
        hashrate_60s=_hashrate_in_window(conn, RECENT_60),
        hashrate_24h=_hashrate_in_window(conn, RECENT_24H),
    )


@app.route("/worker/<address>")
def worker(address: str):
    conn = _db()
    w = conn.execute(
        "SELECT * FROM workers WHERE stealth_address = ?", (address,)
    ).fetchone()
    if not w:
        abort(404)
    payouts = conn.execute(
        "SELECT p.ts, p.txid, l.amount_sats "
        "FROM payout_legs l JOIN payouts p ON p.id = l.payout_id "
        "WHERE l.worker_id = ? ORDER BY p.ts DESC LIMIT 25",
        (w["id"],)
    ).fetchall()
    return render_template(
        "worker.html",
        w=w,
        payouts=payouts,
        hashrate_60s=_hashrate_in_window(conn, RECENT_60, w["id"]),
        hashrate_24h=_hashrate_in_window(conn, RECENT_24H, w["id"]),
    )


@app.errorhandler(404)
def not_found(_e):
    return render_template("404.html"), 404


# ---------- json api ----------

@app.route("/api/stats")
def api_stats():
    conn = _db()
    blocks_total = conn.execute("SELECT COUNT(*) AS n FROM blocks").fetchone()["n"]
    last_block = conn.execute(
        "SELECT height, hash, found_at, reward_sats FROM blocks ORDER BY height DESC LIMIT 1"
    ).fetchone()
    active = conn.execute(
        "SELECT COUNT(DISTINCT worker_id) AS n FROM shares WHERE ts >= ?",
        (int(time.time()) - ACTIVE_S,)
    ).fetchone()["n"]
    return jsonify({
        "name":             POOL_NAME,
        "fee_percent":      POOL_FEE,
        "active_workers":   active,
        "blocks_found":     blocks_total,
        "last_block":       (
            {"height": last_block["height"], "hash": last_block["hash"],
             "found_at": last_block["found_at"], "reward_sats": last_block["reward_sats"]}
            if last_block else None),
        "hashrate_60s_h_per_s":  _hashrate_in_window(conn, RECENT_60),
        "hashrate_24h_h_per_s":  _hashrate_in_window(conn, RECENT_24H),
        "ts": int(time.time()),
    })


@app.route("/api/worker/<address>")
def api_worker(address: str):
    conn = _db()
    w = conn.execute(
        "SELECT id, balance_sats, paid_total_sats, joined_at, last_seen "
        "FROM workers WHERE stealth_address = ?", (address,)
    ).fetchone()
    if not w:
        return jsonify({"error": "unknown worker"}), 404
    return jsonify({
        "balance_sats":          w["balance_sats"],
        "paid_total_sats":       w["paid_total_sats"],
        "joined_at":             w["joined_at"],
        "last_seen":             w["last_seen"],
        "hashrate_60s_h_per_s":  _hashrate_in_window(conn, RECENT_60, w["id"]),
        "hashrate_24h_h_per_s": _hashrate_in_window(conn, RECENT_24H, w["id"]),
    })
