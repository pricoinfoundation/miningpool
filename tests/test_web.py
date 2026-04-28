"""Pool web UI smoke tests — uses Flask's test client + a seeded sqlite DB.
No pricoind needed."""
import os
import sqlite3
import time

import pytest

from pool import db


def _seed_pool_db(path):
    """Seed enough state for the home page + worker page to render."""
    with db.connect(path) as c:
        wid_a = db.upsert_worker(c, "H6alpha" + "x" * 80)
        wid_b = db.upsert_worker(c, "H6beta"  + "y" * 80)
        # Recent shares.
        now = int(time.time())
        for offset in range(0, 120, 30):
            c.execute(
                "INSERT INTO shares (worker_id, difficulty, ts, height_at) "
                "VALUES (?, ?, ?, ?)",
                (wid_a, 1024.0, now - offset, 5),
            )
            c.execute(
                "INSERT INTO shares (worker_id, difficulty, ts, height_at) "
                "VALUES (?, ?, ?, ?)",
                (wid_b, 2048.0, now - offset, 5),
            )
        # A block + a payout for worker A.
        c.execute(
            "INSERT INTO blocks (height, hash, found_at, reward_sats, pool_fee_sats) "
            "VALUES (5, '00bb00bb' || hex(randomblob(28)), ?, 5000000000, 50000000)",
            (now - 30,),
        )
        c.execute("UPDATE workers SET balance_sats = ? WHERE id = ?",
                  (1_000_000_000, wid_a))
        c.execute("UPDATE workers SET balance_sats = ? WHERE id = ?",
                  (50_000_000, wid_b))
        cur = c.execute(
            "INSERT INTO payouts (ts, txid, total_sats, fee_sats, recipient_count) "
            "VALUES (?, 'cafebabe' || hex(randomblob(28)), 99000, 1000, 1) RETURNING id",
            (now - 600,))
        pid = cur.fetchone()["id"]
        c.execute(
            "INSERT INTO payout_legs (payout_id, worker_id, amount_sats) "
            "VALUES (?, ?, ?)", (pid, wid_a, 99000),
        )
    return ("H6alpha" + "x" * 80, "H6beta" + "y" * 80)


@pytest.fixture
def web_client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "pool.sqlite")
    addrs = _seed_pool_db(db_path)
    monkeypatch.setenv("POOL_DB", db_path)
    monkeypatch.setenv("POOL_NAME", "TestPool")
    monkeypatch.setenv("POOL_FEE_PCT", "1.5")
    # Point at a dummy RPC datadir so PricoinRPC's cookie-auth fallback
    # can't actually connect — _confirmations_for catches the exception
    # and returns Nones, which renders as "?".
    monkeypatch.setenv("POOL_DATADIR", str(tmp_path / "fake-pricoin"))
    # Re-import web with the new env in effect.
    import importlib
    import pool.web as pw
    importlib.reload(pw)
    return pw.app.test_client(), addrs


def test_home_renders(web_client):
    client, _ = web_client
    r = client.get("/")
    assert r.status_code == 200
    body = r.data.decode()
    assert "TestPool" in body
    assert "Pool hashrate" in body
    assert "Latest blocks" in body
    assert "1.5%" in body  # pool fee


def test_worker_page(web_client):
    client, (alpha, _beta) = web_client
    r = client.get(f"/worker/{alpha}")
    assert r.status_code == 200
    body = r.data.decode()
    # Pending balance for alpha (10 PRIC) shows up.
    assert "10.00000000" in body
    assert "cafebabe" in body  # the seeded payout txid prefix


def test_worker_unknown(web_client):
    client, _ = web_client
    r = client.get("/worker/H6notarealaddress")
    assert r.status_code == 404


def test_api_stats(web_client):
    client, _ = web_client
    r = client.get("/api/stats")
    assert r.status_code == 200
    j = r.get_json()
    assert j["name"] == "TestPool"
    assert j["fee_percent"] == 1.5
    assert j["blocks_found"] == 1
    assert j["last_block"]["height"] == 5
    assert j["active_workers"] == 2
    assert j["hashrate_60s_h_per_s"] > 0


def test_api_worker(web_client):
    client, (alpha, _) = web_client
    r = client.get(f"/api/worker/{alpha}")
    assert r.status_code == 200
    j = r.get_json()
    assert j["balance_sats"] == 1_000_000_000
    assert j["paid_total_sats"] == 0   # we didn't bump paid_total in seeding
    assert j["hashrate_60s_h_per_s"] > 0


def test_api_worker_unknown(web_client):
    client, _ = web_client
    r = client.get("/api/worker/H6notarealaddress")
    assert r.status_code == 404
