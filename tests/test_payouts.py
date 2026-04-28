"""Payout-daemon unit tests with a mocked walletsendct_multi RPC."""
import sqlite3

import pytest

from pool import db
from pool.payouts import PayoutDaemon, _split_fee


def _seed(db_path, workers):
    """Seed `workers` into the DB. Each tuple = (stealth_addr, balance_sats, min_payout_sats|None)."""
    with db.connect(db_path) as c:
        for addr, bal, mp in workers:
            wid = db.upsert_worker(c, addr)
            c.execute(
                "UPDATE workers SET balance_sats = ?, min_payout_sats = ? WHERE id = ?",
                (bal, mp, wid),
            )


class FakeRPC:
    """Records calls + returns canned responses for the methods PayoutDaemon
    uses (walletsendct_multi for payouts, getblock for the maturity pass).
    Optionally fails walletsendct_multi to exercise the retry path."""
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = []

    def __call__(self, method, *args):
        self.calls.append((method, args))
        if method == "getblock":
            # Whatever block we're asked about, claim it has 100+ confs
            # so the maturity pass promotes pending_credits to balance.
            return {"confirmations": 200}
        if method == "walletsendct_multi":
            if self.fail:
                raise RuntimeError("RPC dead")
            n_recip = len(args[0]) if args and isinstance(args[0], list) else 0
            # Fresh unique txid per call so we don't trip the UNIQUE
            # constraint on payouts.txid when a single run produces
            # multiple batches.
            send_calls = sum(1 for c in self.calls if c[0] == "walletsendct_multi")
            txid = f"{send_calls:064x}"
            return {
                "txid": txid,
                "recipients": n_recip,
                "outputs": n_recip + 1,
                "total_sent": sum(r["amount"] for r in args[0]) if args else 0,
                "fee": args[1] if len(args) > 1 else 0,
            }
        raise AssertionError(f"unexpected RPC method {method!r}")


def test_split_fee_remainder():
    assert _split_fee(7, 3) == [3, 2, 2]
    assert _split_fee(10, 5) == [2, 2, 2, 2, 2]
    assert _split_fee(0, 4) == [0, 0, 0, 0]


def test_run_once_pays_eligible_only(tmp_path):
    db_path = str(tmp_path / "p.sqlite")
    _seed(db_path, [
        ("H6alpha" + "a" * 70,  100_000_000, None),    # 1 PRIC, default 0.1 → eligible
        ("H6beta"  + "b" * 70,    5_000_000, None),    # 0.05 PRIC, below default → no
        ("H6gamma" + "c" * 70, 1_000_000_000, 50_000_000),  # 10 PRIC, custom 0.5 → eligible
        ("H6delta" + "d" * 70,   30_000_000, 50_000_000),   # 0.3 PRIC, custom 0.5 → no
    ])
    rpc = FakeRPC()
    daemon = PayoutDaemon(
        db_path=db_path,
        rpc_call=rpc,
        default_min_payout_sats=10_000_000,            # 0.1 PRIC
        batch_size=50,
        fee_per_payout_sats=10_000,
    )
    stats = daemon.run_once()

    assert stats.eligible_workers == 2
    assert stats.payout_txs == 1
    assert stats.paid_recipients == 2

    # Inspect DB.
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    payouts = conn.execute("SELECT * FROM payouts").fetchall()
    legs    = conn.execute("SELECT * FROM payout_legs ORDER BY worker_id").fetchall()
    workers = conn.execute("SELECT id, stealth_address, balance_sats, paid_total_sats FROM workers ORDER BY id").fetchall()
    conn.close()

    assert len(payouts) == 1
    assert payouts[0]["recipient_count"] == 2
    assert payouts[0]["fee_sats"] == 10_000
    assert len(legs) == 2

    # Each leg pays full balance minus fee_share. Fee split 10000 / 2 = 5000 each.
    leg_amounts = {leg["worker_id"]: leg["amount_sats"] for leg in legs}
    by_addr = {w["stealth_address"]: w for w in workers}
    alpha = by_addr["H6alpha" + "a" * 70]
    gamma = by_addr["H6gamma" + "c" * 70]
    assert leg_amounts[alpha["id"]] == 100_000_000 - 5_000
    assert leg_amounts[gamma["id"]] == 1_000_000_000 - 5_000
    assert alpha["balance_sats"] == 0
    assert gamma["balance_sats"] == 0
    assert alpha["paid_total_sats"] == 100_000_000 - 5_000
    assert gamma["paid_total_sats"] == 1_000_000_000 - 5_000


def test_run_once_handles_rpc_failure(tmp_path):
    db_path = str(tmp_path / "p.sqlite")
    _seed(db_path, [("H6alpha" + "a" * 70, 100_000_000, None)])
    rpc = FakeRPC(fail=True)
    daemon = PayoutDaemon(
        db_path=db_path, rpc_call=rpc,
        default_min_payout_sats=10_000_000, batch_size=50,
        fee_per_payout_sats=10_000,
    )
    stats = daemon.run_once()

    assert stats.eligible_workers == 1
    assert stats.failed_batches == 1
    assert stats.paid_recipients == 0

    # Balance unchanged.
    with sqlite3.connect(db_path) as conn:
        bal = conn.execute("SELECT balance_sats FROM workers").fetchone()[0]
    assert bal == 100_000_000


def test_run_once_batches_over_size(tmp_path):
    db_path = str(tmp_path / "p.sqlite")
    workers_spec = [(f"H6w{i:02d}" + "x" * 70, 100_000_000, None) for i in range(7)]
    _seed(db_path, workers_spec)
    rpc = FakeRPC()
    daemon = PayoutDaemon(
        db_path=db_path, rpc_call=rpc,
        default_min_payout_sats=10_000_000, batch_size=3,
        fee_per_payout_sats=900,
    )
    stats = daemon.run_once()

    # 7 eligible / batch_size 3 → 3 batches (3, 3, 1).
    assert stats.eligible_workers == 7
    assert stats.payout_txs == 3
    assert stats.paid_recipients == 7
    assert len(rpc.calls) == 3
    # Last batch had 1 recipient — its tx fee was the full 900 sats.
    last_recipients = rpc.calls[-1][1][0]
    assert len(last_recipients) == 1


def test_run_once_no_eligible(tmp_path):
    db_path = str(tmp_path / "p.sqlite")
    _seed(db_path, [("H6alpha" + "a" * 70, 1_000_000, None)])  # 0.01 PRIC < 0.1
    rpc = FakeRPC()
    daemon = PayoutDaemon(
        db_path=db_path, rpc_call=rpc,
        default_min_payout_sats=10_000_000, batch_size=50,
        fee_per_payout_sats=10_000,
    )
    stats = daemon.run_once()
    assert stats.eligible_workers == 0
    assert stats.payout_txs == 0
    assert len(rpc.calls) == 0


def test_run_once_drops_legs_when_fee_swamps_balance(tmp_path):
    """A worker juuust above min_payout but below fee_share gets dropped
    rather than being paid a negative amount."""
    db_path = str(tmp_path / "p.sqlite")
    _seed(db_path, [
        ("H6tiny" + "t" * 70,   10_000, None),          # at min, but fee swallows it
    ])
    rpc = FakeRPC()
    daemon = PayoutDaemon(
        db_path=db_path, rpc_call=rpc,
        default_min_payout_sats=10_000, batch_size=50,
        fee_per_payout_sats=20_000,                      # bigger than the balance
    )
    stats = daemon.run_once()
    assert stats.eligible_workers == 1
    assert stats.payout_txs == 0
    assert len(rpc.calls) == 0
    # Balance untouched.
    with sqlite3.connect(db_path) as conn:
        bal = conn.execute("SELECT balance_sats FROM workers").fetchone()[0]
    assert bal == 10_000
