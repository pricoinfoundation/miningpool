"""End-to-end pool smoke test.

Spins up a regtest pricoind, mines one block (so getblocktemplate has
something to template against), starts the StratumServer in this
process against that pricoind, opens a TCP connection like a miner
would, runs login + a few share submits.

Confirms the wire protocol, vardiff retarget, share-recording, and
rxshare integration all line up.
"""
from __future__ import annotations

import asyncio
import base64
import http.client
import json
import os
import pathlib
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pool.jobs import JobManager, BOOTSTRAP_SEED, cache_key_from_seed, difficulty_to_target
from pool.payouts import PayoutDaemon
from pool.rpc import PricoinRPC
from pool.stratum import StratumServer
from pool.vardiff import VardiffConfig
from rxshare import RxShare


PRICOIND = pathlib.Path.home() / "bitcoin/build_pric/bin/pricoind"
PRICOIN_CLI = pathlib.Path.home() / "bitcoin/build_pric/bin/pricoin-cli"


def _wait_rpc_up(rpc: PricoinRPC, deadline: float) -> None:
    while time.time() < deadline:
        try:
            rpc.call("getblockchaininfo")
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("pricoind RPC not up in time")


@pytest.fixture(scope="module")
def regtest_pricoind():
    if not PRICOIND.exists():
        pytest.skip(f"pricoind not built at {PRICOIND}")

    datadir = pathlib.Path(tempfile.mkdtemp(prefix="pool-e2e-"))
    rpc_user = "x"
    rpc_pass = "x"
    port = _free_port()
    rpcport = _free_port()

    proc = subprocess.Popen([
        str(PRICOIND), "-regtest",
        f"-datadir={datadir}",
        f"-port={port}", f"-rpcport={rpcport}",
        f"-rpcuser={rpc_user}", f"-rpcpassword={rpc_pass}",
        "-fallbackfee=0.0001", "-printtoconsole=0",
    ])

    try:
        rpc = PricoinRPC(host="127.0.0.1", port=rpcport, user=rpc_user, password=rpc_pass)
        _wait_rpc_up(rpc, time.time() + 30)
        rpc.call("createwallet", "main")
        rpc.call("createwallet", "recipient")
        rpc_main = PricoinRPC(host="127.0.0.1", port=rpcport,
                              user=rpc_user, password=rpc_pass, wallet="main")
        rpc_rcv = PricoinRPC(host="127.0.0.1", port=rpcport,
                             user=rpc_user, password=rpc_pass, wallet="recipient")
        coinbase_addr = rpc_main.call("getnewaddress", "", "bech32")
        recipient_stealth = rpc_rcv.call("pricoin_getstealthaddress")["address"]
        # 110 blocks → first 10 mature (100-confirmation rule) so the main
        # wallet has ~500 PRIC spendable for the payout test. We're still
        # well below EPOCH_LAG=64? No — 110 > 64, so seed rotates once.
        # That's fine: the rxshare side picks up the new seed via
        # _seed_for_height the next time the height crosses the lag.
        rpc.call("generatetoaddress", 110, coinbase_addr)
        yield rpc, rpc_main, rpc_rcv, coinbase_addr, recipient_stealth
    finally:
        try:
            subprocess.run([str(PRICOIN_CLI), "-regtest",
                            f"-datadir={datadir}", f"-rpcport={rpcport}",
                            f"-rpcuser={rpc_user}", f"-rpcpassword={rpc_pass}",
                            "stop"], check=False, timeout=10)
        except Exception:
            proc.terminate()
        try:
            proc.wait(timeout=15)
        except Exception:
            proc.kill()
        shutil.rmtree(datadir, ignore_errors=True)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------- simulated miner over TCP ----------

class StratumMiner:
    """Minimal Pricoin-pool miner — just enough to drive the smoke test."""

    def __init__(self, host: str, port: int, login: str):
        self.host = host
        self.port = port
        self.login = login
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._next_id = 1
        self.session_id: str | None = None
        self.last_job: dict | None = None

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)

    async def close(self):
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

    async def _send(self, method: str, params: dict) -> dict:
        msg_id = self._next_id
        self._next_id += 1
        line = (json.dumps({"id": msg_id, "jsonrpc": "2.0", "method": method, "params": params}) + "\n").encode()
        self._writer.write(line)
        await self._writer.drain()
        # Read responses until we see one matching our id (server may push
        # 'job' notifications interleaved).
        while True:
            raw = await self._reader.readline()
            if not raw:
                raise RuntimeError("connection closed")
            msg = json.loads(raw)
            if msg.get("method") == "job":
                self.last_job = msg["params"]
                continue
            if msg.get("id") == msg_id:
                return msg

    async def login_(self) -> dict:
        resp = await self._send("login", {"login": self.login, "pass": "x"})
        if resp.get("error"):
            raise RuntimeError(f"login failed: {resp['error']}")
        result = resp["result"]
        self.session_id = result["id"]
        self.last_job = result["job"]
        return result

    async def submit(self, nonce_hex: str, result_hex: str | None = None,
                     job_id: str | None = None) -> dict:
        params = {
            "id": self.session_id,
            "job_id": job_id or self.last_job["job_id"],
            "nonce": nonce_hex,
        }
        if result_hex:
            params["result"] = result_hex
        return await self._send("submit", params)

    async def drain_pending_jobs(self, timeout: float = 0.5) -> None:
        """Read any queued 'job' notifications. The protocol is
        response-before-notification, so right after submit() returns,
        a fresh job push from the pool may still be sitting on the wire."""
        while True:
            try:
                raw = await asyncio.wait_for(self._reader.readline(), timeout=timeout)
            except asyncio.TimeoutError:
                return
            if not raw:
                return
            msg = json.loads(raw)
            if msg.get("method") == "job":
                self.last_job = msg["params"]


import random as _random

def _mine_share_for_target(rx: RxShare, blob_hex: str, target_compact_hex: str) -> tuple[bytes, bytes]:
    """Brute-force a nonce that produces a hash <= the high-32 share target.

    Phase-3 share target is the top 4 bytes of the 256-bit target, sent
    little-endian. Miner's local check is: top 4 bytes of LE-hash <= those
    4 bytes interpreted as int. Pool re-validates the full 256-bit
    comparison. With initial_diff=1024 the share target is ~2^246 so any
    hash with its top 10 bits clear works. ~1024 attempts on average.
    """
    blob = bytes.fromhex(blob_hex)
    cap = int.from_bytes(bytes.fromhex(target_compact_hex), "little")  # top 32 bits
    start = _random.getrandbits(32)
    for i in range(1 << 24):
        n = (start + i) & 0xFFFFFFFF
        nonce = n.to_bytes(4, "little")
        h = rx.hash(blob[:76] + nonce)
        # Compare same way the pool does: int.from_bytes(h, "little")
        # against the full target. The compact target is just the top 32
        # bits; if the top 32 LE bits are <= compact target, the full
        # comparison passes too (with the lower 224 bits as tiebreaker).
        top32 = int.from_bytes(h[28:32], "little")
        if top32 < cap:
            return nonce, h
    raise RuntimeError("could not find share in 2^24 tries — target too tight?")


# ---------- the test ----------

def test_e2e_share_round_trip(regtest_pricoind, tmp_path):
    rpc, _rpc_main, _rpc_rcv, coinbase_addr, recipient_stealth = regtest_pricoind
    db_path = str(tmp_path / "pool.sqlite")
    pool_port = _free_port()
    jobs = JobManager(rpc, coinbase_address=coinbase_addr)
    # Resolve the right seed_hash for the next block before booting
    # rxshare. After 110 blocks of fixture setup we're past EPOCH_LAG=64,
    # so the seed is block(0).hash, not the bootstrap.
    bootstrap_template = jobs.refresh()
    asyncio.run(_run_e2e(rpc, jobs, db_path, pool_port, bootstrap_template.seed_hash,
                         miner_login=recipient_stealth))


async def _run_e2e(rpc, jobs, db_path, pool_port, seed_hash, miner_login):
    rx = RxShare(seed_hash, init_threads=2, full_mem=False)
    try:
        vd = VardiffConfig(target_share_seconds=10.0, retarget_after=4,
                           adjust_high=1.4, adjust_low=0.7, min_diff=1, max_diff=1 << 16)
        server = StratumServer(
            "127.0.0.1", pool_port, jobs, db_path, rx,
            rpc=rpc,
            vardiff_cfg=vd, initial_diff=1024, refresh_interval=120.0,
            pool_fee_pct=1.0, pplns_window=100,
        )
        await server.start()

        try:
            miner = StratumMiner("127.0.0.1", pool_port, miner_login)
            await miner.connect()
            try:
                login_result = await miner.login_()
                job = login_result["job"]
                assert int(job["height"]) > 0
                assert len(bytes.fromhex(job["seed_hash"])) == 32
                assert len(bytes.fromhex(job["blob"])) == 80
                # As of phase 5.4 the pool sends the tagged cache key
                # (so xmrig can use it directly), not the raw seed.
                assert bytes.fromhex(job["seed_hash"]) == cache_key_from_seed(seed_hash)

                # Mine + submit one share. On regtest the chain target
                # ≈ 2^255 is *looser* than our share target (2^256/1024
                # ≈ 2^246), so this share is also a winning block — the
                # pool will call submitblock and refresh the template.
                first_job_id = job["job_id"]
                nonce, h = _mine_share_for_target(rx, job["blob"], job["target"])
                resp = await miner.submit(nonce.hex(), h.hex())
                assert resp.get("error") is None, resp
                assert resp["result"]["status"] == "OK"

                # The block-found path invalidated the template. Submitting
                # against it now should yield "stale job".
                stale = await miner.submit(nonce.hex(), h.hex(), job_id=first_job_id)
                assert stale.get("error") is not None
                assert "stale" in stale["error"]["message"]

                # Three more shares (each will also be a winning block on
                # regtest). Drain pending 'job' notifications between
                # submits so we always mine against the freshest template.
                await miner.drain_pending_jobs()
                for _ in range(3):
                    n, hh = _mine_share_for_target(rx, miner.last_job["blob"],
                                                   miner.last_job["target"])
                    r = await miner.submit(n.hex(), hh.hex())
                    assert r.get("error") is None, r
                    await miner.drain_pending_jobs()
            finally:
                await miner.close()
        finally:
            await server.close()
    finally:
        rx.close()

    # Verify shares were inserted into the pool DB.
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM shares").fetchall()
        assert len(rows) == 4
        for r in rows:
            assert r["difficulty"] >= 1
        workers = conn.execute("SELECT * FROM workers").fetchall()
        assert len(workers) == 1

        # On regtest, the chain target ≈ 2^255 (per powLimit) is *looser*
        # than our share target (2^256 / 1024 ≈ 2^246), so EVERY accepted
        # share is also a winning block. Expect 4 blocks found.
        blocks = conn.execute("SELECT * FROM blocks").fetchall()
        assert len(blocks) == 4, f"expected 4 blocks, got {len(blocks)}"
        for b in blocks:
            assert b["accepted"] == 1
            # Pool fee is 1% by default; with 50 PRIC subsidy that's 0.5 PRIC.
            assert b["pool_fee_sats"] > 0
            assert b["reward_sats"] > 0
        # Phase-5 deferred-credit model: balance_sats stays at 0 until the
        # block matures (100 confs) and PayoutDaemon's maturity pass moves
        # the per-block split out of pending_credits. Pre-maturity, the
        # split lives in pending_credits.
        balance_now = conn.execute("SELECT balance_sats FROM workers WHERE id = ?",
                                   (workers[0]["id"],)).fetchone()["balance_sats"]
        assert balance_now == 0, f"balance should be 0 pre-maturity, got {balance_now}"
        pending_total = conn.execute(
            "SELECT COALESCE(SUM(amount_sats), 0) AS s FROM pending_credits"
        ).fetchone()["s"]
        total_distributable = sum(b["reward_sats"] - b["pool_fee_sats"] for b in blocks)
        assert pending_total == total_distributable, \
            f"pending {pending_total} != distributable {total_distributable}"
        for b in blocks:
            assert b["credited"] == 0, "blocks shouldn't be marked credited yet"


# ---------- payout e2e ----------

async def _run_one_share(rpc, jobs, db_path, pool_port, seed_hash, miner_login):
    rx = RxShare(seed_hash, init_threads=2, full_mem=False)
    try:
        vd = VardiffConfig(target_share_seconds=10.0, retarget_after=4,
                           adjust_high=1.4, adjust_low=0.7, min_diff=1, max_diff=1 << 16)
        server = StratumServer(
            "127.0.0.1", pool_port, jobs, db_path, rx, rpc=rpc,
            vardiff_cfg=vd, initial_diff=1024, refresh_interval=120.0,
            pool_fee_pct=1.0, pplns_window=100,
        )
        await server.start()
        try:
            miner = StratumMiner("127.0.0.1", pool_port, miner_login)
            await miner.connect()
            try:
                await miner.login_()
                nonce, h = _mine_share_for_target(rx, miner.last_job["blob"],
                                                  miner.last_job["target"])
                resp = await miner.submit(nonce.hex(), h.hex())
                assert resp.get("error") is None, resp
            finally:
                await miner.close()
        finally:
            await server.close()
    finally:
        rx.close()


def test_two_miners_get_different_blobs(regtest_pricoind, tmp_path):
    """Per-miner extranonce stamps a unique coinbase into each miner's
    job → unique merkle → unique blob_prefix. Two simultaneous
    connections from the same template must NOT receive the same blob,
    or they'd burn each other's nonce space."""
    rpc, _, rpc_rcv, coinbase_addr, recipient_stealth = regtest_pricoind
    db_path = str(tmp_path / "pool.sqlite")
    pool_port = _free_port()
    jobs = JobManager(rpc, coinbase_address=coinbase_addr)
    bootstrap_template = jobs.refresh()

    asyncio.run(_two_miner_blob_check(rpc, jobs, db_path, pool_port,
                                      bootstrap_template.seed_hash,
                                      recipient_stealth))


async def _two_miner_blob_check(rpc, jobs, db_path, pool_port, seed_hash, login):
    rx = RxShare(seed_hash, init_threads=2, full_mem=False)
    try:
        vd = VardiffConfig(target_share_seconds=10.0, retarget_after=4,
                           adjust_high=1.4, adjust_low=0.7, min_diff=1, max_diff=1 << 16)
        server = StratumServer(
            "127.0.0.1", pool_port, jobs, db_path, rx, rpc=rpc,
            vardiff_cfg=vd, initial_diff=1024, refresh_interval=120.0,
            pool_fee_pct=1.0, pplns_window=100,
        )
        await server.start()
        try:
            m1 = StratumMiner("127.0.0.1", pool_port, login)
            m2 = StratumMiner("127.0.0.1", pool_port, login)
            await m1.connect()
            await m2.connect()
            try:
                await m1.login_()
                await m2.login_()
                blob1 = m1.last_job["blob"]
                blob2 = m2.last_job["blob"]
                # Same template_id but distinct extranonces → distinct blobs.
                assert m1.last_job["job_id"] == m2.last_job["job_id"]
                assert blob1 != blob2, "two miners got identical blobs"
                # Confirm the difference is in the merkle (bytes 36..68)
                # — the extranonce changes the coinbase scriptSig which
                # changes the coinbase txid which changes the merkle root.
                b1 = bytes.fromhex(blob1)
                b2 = bytes.fromhex(blob2)
                assert b1[:36] == b2[:36], "version+prev should be identical"
                assert b1[36:68] != b2[36:68], "merkle should differ"
                assert b1[68:] == b2[68:], "time+bits+nonce should be identical"
            finally:
                await m1.close()
                await m2.close()
        finally:
            await server.close()
    finally:
        rx.close()


def test_e2e_payout(regtest_pricoind, tmp_path):
    """End-to-end: mine a share → balance credit → run payout daemon →
    walletsendct_multi broadcasts → mine confirm → recipient wallet
    recovers the CT output via pricoin_listownct."""
    rpc, rpc_main, rpc_rcv, coinbase_addr, recipient_stealth = regtest_pricoind
    db_path = str(tmp_path / "pool.sqlite")
    pool_port = _free_port()
    jobs = JobManager(rpc, coinbase_address=coinbase_addr)
    bootstrap_template = jobs.refresh()

    asyncio.run(_run_one_share(
        rpc, jobs, db_path, pool_port,
        bootstrap_template.seed_hash, recipient_stealth,
    ))

    # Mine 100 more blocks so the pool's just-found block reaches coinbase
    # maturity. The maturity pass inside PayoutDaemon.run_once() will then
    # promote the pending_credits entry into balance_sats and the payout
    # batch will go out.
    rpc.call("generatetoaddress", 100, coinbase_addr)

    # Recipient's pre-payout CT balance.
    pre = float(rpc_rcv.call("pricoin_listownct", 0)["total_recovered"])

    # Run the payout daemon.
    daemon = PayoutDaemon(
        db_path=db_path,
        rpc_call=lambda m, *a: rpc_main.call(m, *a),
        default_min_payout_sats=10_000_000,    # 0.1 PRIC
        batch_size=50,
        fee_per_payout_sats=10_000,            # 0.0001 PRIC
    )
    stats = daemon.run_once()
    assert stats.matured_blocks >= 1, stats
    assert stats.payout_txs == 1, stats
    assert stats.paid_recipients == 1
    assert stats.paid_total_sats > 0

    # Confirm the payout tx by mining a block.
    rpc.call("generatetoaddress", 1, coinbase_addr)

    # Recipient wallet now sees the new CT output.
    post = float(rpc_rcv.call("pricoin_listownct", 0)["total_recovered"])
    delta = post - pre
    assert delta > 0, f"recipient gained nothing: pre={pre} post={post}"
    # Allow a small floating-point comparison fuzz around the expected
    # paid_total. paid_total_sats is the leg amount; recipient receives
    # exactly that.
    expected_pric = stats.paid_total_sats / 100_000_000
    assert abs(delta - expected_pric) < 1e-7, \
        f"delta={delta} expected={expected_pric}"

    # Pool DB has a payouts row + leg.
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        payouts = conn.execute("SELECT * FROM payouts").fetchall()
        legs = conn.execute("SELECT * FROM payout_legs").fetchall()
        assert len(payouts) == 1
        assert len(legs) == 1
        assert payouts[0]["recipient_count"] == 1
        # Worker balance reduced to 0.
        bal = conn.execute("SELECT balance_sats FROM workers WHERE id = ?",
                           (legs[0]["worker_id"],)).fetchone()["balance_sats"]
        assert bal == 0
