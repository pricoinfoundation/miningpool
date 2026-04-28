#!/usr/bin/env python3
"""End-to-end check: pricoin-miner ↔ pool ↔ pricoind on regtest.

Spins up everything in temp dirs, runs the miner long enough to submit a
handful of shares, asserts the pool accepted them, then cleans up. Prints
a success line and exits 0; any non-zero exit means the share round-trip
broke (most likely fault: the xmrig fork is hashing wrong or the pool's
seed_hash format mismatches).

Usage::

    python3 scripts/test_pricoin_miner.py \\
        [--miner ~/xmrig-pricoin/build/pricoin-miner] \\
        [--pricoind ~/bitcoin/build_pric/bin/pricoind] \\
        [--shares 3]
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import http.client
import json
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pool.jobs import JobManager
from pool.payouts import PayoutDaemon
from pool.rpc import PricoinRPC
from pool.stratum import StratumServer
from pool.vardiff import VardiffConfig
from rxshare import RxShare


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_rpc(rpc, deadline):
    while time.time() < deadline:
        try:
            rpc.call("getblockchaininfo")
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("pricoind RPC not up")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--miner",    default=str(pathlib.Path.home() / "xmrig-pricoin/build/pricoin-miner"))
    ap.add_argument("--pricoind", default=str(pathlib.Path.home() / "bitcoin/build_pric/bin/pricoind"))
    ap.add_argument("--cli",      default=str(pathlib.Path.home() / "bitcoin/build_pric/bin/pricoin-cli"))
    ap.add_argument("--shares",   type=int, default=3)
    ap.add_argument("--timeout",  type=float, default=120.0)
    args = ap.parse_args()

    if not pathlib.Path(args.miner).exists():
        print(f"ERROR: pricoin-miner not built at {args.miner}", file=sys.stderr)
        return 2

    datadir = pathlib.Path(tempfile.mkdtemp(prefix="pricoin-miner-e2e-"))
    rpc_user = rpc_pass = "x"
    rpcport  = free_port()
    p2pport  = free_port()
    stratum_port = free_port()
    print(f"datadir: {datadir}")

    pricoind_proc = subprocess.Popen([
        args.pricoind, "-regtest",
        f"-datadir={datadir}",
        f"-port={p2pport}", f"-rpcport={rpcport}",
        f"-rpcuser={rpc_user}", f"-rpcpassword={rpc_pass}",
        "-fallbackfee=0.0001", "-printtoconsole=0",
    ])

    miner_proc = None
    pool_task = None
    success = False

    try:
        rpc = PricoinRPC(host="127.0.0.1", port=rpcport, user=rpc_user, password=rpc_pass)
        wait_for_rpc(rpc, time.time() + 30)
        rpc.call("createwallet", "main")
        rpc.call("createwallet", "recipient")
        rpc_main = PricoinRPC(host="127.0.0.1", port=rpcport, user=rpc_user, password=rpc_pass, wallet="main")
        rpc_rcv  = PricoinRPC(host="127.0.0.1", port=rpcport, user=rpc_user, password=rpc_pass, wallet="recipient")
        coinbase_addr     = rpc_main.call("getnewaddress", "", "bech32")
        recipient_stealth = rpc_rcv.call("pricoin_getstealthaddress")["address"]
        # Stay below EPOCH_LAG=64 so the seed is bootstrap end-to-end —
        # otherwise we'd need pricoind to expose getblockhash(0) which it
        # does anyway. 5 blocks is enough to give getblocktemplate something.
        rpc.call("generatetoaddress", 5, coinbase_addr)
        print("pricoind regtest up + blocks generated")

        async def _run():
            nonlocal miner_proc, success

            jobs = JobManager(rpc, coinbase_address=coinbase_addr)
            initial = jobs.refresh()
            rx = RxShare(initial.seed_hash, init_threads=2, full_mem=False)
            try:
                vd = VardiffConfig(target_share_seconds=10.0, retarget_after=8,
                                   adjust_high=1.4, adjust_low=0.7, min_diff=1, max_diff=1 << 16)
                server = StratumServer(
                    "127.0.0.1", stratum_port, jobs, str(datadir / "pool.sqlite"),
                    rx, rpc=rpc, vardiff_cfg=vd,
                    initial_diff=64,            # easy share so we get hits fast
                    refresh_interval=5.0,
                    pool_fee_pct=1.0, pplns_window=100,
                )
                await server.start()
                print(f"pool stratum on 127.0.0.1:{stratum_port}")

                # Launch pricoin-miner against the pool.
                miner_log = datadir / "miner.log"
                miner_proc = subprocess.Popen([
                    args.miner,
                    "--algo=rx/pric",
                    f"--url=127.0.0.1:{stratum_port}",
                    f"--user={recipient_stealth}",
                    "--pass=x",
                    "--keepalive",
                    "--no-color",
                    "--threads=1",
                    "--randomx-mode=light",
                    "--print-time=10",
                    f"--log-file={miner_log}",
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print(f"started pricoin-miner pid={miner_proc.pid} log={miner_log}")

                # Watch its stdout AND the pool's blocks_found list. Loop until
                # we've seen N accepted shares (pool side: that's len(server.blocks_found)
                # OR a count from sqlite shares table for non-block shares).
                t0 = time.time()
                accept_re = re.compile(r"accepted|share accepted", re.IGNORECASE)
                share_count = 0
                lines_seen = 0
                # Read miner output non-blocking via select-ish polling.
                import select
                import sqlite3 as _sqlite3
                db_path = str(datadir / "pool.sqlite")

                def _pool_counts() -> tuple[int, int]:
                    """Returns (shares, blocks). 0/0 if DB or tables don't
                    exist yet — they only get created on the first miner
                    login, which can take a few seconds."""
                    try:
                        db = _sqlite3.connect(db_path)
                        s = db.execute("SELECT COUNT(*) FROM shares").fetchone()[0]
                        b = db.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
                        db.close()
                        return s, b
                    except _sqlite3.OperationalError:
                        return 0, 0

                last_log_size = 0
                def _drain_log():
                    nonlocal last_log_size
                    if not miner_log.exists():
                        return 0
                    txt = miner_log.read_text(errors="replace")
                    new = txt[last_log_size:]
                    last_log_size = len(txt)
                    n_acc = 0
                    for line in new.splitlines():
                        print(f"[miner] {line}")
                        if accept_re.search(line):
                            n_acc += 1
                    return n_acc

                while time.time() - t0 < args.timeout:
                    share_count += _drain_log()
                    pool_shares, pool_blocks = _pool_counts()
                    if pool_shares >= args.shares:
                        print(f"\n[pool] shares={pool_shares} blocks={pool_blocks} — target reached")
                        success = True
                        break
                    if miner_proc.poll() is not None:
                        print(f"miner exited early with code {miner_proc.returncode}")
                        break
                    await asyncio.sleep(0.5)
                _drain_log()  # flush remaining log lines
                if not success:
                    print(f"\nTIMED OUT after {args.timeout}s — miner_lines={lines_seen} "
                          f"share_count_in_log={share_count}")
            finally:
                await server.close()
                rx.close()

        pool_task = asyncio.run(_run())
    finally:
        if miner_proc and miner_proc.poll() is None:
            miner_proc.terminate()
            try: miner_proc.wait(timeout=5)
            except Exception: miner_proc.kill()
        try:
            subprocess.run([args.cli, "-regtest",
                            f"-datadir={datadir}", f"-rpcport={rpcport}",
                            f"-rpcuser={rpc_user}", f"-rpcpassword={rpc_pass}",
                            "stop"], check=False, timeout=10)
        except Exception:
            pricoind_proc.terminate()
        try: pricoind_proc.wait(timeout=15)
        except Exception: pricoind_proc.kill()
        shutil.rmtree(datadir, ignore_errors=True)

    if success:
        print("\nOK: pricoin-miner submitted shares to the pool, the pool accepted them.")
        return 0
    print("\nFAIL: no shares made it through within the timeout.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
