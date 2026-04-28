"""Pricoin pool daemon entrypoint.

Reads config.toml, wires up RxShare + JobManager + StratumServer + the
payout loop, and runs until SIGINT/SIGTERM. Logs to stderr; redirect
to a file (or use systemd) for persistence.

Run::

    python3 -m pool.main --config config.toml --db pool.sqlite

Or via systemd (see contrib/pricoinpool.service in the repo).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

try:
    import tomllib                          # py311+
except ModuleNotFoundError:
    import tomli as tomllib                  # py310 fallback

from rxshare import RxShare

from .jobs import JobManager
from .payouts import PayoutDaemon
from .rpc import PricoinRPC
from .stratum import StratumServer
from .vardiff import VardiffConfig


SATS_PER_PRIC = 100_000_000


def load_config(path: str) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def _pricoind_rpc(cfg: dict, *, wallet: str | None = None) -> PricoinRPC:
    p = cfg["pricoind"]
    return PricoinRPC(
        host=p.get("rpc_host", "127.0.0.1"),
        port=int(p.get("rpc_port", 8332)),
        user=p.get("rpc_user") or None,
        password=p.get("rpc_password") or None,
        datadir=p.get("datadir"),
        wallet=wallet,
    )


async def _payout_loop(daemon: PayoutDaemon, interval: float) -> None:
    log = logging.getLogger("pool.payouts")
    loop = asyncio.get_running_loop()
    while True:
        try:
            stats = await loop.run_in_executor(None, daemon.run_once)
            if stats.eligible_workers:
                log.info(
                    "cycle: eligible=%d txs=%d recipients=%d sats=%d failed=%d",
                    stats.eligible_workers, stats.payout_txs,
                    stats.paid_recipients, stats.paid_total_sats,
                    stats.failed_batches,
                )
        except Exception as e:
            log.warning("payout cycle error: %s", e)
        await asyncio.sleep(interval)


async def _amain() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--db",     default="pool.sqlite")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("pool.main")

    cfg = load_config(args.config)

    # ---- RPC clients ----
    rpc_node   = _pricoind_rpc(cfg)
    rpc_wallet = _pricoind_rpc(cfg, wallet=cfg["pricoind"]["wallet"])

    # ---- JobManager: initial refresh to learn the current seed ----
    coinbase_address = cfg["pool"]["coinbase_address"]
    if not coinbase_address:
        log.error("pool.coinbase_address is empty in config")
        return 2
    jobs = JobManager(rpc_node, coinbase_address=coinbase_address)
    initial = jobs.refresh()
    log.info("initial template: height=%d seed=%s coinbasevalue=%d",
             initial.height, initial.seed_hash.hex(), initial.reward_sats)

    # ---- RxShare ----
    rx_cfg = cfg.get("rxshare", {})
    init_threads = int(rx_cfg.get("init_threads", 8))
    full_mem = _resolve_full_mem(rx_cfg.get("flags", "auto"))
    log.info("initializing rxshare full_mem=%s init_threads=%d "
             "(FULL_MEM dataset takes ~30 s on 8 threads, ~2 GB resident)",
             full_mem, init_threads)
    rx = RxShare(initial.seed_hash, init_threads=init_threads, full_mem=full_mem)
    log.info("rxshare ready")

    try:
        # ---- vardiff ----
        vd_cfg = cfg.get("vardiff", {})
        vardiff = VardiffConfig(
            target_share_seconds=float(cfg["pool"].get("share_target_seconds", 10.0)),
            retarget_after=int(vd_cfg.get("retarget_after", 16)),
            adjust_high=float(vd_cfg.get("adjust_high", 1.4)),
            adjust_low=float(vd_cfg.get("adjust_low", 0.7)),
            min_diff=int(vd_cfg.get("min_difficulty", 1)),
            max_diff=int(vd_cfg.get("max_difficulty", 1 << 32)),
        )

        # ---- stratum ----
        st_cfg = cfg.get("stratum", {})
        pl_cfg = cfg.get("pplns", {})
        server = StratumServer(
            listen_host=st_cfg.get("listen_host", "0.0.0.0"),
            listen_port=int(st_cfg.get("listen_port", 3333)),
            jobs=jobs, db_path=args.db, rxshare=rx, rpc=rpc_node,
            vardiff_cfg=vardiff,
            initial_diff=int(vd_cfg.get("initial_diff", 1024)),
            refresh_interval=float(st_cfg.get("refresh_interval_s", 5.0)),
            pool_fee_pct=float(cfg["pool"]["fee_percent"]),
            pplns_window=int(pl_cfg.get("n_shares", 3000)),
        )
        await server.start()
        log.info("stratum listening on %s:%d",
                 *server.listen_address or ("?", 0))

        # ---- payout loop ----
        po_cfg = cfg.get("payouts", {})
        daemon = PayoutDaemon(
            db_path=args.db,
            rpc_call=rpc_wallet.call,
            default_min_payout_sats=int(float(po_cfg.get("default_min_payout", 0.1))
                                        * SATS_PER_PRIC),
            batch_size=int(po_cfg.get("batch_size", 50)),
            fee_per_payout_sats=int(float(po_cfg.get("fee_per_payout", 0.0001))
                                    * SATS_PER_PRIC),
        )
        payout_task = asyncio.create_task(
            _payout_loop(daemon, float(po_cfg.get("payout_interval_s", 600))))

        # ---- run until signaled ----
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass  # Windows
        log.info("pool running; SIGINT or SIGTERM to stop")
        await stop.wait()

        log.info("shutting down")
        payout_task.cancel()
        try: await payout_task
        except asyncio.CancelledError: pass
        await server.close()
    finally:
        rx.close()

    return 0


def _resolve_full_mem(flags_value) -> bool:
    """`flags = 'auto'` defaults to FULL_MEM (production). Caller can opt
    out by setting `flags = 'light'` (testing on small machines)."""
    s = str(flags_value).lower()
    if s == "light":
        return False
    return True


def main() -> int:
    try:
        return asyncio.run(_amain())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
