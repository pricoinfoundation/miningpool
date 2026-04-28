"""Monero-style stratum server.

The protocol is line-delimited JSON-RPC 2.0 over plain TCP. Three methods:

  login(login=<stealth_address>, pass=...) →
      {id: <session>, job: <first_job>, status: "OK"}

  submit(id=<session>, job_id, nonce, result?) →
      {status: "OK"} or error

  keepalived(id=<session>) →
      {status: "KEEPALIVED"}

Server-pushed:

  job(<job_dict>)                 — sent on template refresh or vardiff retarget

A `<job_dict>` is what JobManager.make_job_for() returns:
  {job_id, blob, target, seed_hash, height, algo}

The hash is computed by the miner with rxshare (or a Pricoin-aware miner)
on `blob_prefix(76 bytes) || nonce(4 bytes)` against `seed_hash`. The
pool re-verifies on submit.
"""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from dataclasses import dataclass, field

from rxshare import RxShare

from . import db, pplns
from .coinbase import sha256d
from .jobs import JobManager, difficulty_to_target
from .rpc import PricoinRPC, RPCError
from .vardiff import Vardiff, VardiffConfig


log = logging.getLogger("pool.stratum")


# Light sanity check on the login string. The full base58check decode
# (with Pricoin's 0x53 version byte) lives in src/pricoin/stealth.cpp;
# we don't reimplement it here. The real validation happens at payout
# time inside walletsendct_multi, which decodes each recipient and
# rejects unrecognized strings — at that point a worker who logged in
# with garbage simply doesn't get paid (their balance accrues but every
# payout batch rolls back on RPC failure for that recipient).
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
def _validate_stealth_addr(addr: str) -> bool:
    return 60 <= len(addr) <= 120 and all(ch in _BASE58 for ch in addr)


@dataclass
class Connection:
    session_id: str
    worker_id: int
    stealth_address: str
    vardiff: Vardiff
    extranonce: int                          # unique per-conn 4-byte int
    template_id: int = 0
    # (template_id, nonce) — keyed on both so the same nonce against a
    # different template (after a refresh) is legitimate, and so the
    # dedup state survives job pushes.
    seen_shares: set[tuple[int, bytes]] = field(default_factory=set)
    writer: asyncio.StreamWriter | None = None
    closed: bool = False


class StratumServer:
    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        jobs: JobManager,
        db_path: str,
        rxshare: RxShare,
        rpc: PricoinRPC,
        vardiff_cfg: VardiffConfig,
        initial_diff: int = 1024,
        refresh_interval: float = 10.0,
        pool_fee_pct: float = 1.0,
        pplns_window: int = 3000,
    ):
        self._host = listen_host
        self._port = listen_port
        self._jobs = jobs
        self._db_path = db_path
        self._rx = rxshare
        self._rpc = rpc
        self._vardiff_cfg = vardiff_cfg
        self._initial_diff = initial_diff
        self._refresh_interval = refresh_interval
        self._pool_fee_pct = pool_fee_pct
        self._pplns_window = pplns_window

        self._conns: dict[str, Connection] = {}
        self._lock = asyncio.Lock()
        self._server: asyncio.Server | None = None
        self._refresher: asyncio.Task | None = None
        # Monotonic per-conn extranonce. 4 bytes = 4 billion connections
        # before wrap, fine for any plausible pool lifetime.
        self._next_extranonce: int = 1
        # Stats for tests/UI:
        self.blocks_found: list[dict] = []

    # ---------- lifecycle ----------

    async def start(self) -> None:
        # Need a valid template before accepting any miner.
        self._jobs.refresh()
        self._server = await asyncio.start_server(self._on_client, self._host, self._port)
        self._refresher = asyncio.create_task(self._template_loop())
        log.info("stratum listening on %s:%d", self._host, self._port)

    async def close(self) -> None:
        if self._refresher:
            self._refresher.cancel()
            try:
                await self._refresher
            except asyncio.CancelledError:
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    @property
    def listen_address(self) -> tuple[str, int] | None:
        if self._server and self._server.sockets:
            host, port = self._server.sockets[0].getsockname()[:2]
            return host, port
        return None

    # ---------- server loop ----------

    async def _template_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            try:
                old_t = self._jobs.template
                old_seed = old_t.seed_hash if old_t else None
                old_id   = old_t.template_id if old_t else None
                t = self._jobs.refresh()

                # Epoch rotation: pricoind's PoW seed advances every 2048
                # blocks. Rebuild the rxshare dataset under the new seed.
                # On full_mem this is a ~30 s background build; we run
                # prepare_next + swap in an executor so the asyncio loop
                # stays responsive (kept-alive miners can still get a
                # 'keepalived' response, though their next submit will
                # block briefly during the swap).
                if old_seed is not None and t.seed_hash != old_seed:
                    log.warning("seed rotation at height=%d (was %s, now %s); "
                                "rebuilding rxshare dataset",
                                t.height, old_seed.hex()[:16], t.seed_hash.hex()[:16])
                    try:
                        await loop.run_in_executor(None, self._rx.prepare_next, t.seed_hash)
                        await loop.run_in_executor(None, self._rx.swap)
                        log.info("seed rotation complete")
                    except Exception as e:
                        # Can't recover here without a process restart; light
                        # mode in particular doesn't support swap. Log loudly
                        # and let the operator notice.
                        log.error("seed rotation FAILED (%s) — pool will reject "
                                  "shares until restarted", e)

                if old_id is None or t.template_id != old_id:
                    await self._push_job_to_all()
            except Exception as e:
                log.warning("template refresh failed: %s", e)
            await asyncio.sleep(self._refresh_interval)

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        log.info("client connected: %s", peer)
        conn: Connection | None = None
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    log.debug("ignoring non-JSON from %s: %r", peer, line)
                    continue

                method = msg.get("method")
                msg_id = msg.get("id")
                params = msg.get("params") or {}

                if method == "login":
                    conn = await self._handle_login(writer, params, msg_id, peer)
                    if conn is None:
                        return
                elif method == "submit":
                    if conn is None:
                        await self._send_error(writer, msg_id, -1, "not authorized")
                        continue
                    await self._handle_submit(conn, writer, params, msg_id)
                elif method in ("keepalived", "ping"):
                    await self._send_result(writer, msg_id, {"status": "KEEPALIVED"})
                else:
                    await self._send_error(writer, msg_id, -1, f"unknown method {method!r}")
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if conn:
                async with self._lock:
                    self._conns.pop(conn.session_id, None)
                conn.closed = True
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            log.info("client disconnected: %s", peer)

    # ---------- handlers ----------

    async def _handle_login(self, writer, params, msg_id, peer) -> Connection | None:
        login = params.get("login", "")
        if not _validate_stealth_addr(login):
            await self._send_error(writer, msg_id, -1,
                                   "invalid login: pricoin stealth address (H6...) required")
            return None
        with db.connect(self._db_path) as conn_db:
            wid = db.upsert_worker(conn_db, login)
        session_id = secrets.token_hex(8)
        async with self._lock:
            extranonce = self._next_extranonce
            self._next_extranonce = (self._next_extranonce + 1) & 0xFFFFFFFF
            if self._next_extranonce == 0:
                self._next_extranonce = 1   # avoid wrap-to-zero collision
        c = Connection(
            session_id=session_id,
            worker_id=wid,
            stealth_address=login,
            vardiff=Vardiff(self._vardiff_cfg, initial_diff=self._initial_diff),
            extranonce=extranonce,
            writer=writer,
        )
        async with self._lock:
            self._conns[session_id] = c
        job = self._jobs.make_job_for(c.vardiff.difficulty, c.extranonce)
        c.template_id = self._jobs.template.template_id
        log.info("login: worker_id=%d session=%s diff=%d peer=%s",
                 wid, session_id, c.vardiff.difficulty, peer)
        await self._send_result(writer, msg_id, {
            "id": session_id, "job": job, "status": "OK",
        })
        return c

    async def _handle_submit(self, conn, writer, params, msg_id) -> None:
        # 1) session check.
        if params.get("id") != conn.session_id:
            await self._send_error(writer, msg_id, -1, "bad session id")
            return
        # 2) job freshness — accept the current template OR one of a small
        # ring of recent ones (JobManager keeps ~4 around) so an in-flight
        # share submitted right after a refresh isn't rejected.
        try:
            template_id = int(params.get("job_id"))
        except (TypeError, ValueError):
            await self._send_error(writer, msg_id, -1, "bad job_id")
            return
        t = self._jobs.template_by_id(template_id)
        if t is None:
            await self._send_error(writer, msg_id, -1, "stale job")
            return
        # 3) nonce sanity.
        try:
            nonce = bytes.fromhex(params.get("nonce", ""))
        except ValueError:
            await self._send_error(writer, msg_id, -1, "bad nonce hex")
            return
        if len(nonce) != 4:
            await self._send_error(writer, msg_id, -1, "nonce must be 4 bytes")
            return
        share_key = (template_id, nonce)
        if share_key in conn.seen_shares:
            await self._send_error(writer, msg_id, -1, "duplicate share")
            return
        # 4) reconstruct the per-conn blob (per-miner extranonce stamps
        # a unique coinbase → unique merkle → unique blob_prefix).
        recon = self._jobs.reconstruct_for_submit(template_id, conn.extranonce, nonce)
        if recon is None:
            await self._send_error(writer, msg_id, -1, "stale job")
            return
        blob, full_block_hex = recon
        actual_hash = self._rx.hash(blob)
        actual_int = int.from_bytes(actual_hash, "little")
        share_target = difficulty_to_target(conn.vardiff.difficulty)
        if actual_int > share_target:
            await self._send_error(writer, msg_id, -1, "low difficulty share")
            return
        # 5) optional miner-provided result cross-check.
        client_result = params.get("result")
        if client_result:
            try:
                if bytes.fromhex(client_result) != actual_hash:
                    await self._send_error(writer, msg_id, -1, "result mismatch")
                    return
            except ValueError:
                await self._send_error(writer, msg_id, -1, "bad result hex")
                return

        # Accept.
        conn.seen_shares.add(share_key)
        with db.connect(self._db_path) as conn_db:
            db.insert_share(conn_db, conn.worker_id, conn.vardiff.difficulty, t.height)
        new_diff = conn.vardiff.record_share(time.time())

        await self._send_result(writer, msg_id, {"status": "OK"})

        if actual_int <= t.block_target:
            await self._on_block_found(t, nonce, blob, full_block_hex, conn.worker_id)

        if new_diff is not None:
            log.info("vardiff retarget worker=%d new=%d", conn.worker_id, new_diff)
            await self._push_job(conn)

    async def _on_block_found(self, t, nonce: bytes, header_blob: bytes,
                              block_hex: str, finder_worker_id: int) -> None:
        """Reconstruct + submit the full block, then run PPLNS split if accepted."""
        # submitblock is synchronous and ~milliseconds on regtest; OK on
        # the asyncio thread for now. Move to a thread executor in phase 5
        # if it ever becomes a contention point.
        try:
            result = self._rpc.call("submitblock", block_hex)
        except RPCError as e:
            log.warning("submitblock RPC error: %s", e)
            return
        # Bitcoin Core convention: null/None = accepted; non-empty string = error code.
        if result not in (None, ""):
            log.warning("submitblock rejected: %s", result)
            return

        # Block accepted — drop the template from the ring so additional
        # shares against it don't trigger a sibling-block submit.
        self._jobs.invalidate_template(t.template_id)

        block_hash_internal = sha256d(header_blob)
        block_hash_display = block_hash_internal[::-1].hex()

        # PPLNS: split the reward minus pool fee across the last N shares.
        with db.connect(self._db_path) as conn_db:
            window = pplns.take_window(conn_db, self._pplns_window)
            split, pool_fee = pplns.split_block_reward(
                window, t.reward_sats, self._pool_fee_pct)
            conn_db.execute(
                "INSERT OR REPLACE INTO blocks "
                "(height, hash, found_at, reward_sats, pool_fee_sats, accepted) "
                "VALUES (?, ?, ?, ?, ?, 1)",
                (t.height, block_hash_display, int(time.time()),
                 t.reward_sats, pool_fee),
            )
            if split:
                db.credit_balance(conn_db, list(split.items()))

        log.info("BLOCK ACCEPTED height=%d hash=%s reward=%d_sats fee=%d_sats "
                 "credited_to=%d_workers finder=%d",
                 t.height, block_hash_display, t.reward_sats, pool_fee,
                 len(split), finder_worker_id)
        self.blocks_found.append({
            "height": t.height,
            "hash":   block_hash_display,
            "reward_sats": t.reward_sats,
            "pool_fee_sats": pool_fee,
            "credited_workers": len(split),
            "finder_worker_id": finder_worker_id,
        })

        # Refresh the template so the next job is on top of our just-mined
        # block; push to all miners so they don't keep hashing a now-stale
        # template.
        try:
            self._jobs.refresh()
            await self._push_job_to_all()
        except Exception as e:
            log.warning("post-block refresh failed: %s", e)

    # ---------- push helpers ----------

    async def _push_job_to_all(self) -> None:
        async with self._lock:
            conns = list(self._conns.values())
        for c in conns:
            try:
                await self._push_job(c)
            except Exception as e:
                log.warning("push to session=%s failed: %s", c.session_id, e)

    async def _push_job(self, conn: Connection) -> None:
        if conn.closed or conn.writer is None:
            return
        job = self._jobs.make_job_for(conn.vardiff.difficulty, conn.extranonce)
        conn.template_id = self._jobs.template.template_id
        # Don't clear seen_shares — duplicate detection is per-(template,nonce)
        # so it stays valid across pushes; clearing on push would let a
        # miner re-submit the exact same (job, nonce) tuple any time we
        # advance the template ring without invalidating its key.
        await self._send_notification(conn.writer, "job", job)

    # ---------- wire helpers ----------

    @staticmethod
    async def _send_result(writer, msg_id, result) -> None:
        line = json.dumps({"id": msg_id, "jsonrpc": "2.0", "result": result, "error": None}) + "\n"
        writer.write(line.encode())
        await writer.drain()

    @staticmethod
    async def _send_error(writer, msg_id, code, message) -> None:
        line = json.dumps({"id": msg_id, "jsonrpc": "2.0", "result": None,
                           "error": {"code": code, "message": message}}) + "\n"
        writer.write(line.encode())
        await writer.drain()

    @staticmethod
    async def _send_notification(writer, method, params) -> None:
        line = json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n"
        writer.write(line.encode())
        await writer.drain()
