#!/usr/bin/env python3
"""Cross-check rxshare against pricoind.

Spins up a regtest pricoind, mines N blocks, reconstructs each block's
80-byte header from getblockheader, hashes it via rxshare with the
chain's bootstrap seed (heights < EPOCH_LAG), and verifies the result
satisfies the block's nBits target. If pricoind accepted the block, its
PoW hash is <= target; if rxshare also produces a hash <= target for the
same header, rxshare and pricoind are computing the same function (or
both wrong by the same bias — astronomically unlikely).

Usage::

    python3 scripts/compare_hash.py [--blocks N] [--bin PATH] [--datadir DIR]

Exits 0 if all blocks pass.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import pathlib
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import base64

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from rxshare import RxShare


BOOTSTRAP_SEED = hashlib.sha256(b"Pricoin RandomX fixed seed v1").digest()


def header_bytes(hdr: dict) -> bytes:
    """Reconstruct the 80-byte serialized header from getblockheader's
    parsed JSON. Display-format hex strings are big-endian; on the wire
    they're little-endian, so byte-reverse those fields."""
    return (
        struct.pack("<I", hdr["version"])
        + bytes.fromhex(hdr["previousblockhash"])[::-1]
        + bytes.fromhex(hdr["merkleroot"])[::-1]
        + struct.pack("<I", hdr["time"])
        + bytes.fromhex(hdr["bits"])[::-1]
        + struct.pack("<I", hdr["nonce"])
    )


def bits_to_target(bits_hex: str) -> int:
    bits = int(bits_hex, 16)
    exp = bits >> 24
    mant = bits & 0xFFFFFF
    return mant >> (8 * (3 - exp)) if exp <= 3 else mant << (8 * (exp - 3))


class RpcClient:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def call(self, method, *params):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=30)
        body = json.dumps({"jsonrpc": "1.0", "id": "x", "method": method, "params": list(params)})
        conn.request("POST", "/", body, {
            "Content-Type": "application/json",
            "Authorization": self.auth,
        })
        resp = conn.getresponse()
        data = json.loads(resp.read())
        conn.close()
        if data.get("error"):
            raise RuntimeError(f"{method}: {data['error']}")
        return data["result"]


def wait_for_rpc(rpc, deadline):
    while time.time() < deadline:
        try:
            rpc.call("getblockchaininfo")
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("pricoind RPC did not come up in time")


def main():
    ap = argparse.ArgumentParser()
    # 50 blocks → false-pass odds (1/2)^50 ≈ 1e-15 if rxshare were
    # computing a different function (regtest target is ~2^255, half the
    # hash space, so each block contributes ~1 bit of evidence).
    ap.add_argument("--blocks",  type=int, default=50)
    ap.add_argument("--bin",     default=str(pathlib.Path.home() / "bitcoin/build_pric/bin/pricoind"))
    ap.add_argument("--cli",     default=str(pathlib.Path.home() / "bitcoin/build_pric/bin/pricoin-cli"))
    ap.add_argument("--port",    type=int, default=14999)
    ap.add_argument("--rpcport", type=int, default=14998)
    ap.add_argument("--datadir", default=None)
    ap.add_argument("--full-mem", action="store_true",
                    help="Use FULL_MEM mode (~2 GB resident, ~30 s init)")
    args = ap.parse_args()

    datadir = pathlib.Path(args.datadir or tempfile.mkdtemp(prefix="rxshare-cross-"))
    datadir.mkdir(parents=True, exist_ok=True)
    print(f"datadir: {datadir}")

    rpc_user, rpc_pass = "x", "x"
    proc = subprocess.Popen([
        args.bin, "-regtest",
        f"-datadir={datadir}",
        f"-port={args.port}",
        f"-rpcport={args.rpcport}",
        f"-rpcuser={rpc_user}", f"-rpcpassword={rpc_pass}",
        "-fallbackfee=0.0001",
        "-printtoconsole=0",
    ])

    try:
        rpc = RpcClient("127.0.0.1", args.rpcport, rpc_user, rpc_pass)
        wait_for_rpc(rpc, time.time() + 30)
        print("pricoind regtest up")

        rpc.call("createwallet", "test")
        addr = rpc.call("getnewaddress", "", "bech32")
        rpc.call("generatetoaddress", args.blocks, addr)

        tip = rpc.call("getblockcount")
        assert tip >= args.blocks, f"only mined {tip}"
        print(f"mined {tip} blocks")

        with RxShare(BOOTSTRAP_SEED, init_threads=8, full_mem=args.full_mem) as rx:
            failures = 0
            for h in range(1, tip + 1):
                blk_hash = rpc.call("getblockhash", h)
                hdr = rpc.call("getblockheader", blk_hash, True)
                hb = header_bytes(hdr)
                pow_hash = rx.hash(hb)
                pow_int = int.from_bytes(pow_hash, "little")
                target = bits_to_target(hdr["bits"])
                ok = pow_int <= target
                marker = "OK" if ok else "FAIL"
                print(f"  h={h:3d} bits={hdr['bits']} pow_int<=target={ok} [{marker}]")
                if not ok:
                    failures += 1
                    print(f"    pow_hash (LE bytes): {pow_hash.hex()}")
                    print(f"    pow_int:             {pow_int:064x}")
                    print(f"    target:              {target:064x}")

        if failures:
            print(f"\nFAIL: {failures}/{tip} blocks did not satisfy rxshare PoW.")
            return 2
        print(f"\nOK: all {tip} mined blocks satisfy rxshare's PoW check.")
        return 0
    finally:
        try:
            subprocess.run([args.cli, "-regtest",
                            f"-datadir={datadir}",
                            f"-rpcport={args.rpcport}",
                            f"-rpcuser={rpc_user}", f"-rpcpassword={rpc_pass}",
                            "stop"], check=False, timeout=10)
        except Exception:
            proc.terminate()
        proc.wait(timeout=15)
        # Leave datadir if user supplied one; clean up our tempdir.
        if not args.datadir:
            shutil.rmtree(datadir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
