"""Mining job builder.

Each pricoind `getblocktemplate` becomes a `TemplateBase` — the bits
shared across all miners (height, prev hash, time, bits, mempool txs,
witness commitment, coinbase value/script). Per-miner work then stamps
a unique 4-byte extranonce into the coinbase, recomputes the merkle root
on top of the cached mempool txid leaves, and sends the resulting unique
blob_prefix down to that one connection. With per-miner extranonce the
nonce space scales with miner count instead of forcing all miners to
share the same 4-billion-nonce pool.

JobManager keeps a small ring of recent template bases (`_recent`) so a
share submitted just after a refresh can still be reconstructed for
verification + assembly. After a winning share for a template, that
template is invalidated to prevent sibling-block submission.
"""
from __future__ import annotations

import hashlib
import struct
import threading
from dataclasses import dataclass

from .coinbase import (
    build_coinbase,
    encode_varint,
    merkle_root as compute_merkle_root,
    sha256d,
)
from .rpc import PricoinRPC


# Match consensus constants in src/pow/randomx_pricoin.h.
EPOCH_BLOCKS = 2048
EPOCH_LAG    = 64

BOOTSTRAP_SEED = hashlib.sha256(b"Pricoin RandomX fixed seed v1").digest()


def compute_seed_height(height: int) -> int:
    if height < EPOCH_LAG:
        return 0
    return (height - EPOCH_LAG) & ~(EPOCH_BLOCKS - 1)


def difficulty_to_target(diff: int) -> int:
    if diff <= 0:
        raise ValueError("difficulty must be > 0")
    return min(((1 << 256) // diff), (1 << 256) - 1)


def target_to_compact_le_hex(target: int) -> str:
    return (target >> 224).to_bytes(4, "little").hex()


def bits_to_target(bits_hex: str) -> int:
    bits = int(bits_hex, 16)
    exp = bits >> 24
    mant = bits & 0xFFFFFF
    return mant >> (8 * (3 - exp)) if exp <= 3 else mant << (8 * (exp - 3))


@dataclass
class TemplateBase:
    """Per-template state shared by all miners; the per-miner coinbase +
    merkle + blob is built on top of this on every job request and on
    every submit verification."""
    template_id: int
    height: int
    seed_hash: bytes
    block_target: int

    # Header pieces (all little-endian):
    version_bytes:   bytes
    prev_le:         bytes
    curtime_bytes:   bytes
    bits_le:         bytes
    bits_hex:        str

    # Coinbase inputs:
    coinbase_value_sats:   int
    coinbase_script:       bytes
    witness_commitment:    bytes | None

    # Other txs (template_txs_hex == full hex; template_txids_le ==
    # 32-byte internal-LE txids precomputed for merkle).
    template_txs_hex:      list[str]
    template_txids_le:     list[bytes]

    @property
    def reward_sats(self) -> int:
        return self.coinbase_value_sats


def _build_for_extranonce(base: TemplateBase, extranonce: int) -> tuple[bytes, bytes, bytes]:
    """Returns (blob_prefix_76, coinbase_legacy_bytes, coinbase_witness_bytes)
    for one miner. Pure function — no JobManager state."""
    cb = build_coinbase(
        height=base.height,
        coinbase_value_sats=base.coinbase_value_sats,
        output_script=base.coinbase_script,
        extranonce=extranonce.to_bytes(4, "little"),
        witness_commitment_script=base.witness_commitment,
        version=2,
    )
    merkle = compute_merkle_root([cb.txid] + base.template_txids_le)
    blob_prefix = (
        base.version_bytes + base.prev_le + merkle
        + base.curtime_bytes + base.bits_le
    )
    assert len(blob_prefix) == 76
    return blob_prefix, cb.legacy_bytes, cb.witness_bytes


class JobManager:
    def __init__(self, rpc: PricoinRPC, coinbase_address: str):
        self._rpc = rpc
        self._coinbase_address = coinbase_address
        self._coinbase_script: bytes | None = None
        self._lock = threading.Lock()
        self._template: TemplateBase | None = None
        self._recent: dict[int, TemplateBase] = {}
        self._recent_keep = 4
        self._next_id = 1
        self._cached_seed_height = -1
        self._cached_seed_hash   = BOOTSTRAP_SEED

    @property
    def template(self) -> TemplateBase | None:
        return self._template

    def template_by_id(self, template_id: int) -> TemplateBase | None:
        with self._lock:
            return self._recent.get(template_id)

    def invalidate_template(self, template_id: int) -> None:
        with self._lock:
            self._recent.pop(template_id, None)

    # ---------- helpers ----------

    def _resolve_coinbase_script(self) -> bytes:
        if self._coinbase_script is not None:
            return self._coinbase_script
        info = self._rpc.call("validateaddress", self._coinbase_address)
        if not info.get("isvalid"):
            raise RuntimeError(f"coinbase_address invalid: {self._coinbase_address}")
        self._coinbase_script = bytes.fromhex(info["scriptPubKey"])
        return self._coinbase_script

    def _seed_for_height(self, height: int) -> bytes:
        """Match pricoin::randomx::GetPoWHashOfHeader exactly: bootstrap
        whenever seed_height == 0 (the entire first epoch)."""
        seed_height = compute_seed_height(height)
        if seed_height == 0:
            return BOOTSTRAP_SEED
        if seed_height == self._cached_seed_height:
            return self._cached_seed_hash
        block_hash = self._rpc.call("getblockhash", seed_height)
        seed = bytes.fromhex(block_hash)[::-1]
        self._cached_seed_height = seed_height
        self._cached_seed_hash = seed
        return seed

    # ---------- public ----------

    def refresh(self) -> TemplateBase:
        gbt = self._rpc.call("getblocktemplate", {"rules": ["segwit"]})
        out_script = self._resolve_coinbase_script()
        wc_hex = gbt.get("default_witness_commitment")
        wc_bytes = bytes.fromhex(wc_hex) if wc_hex else None

        with self._lock:
            base = TemplateBase(
                template_id=self._next_id,
                height=gbt["height"],
                seed_hash=self._seed_for_height(gbt["height"]),
                block_target=bits_to_target(gbt["bits"]),
                version_bytes=struct.pack("<I", gbt["version"]),
                prev_le=bytes.fromhex(gbt["previousblockhash"])[::-1],
                curtime_bytes=struct.pack("<I", gbt["curtime"]),
                bits_le=bytes.fromhex(gbt["bits"])[::-1],
                bits_hex=gbt["bits"],
                coinbase_value_sats=int(gbt["coinbasevalue"]),
                coinbase_script=out_script,
                witness_commitment=wc_bytes,
                template_txs_hex=[t["data"] for t in gbt["transactions"]],
                template_txids_le=[bytes.fromhex(t["txid"])[::-1]
                                    for t in gbt["transactions"]],
            )
            self._next_id += 1
            self._template = base
            self._recent[base.template_id] = base
            if len(self._recent) > self._recent_keep:
                old = sorted(self._recent.keys())[: -self._recent_keep]
                for k in old:
                    self._recent.pop(k, None)
            return base

    def make_job_for(self, conn_share_diff: int, extranonce: int) -> dict:
        base = self._template
        if base is None:
            raise RuntimeError("JobManager: no template loaded; call refresh()")
        blob_prefix, _legacy, _witness = _build_for_extranonce(base, extranonce)
        share_target = difficulty_to_target(conn_share_diff)
        return {
            "job_id":     str(base.template_id),
            "blob":       (blob_prefix + b"\x00\x00\x00\x00").hex(),
            "target":     target_to_compact_le_hex(share_target),
            "seed_hash":  base.seed_hash.hex(),
            "height":     base.height,
            "algo":       "rx/pric",
        }

    def reconstruct_for_submit(
        self, template_id: int, extranonce: int, nonce: bytes,
    ) -> tuple[bytes, str] | None:
        """Return (blob_with_nonce_80B, full_block_hex) for verification +
        eventual submitblock, or None if the template has been
        invalidated. Pure function call to _build_for_extranonce so the
        per-conn coinbase + merkle are reconstructed on every share —
        cheap on small mempools (microseconds)."""
        base = self.template_by_id(template_id)
        if base is None:
            return None
        if len(nonce) != 4:
            raise ValueError("nonce must be 4 bytes")
        blob_prefix, _legacy, witness = _build_for_extranonce(base, extranonce)
        blob = blob_prefix + nonce
        n_txs = 1 + len(base.template_txs_hex)
        block = (
            blob
            + encode_varint(n_txs)
            + witness
            + b"".join(bytes.fromhex(h) for h in base.template_txs_hex)
        )
        return blob, block.hex()
