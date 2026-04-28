"""Coinbase transaction builder + small Bitcoin (de)serialization helpers.

What we build is a perfectly ordinary BIP141 coinbase:

  vin[0]:
    prevout = (0x00..00, 0xffffffff)
    scriptSig = <BIP34 height push> <extranonce push>
    sequence = 0xffffffff
    witness = [0x20 || 32 zero bytes]   (BIP141 reserved value)

  vout[0]:  value = subsidy + fees,  script = pool's P2WPKH
  vout[1]:  value = 0,                script = OP_RETURN witness commitment (from
                                                getblocktemplate.default_witness_commitment)

The witness commitment, when present, comes verbatim from pricoind so we
don't have to recompute the witness merkle root for the txs already in
the template (we don't add or reorder any txs, so the daemon's pre-built
commitment stays correct).

Two distinct serializations are returned:
  * `legacy_bytes` — no witness; SHA-256d gives the *txid* used in the
    block header's merkle root and in template[].txid.
  * `witness_bytes` — full BIP141; this is what goes on the wire inside
    the assembled block submitted via submitblock.
"""
from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass


# ---------- low-level varint / pushdata / scriptnum ----------

def encode_varint(n: int) -> bytes:
    if n < 0xFD:
        return bytes([n])
    if n <= 0xFFFF:
        return b"\xfd" + n.to_bytes(2, "little")
    if n <= 0xFFFFFFFF:
        return b"\xfe" + n.to_bytes(4, "little")
    return b"\xff" + n.to_bytes(8, "little")


def encode_pushdata(data: bytes) -> bytes:
    """Push raw `data` onto the script stack with the minimal opcode."""
    n = len(data)
    if n < 0x4C:
        return bytes([n]) + data
    if n < 0x100:
        return b"\x4c" + bytes([n]) + data
    if n < 0x10000:
        return b"\x4d" + n.to_bytes(2, "little") + data
    return b"\x4e" + n.to_bytes(4, "little") + data


def encode_script_num(n: int) -> bytes:
    """Bitcoin's CScriptNum minimal encoding (used by BIP34 height)."""
    if n == 0:
        return b""
    neg = n < 0
    abs_n = abs(n)
    out = bytearray()
    while abs_n:
        out.append(abs_n & 0xFF)
        abs_n >>= 8
    if out[-1] & 0x80:
        out.append(0x80 if neg else 0x00)
    elif neg:
        out[-1] |= 0x80
    return bytes(out)


def push_script_num(n: int) -> bytes:
    """Single-opcode push for small ints (OP_1..OP_16), pushdata otherwise."""
    if n == 0:
        return b"\x00"               # OP_0
    if 1 <= n <= 16:
        return bytes([0x50 + n])     # OP_1 .. OP_16
    return encode_pushdata(encode_script_num(n))


def sha256d(b: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


# ---------- coinbase ----------

WITNESS_RESERVED_VALUE = b"\x00" * 32


@dataclass
class Coinbase:
    txid:           bytes        # 32 bytes, internal LE order
    legacy_bytes:   bytes        # for txid + merkle
    witness_bytes:  bytes        # for block serialization


def build_coinbase(
    *,
    height: int,
    coinbase_value_sats: int,
    output_script: bytes,
    extranonce: bytes,
    witness_commitment_script: bytes | None = None,
    version: int = 2,
) -> Coinbase:
    """Build a Pricoin coinbase. `output_script` is the raw scriptPubKey
    bytes of the pool address (e.g. P2WPKH = 0x00 0x14 || 20-byte hash —
    obtain via pricoind's validateaddress and `bytes.fromhex(scriptPubKey)`).

    `witness_commitment_script` is the value of `default_witness_commitment`
    from getblocktemplate (already a fully-formed OP_RETURN script). Pass
    None only when the template has no witness commitment (no segwit txs
    AND segwit is not enforced — practically never on Pricoin).
    """
    if height < 1:
        raise ValueError("height must be >= 1")
    if coinbase_value_sats < 0:
        raise ValueError("coinbase_value_sats must be >= 0")
    if len(output_script) == 0:
        raise ValueError("output_script empty")

    # --- scriptSig: <BIP34 height push> <extranonce push> ---
    script_sig = push_script_num(height) + encode_pushdata(extranonce)

    # --- vin[0] (no witness section in legacy serialization) ---
    vin = (
        b"\x00" * 32                                 # prevout txid
        + b"\xff\xff\xff\xff"                        # prevout vout
        + encode_varint(len(script_sig)) + script_sig
        + b"\xff\xff\xff\xff"                        # sequence
    )

    # --- vouts ---
    vout0 = (
        coinbase_value_sats.to_bytes(8, "little")
        + encode_varint(len(output_script)) + output_script
    )
    vouts = [vout0]
    if witness_commitment_script:
        vout_wc = (
            (0).to_bytes(8, "little")
            + encode_varint(len(witness_commitment_script)) + witness_commitment_script
        )
        vouts.append(vout_wc)

    n_vouts = encode_varint(len(vouts))
    vouts_bytes = b"".join(vouts)

    # --- legacy serialization (txid input) ---
    legacy = (
        struct.pack("<i", version)
        + b"\x01"                # vin count = 1
        + vin
        + n_vouts
        + vouts_bytes
        + b"\x00\x00\x00\x00"    # nLockTime = 0
    )
    txid = sha256d(legacy)

    # --- witness serialization (BIP141 marker/flag + per-input witness) ---
    # vin[0] witness: 1 stack item, 32-byte reserved value.
    witness = b"\x01" + b"\x20" + WITNESS_RESERVED_VALUE
    witness_bytes = (
        struct.pack("<i", version)
        + b"\x00\x01"            # marker + flag
        + b"\x01"                # vin count = 1
        + vin
        + n_vouts
        + vouts_bytes
        + witness                # one witness blob per vin, in vin order
        + b"\x00\x00\x00\x00"
    )

    return Coinbase(txid=txid, legacy_bytes=legacy, witness_bytes=witness_bytes)


# ---------- merkle ----------

def merkle_root(txids: list[bytes]) -> bytes:
    """Bitcoin merkle root of `txids` (each 32 bytes, internal LE order).
    Odd levels duplicate the last element."""
    if not txids:
        return b"\x00" * 32
    nodes = list(txids)
    while len(nodes) > 1:
        if len(nodes) & 1:
            nodes.append(nodes[-1])
        nodes = [sha256d(nodes[i] + nodes[i + 1]) for i in range(0, len(nodes), 2)]
    return nodes[0]
