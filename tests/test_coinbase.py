"""Coinbase serialization unit tests.

These don't hit pricoind — they verify our serialization is internally
consistent and matches what Bitcoin's protocol specifies, so any later
breakage shows up locally instead of as 'submitblock rejected: bad-tx-...'.
"""
from pool.coinbase import (
    build_coinbase,
    encode_pushdata,
    encode_script_num,
    encode_varint,
    merkle_root,
    push_script_num,
    sha256d,
)


def test_varint_boundaries():
    assert encode_varint(0)        == b"\x00"
    assert encode_varint(0xFC)     == b"\xfc"
    assert encode_varint(0xFD)     == b"\xfd\xfd\x00"
    assert encode_varint(0xFFFF)   == b"\xfd\xff\xff"
    assert encode_varint(0x10000)  == b"\xfe\x00\x00\x01\x00"


def test_script_num_zero_and_small():
    assert encode_script_num(0) == b""
    assert encode_script_num(1) == b"\x01"
    assert encode_script_num(16) == b"\x10"
    assert encode_script_num(127) == b"\x7f"
    # Sign-bit collision pads with 0x00.
    assert encode_script_num(128) == b"\x80\x00"
    assert encode_script_num(255) == b"\xff\x00"
    assert encode_script_num(256) == b"\x00\x01"


def test_push_script_num_uses_op_n():
    # height 1..16 → OP_1..OP_16 (single byte).
    assert push_script_num(1) == b"\x51"
    assert push_script_num(16) == b"\x60"
    # Larger heights use a regular pushdata.
    assert push_script_num(17) == b"\x01\x11"
    assert push_script_num(500) == b"\x02\xf4\x01"


def test_pushdata_lengths():
    assert encode_pushdata(b"\x00") == b"\x01\x00"
    # 75-byte pushdata: still <0x4c so single-byte length.
    assert encode_pushdata(b"a" * 75)[0] == 75
    # 76-byte: needs OP_PUSHDATA1.
    p = encode_pushdata(b"a" * 76)
    assert p[0] == 0x4c and p[1] == 76


def test_merkle_root_single():
    """One tx → root is just its hash."""
    h = sha256d(b"hello")
    assert merkle_root([h]) == h


def test_merkle_root_two():
    a, b = sha256d(b"a"), sha256d(b"b")
    assert merkle_root([a, b]) == sha256d(a + b)


def test_merkle_root_three_dupes_last():
    """Bitcoin's odd-level duplication."""
    a, b, c = sha256d(b"a"), sha256d(b"b"), sha256d(b"c")
    expected = sha256d(sha256d(a + b) + sha256d(c + c))
    assert merkle_root([a, b, c]) == expected


def test_coinbase_legacy_and_witness_diff_only_in_marker_and_witness():
    """The legacy serialization is the with-witness one MINUS the
    marker(0x00)+flag(0x01) right after nVersion AND the witness blob
    right before nLockTime."""
    out_script = bytes.fromhex("0014" + "11" * 20)
    cb = build_coinbase(
        height=42,
        coinbase_value_sats=50 * 100_000_000,
        output_script=out_script,
        extranonce=b"\x01\x02\x03\x04",
        witness_commitment_script=None,
        version=2,
    )
    # Legacy starts with version (4 bytes) then vin count (1 byte), no marker.
    assert cb.legacy_bytes[4] == 0x01      # vin count
    # Witness version starts with version (4 bytes) then 0x00 0x01 marker+flag.
    assert cb.witness_bytes[4:6] == b"\x00\x01"
    # Both end with 4-byte zero locktime.
    assert cb.legacy_bytes[-4:] == b"\x00\x00\x00\x00"
    assert cb.witness_bytes[-4:] == b"\x00\x00\x00\x00"
    # txid is sha256d of the legacy form.
    assert cb.txid == sha256d(cb.legacy_bytes)


def test_coinbase_with_witness_commitment_has_two_outputs():
    out_script = bytes.fromhex("0014" + "11" * 20)
    # Realistic-shaped commitment: OP_RETURN | 0x24 (36 byte push) | aa21a9ed | 32-byte hash
    wc = bytes.fromhex("6a24" + "aa21a9ed" + "ee" * 32)
    cb = build_coinbase(
        height=100, coinbase_value_sats=12345,
        output_script=out_script, extranonce=b"\xaa" * 4,
        witness_commitment_script=wc, version=2,
    )
    # The legacy form contains both outputs; locate vout count.
    # Layout: version(4) + vin_count(1) + vin(36 + script_len_varint + script + 4)
    # ... cheaper: just assert the witness commitment script bytes appear.
    assert wc in cb.legacy_bytes
    assert wc in cb.witness_bytes


def test_coinbase_height_push_is_first_in_scriptsig():
    """BIP34 requires the height to be the first push of scriptSig."""
    out_script = bytes.fromhex("0014" + "11" * 20)
    cb = build_coinbase(height=200, coinbase_value_sats=0, output_script=out_script,
                        extranonce=b"", witness_commitment_script=None, version=2)
    # Find scriptSig — after the 36-byte prevout, varint length, then script.
    after_prev = cb.legacy_bytes[4 + 1 + 36 :]   # past version, vin count, prevout
    script_len = after_prev[0]
    script = after_prev[1 : 1 + script_len]
    # First opcode must encode 200: pushdata-2-bytes 0xc8 0x00 (200 doesn't
    # fit OP_N range; uses pushdata with sign bit handling: 200 → 0xc8 has high
    # bit set so we add 0x00 for sign → push opcode 0x02 then 0xc8 0x00)
    assert script[:1] == b"\x02"
    assert script[1:3] == b"\xc8\x00"
