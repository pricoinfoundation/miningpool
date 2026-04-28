"""Fast rxshare smoke tests — no pricoind required.

Covers:
- API contract: hash returns 32 bytes
- Determinism: same (seed, input) → same hash, twice over and across
  fresh handles
- Mode parity: light-mode and full-mem-mode produce the same hash for
  the same input + seed (full-mem is skipped if alloc fails — usually
  means hugepages aren't configured or RAM < 2 GB)
- prepare_next/swap path: rebuild dataset under a new seed, swap in,
  verify the hash output changes (different seeds → different hashes)
"""
import hashlib

import pytest

from rxshare import RxShare, RxShareError


SEED_A = hashlib.sha256(b"Pricoin RandomX fixed seed v1").digest()  # bootstrap seed
SEED_B = hashlib.sha256(b"alternate seed for swap test").digest()
INPUT_X = b"hello pricoin pool"
INPUT_Y = b"another input"


# ---------- light mode (always available) ----------

def test_light_returns_32_bytes():
    with RxShare(SEED_A, init_threads=2, full_mem=False) as rx:
        h = rx.hash(INPUT_X)
        assert isinstance(h, bytes)
        assert len(h) == 32


def test_light_deterministic():
    with RxShare(SEED_A, init_threads=2, full_mem=False) as rx:
        h1 = rx.hash(INPUT_X)
        h2 = rx.hash(INPUT_X)
        assert h1 == h2


def test_light_deterministic_across_handles():
    with RxShare(SEED_A, init_threads=2, full_mem=False) as rx_a:
        h_a = rx_a.hash(INPUT_X)
    with RxShare(SEED_A, init_threads=2, full_mem=False) as rx_b:
        h_b = rx_b.hash(INPUT_X)
    assert h_a == h_b


def test_light_different_input_different_hash():
    with RxShare(SEED_A, init_threads=2, full_mem=False) as rx:
        assert rx.hash(INPUT_X) != rx.hash(INPUT_Y)


def test_light_different_seed_different_hash():
    with RxShare(SEED_A, init_threads=2, full_mem=False) as rx_a, \
         RxShare(SEED_B, init_threads=2, full_mem=False) as rx_b:
        assert rx_a.hash(INPUT_X) != rx_b.hash(INPUT_X)


def test_light_swap_unsupported():
    with RxShare(SEED_A, init_threads=2, full_mem=False) as rx:
        with pytest.raises(RxShareError, match="not supported in light mode"):
            rx.prepare_next(SEED_B)
        with pytest.raises(RxShareError, match="not supported in light mode"):
            rx.swap()


def test_invalid_seed_length():
    with pytest.raises(ValueError, match="32 bytes"):
        RxShare(b"too short")


def test_close_idempotent():
    rx = RxShare(SEED_A, init_threads=2, full_mem=False)
    rx.close()
    rx.close()  # second close is a no-op
    with pytest.raises(RxShareError, match="closed"):
        rx.hash(INPUT_X)


# ---------- full-mem mode (skipped if alloc fails) ----------

def _try_full_mem(threads=4):
    """Best-effort full-mem ctor; returns RxShare or None."""
    try:
        return RxShare(SEED_A, init_threads=threads, full_mem=True)
    except RxShareError:
        return None


def test_fullmem_matches_light():
    """The whole point: hashes are identical regardless of mode."""
    rx_full = _try_full_mem()
    if rx_full is None:
        pytest.skip("full-mem alloc failed (RAM/hugepages); fine on tiny CI")
    try:
        with RxShare(SEED_A, init_threads=2, full_mem=False) as rx_light:
            for inp in (INPUT_X, INPUT_Y, b"", b"x" * 200):
                assert rx_full.hash(inp) == rx_light.hash(inp), \
                    f"full-mem and light disagree on {inp!r}"
    finally:
        rx_full.close()


def test_fullmem_prepare_then_swap():
    rx = _try_full_mem(threads=8)
    if rx is None:
        pytest.skip("full-mem alloc failed")
    try:
        # Hash under SEED_A.
        h_a = rx.hash(INPUT_X)
        # Build the next dataset under SEED_B in the background, then
        # swap it in. After swap the hash should change.
        rx.prepare_next(SEED_B, init_threads=8)
        rx.swap()
        h_b = rx.hash(INPUT_X)
        assert h_a != h_b, "swap had no effect — same hash under both seeds"

        # Cross-check: a fresh handle bound directly to SEED_B produces h_b.
        with RxShare(SEED_B, init_threads=2, full_mem=False) as rx_b:
            assert rx_b.hash(INPUT_X) == h_b
    finally:
        rx.close()


def test_fullmem_double_prepare_busy():
    rx = _try_full_mem(threads=4)
    if rx is None:
        pytest.skip("full-mem alloc failed")
    try:
        rx.prepare_next(SEED_B, init_threads=2)
        with pytest.raises(RxShareError, match="already in progress"):
            rx.prepare_next(SEED_B, init_threads=2)
        rx.swap()  # drain
    finally:
        rx.close()


def test_fullmem_swap_without_prepare():
    rx = _try_full_mem(threads=2)
    if rx is None:
        pytest.skip("full-mem alloc failed")
    try:
        with pytest.raises(RxShareError, match="no pending build"):
            rx.swap()
    finally:
        rx.close()
