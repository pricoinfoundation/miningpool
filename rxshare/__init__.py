"""Pythonic wrapper around librxshare.so.

Usage::

    from rxshare import RxShare

    seed = bytes.fromhex("00" * 32)         # raw seed_hash; pricoind format
    with RxShare(seed, init_threads=8) as rx:
        digest = rx.hash(header_bytes)      # 32-byte RandomX hash
        # ... later, when the next epoch is approaching:
        rx.prepare_next(next_seed)          # background dataset build
        rx.swap()                           # blocks until pending is ready

The cache-key transform applied to ``seed`` matches pricoind's
``SeedKeyFromHash`` byte-for-byte, so a hash computed here equals
the chain's ``GetPoWHashOfBytes`` result for the same input + seed.
"""
from __future__ import annotations

import ctypes
import os
import pathlib
import threading


_HERE = pathlib.Path(__file__).resolve().parent

# ----- error codes (must match rxshare.h) ------------------------------

RXSHARE_OK              =  0
RXSHARE_ERR_NULL        = -1
RXSHARE_ERR_ALLOC       = -2
RXSHARE_ERR_INIT        = -3
RXSHARE_ERR_BUSY        = -4
RXSHARE_ERR_NOT_READY   = -5
RXSHARE_ERR_NO_SWAP     = -6

_ERR_STR = {
    RXSHARE_ERR_NULL:      "null pointer",
    RXSHARE_ERR_ALLOC:     "allocation failed",
    RXSHARE_ERR_INIT:      "RandomX init failed (check hugepages / RAM)",
    RXSHARE_ERR_BUSY:      "a background build is already in progress",
    RXSHARE_ERR_NOT_READY: "no pending build to swap in",
    RXSHARE_ERR_NO_SWAP:   "swap not supported in light mode",
}


class RxShareError(RuntimeError):
    pass


# ----- locate + load librxshare.so -------------------------------------

def _find_lib() -> str:
    candidates = [
        os.environ.get("RXSHARE_LIB"),
        str(_HERE / "librxshare.so"),
        str(_HERE.parent / "rxshare" / "librxshare.so"),
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    raise RxShareError(
        "librxshare.so not found; build it with `make -C rxshare` "
        "or set RXSHARE_LIB to the path"
    )


_lib = ctypes.CDLL(_find_lib())

_lib.rxshare_create.restype  = ctypes.c_void_p
_lib.rxshare_create.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_int]

_lib.rxshare_hash.restype    = ctypes.c_int
_lib.rxshare_hash.argtypes   = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]

_lib.rxshare_prepare.restype  = ctypes.c_int
_lib.rxshare_prepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int]

_lib.rxshare_swap.restype    = ctypes.c_int
_lib.rxshare_swap.argtypes   = [ctypes.c_void_p]

_lib.rxshare_destroy.restype  = None
_lib.rxshare_destroy.argtypes = [ctypes.c_void_p]


# ----- pythonic façade -------------------------------------------------

class RxShare:
    """Hardware-mode RandomX, double-buffered for epoch rotation.

    Thread-safe: the underlying C handle serializes hash() and swap()
    on its own mutex, so multiple Python threads can call ``hash()`` on
    the same instance — though throughput won't scale until we have a
    VM-pool extension. For now: one RxShare per verifier thread if you
    need parallelism, or hash sequentially.
    """

    def __init__(self, seed_hash: bytes, init_threads: int = 8, full_mem: bool = True):
        if len(seed_hash) != 32:
            raise ValueError("seed_hash must be 32 bytes")
        if init_threads < 1:
            raise ValueError("init_threads must be >= 1")
        h = _lib.rxshare_create(seed_hash, init_threads, 1 if full_mem else 0)
        if not h:
            raise RxShareError(
                "rxshare_create failed — "
                + ("not enough RAM for FULL_MEM (~2 GB)?" if full_mem
                   else "RandomX init failed")
            )
        self._h = h
        self._full_mem = full_mem
        self._lock = threading.Lock()  # python-side guard against double-destroy

    def hash(self, data: bytes) -> bytes:
        if self._h is None:
            raise RxShareError("RxShare is closed")
        out = ctypes.create_string_buffer(32)
        rc = _lib.rxshare_hash(self._h, data, len(data), out)
        if rc != RXSHARE_OK:
            raise RxShareError(f"rxshare_hash: {_ERR_STR.get(rc, rc)}")
        return out.raw

    def prepare_next(self, next_seed: bytes, init_threads: int = 0) -> None:
        if self._h is None:
            raise RxShareError("RxShare is closed")
        if len(next_seed) != 32:
            raise ValueError("next_seed must be 32 bytes")
        rc = _lib.rxshare_prepare(self._h, next_seed, init_threads)
        if rc != RXSHARE_OK:
            raise RxShareError(f"rxshare_prepare: {_ERR_STR.get(rc, rc)}")

    def swap(self) -> None:
        if self._h is None:
            raise RxShareError("RxShare is closed")
        rc = _lib.rxshare_swap(self._h)
        if rc != RXSHARE_OK:
            raise RxShareError(f"rxshare_swap: {_ERR_STR.get(rc, rc)}")

    def close(self) -> None:
        with self._lock:
            if self._h is not None:
                _lib.rxshare_destroy(self._h)
                self._h = None

    def __enter__(self) -> "RxShare":
        return self

    def __exit__(self, *_a) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


__all__ = ["RxShare", "RxShareError"]
