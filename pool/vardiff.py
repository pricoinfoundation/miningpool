"""Per-connection variable difficulty.

A `Vardiff` instance lives on each stratum connection. Every accepted
share is fed in via `record_share`; once we've seen `retarget_after`
shares, we look at the average inter-share interval and adjust:

    observed/target > adjust_high   →  diff /= 2  (miner is slow)
    observed/target < adjust_low    →  diff *= 2  (miner is fast)

Diff is clamped to [min_diff, max_diff]. The new diff (if any) is
returned from `record_share` so the stratum loop can push a
mining.set_difficulty + a fresh job to the miner.

Pure-data; no I/O, easy to unit-test.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class VardiffConfig:
    target_share_seconds: float = 10.0
    retarget_after: int = 16
    adjust_high: float = 1.4
    adjust_low: float = 0.7
    min_diff: int = 1
    max_diff: int = 1 << 32


class Vardiff:
    def __init__(self, cfg: VardiffConfig, initial_diff: int = 1024):
        self._cfg = cfg
        self._diff = max(cfg.min_diff, min(cfg.max_diff, initial_diff))
        self._submits = 0
        # Each retarget window collects exactly retarget_after timestamps,
        # giving (retarget_after - 1) gaps to average. The deque is fully
        # cleared on retarget so an off-window pause never poisons the
        # next measurement.
        self._times: deque[float] = deque(maxlen=cfg.retarget_after)

    @property
    def difficulty(self) -> int:
        return self._diff

    def record_share(self, ts: float) -> int | None:
        """Record an accepted share. Returns the new difficulty if a
        retarget happened on this share, else None."""
        self._times.append(ts)
        self._submits += 1
        if self._submits < self._cfg.retarget_after:
            return None
        # Reset the counter even if the buffer hasn't filled — first retarget.
        if len(self._times) < 2:
            return None

        observed = (self._times[-1] - self._times[0]) / (len(self._times) - 1)
        target   = self._cfg.target_share_seconds
        ratio    = observed / target if target > 0 else 1.0

        new_diff = self._diff
        if ratio > self._cfg.adjust_high:
            new_diff = max(self._cfg.min_diff, self._diff // 2)
        elif ratio < self._cfg.adjust_low:
            new_diff = min(self._cfg.max_diff, self._diff * 2)

        self._submits = 0
        # Fresh window — see comment on self._times.
        self._times.clear()
        if new_diff != self._diff:
            self._diff = new_diff
            return new_diff
        return None
