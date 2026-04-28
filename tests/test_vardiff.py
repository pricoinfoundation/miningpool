"""Vardiff retarget math."""
from pool.vardiff import Vardiff, VardiffConfig


CFG = VardiffConfig(
    target_share_seconds=10.0,
    retarget_after=4,        # short window for tests
    adjust_high=1.4,
    adjust_low=0.7,
    min_diff=1,
    max_diff=1 << 16,
)


def test_no_retarget_below_window():
    v = Vardiff(CFG, initial_diff=1024)
    assert v.record_share(0.0) is None
    assert v.record_share(10.0) is None
    assert v.record_share(20.0) is None
    # Hit the boundary; still no retarget because we've only seen 3
    # submits and retarget_after is 4.
    assert v.difficulty == 1024


def test_too_slow_halves_diff():
    """20 s between shares vs 10 s target → ratio 2.0 > adjust_high."""
    v = Vardiff(CFG, initial_diff=1024)
    for i in range(CFG.retarget_after - 1):
        v.record_share(i * 20.0)
    new = v.record_share((CFG.retarget_after - 1) * 20.0)
    assert new == 512
    assert v.difficulty == 512


def test_too_fast_doubles_diff():
    """3 s between shares → ratio 0.3 < adjust_low."""
    v = Vardiff(CFG, initial_diff=1024)
    for i in range(CFG.retarget_after - 1):
        v.record_share(i * 3.0)
    new = v.record_share((CFG.retarget_after - 1) * 3.0)
    assert new == 2048


def test_in_band_no_change():
    """11 s/share → ratio 1.1, inside [0.7, 1.4]."""
    v = Vardiff(CFG, initial_diff=1024)
    new_diffs = [v.record_share(i * 11.0) for i in range(CFG.retarget_after)]
    assert all(d is None for d in new_diffs)
    assert v.difficulty == 1024


def test_min_diff_clamp():
    v = Vardiff(CFG, initial_diff=2)
    for i in range(CFG.retarget_after - 1):
        v.record_share(i * 100.0)
    new = v.record_share((CFG.retarget_after - 1) * 100.0)
    assert new == 1                      # 2 // 2 = 1
    # next slow round shouldn't dip below floor.
    for i in range(CFG.retarget_after):
        v.record_share(1000 + i * 100.0)
    assert v.difficulty == 1


def test_max_diff_clamp():
    cfg = VardiffConfig(target_share_seconds=10.0, retarget_after=4,
                        adjust_high=1.4, adjust_low=0.7,
                        min_diff=1, max_diff=2048)
    v = Vardiff(cfg, initial_diff=1024)
    for i in range(cfg.retarget_after - 1):
        v.record_share(i * 1.0)
    new = v.record_share((cfg.retarget_after - 1) * 1.0)
    assert new == 2048
    # Already at max; further fast shares can't push past.
    for i in range(cfg.retarget_after):
        v.record_share(10000 + i * 1.0)
    assert v.difficulty == 2048
