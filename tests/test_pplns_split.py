"""PPLNS split math: rounding, fees, edge cases."""
import pytest

from pool.pplns import split_block_reward


def test_simple_two_workers_equal():
    rows = [(1, 100.0), (2, 100.0)]
    pay, fee = split_block_reward(rows, reward_sats=10_000, fee_pct=1.0)
    assert fee == 100
    assert pay == {1: 4_950, 2: 4_950}
    assert sum(pay.values()) + fee == 10_000


def test_largest_remainder_no_lost_satoshi():
    """3 workers splitting 7 sats among them — the floor pieces are 2,2,2
    with 1 sat leftover. That leftover must go to one of them."""
    rows = [(1, 1.0), (2, 1.0), (3, 1.0)]
    pay, fee = split_block_reward(rows, reward_sats=7, fee_pct=0.0)
    assert fee == 0
    assert sum(pay.values()) == 7
    assert sorted(pay.values()) == [2, 2, 3]


def test_zero_window_returns_empty():
    pay, fee = split_block_reward([], reward_sats=10_000, fee_pct=1.0)
    # Pool keeps the fee even with no shares; the rest is undistributed.
    assert pay == {}
    assert fee == 100


def test_proportional_to_difficulty():
    rows = [(1, 30.0), (2, 70.0)]
    pay, fee = split_block_reward(rows, reward_sats=1_000_000, fee_pct=0.0)
    assert fee == 0
    assert pay == {1: 300_000, 2: 700_000}
    assert sum(pay.values()) == 1_000_000


def test_repeats_aggregate_correctly():
    """Same worker contributing multiple shares aggregates to one entry."""
    rows = [(1, 50.0), (2, 25.0), (1, 50.0), (2, 75.0)]
    pay, fee = split_block_reward(rows, reward_sats=200, fee_pct=0.0)
    assert sum(pay.values()) == 200
    # Worker 1: 100/200 → 100; worker 2: 100/200 → 100.
    assert pay == {1: 100, 2: 100}


def test_invalid_fee_rejected():
    with pytest.raises(ValueError):
        split_block_reward([(1, 1.0)], reward_sats=10, fee_pct=100.0)
    with pytest.raises(ValueError):
        split_block_reward([(1, 1.0)], reward_sats=10, fee_pct=-0.1)


def test_reward_zero():
    rows = [(1, 1.0), (2, 1.0)]
    pay, fee = split_block_reward(rows, reward_sats=0, fee_pct=1.0)
    assert fee == 0
    assert sum(pay.values()) == 0
