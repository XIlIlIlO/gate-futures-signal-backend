"""Aggregate 1-minute candles into larger timeframes."""
from __future__ import annotations

from typing import Dict, List

from app.models import Candle

# Timeframe → seconds per period
TIMEFRAME_SECONDS: Dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}

DERIVED_TIMEFRAMES = ["3m", "5m", "15m", "30m", "1h"]


def aggregate_candles(candles_1m: List[Candle], period_seconds: int) -> List[Candle]:
    """
    Group completed 1m candles into buckets of `period_seconds` and
    produce one OHLCV candle per bucket.

    The latest bucket may be incomplete (fewer 1m candles than the full
    period) — this is intentional so the chart shows a live-updating bar.
    """
    if not candles_1m:
        return []

    buckets: Dict[int, List[Candle]] = {}
    for c in candles_1m:
        bucket_start = (c.time // period_seconds) * period_seconds
        buckets.setdefault(bucket_start, []).append(c)

    result: List[Candle] = []
    for bucket_start in sorted(buckets):
        group = sorted(buckets[bucket_start], key=lambda x: x.time)
        result.append(Candle(
            time=bucket_start,
            open=group[0].open,
            high=max(c.high for c in group),
            low=min(c.low for c in group),
            close=group[-1].close,
            volume=sum(c.volume for c in group),
            contract_volume=sum(c.contract_volume for c in group),
        ))

    return result
