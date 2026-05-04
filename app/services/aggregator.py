"""Aggregate 1-minute candles into larger timeframes."""
from __future__ import annotations

from typing import Dict, List, Optional

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

    If the first 1m candle in a bucket doesn't start at the bucket boundary
    (= missing earlier candles due to no trades), the open price is carried
    forward from the previous bucket's close — matching how exchanges handle
    gaps in trading activity.

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
    prev_close: Optional[float] = None

    for bucket_start in sorted(buckets):
        group = sorted(buckets[bucket_start], key=lambda x: x.time)

        # If earliest 1m candle in this bucket doesn't start at the boundary,
        # carry forward the previous close as this bucket's open.
        first_candle = group[0]
        if prev_close is not None and first_candle.time > bucket_start:
            open_price = prev_close
        else:
            open_price = first_candle.open

        high = max(c.high for c in group)
        low = min(c.low for c in group)
        close = group[-1].close

        # Ensure high/low encompass the carried-forward open
        if open_price > high:
            high = open_price
        if open_price < low:
            low = open_price

        result.append(Candle(
            time=bucket_start,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=sum(c.volume for c in group),
            contract_volume=sum(c.contract_volume for c in group),
        ))

        prev_close = close

    return result
