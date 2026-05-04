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

    Gate.io returns 1m candles even for minutes with no trades (v=0,
    OHLC = previous close). Their higher-timeframe candles use the open
    of the first 1m candle that actually has volume. We replicate this:
    - Open  = first 1m candle with volume > 0 in the bucket
    - If no volume at all, fall back to first candle's open (= prev close)
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

        # Find first candle with actual trades
        first_with_volume: Optional[Candle] = None
        for c in group:
            if c.volume > 0 or c.contract_volume > 0:
                first_with_volume = c
                break

        if first_with_volume is not None:
            open_price = first_with_volume.open
        else:
            # No trades in entire period — use first candle's open (= prev close)
            open_price = group[0].open

        # H/L only from candles with actual trades; fall back to open if none
        traded = [c for c in group if c.volume > 0 or c.contract_volume > 0]
        if traded:
            high = max(c.high for c in traded)
            low = min(c.low for c in traded)
        else:
            high = open_price
            low = open_price

        close = group[-1].close
        total_volume = sum(c.volume for c in group)
        total_contract_volume = sum(c.contract_volume for c in group)

        result.append(Candle(
            time=bucket_start,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=total_volume,
            contract_volume=total_contract_volume,
        ))

    return result
