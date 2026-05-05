"""
UT Bot Alerts — with trail_stop/pos cache for incremental updates.

Full ATR is recalculated each call (fast with numpy-free RMA).
Only trail_stop and pos arrays are cached and extended incrementally.
Signals are always regenerated from the full trail_stop/pos arrays.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from app.models import Candle, Signal
from app.services.indicators import atr


class _CacheEntry:
    __slots__ = ("candle_count", "last_candle_time", "trail_stop", "pos")

    def __init__(self):
        self.candle_count: int = 0
        self.last_candle_time: int = 0
        self.trail_stop: List[Optional[float]] = []
        self.pos: List[int] = []


class SignalEngine:
    def __init__(
        self,
        min_score: int = 70,
        key_value: float = 1.0,
        atr_period: int = 10,
        ema_period: int = 1,
    ):
        self.min_score = min_score
        self.key_value = key_value
        self.atr_period = atr_period
        self.ema_period = ema_period
        self._cache: Dict[Tuple[str, str], _CacheEntry] = {}

    def calculate_signals(
        self, symbol: str, timeframe: str, candles: List[Candle]
    ) -> List[Signal]:
        n = len(candles)
        if n < self.atr_period + 2:
            return []

        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]

        # ATR is always fully recalculated (fast O(n) with RMA)
        xATR = atr(highs, lows, closes, self.atr_period)

        key = (symbol, timeframe)
        cache = self._cache.get(key)

        # Determine where to start trail_stop/pos calculation
        if cache and cache.candle_count <= n and cache.last_candle_time == candles[cache.candle_count - 1].time if cache.candle_count > 0 else False:
            # Cache is valid — extend from where we left off
            trail_stop = cache.trail_stop
            pos = cache.pos
            start = cache.candle_count
        else:
            # No cache or invalid — full calculation
            trail_stop = [None] * n
            pos = [0] * n
            start = 1

        # Extend arrays if needed
        while len(trail_stop) < n:
            trail_stop.append(None)
        while len(pos) < n:
            pos.append(0)

        # Calculate trail_stop and pos from start
        for i in range(start, n):
            if xATR[i] is None:
                continue

            nLoss = self.key_value * xATR[i]
            prev_stop = trail_stop[i - 1]

            if prev_stop is None:
                trail_stop[i] = closes[i] - nLoss
                pos[i] = 1 if closes[i] > trail_stop[i] else -1
                continue

            prev_close = closes[i - 1]

            if closes[i] > prev_stop and prev_close > prev_stop:
                trail_stop[i] = max(prev_stop, closes[i] - nLoss)
            elif closes[i] < prev_stop and prev_close < prev_stop:
                trail_stop[i] = min(prev_stop, closes[i] + nLoss)
            elif closes[i] > prev_stop:
                trail_stop[i] = closes[i] - nLoss
            else:
                trail_stop[i] = closes[i] + nLoss

            if prev_close < prev_stop and closes[i] > trail_stop[i]:
                pos[i] = 1
            elif prev_close > prev_stop and closes[i] < trail_stop[i]:
                pos[i] = -1
            else:
                pos[i] = pos[i - 1]

        # Update cache
        entry = _CacheEntry()
        entry.candle_count = n
        entry.last_candle_time = candles[n - 1].time
        entry.trail_stop = trail_stop
        entry.pos = pos
        self._cache[key] = entry

        # Generate signals from full arrays (fast scan)
        signals: List[Signal] = []
        for i in range(1, n):
            if trail_stop[i] is None or xATR[i] is None:
                continue

            prev_pos = pos[i - 1]
            c = candles[i]

            if pos[i] == 1 and prev_pos != 1:
                score = self._score(xATR[i], closes[i])
                if score >= self.min_score:
                    signals.append(Signal(
                        id=f"{symbol}:{timeframe}:{c.time}:BUY",
                        symbol=symbol, timeframe=timeframe, time=c.time,
                        price=c.close, type="BUY", score=score,
                        reason=f"BUY: UT Bot · ATR {xATR[i]:.6g} · Stop {trail_stop[i]:.6g}",
                        marker_position="belowBar", marker_shape="arrowUp",
                    ))
            elif pos[i] == -1 and prev_pos != -1:
                score = self._score(xATR[i], closes[i])
                if score >= self.min_score:
                    signals.append(Signal(
                        id=f"{symbol}:{timeframe}:{c.time}:SELL",
                        symbol=symbol, timeframe=timeframe, time=c.time,
                        price=c.close, type="SELL", score=score,
                        reason=f"SELL: UT Bot · ATR {xATR[i]:.6g} · Stop {trail_stop[i]:.6g}",
                        marker_position="aboveBar", marker_shape="arrowDown",
                    ))

        return signals

    def _score(self, atr_val: float, close: float) -> int:
        if close == 0 or atr_val == 0:
            return 70
        atr_pct = (atr_val / close) * 100
        score = 70
        if atr_pct > 0.5: score += 5
        if atr_pct > 1.0: score += 5
        if atr_pct > 2.0: score += 5
        if atr_pct > 3.0: score += 5
        nLoss_pct = (self.key_value * atr_val / close) * 100
        if nLoss_pct > 1.0: score += 5
        if nLoss_pct > 2.0: score += 5
        return min(100, score)
