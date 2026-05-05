"""
UT Bot Alerts — with incremental calculation cache.

On first call for a symbol/timeframe: full calculation.
On subsequent calls: only recalculate from where new candles start.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from app.models import Candle, Signal
from app.services.indicators import atr, ema


class _CacheEntry:
    __slots__ = ("candle_count", "trail_stop", "pos", "signals", "xATR", "ema_filter")

    def __init__(self):
        self.candle_count: int = 0
        self.trail_stop: List[Optional[float]] = []
        self.pos: List[int] = []
        self.signals: List[Signal] = []
        self.xATR: List[Optional[float]] = []
        self.ema_filter: list = []


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
        # Cache keyed by (symbol, timeframe)
        self._cache: Dict[Tuple[str, str], _CacheEntry] = {}

    def calculate_signals(
        self, symbol: str, timeframe: str, candles: List[Candle]
    ) -> List[Signal]:
        n = len(candles)
        if n < self.atr_period + 2:
            return []

        key = (symbol, timeframe)
        cache = self._cache.get(key)

        # Full recalc if no cache or candle count shrank (data reset)
        if cache is None or cache.candle_count > n:
            return self._full_calc(symbol, timeframe, candles)

        # If no new candles, return cached signals
        if cache.candle_count == n:
            return cache.signals

        # Incremental: recalculate only from where new data starts
        return self._incremental_calc(symbol, timeframe, candles, cache)

    def _full_calc(
        self, symbol: str, timeframe: str, candles: List[Candle]
    ) -> List[Signal]:
        n = len(candles)
        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]

        xATR = atr(highs, lows, closes, self.atr_period)
        ema_filter = ema(closes, self.ema_period) if self.ema_period > 1 else closes

        trail_stop: List[Optional[float]] = [None] * n
        pos: List[int] = [0] * n
        signals: List[Signal] = []

        for i in range(1, n):
            sig = self._step(i, candles, closes, xATR, ema_filter, trail_stop, pos, symbol, timeframe)
            if sig:
                signals.append(sig)

        # Save cache
        cache = _CacheEntry()
        cache.candle_count = n
        cache.trail_stop = trail_stop
        cache.pos = pos
        cache.signals = signals
        cache.xATR = xATR
        cache.ema_filter = ema_filter
        self._cache[(symbol, timeframe)] = cache

        return signals

    def _incremental_calc(
        self, symbol: str, timeframe: str, candles: List[Candle], cache: _CacheEntry
    ) -> List[Signal]:
        old_n = cache.candle_count
        n = len(candles)
        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]

        # Extend ATR incrementally (RMA)
        xATR = list(cache.xATR)
        prev_atr = xATR[old_n - 1] if old_n > 0 and xATR[old_n - 1] is not None else None
        for i in range(old_n, n):
            if i == 0:
                tr_val = highs[i] - lows[i]
            else:
                prev_close = closes[i - 1]
                tr_val = max(highs[i] - lows[i], abs(highs[i] - prev_close), abs(lows[i] - prev_close))

            if i < self.atr_period:
                xATR.append(None)
            elif i == self.atr_period - 1 and prev_atr is None:
                # Shouldn't happen in incremental, but safety
                xATR.append(None)
            elif prev_atr is not None:
                new_atr = (prev_atr * (self.atr_period - 1) + tr_val) / self.atr_period
                xATR.append(new_atr)
                prev_atr = new_atr
            else:
                xATR.append(None)

        # Extend EMA filter
        if self.ema_period > 1:
            ema_filter = list(cache.ema_filter)
            k = 2 / (self.ema_period + 1)
            prev_ema = ema_filter[old_n - 1] if old_n > 0 else None
            for i in range(old_n, n):
                if prev_ema is not None:
                    val = closes[i] * k + prev_ema * (1 - k)
                    ema_filter.append(val)
                    prev_ema = val
                else:
                    ema_filter.append(None)
        else:
            ema_filter = closes

        # Extend trail_stop and pos
        trail_stop = list(cache.trail_stop)
        pos = list(cache.pos)

        # Start from old_n - 1 to re-evaluate the last cached candle (might have updated)
        start = max(1, old_n - 1)

        # Remove signals from the re-evaluated region
        signals = [s for s in cache.signals if s.time < candles[start].time]

        # Extend arrays if needed
        while len(trail_stop) < n:
            trail_stop.append(None)
        while len(pos) < n:
            pos.append(0)

        for i in range(start, n):
            sig = self._step(i, candles, closes, xATR, ema_filter, trail_stop, pos, symbol, timeframe)
            if sig:
                signals.append(sig)

        # Update cache
        cache.candle_count = n
        cache.trail_stop = trail_stop
        cache.pos = pos
        cache.signals = signals
        cache.xATR = xATR
        cache.ema_filter = ema_filter

        return signals

    def _step(
        self, i: int, candles: List[Candle], closes: list,
        xATR: list, ema_filter: list,
        trail_stop: list, pos: list,
        symbol: str, timeframe: str,
    ) -> Optional[Signal]:
        if i < 1 or i >= len(closes):
            return None
        if i >= len(xATR) or xATR[i] is None:
            return None

        nLoss = self.key_value * xATR[i]
        prev_stop = trail_stop[i - 1]

        if prev_stop is None:
            trail_stop[i] = closes[i] - nLoss
            pos[i] = 1 if closes[i] > trail_stop[i] else -1
            return None

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

        prev_pos = pos[i - 1]
        ema_val = ema_filter[i] if i < len(ema_filter) and ema_filter[i] is not None else closes[i]

        c = candles[i]

        if pos[i] == 1 and prev_pos != 1:
            if self.ema_period <= 1 or closes[i] > ema_val:
                score = self._score(xATR[i], closes[i], nLoss)
                if score >= self.min_score:
                    return Signal(
                        id=f"{symbol}:{timeframe}:{c.time}:BUY",
                        symbol=symbol, timeframe=timeframe, time=c.time,
                        price=c.close, type="BUY", score=score,
                        reason=f"BUY: UT Bot · ATR {xATR[i]:.6g} · Stop {trail_stop[i]:.6g}",
                        marker_position="belowBar", marker_shape="arrowUp",
                    )

        elif pos[i] == -1 and prev_pos != -1:
            if self.ema_period <= 1 or closes[i] < ema_val:
                score = self._score(xATR[i], closes[i], nLoss)
                if score >= self.min_score:
                    return Signal(
                        id=f"{symbol}:{timeframe}:{c.time}:SELL",
                        symbol=symbol, timeframe=timeframe, time=c.time,
                        price=c.close, type="SELL", score=score,
                        reason=f"SELL: UT Bot · ATR {xATR[i]:.6g} · Stop {trail_stop[i]:.6g}",
                        marker_position="aboveBar", marker_shape="arrowDown",
                    )

        return None

    def _score(self, atr_val: float, close: float, nLoss: float) -> int:
        if close == 0 or atr_val == 0:
            return 70
        atr_pct = (atr_val / close) * 100
        loss_pct = (nLoss / close) * 100
        score = 70
        if atr_pct > 0.5: score += 5
        if atr_pct > 1.0: score += 5
        if atr_pct > 2.0: score += 5
        if atr_pct > 3.0: score += 5
        if loss_pct > 1.0: score += 5
        if loss_pct > 2.0: score += 5
        return min(100, score)
