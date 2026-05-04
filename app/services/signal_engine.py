"""
UT Bot Alerts — faithful Python port of the TradingView Pine Script.

Original Pine Script by @QuantNomad:
  - key_value (sensitivity): default 1
  - atr_period: default 10
  - ema_period (for trend filter): default 1 (effectively disabled)

Logic:
  1. xATR = atr(atr_period)
  2. nLoss = key_value * xATR
  3. Trailing stop (xATRTrailingStop):
     - If close > prev_stop AND prev_close > prev_stop:
         stop = max(prev_stop, close - nLoss)
     - If close < prev_stop AND prev_close < prev_stop:
         stop = min(prev_stop, close + nLoss)
     - If close > prev_stop:
         stop = close - nLoss
     - Else:
         stop = close + nLoss
  4. Position detection:
     - pos =  1 if close[1] < prev_stop AND close > stop  (cross above)
     - pos = -1 if close[1] > prev_stop AND close < stop  (cross below)
     - else keep previous pos
  5. Signal:
     - BUY  when pos == 1 AND prev_pos != 1  (AND close > ema if filter on)
     - SELL when pos == -1 AND prev_pos != -1 (AND close < ema if filter on)
"""
from __future__ import annotations

from typing import List, Optional

from app.models import Candle, Signal
from app.services.indicators import atr, ema


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

    def calculate_signals(
        self, symbol: str, timeframe: str, candles: List[Candle]
    ) -> List[Signal]:
        n = len(candles)
        if n < self.atr_period + 2:
            return []

        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]

        # ATR
        xATR = atr(highs, lows, closes, self.atr_period)

        # EMA filter (period=1 means just the close itself)
        ema_filter = ema(closes, self.ema_period) if self.ema_period > 1 else closes

        # Trailing stop calculation
        trail_stop: List[Optional[float]] = [None] * n
        pos: List[int] = [0] * n  # 1 = long, -1 = short

        signals: List[Signal] = []

        for i in range(1, n):
            if xATR[i] is None:
                continue

            nLoss = self.key_value * xATR[i]
            prev_stop = trail_stop[i - 1]

            # First valid bar
            if prev_stop is None:
                trail_stop[i] = closes[i] - nLoss
                pos[i] = 1 if closes[i] > trail_stop[i] else -1
                continue

            prev_close = closes[i - 1]

            # Trailing stop logic (matches Pine Script exactly)
            if closes[i] > prev_stop and prev_close > prev_stop:
                trail_stop[i] = max(prev_stop, closes[i] - nLoss)
            elif closes[i] < prev_stop and prev_close < prev_stop:
                trail_stop[i] = min(prev_stop, closes[i] + nLoss)
            elif closes[i] > prev_stop:
                trail_stop[i] = closes[i] - nLoss
            else:
                trail_stop[i] = closes[i] + nLoss

            # Position detection
            if prev_close < prev_stop and closes[i] > trail_stop[i]:
                pos[i] = 1
            elif prev_close > prev_stop and closes[i] < trail_stop[i]:
                pos[i] = -1
            else:
                pos[i] = pos[i - 1]

            # Signal generation
            prev_pos = pos[i - 1]

            # EMA filter value
            ema_val = ema_filter[i] if isinstance(ema_filter, list) and i < len(ema_filter) and ema_filter[i] is not None else closes[i]

            if pos[i] == 1 and prev_pos != 1:
                # BUY signal
                if self.ema_period <= 1 or closes[i] > ema_val:
                    score = self._score(xATR[i], closes[i], nLoss)
                    if score >= self.min_score:
                        c = candles[i]
                        signals.append(Signal(
                            id=f"{symbol}:{timeframe}:{c.time}:BUY",
                            symbol=symbol,
                            timeframe=timeframe,
                            time=c.time,
                            price=c.close,
                            type="BUY",
                            score=score,
                            reason=f"BUY: UT Bot · ATR {xATR[i]:.6g} · Stop {trail_stop[i]:.6g}",
                            marker_position="belowBar",
                            marker_shape="arrowUp",
                        ))

            elif pos[i] == -1 and prev_pos != -1:
                # SELL signal
                if self.ema_period <= 1 or closes[i] < ema_val:
                    score = self._score(xATR[i], closes[i], nLoss)
                    if score >= self.min_score:
                        c = candles[i]
                        signals.append(Signal(
                            id=f"{symbol}:{timeframe}:{c.time}:SELL",
                            symbol=symbol,
                            timeframe=timeframe,
                            time=c.time,
                            price=c.close,
                            type="SELL",
                            score=score,
                            reason=f"SELL: UT Bot · ATR {xATR[i]:.6g} · Stop {trail_stop[i]:.6g}",
                            marker_position="aboveBar",
                            marker_shape="arrowDown",
                        ))

        return signals

    def _score(self, atr_val: float, close: float, nLoss: float) -> int:
        """
        Score 0–100 based on how significant the move is relative to ATR.
        All UT Bot signals are structurally valid, so base score is high.
        """
        if close == 0 or atr_val == 0:
            return 70

        # ATR as % of price — larger ATR = more volatile = stronger signal
        atr_pct = (atr_val / close) * 100
        score = 70

        if atr_pct > 0.5:
            score += 5
        if atr_pct > 1.0:
            score += 5
        if atr_pct > 2.0:
            score += 5
        if atr_pct > 3.0:
            score += 5

        # nLoss distance as confirmation
        loss_pct = (nLoss / close) * 100
        if loss_pct > 1.0:
            score += 5
        if loss_pct > 2.0:
            score += 5

        return min(100, score)
