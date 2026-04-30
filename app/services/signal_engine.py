from __future__ import annotations

from typing import List, Optional

from app.models import Candle, Signal
from app.services.indicators import ema, rsi, sma


class SignalEngine:
    def __init__(self, min_score: int = 70):
        self.min_score = min_score

    def calculate_signals(self, symbol: str, timeframe: str, candles: List[Candle]) -> List[Signal]:
        if len(candles) < 60:
            return []

        closes = [c.close for c in candles]
        volumes = [c.volume for c in candles]

        ema_fast = ema(closes, 20)
        ema_slow = ema(closes, 50)
        rsi14 = rsi(closes, 14)
        vol_sma20 = sma(volumes, 20)

        signals: List[Signal] = []

        for i in range(51, len(candles)):
            sig = self._signal_at(
                symbol=symbol,
                timeframe=timeframe,
                candles=candles,
                i=i,
                ema_fast=ema_fast,
                ema_slow=ema_slow,
                rsi14=rsi14,
                vol_sma20=vol_sma20,
            )
            if sig:
                signals.append(sig)

        return signals

    def _signal_at(
        self,
        symbol: str,
        timeframe: str,
        candles: List[Candle],
        i: int,
        ema_fast: list,
        ema_slow: list,
        rsi14: list,
        vol_sma20: list,
    ) -> Optional[Signal]:
        c = candles[i]
        prev = candles[i - 1]

        ef = ema_fast[i]
        es = ema_slow[i]
        prev_ef = ema_fast[i - 1]
        prev_es = ema_slow[i - 1]
        rv = rsi14[i]
        vavg = vol_sma20[i]

        if None in (ef, es, prev_ef, prev_es, rv, vavg):
            return None

        vol_ratio = c.volume / vavg if vavg and vavg > 0 else 1.0

        cross_up = prev_ef <= prev_es and ef > es
        cross_down = prev_ef >= prev_es and ef < es

        bullish_break = (
            c.close > ef
            and prev.close <= prev_ef
            and rv >= 52
            and vol_ratio >= 1.15
        )

        bearish_break = (
            c.close < ef
            and prev.close >= prev_ef
            and rv <= 48
            and vol_ratio >= 1.15
        )

        if cross_up or bullish_break:
            score = self._buy_score(c.close, ef, es, rv, vol_ratio, cross_up)
            if score >= self.min_score:
                reason = self._reason("BUY", cross_up, bullish_break, rv, vol_ratio)
                return Signal(
                    id=f"{symbol}:{timeframe}:{c.time}:BUY",
                    symbol=symbol,
                    timeframe=timeframe,
                    time=c.time,
                    price=c.close,
                    type="BUY",
                    score=score,
                    reason=reason,
                    marker_position="belowBar",
                    marker_shape="arrowUp",
                )

        if cross_down or bearish_break:
            score = self._sell_score(c.close, ef, es, rv, vol_ratio, cross_down)
            if score >= self.min_score:
                reason = self._reason("SELL", cross_down, bearish_break, rv, vol_ratio)
                return Signal(
                    id=f"{symbol}:{timeframe}:{c.time}:SELL",
                    symbol=symbol,
                    timeframe=timeframe,
                    time=c.time,
                    price=c.close,
                    type="SELL",
                    score=score,
                    reason=reason,
                    marker_position="aboveBar",
                    marker_shape="arrowDown",
                )

        return None

    def _buy_score(self, close: float, ef: float, es: float, rsi_value: float, vol_ratio: float, cross: bool) -> int:
        score = 50
        if close > ef:
            score += 8
        if ef > es:
            score += 10
        if cross:
            score += 12
        if 52 <= rsi_value <= 72:
            score += 12
        elif rsi_value > 72:
            score += 4
        if vol_ratio >= 1.15:
            score += min(18, int((vol_ratio - 1.0) * 20))
        return max(0, min(100, score))

    def _sell_score(self, close: float, ef: float, es: float, rsi_value: float, vol_ratio: float, cross: bool) -> int:
        score = 50
        if close < ef:
            score += 8
        if ef < es:
            score += 10
        if cross:
            score += 12
        if 28 <= rsi_value <= 48:
            score += 12
        elif rsi_value < 28:
            score += 4
        if vol_ratio >= 1.15:
            score += min(18, int((vol_ratio - 1.0) * 20))
        return max(0, min(100, score))

    def _reason(self, side: str, cross: bool, break_signal: bool, rsi_value: float, vol_ratio: float) -> str:
        parts = []
        if cross:
            parts.append("EMA20/EMA50 cross")
        if break_signal:
            parts.append("EMA20 breakout")
        parts.append(f"RSI {rsi_value:.1f}")
        parts.append(f"Volume x{vol_ratio:.2f}")
        return f"{side}: " + " · ".join(parts)
