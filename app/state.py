from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Tuple

from app.config import Settings
from app.models import Candle, Signal


class MarketState:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.lock = asyncio.Lock()

        self.symbols: List[str] = []

        # candles[symbol][timeframe] = [Candle, ...] (sorted by time)
        self.candles: Dict[str, Dict[str, List[Candle]]] = defaultdict(dict)

        # signals_by_symbol_tf[(symbol, timeframe)] = [Signal, ...]
        self.signals_by_symbol_tf: Dict[Tuple[str, str], List[Signal]] = defaultdict(list)

        self.recent_signals: Deque[Signal] = deque(maxlen=settings.max_recent_signals)

        self.emitted_signal_ids: Deque[str] = deque(maxlen=10000)
        self._emitted_signal_id_set: set[str] = set()

        self.last_scan_started_at: Optional[int] = None
        self.last_scan_finished_at: Optional[int] = None
        self.scanner_running: bool = False
        self.current_phase: str = ""
        self.errors: Deque[str] = deque(maxlen=50)

    async def set_symbols(self, symbols: List[str]) -> None:
        async with self.lock:
            self.symbols = symbols

    async def set_candles(self, symbol: str, timeframe: str, candles: List[Candle]) -> None:
        """Set candles directly (already sorted)."""
        limit = self.settings.candle_limit_for(timeframe)
        async with self.lock:
            self.candles[symbol][timeframe] = candles[-limit:]

    async def merge_candles(self, symbol: str, timeframe: str, new_candles: List[Candle]) -> List[Candle]:
        """Merge new candles into existing (both assumed sorted by time)."""
        limit = self.settings.candle_limit_for(timeframe)

        async with self.lock:
            existing = self.candles.get(symbol, {}).get(timeframe, [])
            if not existing:
                merged = new_candles[-limit:]
                self.candles[symbol][timeframe] = merged
                return list(merged)

            # Fast merge: existing is sorted, new_candles is sorted
            by_time = {c.time: c for c in existing}
            for c in new_candles:
                by_time[c.time] = c

            merged = sorted(by_time.values(), key=lambda x: x.time)[-limit:]
            self.candles[symbol][timeframe] = merged
            return list(merged)

    async def get_candles_count(self, symbol: str, timeframe: str) -> int:
        """Get candle count without copying data."""
        async with self.lock:
            data = self.candles.get(symbol, {}).get(timeframe, [])
            return len(data)

    async def set_signals_for_symbol_tf(self, symbol: str, timeframe: str, signals: List[Signal]) -> None:
        async with self.lock:
            self.signals_by_symbol_tf[(symbol, timeframe)] = signals

    async def add_recent_signal_if_new(self, signal: Signal) -> bool:
        async with self.lock:
            if signal.id in self._emitted_signal_id_set:
                return False

            self.recent_signals.appendleft(signal)
            self.emitted_signal_ids.append(signal.id)
            self._emitted_signal_id_set.add(signal.id)

            while len(self._emitted_signal_id_set) > self.emitted_signal_ids.maxlen:
                old = self.emitted_signal_ids.popleft()
                self._emitted_signal_id_set.discard(old)

            return True

    async def add_recent_signals_batch(self, signals: List[Signal]) -> None:
        """Add multiple signals at once with a single lock acquisition."""
        async with self.lock:
            for signal in signals:
                if signal.id in self._emitted_signal_id_set:
                    continue
                self.recent_signals.appendleft(signal)
                self.emitted_signal_ids.append(signal.id)
                self._emitted_signal_id_set.add(signal.id)

                while len(self._emitted_signal_id_set) > self.emitted_signal_ids.maxlen:
                    old = self.emitted_signal_ids.popleft()
                    self._emitted_signal_id_set.discard(old)

    async def batch_update_symbol(
        self,
        symbol: str,
        updates: List[Tuple[str, List[Candle], List[Signal]]],
    ) -> None:
        """
        Batch update multiple timeframes for one symbol in a single lock.
        updates: list of (timeframe, candles, signals)
        """
        async with self.lock:
            for timeframe, candles, signals in updates:
                limit = self.settings.candle_limit_for(timeframe)
                self.candles[symbol][timeframe] = candles[-limit:]
                self.signals_by_symbol_tf[(symbol, timeframe)] = signals

    async def get_candles(self, symbol: str, timeframe: str, limit: int) -> List[Candle]:
        async with self.lock:
            data = self.candles.get(symbol, {}).get(timeframe, [])
            return data[-limit:]

    async def get_signals(self, symbol: str, timeframe: str, limit: int = 200) -> List[Signal]:
        async with self.lock:
            data = self.signals_by_symbol_tf.get((symbol, timeframe), [])
            return data[-limit:]

    async def get_recent_signals(self, timeframe: str = "all", limit: int = 100) -> List[Signal]:
        async with self.lock:
            data = list(self.recent_signals)
            if timeframe != "all":
                data = [s for s in data if s.timeframe == timeframe]
            return data[:limit]

    async def add_error(self, msg: str) -> None:
        async with self.lock:
            self.errors.appendleft(msg)

    async def set_phase(self, phase: str) -> None:
        async with self.lock:
            self.current_phase = phase

    async def snapshot_status(self) -> dict:
        async with self.lock:
            n = len(self.symbols)
            scan_interval = self.settings.scan_interval_seconds
            estimated_rps = n / max(1, scan_interval)

            return {
                "ok": True,
                "market": "gate_usdt_perpetual_futures",
                "settle": self.settings.gate_settle,
                "symbols_loaded": n,
                "timeframes": self.settings.timeframe_list,
                "recent_signals": len(self.recent_signals),
                "public_rps_limit": self.settings.public_rps_limit,
                "estimated_request_rate_per_sec": round(estimated_rps, 4),
                "estimated_request_rate_per_10s": round(estimated_rps * 10, 2),
                "last_scan_started_at": self.last_scan_started_at,
                "last_scan_finished_at": self.last_scan_finished_at,
                "scanner_running": self.scanner_running,
                "current_phase": self.current_phase,
                "errors": list(self.errors),
            }
