from __future__ import annotations

import asyncio
import time
from typing import Dict, Optional, Tuple, List

from app.config import Settings
from app.models import Candle, Signal
from app.services.aggregator import (
    DERIVED_TIMEFRAMES, TIMEFRAME_SECONDS,
    aggregate_candles, aggregate_candles_incremental,
)
from app.services.futures_client import GateFuturesClient
from app.services.signal_engine import SignalEngine
from app.services.webhook import WebhookSender
from app.state import MarketState
from app.ws_manager import WebSocketManager


def _current_period_start(timeframe: str) -> int:
    now = int(time.time())
    period = TIMEFRAME_SECONDS.get(timeframe, 60)
    return (now // period) * period


class MarketScanner:
    def __init__(
        self,
        settings: Settings,
        futures_client: GateFuturesClient,
        state: MarketState,
        ws_manager: WebSocketManager,
        webhook_sender: WebhookSender,
    ):
        self.settings = settings
        self.futures_client = futures_client
        self.state = state
        self.ws_manager = ws_manager
        self.webhook_sender = webhook_sender
        self.engine = SignalEngine(min_score=settings.signal_min_score)

        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

        # Incremental aggregation cache: (symbol, tf) -> (prev_result, prev_1m_count)
        self._agg_cache: Dict[Tuple[str, str], Tuple[List[Candle], int]] = {}

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass

    async def run(self) -> None:
        try:
            await self.state.set_phase("loading_symbols")
            symbols = await self.futures_client.list_symbols()
            await self.state.set_symbols(symbols)

            if self.settings.bootstrap_on_start:
                await self._bootstrap()

            await self.state.set_phase("scheduled_scanning")

            while not self._stop_event.is_set():
                await self._scan_all()

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

        except asyncio.CancelledError:
            return
        except Exception as e:
            await self.state.add_error(f"scanner fatal error: {type(e).__name__}: {e}")

    async def _bootstrap(self) -> None:
        await self.state.set_phase("bootstrap")
        symbols = list(self.state.symbols)
        bootstrap_limit = self.settings.candle_limit_for("1m")

        for idx, symbol in enumerate(symbols, start=1):
            if self._stop_event.is_set():
                break
            await self._scan_symbol(symbol, fetch_limit=bootstrap_limit)
            if idx % 25 == 0:
                await self.state.set_phase(f"bootstrap_{idx}/{len(symbols)}")

    async def _scan_all(self) -> None:
        symbols = list(self.state.symbols)
        if not symbols:
            return

        async with self.state.lock:
            self.state.scanner_running = True
            self.state.last_scan_started_at = int(time.time())
            self.state.current_phase = "scan_1m"

        try:
            limit = self.settings.incremental_candle_limit
            for idx, symbol in enumerate(symbols, start=1):
                if self._stop_event.is_set():
                    break
                await self._scan_symbol(symbol, fetch_limit=limit)
                if idx % 25 == 0:
                    await self.state.set_phase(f"scan_1m_{idx}/{len(symbols)}")
        finally:
            async with self.state.lock:
                self.state.scanner_running = False
                self.state.last_scan_finished_at = int(time.time())
                self.state.current_phase = "idle"

    async def _scan_symbol(self, symbol: str, fetch_limit: int) -> None:
        try:
            # 1) Fetch 1m candles
            new_candles = await self.futures_client.fetch_candles(symbol, "1m", fetch_limit)
            if not new_candles:
                return

            # 2) Filter incomplete 1m candle
            cutoff = _current_period_start("1m")
            new_candles = [c for c in new_candles if c.time < cutoff]
            if not new_candles:
                return

            # 3) Merge 1m (sorting happens inside merge_candles)
            prev_last_1m = await self._get_last_candle(symbol, "1m")
            merged_1m = await self.state.merge_candles(symbol, "1m", new_candles)

            # 4) 1m signals (cached incrementally in engine)
            signals_1m = self.engine.calculate_signals(symbol, "1m", merged_1m)

            # 5) Derive higher timeframes — batch all updates
            derived_updates: List[Tuple[str, List[Candle], List[Signal]]] = []
            broadcast_candles: List[Tuple[str, Candle, Optional[Candle]]] = []
            all_signals: List[Signal] = list(signals_1m)

            for tf in DERIVED_TIMEFRAMES:
                period = TIMEFRAME_SECONDS[tf]
                cache_key = (symbol, tf)

                # Incremental aggregation
                prev_agg, prev_count = self._agg_cache.get(cache_key, ([], 0))
                aggregated = aggregate_candles_incremental(
                    prev_agg, merged_1m, period, prev_count,
                )
                self._agg_cache[cache_key] = (aggregated, len(merged_1m))

                signals_tf = self.engine.calculate_signals(symbol, tf, aggregated)
                derived_updates.append((tf, aggregated, signals_tf))
                all_signals.extend(signals_tf)

                # Track candle changes for broadcast
                prev_last_tf = prev_agg[-1] if prev_agg else None
                if aggregated:
                    broadcast_candles.append((tf, aggregated[-1], prev_last_tf))

            # 6) Batch state update — single lock for all derived timeframes
            await self.state.batch_update_symbol(symbol, derived_updates)

            # 7) Broadcast 1m candle
            if merged_1m:
                latest_1m = merged_1m[-1]
                if self._candle_changed(prev_last_1m, latest_1m):
                    await self.ws_manager.broadcast_candle(symbol, "1m", latest_1m)

            # 8) Broadcast derived candle updates
            for tf, latest, prev_last in broadcast_candles:
                if self._candle_changed(prev_last, latest):
                    await self.ws_manager.broadcast_candle(symbol, tf, latest)

            # 9) Collect all signals — batch add + broadcast newest per tf
            await self._collect_signals(all_signals)

        except Exception as e:
            await self.state.add_error(f"{symbol}: {type(e).__name__}: {e}")

    async def _get_last_candle(self, symbol: str, timeframe: str) -> Optional[Candle]:
        data = await self.state.get_candles(symbol, timeframe, limit=1)
        return data[-1] if data else None

    async def _collect_signals(self, signals: List) -> None:
        if not signals:
            return
        # Batch add all historical signals (single lock)
        if len(signals) > 1:
            await self.state.add_recent_signals_batch(signals[:-1])
        # Last signal: add + broadcast if new
        latest = signals[-1]
        added = await self.state.add_recent_signal_if_new(latest)
        if added:
            await self.ws_manager.broadcast_signal(latest)
            await self.webhook_sender.send_signal(latest)

    # Public helpers
    async def scan_once(self) -> None:
        await self._scan_all()

    async def scan_timeframe(self, timeframe: str, full_bootstrap: bool = False) -> None:
        if timeframe == "1m" or full_bootstrap:
            await self._bootstrap() if full_bootstrap else await self._scan_all()

    async def fetch_symbol_timeframe(self, symbol: str, timeframe: str, force_limit: Optional[int] = None) -> None:
        limit = force_limit or self.settings.candle_limit_for("1m")
        await self._scan_symbol(symbol.upper(), fetch_limit=limit)

    def _candle_changed(self, prev: Optional[Candle], current: Candle) -> bool:
        if prev is None:
            return True
        return (
            prev.time != current.time
            or prev.close != current.close
            or prev.high != current.high
            or prev.low != current.low
            or prev.volume != current.volume
        )
