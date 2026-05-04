from __future__ import annotations

import asyncio
import time
from typing import Optional

from app.config import Settings
from app.models import Candle
from app.services.aggregator import DERIVED_TIMEFRAMES, TIMEFRAME_SECONDS, aggregate_candles
from app.services.futures_client import GateFuturesClient
from app.services.signal_engine import SignalEngine
from app.services.webhook import WebhookSender
from app.state import MarketState
from app.ws_manager import WebSocketManager


def _current_period_start(timeframe: str) -> int:
    """Unix timestamp where the current (incomplete) candle started."""
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

    # ── Main loop ──────────────────────────────────────────────

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

                # 1초 휴식 후 바로 다음 사이클
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

        except asyncio.CancelledError:
            return
        except Exception as e:
            await self.state.add_error(f"scanner fatal error: {type(e).__name__}: {e}")

    # ── Bootstrap ──────────────────────────────────────────────

    async def _bootstrap(self) -> None:
        """Fetch full 2000 1m candles per symbol, then derive all timeframes."""
        await self.state.set_phase("bootstrap")
        symbols = list(self.state.symbols)
        bootstrap_limit = self.settings.candle_limit_for("1m")  # 2000

        for idx, symbol in enumerate(symbols, start=1):
            if self._stop_event.is_set():
                break
            await self._scan_symbol(symbol, fetch_limit=bootstrap_limit)
            if idx % 25 == 0:
                await self.state.set_phase(f"bootstrap_{idx}/{len(symbols)}")

    # ── Incremental scan ───────────────────────────────────────

    async def _scan_all(self) -> None:
        """Incremental scan: fetch recent 1m candles, re-derive all timeframes."""
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

    # ── Per-symbol work ────────────────────────────────────────

    async def _scan_symbol(self, symbol: str, fetch_limit: int) -> None:
        try:
            # 1) Fetch 1m candles from Gate API
            new_candles = await self.futures_client.fetch_candles(symbol, "1m", fetch_limit)
            if not new_candles:
                return

            # 2) Filter out the current incomplete 1m candle
            cutoff = _current_period_start("1m")
            new_candles = [c for c in new_candles if c.time < cutoff]
            if not new_candles:
                return

            # 3) Merge into 1m store & remember previous last candle
            prev_1m = await self.state.get_candles(symbol, "1m", limit=1)
            prev_last_1m = prev_1m[-1] if prev_1m else None

            merged_1m = await self.state.merge_candles(symbol, "1m", new_candles)

            # 4) 1m signals
            signals_1m = self.engine.calculate_signals(symbol, "1m", merged_1m)
            await self.state.set_signals_for_symbol_tf(symbol, "1m", signals_1m)

            # 5) Broadcast 1m candle update
            if merged_1m:
                latest_1m = merged_1m[-1]
                if self._candle_changed(prev_last_1m, latest_1m):
                    await self.ws_manager.broadcast_candle(symbol, "1m", latest_1m)

            # 6) Broadcast new 1m signal
            if signals_1m:
                await self._try_broadcast_signal(signals_1m[-1])

            # 7) Derive higher timeframes from 1m data
            for tf in DERIVED_TIMEFRAMES:
                period = TIMEFRAME_SECONDS[tf]
                aggregated = aggregate_candles(merged_1m, period)

                prev_tf = await self.state.get_candles(symbol, tf, limit=1)
                prev_last_tf = prev_tf[-1] if prev_tf else None

                await self.state.set_candles(symbol, tf, aggregated)

                signals_tf = self.engine.calculate_signals(symbol, tf, aggregated)
                await self.state.set_signals_for_symbol_tf(symbol, tf, signals_tf)

                # Broadcast derived candle update
                if aggregated:
                    latest_tf = aggregated[-1]
                    if self._candle_changed(prev_last_tf, latest_tf):
                        await self.ws_manager.broadcast_candle(symbol, tf, latest_tf)

                # Broadcast new derived signal
                if signals_tf:
                    await self._try_broadcast_signal(signals_tf[-1])

        except Exception as e:
            await self.state.add_error(f"{symbol}: {type(e).__name__}: {e}")

    async def _try_broadcast_signal(self, signal) -> None:
        added = await self.state.add_recent_signal_if_new(signal)
        if added:
            await self.ws_manager.broadcast_signal(signal)
            await self.webhook_sender.send_signal(signal)

    # ── Public helpers (for REST endpoints) ────────────────────

    async def scan_once(self) -> None:
        await self._scan_all()

    async def scan_timeframe(self, timeframe: str, full_bootstrap: bool = False) -> None:
        """Manual trigger for a specific timeframe (used by /api/scan/timeframe)."""
        if timeframe == "1m" or full_bootstrap:
            await self._bootstrap() if full_bootstrap else await self._scan_all()
        # Derived timeframes are automatically updated when 1m is scanned

    async def fetch_symbol_timeframe(self, symbol: str, timeframe: str, force_limit: Optional[int] = None) -> None:
        """On-demand fetch for a single symbol (e.g. when chart is opened)."""
        limit = force_limit or self.settings.candle_limit_for("1m")
        await self._scan_symbol(symbol.upper(), fetch_limit=limit)

    # ── Utilities ──────────────────────────────────────────────

    def _candle_changed(self, prev: Optional[Candle], current: Candle) -> bool:
        if prev is None:
            return True
        return (
            prev.time != current.time
            or prev.open != current.open
            or prev.high != current.high
            or prev.low != current.low
            or prev.close != current.close
            or prev.volume != current.volume
        )
