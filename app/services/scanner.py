from __future__ import annotations

import asyncio
import time
from typing import List, Optional

from app.config import Settings
from app.models import Candle
from app.services.futures_client import GateFuturesClient
from app.services.signal_engine import SignalEngine
from app.services.webhook import WebhookSender
from app.state import MarketState
from app.ws_manager import WebSocketManager


def _current_period_start(timeframe: str) -> int:
    """Return the unix timestamp where the current (incomplete) candle started."""
    now = int(time.time())
    seconds = {"1m": 60, "5m": 300, "15m": 900}
    period = seconds.get(timeframe, 60)
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

        # 5m/15m incremental queue: symbols remaining to scan this cycle
        self._sub_queue: dict[str, List[str]] = {}
        self._sub_next_due: dict[str, float] = {}

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
                await self.bootstrap_all()

            # Initialize sub-timeframe queues (5m, 15m)
            now = time.time()
            for tf in self.settings.timeframe_list:
                if tf == "1m":
                    continue
                self._sub_queue[tf] = []
                self._sub_next_due[tf] = now  # due immediately for first run

            await self.state.set_phase("scheduled_scanning")

            while not self._stop_event.is_set():
                cycle_start = time.time()
                deadline = cycle_start + self.settings.scan_seconds_for("1m")  # 60s

                # ── Phase 1: 1m scan (priority, all symbols) ──
                await self._scan_all_symbols("1m")

                # ── Phase 2: fill remaining time with 5m/15m ──
                for tf in ["5m", "15m"]:
                    if tf not in self._sub_queue:
                        continue

                    # Refill queue when it's time for a new scan cycle
                    now = time.time()
                    if now >= self._sub_next_due[tf] and not self._sub_queue[tf]:
                        self._sub_queue[tf] = list(self.state.symbols)
                        self._sub_next_due[tf] = now + self.settings.scan_seconds_for(tf)

                    # Process as many symbols as possible before deadline
                    while self._sub_queue[tf] and time.time() < deadline - 0.5:
                        if self._stop_event.is_set():
                            break
                        symbol = self._sub_queue[tf].pop(0)
                        await self._scan_symbol_timeframe(
                            symbol, tf,
                            fetch_limit=self.settings.incremental_candle_limit,
                        )

                # ── Wait until next 1m cycle ──
                remaining = deadline - time.time()
                if remaining > 0:
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=remaining)
                    except asyncio.TimeoutError:
                        pass

        except asyncio.CancelledError:
            return
        except Exception as e:
            await self.state.add_error(f"scanner fatal error: {type(e).__name__}: {e}")

    async def bootstrap_all(self) -> None:
        await self.state.set_phase("bootstrap_full_candles")
        for timeframe in self.settings.timeframe_list:
            await self.scan_timeframe(timeframe, full_bootstrap=True)

    async def scan_once(self) -> None:
        for timeframe in self.settings.timeframe_list:
            await self._scan_all_symbols(timeframe)

    async def scan_timeframe(self, timeframe: str, full_bootstrap: bool = False) -> None:
        """Full scan of a timeframe (used for bootstrap and manual triggers)."""
        started_at = int(time.time())

        async with self.state.lock:
            self.state.scanner_running = True
            self.state.last_scan_started_at = started_at
            self.state.current_phase = f"scan_{timeframe}"

        try:
            symbols = list(self.state.symbols)
            if not symbols:
                symbols = await self.futures_client.list_symbols()
                await self.state.set_symbols(symbols)

            limit = (
                self.settings.candle_limit_for(timeframe)
                if full_bootstrap
                else self.settings.incremental_candle_limit
            )

            for idx, symbol in enumerate(symbols, start=1):
                if self._stop_event.is_set():
                    break
                await self._scan_symbol_timeframe(symbol, timeframe, fetch_limit=limit)
                if idx % 25 == 0:
                    await self.state.set_phase(f"scan_{timeframe}_{idx}/{len(symbols)}")

        finally:
            async with self.state.lock:
                self.state.scanner_running = False
                self.state.last_scan_finished_at = int(time.time())
                self.state.current_phase = "idle"

    async def fetch_symbol_timeframe(self, symbol: str, timeframe: str, force_limit: Optional[int] = None) -> None:
        await self._scan_symbol_timeframe(
            symbol.upper(),
            timeframe,
            fetch_limit=force_limit or self.settings.candle_limit_for(timeframe),
        )

    async def _scan_all_symbols(self, timeframe: str) -> None:
        """Incremental scan of all symbols for one timeframe."""
        symbols = list(self.state.symbols)
        if not symbols:
            return

        async with self.state.lock:
            self.state.scanner_running = True
            self.state.last_scan_started_at = int(time.time())
            self.state.current_phase = f"scan_{timeframe}"

        try:
            limit = self.settings.incremental_candle_limit
            for idx, symbol in enumerate(symbols, start=1):
                if self._stop_event.is_set():
                    break
                await self._scan_symbol_timeframe(symbol, timeframe, fetch_limit=limit)
                if idx % 25 == 0:
                    await self.state.set_phase(f"scan_{timeframe}_{idx}/{len(symbols)}")
        finally:
            async with self.state.lock:
                self.state.scanner_running = False
                self.state.last_scan_finished_at = int(time.time())
                self.state.current_phase = "idle"

    async def _scan_symbol_timeframe(self, symbol: str, timeframe: str, fetch_limit: int) -> None:
        try:
            previous = await self.state.get_candles(symbol, timeframe, limit=1)
            previous_last = previous[-1] if previous else None

            new_candles = await self.futures_client.fetch_candles(symbol, timeframe, fetch_limit)
            if not new_candles:
                return

            # ── Filter out incomplete (current) candle ──
            cutoff = _current_period_start(timeframe)
            new_candles = [c for c in new_candles if c.time < cutoff]
            if not new_candles:
                return

            merged = await self.state.merge_candles(symbol, timeframe, new_candles)

            all_signals = self.engine.calculate_signals(symbol, timeframe, merged)
            await self.state.set_signals_for_symbol_tf(symbol, timeframe, all_signals)

            latest_candle = merged[-1]
            if self._candle_changed(previous_last, latest_candle):
                await self.ws_manager.broadcast_candle(symbol, timeframe, latest_candle)

            if all_signals:
                latest_signal = all_signals[-1]
                added = await self.state.add_recent_signal_if_new(latest_signal)
                if added:
                    await self.ws_manager.broadcast_signal(latest_signal)
                    await self.webhook_sender.send_signal(latest_signal)

        except Exception as e:
            await self.state.add_error(f"{symbol} {timeframe}: {type(e).__name__}: {e}")

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
