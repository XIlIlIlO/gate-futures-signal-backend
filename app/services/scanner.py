from __future__ import annotations

import asyncio
import time
from typing import Optional

from app.config import Settings
from app.models import Candle
from app.services.futures_client import GateFuturesClient
from app.services.signal_engine import SignalEngine
from app.services.webhook import WebhookSender
from app.state import MarketState
from app.ws_manager import WebSocketManager


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
        self._next_due: dict[str, float] = {}

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

            now = time.time()
            self._next_due = {
                tf: now + self.settings.scan_seconds_for(tf)
                for tf in self.settings.timeframe_list
            }

            await self.state.set_phase("scheduled_scanning")

            while not self._stop_event.is_set():
                now = time.time()
                due_timeframes = [
                    tf for tf in self.settings.timeframe_list
                    if now >= self._next_due.get(tf, now)
                ]

                for tf in due_timeframes:
                    await self.scan_timeframe(tf, full_bootstrap=False)
                    self._next_due[tf] = time.time() + self.settings.scan_seconds_for(tf)

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
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
            await self.scan_timeframe(timeframe, full_bootstrap=False)

    async def scan_timeframe(self, timeframe: str, full_bootstrap: bool = False) -> None:
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

                # 상태 표시용. 너무 자주 lock을 잡지 않도록 25개마다만 업데이트.
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

    async def _scan_symbol_timeframe(self, symbol: str, timeframe: str, fetch_limit: int) -> None:
        try:
            previous = await self.state.get_candles(symbol, timeframe, limit=1)
            previous_last = previous[-1] if previous else None

            new_candles = await self.futures_client.fetch_candles(symbol, timeframe, fetch_limit)
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
