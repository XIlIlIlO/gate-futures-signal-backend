from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from fastapi import WebSocket

from app.models import Candle, Signal


class WebSocketManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.signal_clients: Set[WebSocket] = set()
        self.candle_clients: Dict[Tuple[str, str], Set[WebSocket]] = defaultdict(set)
        # Track dead clients for deferred cleanup
        self._dead: List[WebSocket] = []

    async def add_signal_client(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.signal_clients.add(websocket)

    async def remove_signal_client(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.signal_clients.discard(websocket)

    async def add_candle_client(self, symbol: str, timeframe: str, websocket: WebSocket) -> None:
        async with self._lock:
            self.candle_clients[(symbol, timeframe)].add(websocket)

    async def remove_candle_client(self, symbol: str, timeframe: str, websocket: WebSocket) -> None:
        async with self._lock:
            clients = self.candle_clients.get((symbol, timeframe))
            if clients:
                clients.discard(websocket)

    async def broadcast_signal(self, signal: Signal) -> None:
        payload = {"event": "signal", "data": signal.model_dump()}
        async with self._lock:
            clients = list(self.signal_clients)
            candle_watchers = list(
                self.candle_clients.get((signal.symbol, signal.timeframe), set())
            )

        # Broadcast without lock held
        self._send_all(clients, payload)
        self._send_all(candle_watchers, payload)
        await self._flush_dead()

    async def broadcast_candle(self, symbol: str, timeframe: str, candle: Candle) -> None:
        payload = {
            "event": "candle_update",
            "symbol": symbol,
            "timeframe": timeframe,
            "data": candle.model_dump(),
        }
        async with self._lock:
            clients = list(self.candle_clients.get((symbol, timeframe), set()))

        self._send_all(clients, payload)
        await self._flush_dead()

    def _send_all(self, clients: List[WebSocket], payload: dict) -> None:
        """Non-async fire-and-forget send. Dead clients collected for deferred cleanup."""
        for ws in clients:
            try:
                # WebSocket.send_json is sync internally in Starlette when using uvicorn
                asyncio.ensure_future(ws.send_json(payload))
            except Exception:
                self._dead.append(ws)

    async def _flush_dead(self) -> None:
        """Remove dead clients in batch — single lock acquisition."""
        if not self._dead:
            return
        dead = self._dead
        self._dead = []
        async with self._lock:
            for ws in dead:
                self.signal_clients.discard(ws)
                for group in self.candle_clients.values():
                    group.discard(ws)
