from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Dict, Set, Tuple

from fastapi import WebSocket

from app.models import Candle, Signal


class WebSocketManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.signal_clients: Set[WebSocket] = set()
        self.candle_clients: Dict[Tuple[str, str], Set[WebSocket]] = defaultdict(set)

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

        await self._safe_broadcast(clients, payload)

    async def broadcast_candle(self, symbol: str, timeframe: str, candle: Candle) -> None:
        payload = {
            "event": "candle_update",
            "symbol": symbol,
            "timeframe": timeframe,
            "data": candle.model_dump(),
        }
        async with self._lock:
            clients = list(self.candle_clients.get((symbol, timeframe), set()))

        await self._safe_broadcast(clients, payload)

    async def _safe_broadcast(self, clients: list[WebSocket], payload: dict) -> None:
        if not clients:
            return

        dead_clients: list[WebSocket] = []
        for ws in clients:
            try:
                await ws.send_json(payload)
            except Exception:
                dead_clients.append(ws)

        if dead_clients:
            async with self._lock:
                for ws in dead_clients:
                    self.signal_clients.discard(ws)
                    for group in self.candle_clients.values():
                        group.discard(ws)
