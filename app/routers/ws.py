from __future__ import annotations

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["websocket"])


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace("-", "_").replace("/", "_")


@router.websocket("/ws/signals")
async def ws_signals(websocket: WebSocket):
    await websocket.accept()

    manager = websocket.app.state.ws_manager
    state = websocket.app.state.market_state

    await manager.add_signal_client(websocket)

    try:
        recent = await state.get_recent_signals(timeframe="all", limit=500)
        await websocket.send_json({
            "event": "snapshot",
            "market": "gate_usdt_perpetual_futures",
            "data": [s.model_dump() for s in recent],
        })

        while True:
            msg = await websocket.receive_text()
            if msg.lower() == "ping":
                await websocket.send_json({"event": "pong"})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await manager.remove_signal_client(websocket)


@router.websocket("/ws/candles")
async def ws_candles(
    websocket: WebSocket,
    symbol: str = Query(...),
    timeframe: str = Query("1m"),
):
    await websocket.accept()

    symbol = normalize_symbol(symbol)
    settings = websocket.app.state.settings
    manager = websocket.app.state.ws_manager
    state = websocket.app.state.market_state
    scanner = websocket.app.state.scanner

    if timeframe not in settings.timeframe_list:
        await websocket.send_json({"event": "error", "message": "invalid timeframe"})
        await websocket.close()
        return

    await manager.add_candle_client(symbol, timeframe, websocket)

    try:
        candles = await state.get_candles(symbol, timeframe, limit=settings.candle_limit_for(timeframe))
        if not candles:
            await scanner.fetch_symbol_timeframe(symbol, timeframe, force_limit=settings.candle_limit_for(timeframe))
            candles = await state.get_candles(symbol, timeframe, limit=settings.candle_limit_for(timeframe))

        signals = await state.get_signals(symbol, timeframe, limit=1000)

        await websocket.send_json({
            "event": "snapshot",
            "market": "gate_usdt_perpetual_futures",
            "symbol": symbol,
            "timeframe": timeframe,
            "candles": [c.model_dump() for c in candles],
            "signals": [s.model_dump() for s in signals],
        })

        while True:
            msg = await websocket.receive_text()
            if msg.lower() == "ping":
                await websocket.send_json({"event": "pong"})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await manager.remove_candle_client(symbol, timeframe, websocket)
