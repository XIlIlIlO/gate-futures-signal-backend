from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers.market import router as market_router
from app.routers.ws import router as ws_router
from app.services.futures_client import GateFuturesClient
from app.services.scanner import MarketScanner
from app.services.webhook import WebhookSender
from app.state import MarketState
from app.ws_manager import WebSocketManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    futures_client = GateFuturesClient(settings)
    market_state = MarketState(settings)
    ws_manager = WebSocketManager()
    webhook_sender = WebhookSender(settings.webhook_url)

    scanner = MarketScanner(
        settings=settings,
        futures_client=futures_client,
        state=market_state,
        ws_manager=ws_manager,
        webhook_sender=webhook_sender,
    )

    app.state.settings = settings
    app.state.futures_client = futures_client
    app.state.market_state = market_state
    app.state.ws_manager = ws_manager
    app.state.webhook_sender = webhook_sender
    app.state.scanner = scanner

    scanner.start()

    yield

    await scanner.stop()
    await futures_client.close()
    await webhook_sender.close()


app = FastAPI(
    title="Gate USDT Perpetual Futures Signal Backend",
    version="0.2.0",
    lifespan=lifespan,
)

settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(market_router)
app.include_router(ws_router)


@app.get("/")
async def root():
    return {
        "ok": True,
        "name": "Gate USDT Perpetual Futures Signal Backend",
        "market": "gate_usdt_perpetual_futures",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
async def health():
    return {"ok": True}
