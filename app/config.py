from __future__ import annotations

import json
from functools import lru_cache
from typing import Dict, List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    gate_base_url: str = "https://api.gateio.ws/api/v4"
    gate_settle: str = "usdt"
    market_quote: str = "USDT"

    symbol_limit: int = 0
    use_top_volume: bool = True
    exclude_delisting: bool = True

    timeframes: str = "1m,5m,15m"
    candle_limits_json: str = '{"1m":300,"5m":100,"15m":60}'
    incremental_candle_limit: int = 5
    timeframe_scan_seconds_json: str = '{"1m":60,"5m":300,"15m":900}'

    public_rps_limit: float = 12.0
    public_burst: int = 3

    max_retries: int = 3
    request_timeout_seconds: int = 20
    rate_limit_backoff_seconds: int = 2

    bootstrap_on_start: bool = True

    max_recent_signals: int = 1000
    cors_origins: str = "*"

    webhook_url: str = ""
    signal_min_score: int = 70

    @property
    def timeframe_list(self) -> List[str]:
        return [x.strip() for x in self.timeframes.split(",") if x.strip()]

    @property
    def candle_limits(self) -> Dict[str, int]:
        try:
            data = json.loads(self.candle_limits_json)
            return {str(k): min(int(v), 2000) for k, v in data.items()}
        except Exception:
            return {"1m": 300, "5m": 100, "15m": 60}

    @property
    def timeframe_scan_seconds(self) -> Dict[str, int]:
        try:
            data = json.loads(self.timeframe_scan_seconds_json)
            return {str(k): int(v) for k, v in data.items()}
        except Exception:
            return {"1m": 60, "5m": 300, "15m": 900}

    @property
    def cors_origin_list(self) -> List[str]:
        raw = self.cors_origins.strip()
        if raw == "*":
            return ["*"]
        return [x.strip() for x in raw.split(",") if x.strip()]

    def candle_limit_for(self, timeframe: str) -> int:
        return int(self.candle_limits.get(timeframe, 100))

    def scan_seconds_for(self, timeframe: str) -> int:
        return int(self.timeframe_scan_seconds.get(timeframe, 60))


@lru_cache
def get_settings() -> Settings:
    return Settings()
