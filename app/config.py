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

    # All timeframes served (1m fetched from API, rest derived)
    timeframes: str = "1m,3m,5m,15m,30m,1h"
    candle_limits_json: str = '{"1m":2000,"3m":666,"5m":400,"15m":133,"30m":66,"1h":33}'
    incremental_candle_limit: int = 5

    # Only controls the main 1m scan cycle interval
    scan_interval_seconds: int = 60

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
            return {"1m": 2000, "3m": 666, "5m": 400, "15m": 133, "30m": 66, "1h": 33}

    @property
    def cors_origin_list(self) -> List[str]:
        raw = self.cors_origins.strip()
        if raw == "*":
            return ["*"]
        return [x.strip() for x in raw.split(",") if x.strip()]

    def candle_limit_for(self, timeframe: str) -> int:
        return int(self.candle_limits.get(timeframe, 100))


@lru_cache
def get_settings() -> Settings:
    return Settings()
