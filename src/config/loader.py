from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator


class ApiSettings(BaseModel):
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    gamma_markets_endpoint: str = "/markets"
    clob_base_url: str = "https://clob.polymarket.com"
    gamma_page_size: int = 200
    gamma_max_pages: int = 10
    ws_market_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class StrategySettings(BaseModel):
    entry_threshold_sum_ask: float = 0.985
    min_ask: float = 0.01
    max_ask: float = 0.99
    expiry_block_minutes: int = 180
    signal_cooldown_seconds: int = 30
    enable_quality_guards: bool = True
    max_spread_per_leg: float = 0.05
    min_depth_per_leg: float = 5.0
    max_quote_age_ms_for_signal: int = 3_000
    adjusted_edge_min: float = 0.0
    slippage_penalty_ticks: float = 1.0

    @model_validator(mode="after")
    def validate_bounds(self) -> StrategySettings:
        if self.min_ask < 0 or self.max_ask > 1 or self.min_ask > self.max_ask:
            raise ValueError("strategy min_ask/max_ask must satisfy 0 <= min_ask <= max_ask <= 1")
        return self


class RiskSettings(BaseModel):
    max_open_positions: int = 5
    max_positions_per_market: int = 1
    paper_order_size_usdc: float = 5.0
    max_daily_signals: int = 200
    min_book_size: float = 0.0
    stale_quote_ms: int = 3000
    fill_latency_ms: int = 300
    slip_ticks: int = 1
    base_fill_probability: float = 0.95
    allow_partial_fills: bool = True


class StorageSettings(BaseModel):
    sqlite_path: str = "data/state/state.db"
    export_dir: str = "data/exports"
    log_dir: str = "data/logs"


class RuntimeSettings(BaseModel):
    reconnect_base_seconds: float = 2.0
    reconnect_max_seconds: float = 30.0
    websocket_ping_interval_seconds: int = 20
    websocket_ping_timeout_seconds: int = 20
    market_refresh_minutes: int = 60
    stale_asset_ms: int = 15_000
    book_resync_idle_ms: int = 20_000
    resync_batch_size: int = 100


class MarketFilterSettings(BaseModel):
    exclude_categories: list[str] = Field(default_factory=list)
    exclude_keywords: list[str] = Field(default_factory=list)


class SecretSettings(BaseModel):
    polymarket_private_key: str | None = None
    polymarket_api_key: str | None = None
    polymarket_api_secret: str | None = None
    polymarket_passphrase: str | None = None


class Settings(BaseModel):
    api: ApiSettings = Field(default_factory=ApiSettings)
    strategy: StrategySettings = Field(default_factory=StrategySettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)
    guardrails: GuardrailSettings = Field(default_factory=GuardrailSettings)
    market_filters: MarketFilterSettings = Field(default_factory=MarketFilterSettings)
    secrets: SecretSettings = Field(default_factory=SecretSettings)


class MarketsConfig(BaseModel):
    include_slugs: list[str] = Field(default_factory=list)
    exclude_slugs: list[str] = Field(default_factory=list)
    exclude_categories: list[str] = Field(default_factory=list)
    exclude_keywords: list[str] = Field(default_factory=list)


class AppConfig(BaseModel):
    root_dir: Path
    settings: Settings
    markets: MarketsConfig

    @property
    def sqlite_path(self) -> Path:
        return self.root_dir / self.settings.storage.sqlite_path

    @property
    def export_dir(self) -> Path:
        return self.root_dir / self.settings.storage.export_dir

    @property
    def log_dir(self) -> Path:
        return self.root_dir / self.settings.storage.log_dir


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be mapping: {path}")
    return data


def _load_dotenv(env_path: Path) -> None:
    if not env_path.exists():
        return
    with env_path.open("r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def _load_secrets_from_env() -> SecretSettings:
    return SecretSettings(
        polymarket_private_key=os.getenv("POLYMARKET_PRIVATE_KEY"),
        polymarket_api_key=os.getenv("POLYMARKET_API_KEY"),
        polymarket_api_secret=os.getenv("POLYMARKET_API_SECRET"),
        polymarket_passphrase=os.getenv("POLYMARKET_PASSPHRASE"),
    )


def load_app_config(
    root_dir: str | Path,
    settings_path: str = "config/settings.yaml",
    markets_path: str = "config/markets.yaml",
    env_path: str = ".env",
) -> AppConfig:
    root = Path(root_dir).resolve()
    _load_dotenv(root / env_path)

    settings_data = _read_yaml(root / settings_path)
    markets_data = _read_yaml(root / markets_path)

    settings = Settings(**settings_data)
    settings.secrets = _load_secrets_from_env()
    markets = MarketsConfig(**markets_data)

    return AppConfig(root_dir=root, settings=settings, markets=markets)
