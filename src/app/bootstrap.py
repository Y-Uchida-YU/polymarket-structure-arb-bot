from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from uuid import uuid4

from src.app.scheduler import run_periodic
from src.clients.book_client import BookClient
from src.clients.gamma_client import GammaClient
from src.clients.tick_size_client import TickSizeClient
from src.clients.ws_client import MarketWebSocketClient
from src.config.loader import AppConfig, load_app_config
from src.domain.book import TickSizeUpdate
from src.domain.market import BinaryMarket
from src.domain.position import Position
from src.domain.run import RunSnapshot, RunSummary
from src.domain.signal import ArbSignal
from src.execution.order_router import PaperExecutionResult, PaperOrderRouter
from src.execution.quote_manager import BestBidAskUpdate, QuoteManager
from src.monitoring.healthcheck import HealthCheck
from src.monitoring.notifier import Notifier
from src.reporting.daily_report import DailyReportGenerator
from src.risk.exposure import ExposureManager
from src.risk.guardrails import GuardrailDecision, GuardrailMonitor
from src.risk.kill_switch import KillSwitch
from src.risk.limits import RiskLimiter
from src.storage.csv_logger import CsvEventLogger
from src.storage.sqlite_store import SQLiteStore
from src.strategy.complement_arb import ComplementArbConfig, ComplementArbStrategy
from src.strategy.filters import extract_binary_markets_with_stats
from src.utils.clock import utc_now

RESYNC_REASON_WS_CONNECTED = "ws_connected"
RESYNC_REASON_WS_RECONNECT = "ws_reconnect"
RESYNC_REASON_IDLE_TIMEOUT = "idle_timeout"
RESYNC_REASON_MISSING_BOOK_STATE = "missing_book_state"
RESYNC_REASON_STALE_ASSET = "stale_asset"
RESYNC_REASON_MARKET_UNIVERSE_CHANGED = "market_universe_changed"
RESYNC_REASON_MANUAL_REFRESH = "manual_refresh"

SAFE_MODE_SCOPE_GLOBAL = "global"
SAFE_MODE_SCOPE_ASSET = "asset"
SAFE_MODE_SCOPE_MARKET = "market"
SAFE_MODE_REASON_ALL_ASSETS_STALE = "all_assets_stale"
SAFE_MODE_REASON_WS_UNHEALTHY = "ws_unhealthy"
SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY = "book_state_unhealthy"


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("polymarket_arb_bot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = TimedRotatingFileHandler(
        filename=log_dir / "bot.log",
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


@dataclass(slots=True)
class AppState:
    last_signal_at_by_market: dict[str, float]
    last_resync_reason_by_asset: dict[str, str] = field(default_factory=dict)
    last_resync_at_by_asset: dict[str, float] = field(default_factory=dict)
    last_resync_at_by_asset_reason: dict[str, float] = field(default_factory=dict)
    last_resync_at_by_reason: dict[str, float] = field(default_factory=dict)
    last_no_data_resync_at_by_asset: dict[str, float] = field(default_factory=dict)
    stale_assets: set[str] = field(default_factory=set)
    safe_mode_active: bool = False
    safe_mode_reason: str | None = None
    safe_mode_scope: str | None = None
    safe_mode_blocked_assets: set[str] = field(default_factory=set)
    safe_mode_blocked_markets: set[str] = field(default_factory=set)
    safe_mode_reason_counts: dict[str, int] = field(default_factory=dict)
    safe_mode_scope_counts: dict[str, int] = field(default_factory=dict)
    asset_safe_mode_reason: str | None = None
    market_safe_mode_reason: str | None = None
    safe_mode_entered_at: datetime | None = None
    safe_mode_count: int = 0
    ws_connected_at: datetime | None = None
    subscription_started_at: datetime | None = None
    first_quote_received_at: datetime | None = None
    ws_unhealthy: bool = False
    book_state_unhealthy: bool = False
    asset_subscribed_at: dict[str, datetime] = field(default_factory=dict)
    ready_assets: set[str] = field(default_factory=set)
    ever_ready_assets: set[str] = field(default_factory=set)
    asset_recovering_until: dict[str, datetime] = field(default_factory=dict)
    last_book_missing_reason_by_asset: dict[str, str] = field(default_factory=dict)
    last_ws_idle_resync_ts: float = 0.0
    run_id: str = field(default_factory=lambda: uuid4().hex)
    run_started_at: datetime = field(default_factory=utc_now)
    run_mode: str = "paper"
    total_signals: int = 0
    total_fills: int = 0
    total_rejects: int = 0
    total_resync_events: int = 0
    total_exceptions: int = 0
    total_stale_events: int = 0
    total_unmatched_events: int = 0
    cumulative_projected_pnl: float = 0.0
    cumulative_unmatched_inventory: float = 0.0
    pending_connect_resync_reason: str | None = None
    no_signal_reason_counts: dict[str, int] = field(default_factory=dict)
    resync_reason_counts: dict[str, int] = field(default_factory=dict)
    watched_markets: int = 0
    subscribed_assets: int = 0
    cumulative_watched_market_ids: set[str] = field(default_factory=set)
    cumulative_subscribed_asset_ids: set[str] = field(default_factory=set)
    global_unhealthy_reason: str | None = None
    global_unhealthy_since: datetime | None = None
    global_unhealthy_consecutive_count: int = 0
    pending_universe_signature: str | None = None
    pending_universe_confirmation_count: int = 0
    market_unhealthy_consecutive_cycles: dict[str, int] = field(default_factory=dict)
    market_block_started_at: dict[str, datetime] = field(default_factory=dict)


@dataclass(slots=True)
class MarketLoadResult:
    market_count: int
    asset_ids_changed: bool
    asset_ids: list[str]
    added_assets: list[str] = field(default_factory=list)
    removed_assets: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MarketSnapshot:
    extraction_raw_market_count: int
    excluded_counts: dict[str, int]
    markets_by_id: dict[str, BinaryMarket]
    token_to_market_side: dict[str, tuple[str, str]]


class PolymarketStructureArbApp:
    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        started_at = utc_now()

        self.gamma_client = GammaClient(
            base_url=config.settings.api.gamma_base_url,
            markets_endpoint=config.settings.api.gamma_markets_endpoint,
        )
        self.book_client = BookClient(base_url=config.settings.api.clob_base_url)
        self.tick_size_client = TickSizeClient(base_url=config.settings.api.clob_base_url)
        self.strategy = ComplementArbStrategy(
            ComplementArbConfig(
                entry_threshold_sum_ask=config.settings.strategy.entry_threshold_sum_ask,
                min_ask=config.settings.strategy.min_ask,
                max_ask=config.settings.strategy.max_ask,
                enable_quality_guards=config.settings.strategy.enable_quality_guards,
                max_spread_per_leg=config.settings.strategy.max_spread_per_leg,
                min_depth_per_leg=config.settings.strategy.min_depth_per_leg,
                max_quote_age_ms_for_signal=config.settings.strategy.max_quote_age_ms_for_signal,
                adjusted_edge_min=config.settings.strategy.adjusted_edge_min,
                slippage_penalty_ticks=config.settings.strategy.slippage_penalty_ticks,
            )
        )
        self.risk_limiter = RiskLimiter(
            max_open_positions=config.settings.risk.max_open_positions,
            max_positions_per_market=config.settings.risk.max_positions_per_market,
            max_daily_signals=config.settings.risk.max_daily_signals,
            expiry_block_minutes=config.settings.strategy.expiry_block_minutes,
        )
        self.order_router = PaperOrderRouter(
            order_size_usdc=config.settings.risk.paper_order_size_usdc,
            min_book_size=config.settings.risk.min_book_size,
            stale_quote_ms=config.settings.risk.stale_quote_ms,
            fill_latency_ms=config.settings.risk.fill_latency_ms,
            slip_ticks=config.settings.risk.slip_ticks,
            base_fill_probability=config.settings.risk.base_fill_probability,
            allow_partial_fills=config.settings.risk.allow_partial_fills,
        )
        self.csv_logger = CsvEventLogger(export_dir=config.export_dir)
        self.sqlite_store = SQLiteStore(db_path=config.sqlite_path)
        self.exposure = ExposureManager()
        self.kill_switch = KillSwitch(max_consecutive_errors=20)
        self.healthcheck = HealthCheck(started_at=started_at)
        self.guardrail_monitor = GuardrailMonitor(settings=config.settings.guardrails)
        self.notifier = Notifier(logger=logger)
        self.report_generator = DailyReportGenerator(
            db_path=config.sqlite_path,
            export_dir=config.export_dir,
        )

        self.markets_by_id: dict[str, BinaryMarket] = {}
        self.token_to_market_side: dict[str, tuple[str, str]] = {}
        self.quote_manager = QuoteManager(token_to_market_side={})
        self.state = AppState(last_signal_at_by_market={}, run_started_at=started_at)
        self.stop_event = asyncio.Event()
        self.resubscribe_event = asyncio.Event()
        self._run_summary_emitted = False

    async def load_markets(self) -> MarketLoadResult:
        previous_asset_ids = set(self.token_to_market_side.keys())
        snapshot = await self._fetch_market_snapshot()
        return await self._apply_market_snapshot(
            snapshot=snapshot,
            previous_asset_ids=previous_asset_ids,
        )

    async def _fetch_market_snapshot(self) -> MarketSnapshot:
        raw_markets = await self.gamma_client.fetch_active_markets(
            page_size=self.config.settings.api.gamma_page_size,
            max_pages=self.config.settings.api.gamma_max_pages,
        )
        extraction = extract_binary_markets_with_stats(
            raw_markets=raw_markets,
            market_filters=self.config.settings.market_filters,
            markets_config=self.config.markets,
        )
        markets_by_id = {market.market_id: market for market in extraction.markets}
        token_to_market_side: dict[str, tuple[str, str]] = {}
        for market in extraction.markets:
            token_to_market_side[market.yes_token_id] = (market.market_id, "yes")
            token_to_market_side[market.no_token_id] = (market.market_id, "no")
        return MarketSnapshot(
            extraction_raw_market_count=extraction.raw_market_count,
            excluded_counts=extraction.excluded_counts,
            markets_by_id=markets_by_id,
            token_to_market_side=token_to_market_side,
        )

    async def _apply_market_snapshot(
        self,
        *,
        snapshot: MarketSnapshot,
        previous_asset_ids: set[str],
    ) -> MarketLoadResult:
        self.markets_by_id = snapshot.markets_by_id
        now = utc_now()
        for market in self.markets_by_id.values():
            self.sqlite_store.upsert_market(market, updated_at_iso=now.isoformat())

        self.token_to_market_side = snapshot.token_to_market_side
        self.quote_manager.update_token_mapping(snapshot.token_to_market_side)
        asset_ids = list(snapshot.token_to_market_side.keys())
        current_asset_ids = set(asset_ids)
        added_assets = sorted(current_asset_ids - previous_asset_ids)
        removed_assets = sorted(previous_asset_ids - current_asset_ids)
        if added_assets:
            await self._load_initial_tick_sizes(added_assets)

        self.state.watched_markets = len(self.markets_by_id)
        self.state.subscribed_assets = len(asset_ids)
        self.state.cumulative_watched_market_ids.update(self.markets_by_id.keys())
        self.state.cumulative_subscribed_asset_ids.update(asset_ids)

        current_markets = set(self.markets_by_id.keys())
        self.state.safe_mode_blocked_assets &= current_asset_ids
        self.state.safe_mode_blocked_markets &= current_markets
        self.state.ready_assets &= current_asset_ids
        self.state.ever_ready_assets &= current_asset_ids
        self.state.market_unhealthy_consecutive_cycles = {
            market_id: count
            for market_id, count in self.state.market_unhealthy_consecutive_cycles.items()
            if market_id in current_markets
        }
        self.state.market_block_started_at = {
            market_id: started_at
            for market_id, started_at in self.state.market_block_started_at.items()
            if market_id in current_markets
        }
        self.state.asset_recovering_until = {
            asset_id: recovering_until
            for asset_id, recovering_until in self.state.asset_recovering_until.items()
            if asset_id in current_asset_ids
        }
        self.state.last_book_missing_reason_by_asset = {
            asset_id: reason
            for asset_id, reason in self.state.last_book_missing_reason_by_asset.items()
            if asset_id in current_asset_ids
        }
        self.state.last_no_data_resync_at_by_asset = {
            asset_id: ts
            for asset_id, ts in self.state.last_no_data_resync_at_by_asset.items()
            if asset_id in current_asset_ids
        }
        self.state.asset_subscribed_at = {
            asset_id: subscribed_at
            for asset_id, subscribed_at in self.state.asset_subscribed_at.items()
            if asset_id in current_asset_ids
        }
        for asset_id in asset_ids:
            self.state.asset_subscribed_at.setdefault(asset_id, now)
        self.state.last_signal_at_by_market = {
            market_id: ts
            for market_id, ts in self.state.last_signal_at_by_market.items()
            if market_id in current_markets
        }
        self._record_universe_metrics(now_utc=now)
        self._record_market_filter_exclusion_metrics(
            excluded_counts=snapshot.excluded_counts,
            now_utc=now,
        )

        asset_ids_changed = bool(previous_asset_ids) and previous_asset_ids != current_asset_ids
        self.logger.info(
            (
                "Loaded raw_markets=%s watched_markets(current/cumulative)=%s/%s "
                "subscribed_assets(current/cumulative)=%s/%s changed=%s "
                "added_assets=%s removed_assets=%s universe_limit=%s"
            ),
            snapshot.extraction_raw_market_count,
            self.state.watched_markets,
            len(self.state.cumulative_watched_market_ids),
            self.state.subscribed_assets,
            len(self.state.cumulative_subscribed_asset_ids),
            asset_ids_changed,
            len(added_assets),
            len(removed_assets),
            self.config.settings.market_filters.max_markets_to_watch,
        )
        self.logger.info(
            "Universe thresholds stale_asset_ms=%s book_resync_idle_ms=%s resync_cooldown_ms=%s",
            self.config.settings.runtime.stale_asset_ms,
            self.config.settings.runtime.book_resync_idle_ms,
            self.config.settings.runtime.resync_cooldown_ms,
        )
        return MarketLoadResult(
            market_count=len(self.markets_by_id),
            asset_ids_changed=asset_ids_changed,
            asset_ids=asset_ids,
            added_assets=added_assets,
            removed_assets=removed_assets,
        )

    def _record_market_filter_exclusion_metrics(
        self,
        *,
        excluded_counts: dict[str, int],
        now_utc: datetime,
    ) -> None:
        if not excluded_counts:
            return
        reason_summary = ",".join(
            f"{reason}:{count}"
            for reason, count in sorted(
                excluded_counts.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:8]
        )
        self.sqlite_store.save_metric(
            metric_name="market_filter_exclusion_summary",
            metric_value=float(sum(excluded_counts.values())),
            details=reason_summary,
            created_at_iso=now_utc.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now_utc.isoformat(),
                "metric_name": "market_filter_exclusion_summary",
                "metric_value": sum(excluded_counts.values()),
                "details": reason_summary,
            },
            now_utc=now_utc,
        )

    async def _load_initial_tick_sizes(self, asset_ids: list[str]) -> None:
        if not asset_ids:
            return

        semaphore = asyncio.Semaphore(20)

        async def _fetch(asset_id: str) -> TickSizeUpdate | None:
            async with semaphore:
                try:
                    return await self.tick_size_client.fetch_tick_size(asset_id)
                except Exception:  # noqa: BLE001
                    return None

        results = await asyncio.gather(
            *[_fetch(asset_id) for asset_id in asset_ids], return_exceptions=False
        )
        for item in results:
            if item is None:
                continue
            self.quote_manager.apply_tick_size_snapshot(
                asset_id=item.asset_id,
                tick_size=item.tick_size,
                source=item.source,
                timestamp=item.updated_at,
            )
            self._persist_tick_size_update(item)

    async def handle_ws_message(self, raw_message: str) -> None:
        now = utc_now()
        try:
            updates = self.quote_manager.ingest_ws_message(raw_message)
            tick_updates = self.quote_manager.drain_tick_size_updates()
            for update in tick_updates:
                self._persist_tick_size_update(update)

            if not updates:
                return
            if self.state.first_quote_received_at is None:
                self.state.first_quote_received_at = now

            for update in updates:
                self.healthcheck.on_ws_message(now)
                self.healthcheck.on_asset_quote_update(update.asset_id, update.timestamp)
                mapping = self.token_to_market_side.get(update.asset_id)
                if mapping is None:
                    continue

                market_id, side = mapping
                quote_age_ms = max(0.0, (now - update.timestamp).total_seconds() * 1000.0)
                tick_size = self.quote_manager.get_tick_size(update.asset_id)
                self.sqlite_store.save_quote(
                    market_id=market_id,
                    side=side,
                    update=update,
                    run_id=self.state.run_id,
                    quote_age_ms=quote_age_ms,
                    tick_size=tick_size,
                    source=update.event_type,
                )
                self._mark_asset_ready(update.asset_id)

                market = self.markets_by_id.get(market_id)
                if market is None:
                    self._record_no_signal_reason(
                        reason="market_filtered_out",
                        now_utc=now,
                        market_id=market_id,
                    )
                    continue
                if self.state.safe_mode_active:
                    self._record_no_signal_reason(
                        reason="safe_mode_blocked_global",
                        now_utc=now,
                        market_id=market_id,
                        details={
                            "safe_mode_reason": self.state.safe_mode_reason,
                            "safe_mode_scope": self.state.safe_mode_scope or SAFE_MODE_SCOPE_GLOBAL,
                        },
                    )
                    continue
                if market_id in self.state.safe_mode_blocked_markets:
                    self._record_no_signal_reason(
                        reason="safe_mode_blocked_market",
                        now_utc=now,
                        market_id=market_id,
                        details={
                            "safe_mode_reason": self.state.market_safe_mode_reason
                            or SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY,
                            "safe_mode_scope": SAFE_MODE_SCOPE_MARKET,
                        },
                    )
                    continue
                blocked_assets = [
                    asset_id
                    for asset_id in (market.yes_token_id, market.no_token_id)
                    if asset_id in self.state.safe_mode_blocked_assets
                ]
                if blocked_assets:
                    self._record_no_signal_reason(
                        reason="safe_mode_blocked_asset",
                        now_utc=now,
                        market_id=market_id,
                        details={
                            "safe_mode_reason": self.state.asset_safe_mode_reason
                            or SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY,
                            "safe_mode_scope": SAFE_MODE_SCOPE_ASSET,
                            "blocked_assets": ",".join(blocked_assets),
                        },
                    )
                    continue
                if not self.quote_manager.is_market_ready(market_id):
                    market_missing_reasons = {
                        self._classify_missing_book_reason(market.yes_token_id, now_utc=now),
                        self._classify_missing_book_reason(market.no_token_id, now_utc=now),
                    }
                    no_signal_reason = (
                        "book_recovering"
                        if "book_recovering" in market_missing_reasons
                        else "book_not_ready"
                    )
                    self._record_no_signal_reason(
                        reason=no_signal_reason,
                        now_utc=now,
                        market_id=market_id,
                    )
                    continue
                yes_quote, no_quote = self.quote_manager.get_market_quotes(market_id)
                if yes_quote is None or no_quote is None:
                    market_missing_reasons = {
                        self._classify_missing_book_reason(market.yes_token_id, now_utc=now),
                        self._classify_missing_book_reason(market.no_token_id, now_utc=now),
                    }
                    no_signal_reason = (
                        "book_recovering"
                        if "book_recovering" in market_missing_reasons
                        else "book_not_ready"
                    )
                    self._record_no_signal_reason(
                        reason=no_signal_reason,
                        now_utc=now,
                        market_id=market_id,
                    )
                    continue
                if self._market_has_stale_quotes(market=market, now_utc=now):
                    self._record_no_signal_reason(
                        reason="quote_too_old",
                        now_utc=now,
                        market_id=market_id,
                    )
                    continue

                tick_yes = self.quote_manager.get_tick_size(market.yes_token_id)
                tick_no = self.quote_manager.get_tick_size(market.no_token_id)
                diagnostics = self.strategy.diagnose_with_quotes(
                    yes_quote=yes_quote,
                    no_quote=no_quote,
                    now_utc=now,
                    tick_size_yes=tick_yes,
                    tick_size_no=tick_no,
                )
                if not diagnostics.signal_ok:
                    self._record_no_signal_reason(
                        reason=self._map_strategy_reject_reason(diagnostics.reason),
                        now_utc=now,
                        market_id=market_id,
                    )
                    continue
                signal = self.strategy.build_signal_from_diagnostics(
                    market=market,
                    yes_quote=yes_quote,
                    no_quote=no_quote,
                    now_utc=now,
                    tick_size_yes=tick_yes,
                    tick_size_no=tick_no,
                    order_size_usdc=self.config.settings.risk.paper_order_size_usdc,
                    diagnostics=diagnostics,
                )
                if not self._passes_cooldown(market_id=market_id, now_ts=now.timestamp()):
                    self._record_no_signal_reason(
                        reason="cooldown_active",
                        now_utc=now,
                        market_id=market_id,
                    )
                    continue

                risk_decision = self.risk_limiter.evaluate_new_signal(
                    market=market,
                    exposure=self.exposure,
                    now_utc=now,
                )
                if not risk_decision.allowed:
                    self._record_no_signal_reason(
                        reason="risk_blocked",
                        now_utc=now,
                        market_id=market_id,
                    )
                    continue

                self._execute_paper_trade(
                    signal=signal,
                    now=now,
                    yes_quote=yes_quote,
                    no_quote=no_quote,
                    tick_yes=tick_yes,
                    tick_no=tick_no,
                )

                self._record_signal(signal)
                self.state.last_signal_at_by_market[market_id] = now.timestamp()
                self.exposure.increment_daily_signal_count(now.date())
                self.healthcheck.on_signal(now)
                self.kill_switch.record_success()
                self._evaluate_guardrails(now_utc=now, stale_asset_rate=self._stale_asset_rate())
        except Exception as exc:  # noqa: BLE001
            self.kill_switch.record_error()
            self.healthcheck.on_error(now)
            self.state.total_exceptions += 1
            self.guardrail_monitor.record_exception(now)
            error_text = f"{type(exc).__name__}: {exc}"
            self.logger.exception("Failed to process ws message: %s", error_text)
            self.sqlite_store.save_error(
                stage="ws_message",
                error_message=error_text,
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_error(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "stage": "ws_message",
                    "error_message": error_text,
                },
                now_utc=now,
            )
            self._evaluate_guardrails(now_utc=now, stale_asset_rate=self._stale_asset_rate())
            if self.kill_switch.is_triggered():
                self.notifier.error("Kill switch triggered. Stopping app.")
                self.stop_event.set()

    def _persist_tick_size_update(self, update: TickSizeUpdate) -> None:
        self.sqlite_store.save_tick_size_update(update, run_id=self.state.run_id)
        self.csv_logger.log_tick_size(
            {
                "run_id": self.state.run_id,
                "asset_id": update.asset_id,
                "tick_size": update.tick_size,
                "source": update.source,
                "updated_at": update.updated_at.isoformat(),
            },
            now_utc=update.updated_at,
        )
        self.logger.info(
            "Tick size updated asset=%s tick_size=%.6f source=%s",
            update.asset_id,
            update.tick_size,
            update.source,
        )

    def _record_universe_metrics(self, now_utc: datetime) -> None:
        metrics = {
            "universe_current_watched_markets": float(self.state.watched_markets),
            "universe_current_subscribed_assets": float(self.state.subscribed_assets),
            "universe_cumulative_watched_markets": float(
                len(self.state.cumulative_watched_market_ids)
            ),
            "universe_cumulative_subscribed_assets": float(
                len(self.state.cumulative_subscribed_asset_ids)
            ),
        }
        for name, value in metrics.items():
            self.sqlite_store.save_metric(
                metric_name=name,
                metric_value=value,
                details=f"env={self.config.settings.runtime.environment_name}",
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now_utc.isoformat(),
                    "metric_name": name,
                    "metric_value": value,
                },
                now_utc=now_utc,
            )

    def _mark_asset_ready(self, asset_id: str) -> None:
        if not self.quote_manager.is_asset_ready(asset_id):
            return
        self.state.ready_assets.add(asset_id)
        self.state.ever_ready_assets.add(asset_id)
        self.state.asset_recovering_until.pop(asset_id, None)
        self.state.last_book_missing_reason_by_asset.pop(asset_id, None)
        self.state.safe_mode_blocked_assets.discard(asset_id)

    def _mark_asset_recovering(self, asset_id: str, now_utc: datetime) -> None:
        grace_ms = max(0, self.config.settings.runtime.resync_recovery_grace_ms)
        recovering_until = now_utc + timedelta(milliseconds=grace_ms)
        self.state.asset_recovering_until[asset_id] = recovering_until
        self.state.ready_assets.discard(asset_id)

    def _is_asset_recovering(self, asset_id: str, now_utc: datetime) -> bool:
        recovering_until = self.state.asset_recovering_until.get(asset_id)
        if recovering_until is None:
            return False
        if now_utc > recovering_until:
            self.state.asset_recovering_until.pop(asset_id, None)
            return False
        return True

    def _classify_missing_book_reason(self, asset_id: str, now_utc: datetime) -> str:
        if self._is_asset_recovering(asset_id=asset_id, now_utc=now_utc):
            return "book_recovering"
        subscribed_at = self.state.asset_subscribed_at.get(asset_id)
        if subscribed_at is not None:
            elapsed_ms = max(0.0, (now_utc - subscribed_at).total_seconds() * 1000.0)
            if elapsed_ms < max(0, self.config.settings.runtime.per_asset_book_grace_ms):
                return "book_not_ready"
        if asset_id in self.state.ever_ready_assets:
            return "book_evicted"
        reason_key = f"{asset_id}|{RESYNC_REASON_MISSING_BOOK_STATE}"
        last_missing_resync_ts = self.state.last_resync_at_by_asset_reason.get(reason_key)
        if last_missing_resync_ts is None:
            return "no_initial_book"
        last_no_data_resync = self.state.last_no_data_resync_at_by_asset.get(asset_id)
        if last_no_data_resync is not None and last_no_data_resync >= last_missing_resync_ts:
            elapsed_ms_since_no_data = max(
                0.0,
                (now_utc.timestamp() - last_no_data_resync) * 1000.0,
            )
            if elapsed_ms_since_no_data < max(
                0,
                self.config.settings.runtime.quote_missing_after_resync_delay_ms,
            ):
                return "book_recovering"
            return "quote_missing_after_resync"
        return "book_not_resynced_yet"

    def _update_asset_level_safe_mode(
        self,
        *,
        blocked_assets: set[str],
        blocked_markets: set[str] | None = None,
        reason: str,
        now_utc: datetime,
    ) -> None:
        if self.state.safe_mode_active:
            return
        previous_assets = set(self.state.safe_mode_blocked_assets)
        previous_markets = set(self.state.safe_mode_blocked_markets)
        previous_reason = self.state.asset_safe_mode_reason
        previous_market_reason = self.state.market_safe_mode_reason
        normalized = set(blocked_assets)
        normalized_markets = set(blocked_markets or set())
        started_markets = normalized_markets - previous_markets
        cleared_markets = previous_markets - normalized_markets
        self.state.safe_mode_blocked_assets = normalized
        self.state.safe_mode_blocked_markets = normalized_markets
        self.state.asset_safe_mode_reason = reason if normalized else None
        self.state.market_safe_mode_reason = reason if normalized_markets else None
        for market_id in started_markets:
            self.state.market_block_started_at[market_id] = now_utc
        for market_id in cleared_markets:
            self.state.market_block_started_at.pop(market_id, None)
        if normalized_markets:
            self.state.safe_mode_scope = SAFE_MODE_SCOPE_MARKET
        elif normalized:
            self.state.safe_mode_scope = SAFE_MODE_SCOPE_ASSET
        else:
            self.state.safe_mode_scope = None
        if (
            normalized == previous_assets
            and normalized_markets == previous_markets
            and self.state.asset_safe_mode_reason == previous_reason
            and self.state.market_safe_mode_reason == previous_market_reason
        ):
            if normalized_markets:
                self.sqlite_store.save_metric(
                    metric_name="safe_mode_market_block_active",
                    metric_value=float(len(normalized_markets)),
                    details=(
                        f"scope=market;event=active;reason={reason};"
                        f"blocked_markets={len(normalized_markets)}"
                    ),
                    created_at_iso=now_utc.isoformat(),
                    run_id=self.state.run_id,
                )
                self.csv_logger.log_metric(
                    {
                        "run_id": self.state.run_id,
                        "created_at": now_utc.isoformat(),
                        "metric_name": "safe_mode_market_block_active",
                        "metric_value": len(normalized_markets),
                        "scope": SAFE_MODE_SCOPE_MARKET,
                        "safe_mode_reason": reason,
                    },
                    now_utc=now_utc,
                )
            return

        if started_markets:
            started_details = (
                f"scope=market;event=started;reason={reason};"
                f"started_markets={len(started_markets)};active_markets={len(normalized_markets)}"
            )
            self.sqlite_store.save_metric(
                metric_name="safe_mode_market_block_started",
                metric_value=float(len(started_markets)),
                details=started_details,
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now_utc.isoformat(),
                    "metric_name": "safe_mode_market_block_started",
                    "metric_value": len(started_markets),
                    "scope": SAFE_MODE_SCOPE_MARKET,
                    "safe_mode_reason": reason,
                    "active_markets": len(normalized_markets),
                },
                now_utc=now_utc,
            )
        if cleared_markets:
            cleared_details = (
                f"scope=market;event=cleared;reason={reason};"
                f"cleared_markets={len(cleared_markets)};active_markets={len(normalized_markets)}"
            )
            self.sqlite_store.save_metric(
                metric_name="safe_mode_market_block_cleared",
                metric_value=float(len(cleared_markets)),
                details=cleared_details,
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now_utc.isoformat(),
                    "metric_name": "safe_mode_market_block_cleared",
                    "metric_value": len(cleared_markets),
                    "scope": SAFE_MODE_SCOPE_MARKET,
                    "safe_mode_reason": reason,
                    "active_markets": len(normalized_markets),
                },
                now_utc=now_utc,
            )

        if normalized_markets:
            metric_name = "safe_mode_market_blocked"
            details = (
                f"scope=market;event=active;reason={reason};"
                f"blocked_markets={len(normalized_markets)};blocked_assets={len(normalized)}"
            )
            metric_value = float(len(normalized_markets))
        elif normalized:
            metric_name = "safe_mode_asset_blocked"
            details = f"scope=asset;reason={reason};blocked_assets={len(normalized)}"
            metric_value = float(len(normalized))
        else:
            metric_name = "safe_mode_asset_cleared"
            details = "scope=asset;reason=none;blocked_assets=0"
            metric_value = 1.0
        self.sqlite_store.save_metric(
            metric_name=metric_name,
            metric_value=metric_value,
            details=details,
            created_at_iso=now_utc.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now_utc.isoformat(),
                "metric_name": metric_name,
                "metric_value": metric_value,
                "scope": SAFE_MODE_SCOPE_MARKET if normalized_markets else SAFE_MODE_SCOPE_ASSET,
                "safe_mode_reason": reason if (normalized or normalized_markets) else None,
                "market_safe_mode_reason": reason if normalized_markets else None,
                "blocked_assets": len(normalized),
                "blocked_markets": len(normalized_markets),
            },
            now_utc=now_utc,
        )

    def _derive_market_blocks(
        self,
        *,
        blocked_assets: set[str],
        now_utc: datetime,
    ) -> set[str]:
        del now_utc  # reserved for future timing policies
        candidate_markets: set[str] = set()
        for market_id, market in self.markets_by_id.items():
            if market.yes_token_id in blocked_assets and market.no_token_id in blocked_assets:
                candidate_markets.add(market_id)

        min_cycles = max(
            1,
            self.config.settings.runtime.market_block_min_consecutive_unhealthy_cycles,
        )
        blocked_markets: set[str] = set()
        for market_id in self.markets_by_id:
            previous = self.state.market_unhealthy_consecutive_cycles.get(market_id, 0)
            if market_id in candidate_markets:
                current = previous + 1
            else:
                current = 0
            if current > 0:
                self.state.market_unhealthy_consecutive_cycles[market_id] = current
            else:
                self.state.market_unhealthy_consecutive_cycles.pop(market_id, None)
            if current >= min_cycles:
                blocked_markets.add(market_id)
        return blocked_markets

    def _global_unhealthy_candidate_reason(
        self,
        *,
        tracked_assets: list[str],
        stale_assets: list[str],
        unhealthy_assets: set[str],
    ) -> str | None:
        if not tracked_assets:
            return None
        total_assets = len(tracked_assets)
        unhealthy_ratio = len(unhealthy_assets) / max(1, total_assets)
        if (
            len(stale_assets) == total_assets
            and total_assets > 0
            and unhealthy_ratio >= self.config.settings.guardrails.global_unhealthy_min_asset_ratio
        ):
            return SAFE_MODE_REASON_ALL_ASSETS_STALE
        if (
            self.state.ws_unhealthy
            and unhealthy_ratio
            >= self.config.settings.guardrails.global_ws_unhealthy_min_asset_ratio
        ):
            return SAFE_MODE_REASON_WS_UNHEALTHY
        if (
            self.state.book_state_unhealthy
            and unhealthy_ratio >= self.config.settings.guardrails.global_unhealthy_min_asset_ratio
        ):
            return SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY
        return None

    def _update_global_unhealthy_state(
        self, now_utc: datetime, candidate_reason: str | None
    ) -> tuple[bool, str | None]:
        if candidate_reason is None:
            self.state.global_unhealthy_reason = None
            self.state.global_unhealthy_since = None
            self.state.global_unhealthy_consecutive_count = 0
            return False, None
        if self.state.global_unhealthy_reason == candidate_reason:
            self.state.global_unhealthy_consecutive_count += 1
        else:
            self.state.global_unhealthy_reason = candidate_reason
            self.state.global_unhealthy_since = now_utc
            self.state.global_unhealthy_consecutive_count = 1

        since = self.state.global_unhealthy_since or now_utc
        duration_seconds = max(0.0, (now_utc - since).total_seconds())
        enough_consecutive = (
            self.state.global_unhealthy_consecutive_count
            >= self.config.settings.guardrails.global_unhealthy_consecutive_count
        )
        enough_duration = (
            duration_seconds
            >= self.config.settings.guardrails.global_unhealthy_min_duration_seconds
        )
        return enough_consecutive and enough_duration, candidate_reason

    def _market_has_stale_quotes(self, market: BinaryMarket, now_utc: datetime) -> bool:
        if (
            market.yes_token_id in self.state.stale_assets
            or market.no_token_id in self.state.stale_assets
        ):
            return True
        max_age_ms = self.config.settings.strategy.max_quote_age_ms_for_signal
        return self.quote_manager.is_asset_stale(
            asset_id=market.yes_token_id,
            now_utc=now_utc,
            max_age_ms=max_age_ms,
        ) or self.quote_manager.is_asset_stale(
            asset_id=market.no_token_id,
            now_utc=now_utc,
            max_age_ms=max_age_ms,
        )

    def _passes_cooldown(self, market_id: str, now_ts: float) -> bool:
        cooldown = self.config.settings.strategy.signal_cooldown_seconds
        last_ts = self.state.last_signal_at_by_market.get(market_id)
        if last_ts is None:
            return True
        return now_ts - last_ts >= cooldown

    @staticmethod
    def _map_strategy_reject_reason(reason: str) -> str:
        mapping = {
            "sum_ask_or_ask_bounds": "edge_below_threshold",
            "adjusted_edge_below_min": "edge_below_threshold",
            "spread_guard": "spread_too_wide",
            "min_depth_guard": "depth_too_low",
            "quote_age_exceeded": "quote_too_old",
            "missing_quote": "book_not_ready",
            "missing_best_ask": "book_not_ready",
            "tick_alignment_guard": "book_not_ready",
        }
        return mapping.get(reason, reason or "strategy_rejected")

    def _record_no_signal_reason(
        self,
        reason: str,
        now_utc: datetime,
        market_id: str | None = None,
        asset_id: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        normalized_reason = (reason or "unknown").strip() or "unknown"
        self.state.no_signal_reason_counts[normalized_reason] = (
            self.state.no_signal_reason_counts.get(normalized_reason, 0) + 1
        )
        detail_parts: list[str] = []
        if market_id:
            detail_parts.append(f"market_id={market_id}")
        if asset_id:
            detail_parts.append(f"asset_id={asset_id}")
        if details:
            for key, value in sorted(details.items()):
                if value is None:
                    continue
                detail_parts.append(f"{key}={value}")
        details_text = ";".join(detail_parts)
        self.sqlite_store.save_metric(
            metric_name=f"no_signal_reason:{normalized_reason}",
            metric_value=1.0,
            details=details_text,
            created_at_iso=now_utc.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now_utc.isoformat(),
                "metric_name": f"no_signal_reason:{normalized_reason}",
                "metric_value": 1.0,
                "market_id": market_id,
                "asset_id": asset_id,
                "reason": normalized_reason,
                "details": details_text,
            },
            now_utc=now_utc,
        )

    def _record_signal(self, signal: ArbSignal) -> None:
        self.sqlite_store.save_signal_with_run(signal=signal, run_id=self.state.run_id)
        self.state.total_signals += 1
        self.guardrail_monitor.record_signal(signal.detected_at)
        self.csv_logger.log_signal(
            {
                "run_id": self.state.run_id,
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "slug": signal.slug,
                "yes_token_id": signal.yes_token_id,
                "no_token_id": signal.no_token_id,
                "signal_timestamp": signal.detected_at.isoformat(),
                "ask_yes": signal.ask_yes,
                "ask_no": signal.ask_no,
                "yes_ask": signal.ask_yes,
                "no_ask": signal.ask_no,
                "yes_bid": signal.bid_yes,
                "no_bid": signal.bid_no,
                "yes_size": signal.size_yes,
                "no_size": signal.size_no,
                "tick_size_yes": signal.tick_size_yes,
                "tick_size_no": signal.tick_size_no,
                "sum_ask": signal.sum_ask,
                "threshold": signal.threshold,
                "quote_age_ms": signal.quote_age_ms,
                "signal_edge_raw": signal.raw_edge,
                "adjusted_edge": signal.adjusted_edge,
                "safe_mode_reason": self.state.safe_mode_reason,
                "resync_reason": self.state.last_resync_reason_by_asset.get(signal.yes_token_id),
                "reason": signal.reason,
            },
            now_utc=signal.detected_at,
        )
        self.logger.info(
            "Signal detected market=%s slug=%s raw_edge=%.6f adjusted_edge=%.6f",
            signal.market_id,
            signal.slug,
            signal.raw_edge,
            signal.adjusted_edge,
        )

    def _execute_paper_trade(
        self,
        signal: ArbSignal,
        now: datetime,
        yes_quote: BestBidAskUpdate,
        no_quote: BestBidAskUpdate,
        tick_yes: float,
        tick_no: float,
    ) -> PaperExecutionResult:
        result = self.order_router.execute_signal(
            signal=signal,
            now_utc=now,
            yes_quote=yes_quote,
            no_quote=no_quote,
            yes_tick_size=tick_yes,
            no_tick_size=tick_no,
        )
        self.state.total_fills += len(result.fills)
        self.state.cumulative_projected_pnl += result.total_projected_pnl
        self.state.cumulative_unmatched_inventory += (
            result.inventory_snapshot.unmatched_yes_qty + result.inventory_snapshot.unmatched_no_qty
        )
        if (
            result.inventory_snapshot.unmatched_yes_qty > 0
            or result.inventory_snapshot.unmatched_no_qty > 0
        ):
            self.state.total_unmatched_events += 1
            self.guardrail_monitor.record_unmatched(now)
        if result.fill_status in {"one_leg_yes_only", "one_leg_no_only"}:
            self.guardrail_monitor.record_one_leg(now)
        if result.inventory_snapshot.matched_qty > 0:
            self.exposure.add_position(
                Position(
                    market_id=signal.market_id,
                    signal_id=signal.signal_id,
                    yes_qty=result.inventory_snapshot.matched_qty,
                    no_qty=result.inventory_snapshot.matched_qty,
                    yes_entry_price=result.inventory_snapshot.avg_fill_price_yes,
                    no_entry_price=result.inventory_snapshot.avg_fill_price_no,
                    opened_at=now,
                )
            )

        if not result.accepted:
            self.state.total_rejects += 1
            self.guardrail_monitor.record_reject(now)
            rejection_reason = result.rejection_reason or "unknown_rejection"
            self.logger.info(
                "Paper execution rejected market=%s signal=%s reason=%s status=%s",
                signal.market_id,
                signal.signal_id,
                rejection_reason,
                result.fill_status,
            )
            self.sqlite_store.save_error(
                stage="paper_execution",
                error_message=rejection_reason,
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_error(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "stage": "paper_execution",
                    "error_message": rejection_reason,
                },
                now_utc=now,
            )

        for order in result.orders:
            self.sqlite_store.save_order(order, run_id=self.state.run_id)
            self.csv_logger.log_order(
                {
                    "run_id": self.state.run_id,
                    "order_id": order.order_id,
                    "signal_id": order.signal_id,
                    "market_id": order.market_id,
                    "token_id": order.token_id,
                    "side": order.side,
                    "quantity": order.quantity,
                    "limit_price": order.limit_price,
                    "status": order.status,
                    "created_at": order.created_at.isoformat(),
                },
                now_utc=now,
            )

        for fill in result.fills:
            self.sqlite_store.save_fill(fill, run_id=self.state.run_id)
            self.csv_logger.log_fill(
                {
                    "run_id": self.state.run_id,
                    "fill_id": fill.fill_id,
                    "order_id": fill.order_id,
                    "signal_id": fill.signal_id,
                    "market_id": fill.market_id,
                    "token_id": fill.token_id,
                    "filled_qty": fill.filled_qty,
                    "fill_price": fill.fill_price,
                    "fee": fill.fee,
                    "filled_at": fill.filled_at.isoformat(),
                    "fill_status": result.fill_status,
                    "reject_reason": result.rejection_reason,
                    "partial_fill": result.partial_fill,
                    "quote_age_ms": result.quote_age_ms,
                    "quote_age_ms_at_signal": signal.quote_age_ms,
                    "signal_edge_after_slippage": result.signal_edge_after_slippage,
                },
                now_utc=now,
            )

        self.sqlite_store.save_inventory_snapshot(
            result.inventory_snapshot,
            run_id=self.state.run_id,
        )
        self.csv_logger.log_inventory(
            {
                "run_id": self.state.run_id,
                "signal_id": result.inventory_snapshot.signal_id,
                "market_id": result.inventory_snapshot.market_id,
                "market_slug": result.inventory_snapshot.market_slug,
                "timestamp": result.inventory_snapshot.timestamp.isoformat(),
                "yes_filled_qty": result.inventory_snapshot.yes_filled_qty,
                "no_filled_qty": result.inventory_snapshot.no_filled_qty,
                "matched_qty": result.inventory_snapshot.matched_qty,
                "unmatched_yes_qty": result.inventory_snapshot.unmatched_yes_qty,
                "unmatched_no_qty": result.inventory_snapshot.unmatched_no_qty,
                "avg_fill_price_yes": result.inventory_snapshot.avg_fill_price_yes,
                "avg_fill_price_no": result.inventory_snapshot.avg_fill_price_no,
                "yes_mark_price": result.inventory_snapshot.yes_mark_price,
                "no_mark_price": result.inventory_snapshot.no_mark_price,
                "valuation_mode": result.inventory_snapshot.valuation_mode,
                "safe_mode_reason": self.state.safe_mode_reason,
            },
            now_utc=now,
        )

        self.sqlite_store.save_pnl_snapshot(result.pnl_snapshot, run_id=self.state.run_id)
        self.csv_logger.log_pnl(
            {
                "run_id": self.state.run_id,
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "market_slug": signal.slug,
                "created_at": now.isoformat(),
                "estimated_edge_at_signal": result.estimated_edge_at_signal,
                "projected_matched_pnl": result.projected_matched_pnl,
                "unmatched_inventory_mtm": result.unmatched_inventory_mtm,
                "total_projected_pnl": result.total_projected_pnl,
                "fill_status": result.fill_status,
                "reject_reason": result.rejection_reason,
                "matched_qty": result.inventory_snapshot.matched_qty,
                "unmatched_yes_qty": result.inventory_snapshot.unmatched_yes_qty,
                "unmatched_no_qty": result.inventory_snapshot.unmatched_no_qty,
                "avg_fill_price_yes": result.inventory_snapshot.avg_fill_price_yes,
                "avg_fill_price_no": result.inventory_snapshot.avg_fill_price_no,
                "safe_mode_reason": self.state.safe_mode_reason,
                "resync_reason": self.state.last_resync_reason_by_asset.get(signal.yes_token_id),
            },
            now_utc=now,
        )
        signal_to_complete_latency_ms = max(
            0.0, (now - signal.detected_at).total_seconds() * 1000.0
        )
        execution_row = {
            "run_id": self.state.run_id,
            "signal_id": signal.signal_id,
            "market_id": signal.market_id,
            "market_slug": signal.slug,
            "fill_status": result.fill_status,
            "detected_at": signal.detected_at.isoformat(),
            "completed_at": now.isoformat(),
            "signal_to_fill_latency_ms": signal_to_complete_latency_ms if result.accepted else None,
            "signal_to_reject_latency_ms": (
                signal_to_complete_latency_ms if not result.accepted else None
            ),
            "quote_age_ms_at_signal": signal.quote_age_ms,
            "quote_age_ms_at_fill": result.quote_age_ms,
            "raw_edge": signal.raw_edge,
            "adjusted_edge": signal.adjusted_edge,
            "avg_fill_price_yes": result.inventory_snapshot.avg_fill_price_yes,
            "avg_fill_price_no": result.inventory_snapshot.avg_fill_price_no,
            "matched_qty": result.inventory_snapshot.matched_qty,
            "unmatched_yes_qty": result.inventory_snapshot.unmatched_yes_qty,
            "unmatched_no_qty": result.inventory_snapshot.unmatched_no_qty,
            "total_projected_pnl": result.total_projected_pnl,
            "reject_reason": result.rejection_reason,
            "safe_mode_reason": self.state.safe_mode_reason,
            "resync_reason": self.state.last_resync_reason_by_asset.get(signal.yes_token_id),
        }
        self.sqlite_store.save_execution_event(execution_row)
        self.csv_logger.log_execution(execution_row, now_utc=now)
        return result

    def get_subscribed_asset_ids(self) -> list[str]:
        return list(self.token_to_market_side.keys())

    @staticmethod
    def _asset_universe_signature(asset_ids: set[str]) -> str:
        return "|".join(sorted(asset_ids))

    def _clear_pending_universe_change(self) -> None:
        self.state.pending_universe_signature = None
        self.state.pending_universe_confirmation_count = 0

    async def refresh_market_universe(self) -> None:
        try:
            previous_asset_ids = set(self.token_to_market_side.keys())
            snapshot = await self._fetch_market_snapshot()
            candidate_asset_ids = set(snapshot.token_to_market_side.keys())
            if candidate_asset_ids == previous_asset_ids:
                self._clear_pending_universe_change()
                await self._apply_market_snapshot(
                    snapshot=snapshot,
                    previous_asset_ids=previous_asset_ids,
                )
                return

            added_assets = sorted(candidate_asset_ids - previous_asset_ids)
            removed_assets = sorted(previous_asset_ids - candidate_asset_ids)
            change_count = len(added_assets) + len(removed_assets)
            candidate_signature = self._asset_universe_signature(candidate_asset_ids)
            if candidate_signature == self.state.pending_universe_signature:
                self.state.pending_universe_confirmation_count += 1
            else:
                self.state.pending_universe_signature = candidate_signature
                self.state.pending_universe_confirmation_count = 1

            confirmation_target = max(
                1,
                self.config.settings.runtime.market_universe_change_confirmations,
            )
            force_delta_target = max(
                1,
                self.config.settings.runtime.market_universe_change_min_asset_delta,
            )
            confirmed_change = (
                self.state.pending_universe_confirmation_count >= confirmation_target
                or change_count >= force_delta_target
            )
            now = utc_now()
            self.sqlite_store.save_metric(
                metric_name="market_universe_change_candidate",
                metric_value=float(change_count),
                details=(
                    f"added={len(added_assets)};removed={len(removed_assets)};"
                    f"confirmations={self.state.pending_universe_confirmation_count};"
                    f"required={confirmation_target};"
                    f"forced={int(change_count >= force_delta_target)}"
                ),
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": "market_universe_change_candidate",
                    "metric_value": change_count,
                    "added_assets": len(added_assets),
                    "removed_assets": len(removed_assets),
                    "confirmation_count": self.state.pending_universe_confirmation_count,
                    "confirmation_required": confirmation_target,
                    "forced_change": change_count >= force_delta_target,
                },
                now_utc=now,
            )
            if not confirmed_change:
                self.logger.info(
                    (
                        "Market universe change candidate pending "
                        "added_assets=%s removed_assets=%s confirmation=%s/%s"
                    ),
                    len(added_assets),
                    len(removed_assets),
                    self.state.pending_universe_confirmation_count,
                    confirmation_target,
                )
                return

            result = await self._apply_market_snapshot(
                snapshot=snapshot,
                previous_asset_ids=previous_asset_ids,
            )
            self._clear_pending_universe_change()
            if result.asset_ids_changed:
                self.logger.info(
                    (
                        "Market universe changed; requesting websocket resubscribe "
                        "added_assets=%s removed_assets=%s"
                    ),
                    len(result.added_assets),
                    len(result.removed_assets),
                )
                self.state.pending_connect_resync_reason = RESYNC_REASON_MARKET_UNIVERSE_CHANGED
                self.resubscribe_event.set()
                changed_now = utc_now()
                self.sqlite_store.save_metric(
                    metric_name="market_universe_changed",
                    metric_value=float(len(result.asset_ids)),
                    details=(
                        f"added={len(result.added_assets)};"
                        f"removed={len(result.removed_assets)};"
                        f"watched_markets={self.state.watched_markets}"
                    ),
                    created_at_iso=changed_now.isoformat(),
                    run_id=self.state.run_id,
                )
                self.csv_logger.log_metric(
                    {
                        "run_id": self.state.run_id,
                        "created_at": changed_now.isoformat(),
                        "metric_name": "market_universe_changed",
                        "metric_value": len(result.asset_ids),
                        "added_assets": len(result.added_assets),
                        "removed_assets": len(result.removed_assets),
                        "watched_markets": self.state.watched_markets,
                    },
                    now_utc=changed_now,
                )
        except Exception as exc:  # noqa: BLE001
            now = utc_now()
            self.state.total_exceptions += 1
            self.guardrail_monitor.record_exception(now)
            error_text = f"{type(exc).__name__}: {exc}"
            self.logger.exception("Market refresh failed: %s", error_text)
            self.sqlite_store.save_error(
                stage="market_refresh",
                error_message=error_text,
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_error(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "stage": "market_refresh",
                    "error_message": error_text,
                },
                now_utc=now,
            )
            self._evaluate_guardrails(now_utc=now, stale_asset_rate=self._stale_asset_rate())

    async def on_ws_connected(self, asset_ids: list[str]) -> None:
        now = utc_now()
        self.state.ws_connected_at = now
        self.state.subscription_started_at = now
        for asset_id in asset_ids:
            self.state.asset_subscribed_at.setdefault(asset_id, now)
        reason = self.state.pending_connect_resync_reason or RESYNC_REASON_WS_CONNECTED
        self.state.pending_connect_resync_reason = None
        await self.resync_assets(asset_ids=asset_ids, reason=reason, force=True)

    def _market_data_warmup_state(self, now_utc: datetime) -> tuple[bool, str]:
        if self.state.ws_connected_at is None:
            return True, "ws_not_connected"
        if self.state.first_quote_received_at is not None:
            return False, "ready"
        if self.state.subscription_started_at is None:
            return True, "waiting_subscription_start"

        grace_ms = max(0, self.config.settings.runtime.initial_market_data_grace_ms)
        elapsed_ms = max(
            0.0,
            (now_utc - self.state.subscription_started_at).total_seconds() * 1000.0,
        )
        if elapsed_ms < grace_ms:
            return True, "waiting_initial_market_data_grace"
        return False, "grace_elapsed"

    def on_ws_reconnect_required(self, reason: str) -> None:
        self.logger.warning("WebSocket reconnect requested reason=%s", reason)
        mapped_reason = (
            RESYNC_REASON_MARKET_UNIVERSE_CHANGED
            if reason == "asset_universe_changed"
            else RESYNC_REASON_WS_RECONNECT
        )
        self.state.pending_connect_resync_reason = mapped_reason
        now = utc_now()
        self.sqlite_store.save_metric(
            metric_name="ws_reconnect_required",
            metric_value=1.0,
            details=f"reason={reason};mapped={mapped_reason}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "ws_reconnect_required",
                "metric_value": 1.0,
                "reason": reason,
                "mapped_reason": mapped_reason,
            },
            now_utc=now,
        )

    async def check_data_freshness_and_resync(self) -> None:
        now = utc_now()
        tracked_assets = list(self.token_to_market_side.keys())
        last_ws_message_at = self.healthcheck.state.last_ws_message_at
        ws_idle_ms = (
            max(0.0, (now - last_ws_message_at).total_seconds() * 1000.0)
            if last_ws_message_at is not None
            else 0.0
        )
        self.sqlite_store.save_metric(
            metric_name="ws_idle_ms",
            metric_value=ws_idle_ms,
            details=f"has_last_ws_message={last_ws_message_at is not None}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "ws_idle_ms",
                "metric_value": ws_idle_ms,
                "has_last_ws_message": last_ws_message_at is not None,
            },
            now_utc=now,
        )
        is_warming_up, warmup_reason = self._market_data_warmup_state(now)
        if is_warming_up:
            warming_up_assets = [
                asset_id
                for asset_id in tracked_assets
                if not self.quote_manager.is_asset_ready(asset_id)
            ]
            self.state.stale_assets = set()
            # During startup/warm-up we intentionally suppress ws health escalation.
            self.state.ws_unhealthy = False
            self.state.book_state_unhealthy = False
            self.state.global_unhealthy_reason = None
            self.state.global_unhealthy_since = None
            self.state.global_unhealthy_consecutive_count = 0
            self._update_asset_level_safe_mode(
                blocked_assets=set(),
                reason=SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY,
                now_utc=now,
            )
            self.sqlite_store.save_metric(
                metric_name="warming_up_asset_count",
                metric_value=float(len(warming_up_assets)),
                details=warmup_reason,
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": "warming_up_asset_count",
                    "metric_value": len(warming_up_assets),
                    "warmup_reason": warmup_reason,
                },
                now_utc=now,
            )
            if self.state.safe_mode_reason in {
                SAFE_MODE_REASON_ALL_ASSETS_STALE,
                SAFE_MODE_REASON_WS_UNHEALTHY,
                SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY,
            }:
                self._maybe_exit_safe_mode(now)
            self._evaluate_guardrails(now_utc=now, stale_asset_rate=0.0)
            return

        for asset_id in tracked_assets:
            if self._is_asset_recovering(asset_id=asset_id, now_utc=now):
                continue
            if self.quote_manager.is_asset_ready(asset_id):
                self._mark_asset_ready(asset_id)

        missing_assets: list[str] = []
        missing_assets_by_reason: dict[str, list[str]] = {}
        for asset_id in tracked_assets:
            if self.quote_manager.is_asset_ready(asset_id):
                self.state.last_book_missing_reason_by_asset.pop(asset_id, None)
                continue
            reason = self._classify_missing_book_reason(asset_id=asset_id, now_utc=now)
            self.state.last_book_missing_reason_by_asset[asset_id] = reason
            missing_assets.append(asset_id)
            missing_assets_by_reason.setdefault(reason, []).append(asset_id)

        stale_assets = self.healthcheck.stale_assets(
            tracked_assets=tracked_assets,
            now_utc=now,
            max_age_ms=self.config.settings.runtime.stale_asset_ms,
        )
        recovering_assets = [
            asset_id
            for asset_id in stale_assets
            if self._is_asset_recovering(asset_id=asset_id, now_utc=now)
        ]
        effective_stale_assets = [
            asset_id for asset_id in stale_assets if asset_id not in set(recovering_assets)
        ]
        self.state.stale_assets = set(effective_stale_assets)
        missing_reasons_by_asset = self.state.last_book_missing_reason_by_asset
        blocking_missing_assets = {
            asset_id
            for asset_id in missing_assets
            if missing_reasons_by_asset.get(asset_id) not in {"book_recovering", "book_not_ready"}
        }
        unhealthy_assets = set(effective_stale_assets) | set(missing_assets)
        blocking_unhealthy_assets = set(effective_stale_assets) | blocking_missing_assets
        tracked_count = max(1, len(tracked_assets))
        unhealthy_ratio = len(unhealthy_assets) / tracked_count

        self.sqlite_store.save_metric(
            metric_name="stale_asset_count",
            metric_value=float(len(effective_stale_assets)),
            details=f"tracked_assets={len(tracked_assets)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "stale_asset_count",
                "metric_value": len(effective_stale_assets),
                "recovering_assets": len(recovering_assets),
                "tracked_assets": len(tracked_assets),
            },
            now_utc=now,
        )
        self.sqlite_store.save_metric(
            metric_name="recovering_asset_count",
            metric_value=float(len(recovering_assets)),
            details=f"tracked_assets={len(tracked_assets)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "recovering_asset_count",
                "metric_value": len(recovering_assets),
                "tracked_assets": len(tracked_assets),
            },
            now_utc=now,
        )
        self.sqlite_store.save_metric(
            metric_name="missing_book_state_count",
            metric_value=float(len(missing_assets)),
            details=f"tracked_assets={len(tracked_assets)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "missing_book_state_count",
                "metric_value": len(missing_assets),
                "tracked_assets": len(tracked_assets),
            },
            now_utc=now,
        )
        for reason, assets in sorted(missing_assets_by_reason.items()):
            self.sqlite_store.save_metric(
                metric_name=f"missing_book_state_reason:{reason}",
                metric_value=float(len(assets)),
                details=f"tracked_assets={len(tracked_assets)}",
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": f"missing_book_state_reason:{reason}",
                    "metric_value": len(assets),
                    "tracked_assets": len(tracked_assets),
                },
                now_utc=now,
            )

        self.state.ws_unhealthy = self._is_ws_idle(now)
        self.state.book_state_unhealthy = unhealthy_ratio >= max(
            0.01, self.config.settings.guardrails.max_stale_asset_rate
        )
        if effective_stale_assets:
            self.state.total_stale_events += 1
        if effective_stale_assets:
            stale_ratio = len(effective_stale_assets) / max(1, len(tracked_assets))
            self.logger.warning(
                "Stale assets detected count=%s total=%s ratio=%.3f recovering=%s",
                len(effective_stale_assets),
                len(tracked_assets),
                stale_ratio,
                len(recovering_assets),
            )

        partial_unhealthy_assets = (
            blocking_unhealthy_assets
            if len(blocking_unhealthy_assets) < len(tracked_assets)
            else set()
        )
        partial_unhealthy_markets = self._derive_market_blocks(
            blocked_assets=partial_unhealthy_assets,
            now_utc=now,
        )

        self._update_asset_level_safe_mode(
            blocked_assets=partial_unhealthy_assets,
            blocked_markets=partial_unhealthy_markets,
            reason=SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY,
            now_utc=now,
        )

        candidate_global_reason = self._global_unhealthy_candidate_reason(
            tracked_assets=tracked_assets,
            stale_assets=effective_stale_assets,
            unhealthy_assets=unhealthy_assets,
        )
        should_enter_global, trigger_reason = self._update_global_unhealthy_state(
            now_utc=now,
            candidate_reason=candidate_global_reason,
        )
        self.sqlite_store.save_metric(
            metric_name="global_unhealthy_consecutive_count",
            metric_value=float(self.state.global_unhealthy_consecutive_count),
            details=f"reason={self.state.global_unhealthy_reason or ''}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "global_unhealthy_consecutive_count",
                "metric_value": self.state.global_unhealthy_consecutive_count,
                "reason": self.state.global_unhealthy_reason,
            },
            now_utc=now,
        )
        if should_enter_global and trigger_reason is not None:
            self._enter_safe_mode(
                reason=trigger_reason,
                now_utc=now,
                scope=SAFE_MODE_SCOPE_GLOBAL,
            )
        elif candidate_global_reason is None and self.state.safe_mode_reason in {
            SAFE_MODE_REASON_ALL_ASSETS_STALE,
            SAFE_MODE_REASON_WS_UNHEALTHY,
            SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY,
        }:
            self._maybe_exit_safe_mode(now)

        missing_resync_candidates: list[str] = []
        for reason, assets in missing_assets_by_reason.items():
            if reason in {
                "no_initial_book",
                "book_evicted",
                "quote_missing_after_resync",
                "book_not_resynced_yet",
            }:
                missing_resync_candidates.extend(assets)
        missing_resync_assets = self._select_resync_assets(
            asset_ids=missing_resync_candidates,
            reason=RESYNC_REASON_MISSING_BOOK_STATE,
            now_utc=now,
        )
        if missing_resync_assets:
            await self.resync_assets(
                asset_ids=missing_resync_assets,
                reason=RESYNC_REASON_MISSING_BOOK_STATE,
            )

        missing_asset_set = set(missing_assets)
        stale_only_assets = [
            asset_id
            for asset_id in effective_stale_assets
            if asset_id not in missing_asset_set
            and not self._is_asset_recovering(asset_id=asset_id, now_utc=now)
        ]
        stale_resync_assets = self._select_resync_assets(
            asset_ids=stale_only_assets,
            reason=RESYNC_REASON_STALE_ASSET,
            now_utc=now,
        )
        if stale_resync_assets:
            await self.resync_assets(
                asset_ids=stale_resync_assets,
                reason=RESYNC_REASON_STALE_ASSET,
            )

        if self._is_ws_idle(now):
            idle_resync_assets = self._select_resync_assets(
                asset_ids=tracked_assets,
                reason=RESYNC_REASON_IDLE_TIMEOUT,
                now_utc=now,
                is_full_resync=True,
            )
            if idle_resync_assets:
                self.logger.warning(
                    "WebSocket stream idle over %sms. Triggering resync assets=%s",
                    self.config.settings.runtime.book_resync_idle_ms,
                    len(idle_resync_assets),
                )
                self.state.last_ws_idle_resync_ts = now.timestamp()
                await self.resync_assets(
                    asset_ids=idle_resync_assets,
                    reason=RESYNC_REASON_IDLE_TIMEOUT,
                )

        self._evaluate_guardrails(
            now_utc=now,
            stale_asset_rate=len(effective_stale_assets) / max(1, len(tracked_assets)),
        )

    def _enter_safe_mode(
        self,
        reason: str,
        now_utc: datetime,
        scope: str = SAFE_MODE_SCOPE_GLOBAL,
    ) -> None:
        if scope != SAFE_MODE_SCOPE_GLOBAL:
            self._update_asset_level_safe_mode(
                blocked_assets=set(self.state.safe_mode_blocked_assets),
                reason=reason,
                now_utc=now_utc,
            )
            return
        if (
            self.state.safe_mode_active
            and self.state.safe_mode_reason == reason
            and self.state.safe_mode_scope == scope
        ):
            return
        self.state.safe_mode_active = True
        self.state.safe_mode_reason = reason
        self.state.safe_mode_scope = scope
        self.state.asset_safe_mode_reason = None
        self.state.market_safe_mode_reason = None
        self.state.safe_mode_blocked_assets.clear()
        self.state.safe_mode_blocked_markets.clear()
        self.state.safe_mode_entered_at = now_utc
        self.state.safe_mode_count += 1
        self.state.safe_mode_reason_counts[reason] = (
            self.state.safe_mode_reason_counts.get(reason, 0) + 1
        )
        self.state.safe_mode_scope_counts[scope] = (
            self.state.safe_mode_scope_counts.get(scope, 0) + 1
        )
        self.logger.error("Safe mode enabled reason=%s scope=%s", reason, scope)
        self.sqlite_store.save_metric(
            metric_name="safe_mode_entered",
            metric_value=1.0,
            details=f"scope={scope};reason={reason}",
            created_at_iso=now_utc.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now_utc.isoformat(),
                "metric_name": "safe_mode_entered",
                "metric_value": 1.0,
                "safe_mode_reason": reason,
                "safe_mode_scope": scope,
            },
            now_utc=now_utc,
        )

    def _maybe_exit_safe_mode(self, now_utc: datetime) -> None:
        if not self.state.safe_mode_active:
            return
        self.state.safe_mode_active = False
        previous_reason = self.state.safe_mode_reason
        previous_scope = self.state.safe_mode_scope
        self.state.safe_mode_reason = None
        self.state.safe_mode_scope = None
        self.state.asset_safe_mode_reason = None
        self.state.market_safe_mode_reason = None
        self.state.safe_mode_blocked_assets.clear()
        self.state.safe_mode_blocked_markets.clear()
        self.state.safe_mode_entered_at = None
        self.logger.warning(
            "Safe mode disabled previous_reason=%s previous_scope=%s",
            previous_reason,
            previous_scope,
        )
        self.sqlite_store.save_metric(
            metric_name="safe_mode_exited",
            metric_value=1.0,
            details=f"scope={previous_scope or ''};reason={previous_reason or ''}",
            created_at_iso=now_utc.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now_utc.isoformat(),
                "metric_name": "safe_mode_exited",
                "metric_value": 1.0,
                "safe_mode_reason": previous_reason,
                "safe_mode_scope": previous_scope,
            },
            now_utc=now_utc,
        )

    def _stale_asset_rate(self) -> float:
        tracked_assets = len(self.token_to_market_side)
        if tracked_assets <= 0:
            return 0.0
        return len(self.state.stale_assets) / tracked_assets

    def _evaluate_guardrails(self, now_utc: datetime, stale_asset_rate: float) -> None:
        decision = self.guardrail_monitor.evaluate(
            now_utc=now_utc,
            stale_asset_rate=stale_asset_rate,
            ws_unhealthy=self.state.ws_unhealthy,
            book_state_unhealthy=self.state.book_state_unhealthy,
            safe_mode_active=self.state.safe_mode_active,
            safe_mode_entered_at=self.state.safe_mode_entered_at,
        )
        self._apply_guardrail_decision(now_utc=now_utc, decision=decision)

    def _apply_guardrail_decision(self, now_utc: datetime, decision: GuardrailDecision) -> None:
        for warning in decision.warnings:
            self.logger.warning(
                (
                    "Guardrail warning=%s signal_rate=%.2f reject_rate=%.2f "
                    "one_leg_rate=%.2f stale_rate=%.2f resync_rate=%.2f "
                    "exception_rate=%.2f ws_unhealthy=%s book_state_unhealthy=%s"
                ),
                warning,
                decision.metrics.signal_rate_per_min,
                decision.metrics.reject_rate,
                decision.metrics.one_leg_rate,
                decision.metrics.stale_asset_rate,
                decision.metrics.resync_rate_per_min,
                decision.metrics.exception_rate_per_min,
                self.state.ws_unhealthy,
                self.state.book_state_unhealthy,
            )
            self.sqlite_store.save_metric(
                metric_name=f"guardrail_warning_{warning}",
                metric_value=1.0,
                details=(
                    "signal_rate_per_min="
                    f"{decision.metrics.signal_rate_per_min:.4f};"
                    f"reject_rate={decision.metrics.reject_rate:.4f};"
                    f"one_leg_rate={decision.metrics.one_leg_rate:.4f};"
                    f"unmatched_rate={decision.metrics.unmatched_rate:.4f};"
                    f"stale_asset_rate={decision.metrics.stale_asset_rate:.4f};"
                    f"resync_rate_per_min={decision.metrics.resync_rate_per_min:.4f};"
                    f"exception_rate_per_min={decision.metrics.exception_rate_per_min:.4f};"
                    f"ws_unhealthy={int(self.state.ws_unhealthy)};"
                    f"book_state_unhealthy={int(self.state.book_state_unhealthy)}"
                ),
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now_utc.isoformat(),
                    "metric_name": f"guardrail_warning_{warning}",
                    "metric_value": 1.0,
                    "signal_rate_per_min": decision.metrics.signal_rate_per_min,
                    "reject_rate": decision.metrics.reject_rate,
                    "one_leg_rate": decision.metrics.one_leg_rate,
                    "unmatched_rate": decision.metrics.unmatched_rate,
                    "stale_asset_rate": decision.metrics.stale_asset_rate,
                    "resync_rate_per_min": decision.metrics.resync_rate_per_min,
                    "exception_rate_per_min": decision.metrics.exception_rate_per_min,
                    "ws_unhealthy": self.state.ws_unhealthy,
                    "book_state_unhealthy": self.state.book_state_unhealthy,
                },
                now_utc=now_utc,
            )
        if decision.enter_safe_mode_reason is not None:
            self._enter_safe_mode(
                reason=decision.enter_safe_mode_reason,
                now_utc=now_utc,
                scope=decision.enter_safe_mode_scope or SAFE_MODE_SCOPE_GLOBAL,
            )
        elif decision.exit_safe_mode and self.state.safe_mode_reason in {
            "resync_storm",
            "exception_rate_exceeded",
            SAFE_MODE_REASON_BOOK_STATE_UNHEALTHY,
        }:
            self._maybe_exit_safe_mode(now_utc)

        if decision.hard_stop_reason is not None:
            self.logger.error("Guardrail hard stop reason=%s", decision.hard_stop_reason)
            self.sqlite_store.save_metric(
                metric_name="guardrail_hard_stop",
                metric_value=1.0,
                details=decision.hard_stop_reason,
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.stop_event.set()

    def _is_ws_idle(self, now_utc: datetime) -> bool:
        last_ws_message_at = self.healthcheck.state.last_ws_message_at
        if last_ws_message_at is None:
            return False
        age_ms = (now_utc - last_ws_message_at).total_seconds() * 1000.0
        if age_ms < self.config.settings.runtime.book_resync_idle_ms:
            return False
        return (
            now_utc.timestamp() - self.state.last_ws_idle_resync_ts
        ) * 1000.0 >= self.config.settings.runtime.book_resync_idle_ms

    def _select_resync_assets(
        self,
        *,
        asset_ids: list[str],
        reason: str,
        now_utc: datetime,
        is_full_resync: bool = False,
    ) -> list[str]:
        if not asset_ids:
            return []
        now_ts = now_utc.timestamp()
        unique_assets = list(dict.fromkeys(asset_ids))
        max_assets = max(1, self.config.settings.runtime.max_resync_assets_per_cycle)
        resync_cooldown_ms = max(0, self.config.settings.runtime.resync_cooldown_ms)
        full_resync_cooldown_ms = max(0, self.config.settings.runtime.full_resync_cooldown_ms)
        same_reason_cooldown_ms = max(
            0, self.config.settings.runtime.same_reason_resync_cooldown_ms
        )
        no_data_cooldown_ms = max(0, self.config.settings.runtime.no_data_resync_cooldown_ms)
        if reason == RESYNC_REASON_STALE_ASSET:
            same_reason_cooldown_ms = max(
                same_reason_cooldown_ms,
                self.config.settings.runtime.stale_asset_resync_cooldown_ms,
            )
        elif reason == RESYNC_REASON_MISSING_BOOK_STATE:
            same_reason_cooldown_ms = max(
                same_reason_cooldown_ms,
                self.config.settings.runtime.missing_book_resync_cooldown_ms,
            )

        if is_full_resync:
            last_reason_ts = self.state.last_resync_at_by_reason.get(reason)
            if last_reason_ts is not None:
                elapsed_ms = (now_ts - last_reason_ts) * 1000.0
                if elapsed_ms < full_resync_cooldown_ms:
                    return []

        selected: list[str] = []
        for asset_id in unique_assets:
            if reason == RESYNC_REASON_STALE_ASSET and asset_id not in self.state.ready_assets:
                continue
            if reason in {RESYNC_REASON_STALE_ASSET, RESYNC_REASON_MISSING_BOOK_STATE} and (
                self._is_asset_recovering(asset_id=asset_id, now_utc=now_utc)
            ):
                continue
            last_no_data_resync = self.state.last_no_data_resync_at_by_asset.get(asset_id)
            if last_no_data_resync is not None:
                elapsed_no_data_ms = (now_ts - last_no_data_resync) * 1000.0
                if elapsed_no_data_ms < no_data_cooldown_ms:
                    continue
            last_resync_ts = self.state.last_resync_at_by_asset.get(asset_id)
            if last_resync_ts is not None:
                elapsed_ms = (now_ts - last_resync_ts) * 1000.0
                if elapsed_ms < resync_cooldown_ms:
                    continue
            reason_key = f"{asset_id}|{reason}"
            last_same_reason_ts = self.state.last_resync_at_by_asset_reason.get(reason_key)
            if last_same_reason_ts is not None:
                elapsed_reason_ms = (now_ts - last_same_reason_ts) * 1000.0
                if elapsed_reason_ms < same_reason_cooldown_ms:
                    continue
            selected.append(asset_id)
            if len(selected) >= max_assets:
                break
        return selected

    async def resync_assets(self, asset_ids: list[str], reason: str, force: bool = False) -> None:
        if not asset_ids:
            return
        now = utc_now()
        batch_size = max(1, self.config.settings.runtime.resync_batch_size)
        for index in range(0, len(asset_ids), batch_size):
            batch = asset_ids[index : index + batch_size]
            for asset_id in batch:
                if not force:
                    allowed = self._select_resync_assets(
                        asset_ids=[asset_id],
                        reason=reason,
                        now_utc=now,
                    )
                    if not allowed:
                        continue
                self.state.last_resync_at_by_asset[asset_id] = now.timestamp()
                self.state.last_resync_at_by_reason[reason] = now.timestamp()
                self.state.last_resync_at_by_asset_reason[f"{asset_id}|{reason}"] = now.timestamp()
                self.state.resync_reason_counts[reason] = (
                    self.state.resync_reason_counts.get(reason, 0) + 1
                )
                self.state.total_resync_events += 1
                self.guardrail_monitor.record_resync(now)
                try:
                    summary = await self.book_client.fetch_book_summary(asset_id)
                    if summary is None:
                        self.sqlite_store.save_resync_event(
                            asset_id=asset_id,
                            reason=reason,
                            status="no_data",
                            details="book summary missing",
                            created_at_iso=now.isoformat(),
                            run_id=self.state.run_id,
                        )
                        self.csv_logger.log_resync(
                            {
                                "run_id": self.state.run_id,
                                "created_at": now.isoformat(),
                                "asset_id": asset_id,
                                "resync_reason": reason,
                                "status": "no_data",
                                "details": "book summary missing",
                            },
                            now_utc=now,
                        )
                        self.state.last_no_data_resync_at_by_asset[asset_id] = now.timestamp()
                        self._mark_asset_recovering(asset_id=asset_id, now_utc=now)
                        self._record_no_signal_reason(
                            reason="book_recovering",
                            now_utc=now,
                            market_id=self.token_to_market_side.get(asset_id, ("", ""))[0] or None,
                            asset_id=asset_id,
                            details={"resync_reason": reason},
                        )
                        continue

                    update = self.quote_manager.apply_book_resync(summary)
                    self.healthcheck.on_asset_quote_update(asset_id=asset_id, ts=summary.timestamp)
                    self._mark_asset_ready(asset_id)
                    self.state.last_no_data_resync_at_by_asset.pop(asset_id, None)
                    mapping = self.token_to_market_side.get(asset_id)
                    if mapping is not None:
                        market_id, side = mapping
                        quote_age_ms = max(0.0, (now - summary.timestamp).total_seconds() * 1000.0)
                        self.sqlite_store.save_quote(
                            market_id=market_id,
                            side=side,
                            update=update,
                            run_id=self.state.run_id,
                            quote_age_ms=quote_age_ms,
                            tick_size=self.quote_manager.get_tick_size(asset_id),
                            source="book_resync",
                            resync_reason=reason,
                        )

                    self.state.last_resync_reason_by_asset[asset_id] = reason
                    self.sqlite_store.save_resync_event(
                        asset_id=asset_id,
                        reason=reason,
                        status="ok",
                        details="resync_applied",
                        created_at_iso=now.isoformat(),
                        run_id=self.state.run_id,
                    )
                    self.csv_logger.log_resync(
                        {
                            "run_id": self.state.run_id,
                            "created_at": now.isoformat(),
                            "asset_id": asset_id,
                            "resync_reason": reason,
                            "status": "ok",
                            "best_bid": summary.best_bid,
                            "best_ask": summary.best_ask,
                            "best_bid_size": summary.best_bid_size,
                            "best_ask_size": summary.best_ask_size,
                        },
                        now_utc=now,
                    )
                except Exception as exc:  # noqa: BLE001
                    self.state.total_exceptions += 1
                    self.guardrail_monitor.record_exception(now)
                    error_text = f"{type(exc).__name__}: {exc}"
                    self.sqlite_store.save_resync_event(
                        asset_id=asset_id,
                        reason=reason,
                        status="failed",
                        details=error_text,
                        created_at_iso=now.isoformat(),
                        run_id=self.state.run_id,
                    )
                    self.csv_logger.log_resync(
                        {
                            "run_id": self.state.run_id,
                            "created_at": now.isoformat(),
                            "asset_id": asset_id,
                            "resync_reason": reason,
                            "status": "failed",
                            "details": error_text,
                        },
                        now_utc=now,
                    )
                    self.logger.exception(
                        "Resync failed asset=%s reason=%s error=%s", asset_id, reason, error_text
                    )
                self._evaluate_guardrails(now_utc=now, stale_asset_rate=self._stale_asset_rate())

    async def emit_periodic_report(self) -> None:
        now = utc_now()
        try:
            report = self.report_generator.generate(
                date=None,
                last_hours=24,
                run_id=self.state.run_id,
            )
            json_path, csv_path = self.report_generator.save(report)
            self.logger.info(
                "Periodic report exported run_id=%s json=%s csv=%s",
                self.state.run_id,
                json_path,
                csv_path,
            )
            self.sqlite_store.save_metric(
                metric_name="periodic_report_exported",
                metric_value=1.0,
                details=f"json={json_path.name};csv={csv_path.name}",
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": "periodic_report_exported",
                    "metric_value": 1.0,
                    "json_file": json_path.name,
                    "csv_file": csv_path.name,
                },
                now_utc=now,
            )
        except Exception as exc:  # noqa: BLE001
            self.state.total_exceptions += 1
            self.guardrail_monitor.record_exception(now)
            error_text = f"{type(exc).__name__}: {exc}"
            self.logger.exception("Periodic report export failed: %s", error_text)
            self.sqlite_store.save_error(
                stage="periodic_report",
                error_message=error_text,
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_error(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "stage": "periodic_report",
                    "error_message": error_text,
                },
                now_utc=now,
            )

    async def emit_periodic_snapshot(self) -> None:
        now = utc_now()
        snapshot = RunSnapshot(
            run_id=self.state.run_id,
            timestamp=now,
            active_markets=len(self.markets_by_id),
            stale_assets=len(self.state.stale_assets),
            total_signals=self.state.total_signals,
            total_fills=self.state.total_fills,
            open_unmatched_inventory=self.state.cumulative_unmatched_inventory,
            cumulative_projected_pnl=self.state.cumulative_projected_pnl,
            safe_mode_active=self.state.safe_mode_active,
            safe_mode_reason=self.state.safe_mode_reason,
            resync_cumulative_count=self.state.total_resync_events,
        )
        self.sqlite_store.save_run_snapshot(snapshot)
        self.csv_logger.log_snapshot(
            {
                "run_id": snapshot.run_id,
                "timestamp": snapshot.timestamp.isoformat(),
                "active_markets": snapshot.active_markets,
                "stale_assets": snapshot.stale_assets,
                "total_signals": snapshot.total_signals,
                "total_fills": snapshot.total_fills,
                "open_unmatched_inventory": snapshot.open_unmatched_inventory,
                "cumulative_projected_pnl": snapshot.cumulative_projected_pnl,
                "safe_mode_active": snapshot.safe_mode_active,
                "safe_mode_reason": snapshot.safe_mode_reason,
                "resync_cumulative_count": snapshot.resync_cumulative_count,
            },
            now_utc=now,
        )

    def emit_run_summary(self, status: str = "completed") -> None:
        if self._run_summary_emitted:
            return
        self._run_summary_emitted = True
        ended_at = utc_now()
        uptime_seconds = max(0.0, (ended_at - self.state.run_started_at).total_seconds())
        summary = RunSummary(
            run_id=self.state.run_id,
            mode=f"{self.state.run_mode}:{status}",
            started_at=self.state.run_started_at,
            ended_at=ended_at,
            uptime_seconds=uptime_seconds,
            total_signals=self.state.total_signals,
            total_fills=self.state.total_fills,
            total_projected_pnl=self.state.cumulative_projected_pnl,
            safe_mode_count=self.state.safe_mode_count,
            exception_count=self.state.total_exceptions,
            stale_events=self.state.total_stale_events,
            resync_events=self.state.total_resync_events,
        )
        self.sqlite_store.save_run_summary(summary)
        self.csv_logger.log_run_summary(
            {
                "run_id": summary.run_id,
                "mode": summary.mode,
                "started_at": summary.started_at.isoformat(),
                "ended_at": summary.ended_at.isoformat(),
                "uptime_seconds": summary.uptime_seconds,
                "total_signals": summary.total_signals,
                "total_fills": summary.total_fills,
                "total_projected_pnl": summary.total_projected_pnl,
                "safe_mode_count": summary.safe_mode_count,
                "exception_count": summary.exception_count,
                "stale_events": summary.stale_events,
                "resync_events": summary.resync_events,
            },
            now_utc=ended_at,
        )
        self.logger.info(
            (
                "Run summary run_id=%s mode=%s uptime_sec=%.1f signals=%s fills=%s "
                "projected_pnl=%.4f safe_mode_count=%s exceptions=%s "
                "stale_events=%s resync_events=%s"
            ),
            summary.run_id,
            summary.mode,
            summary.uptime_seconds,
            summary.total_signals,
            summary.total_fills,
            summary.total_projected_pnl,
            summary.safe_mode_count,
            summary.exception_count,
            summary.stale_events,
            summary.resync_events,
        )
        if self.state.resync_reason_counts:
            resync_breakdown = ",".join(
                f"{reason}:{count}"
                for reason, count in sorted(
                    self.state.resync_reason_counts.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            )
            self.logger.info(
                "Run resync breakdown run_id=%s reasons=%s",
                summary.run_id,
                resync_breakdown,
            )
        if self.state.no_signal_reason_counts:
            no_signal_breakdown = ",".join(
                f"{reason}:{count}"
                for reason, count in sorted(
                    self.state.no_signal_reason_counts.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[:10]
            )
            self.logger.info(
                "Run no-signal breakdown run_id=%s reasons=%s",
                summary.run_id,
                no_signal_breakdown,
            )

    async def run(
        self,
        once: bool = False,
        paper_mode: bool = True,
        dry_run: bool = False,
        shadow_paper: bool = False,
    ) -> None:
        self.state.run_mode = "shadow_paper" if shadow_paper else "paper"
        initial_load_result = await self.load_markets()
        if initial_load_result.market_count == 0:
            self.notifier.error("No eligible binary markets after filters.")
            return
        self.logger.info(
            (
                "Run startup universe watched_markets(current/cumulative)=%s/%s "
                "subscribed_assets(current/cumulative)=%s/%s "
                "universe_limit=%s stale_asset_ms=%s resync_idle_ms=%s"
            ),
            self.state.watched_markets,
            len(self.state.cumulative_watched_market_ids),
            self.state.subscribed_assets,
            len(self.state.cumulative_subscribed_asset_ids),
            self.config.settings.market_filters.max_markets_to_watch,
            self.config.settings.runtime.stale_asset_ms,
            self.config.settings.runtime.book_resync_idle_ms,
        )
        if once or dry_run:
            self.notifier.info("One-shot/dry-run mode finished.")
            return
        if not paper_mode:
            self.notifier.error("Live trading is not implemented. Use --paper mode.")
            return

        ws_client = MarketWebSocketClient(
            url=self.config.settings.api.ws_market_url,
            asset_ids=[],
            get_asset_ids=self.get_subscribed_asset_ids,
            on_message=self.handle_ws_message,
            on_connected=self.on_ws_connected,
            on_reconnect_required=self.on_ws_reconnect_required,
            logger=self.logger,
            reconnect_base_seconds=self.config.settings.runtime.reconnect_base_seconds,
            reconnect_max_seconds=self.config.settings.runtime.reconnect_max_seconds,
            ping_interval_seconds=self.config.settings.runtime.websocket_ping_interval_seconds,
            ping_timeout_seconds=self.config.settings.runtime.websocket_ping_timeout_seconds,
        )
        refresh_interval_seconds = max(
            60.0,
            self.config.settings.runtime.market_refresh_minutes * 60.0,
        )
        freshness_interval_seconds = max(5.0, self.config.settings.runtime.stale_asset_ms / 2000.0)
        refresh_task = asyncio.create_task(
            run_periodic(
                task=self.refresh_market_universe,
                interval_seconds=refresh_interval_seconds,
                stop_event=self.stop_event,
            )
        )
        freshness_task = asyncio.create_task(
            run_periodic(
                task=self.check_data_freshness_and_resync,
                interval_seconds=freshness_interval_seconds,
                stop_event=self.stop_event,
            )
        )
        snapshot_interval_seconds = max(
            60.0,
            float(self.config.settings.runtime.snapshot_interval_minutes) * 60.0,
        )
        snapshot_task = asyncio.create_task(
            run_periodic(
                task=self.emit_periodic_snapshot,
                interval_seconds=snapshot_interval_seconds,
                stop_event=self.stop_event,
            )
        )
        report_task: asyncio.Task[None] | None = None
        if shadow_paper:
            report_interval_seconds = max(
                300.0,
                float(self.config.settings.runtime.report_export_interval_minutes) * 60.0,
            )
            report_task = asyncio.create_task(
                run_periodic(
                    task=self.emit_periodic_report,
                    interval_seconds=report_interval_seconds,
                    stop_event=self.stop_event,
                )
            )

        try:
            await ws_client.run_forever(
                stop_event=self.stop_event,
                resubscribe_event=self.resubscribe_event,
            )
        finally:
            self.stop_event.set()
            refresh_task.cancel()
            freshness_task.cancel()
            snapshot_task.cancel()
            if report_task is not None:
                report_task.cancel()
            gather_tasks: list[asyncio.Task[object]] = [refresh_task, freshness_task, snapshot_task]
            if report_task is not None:
                gather_tasks.append(report_task)
            await asyncio.gather(
                *gather_tasks,
                return_exceptions=True,
            )

    async def shutdown(self) -> None:
        self.sqlite_store.close()


async def run_app(
    root_dir: Path,
    once: bool = False,
    paper_mode: bool = True,
    dry_run: bool = False,
    shadow_paper: bool = False,
    settings_path: str = "config/settings.yaml",
) -> None:
    config = load_app_config(root_dir=root_dir, settings_path=settings_path)
    logger = setup_logging(config.log_dir)
    app = PolymarketStructureArbApp(config=config, logger=logger)
    run_status = "completed"
    try:
        await app.run(
            once=once,
            paper_mode=paper_mode,
            dry_run=dry_run,
            shadow_paper=shadow_paper,
        )
    except Exception:  # noqa: BLE001
        run_status = "crashed"
        raise
    finally:
        if shadow_paper:
            await app.emit_periodic_report()
        app.emit_run_summary(status=run_status)
        await app.shutdown()
