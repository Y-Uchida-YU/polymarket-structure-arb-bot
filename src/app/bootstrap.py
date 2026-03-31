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
from src.config.loader import AppConfig, MarketFilterSettings, load_app_config
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
from src.utils.stale_diagnostics import (
    STALE_NO_SIGNAL_REASON_NO_RECENT_QUOTE,
    STALE_NO_SIGNAL_REASON_QUOTE_AGE,
    STALE_REASON_LEG_TIMESTAMP_MISMATCH,
    STALE_REASON_MISSING_LEG_QUOTE,
    STALE_REASON_NO_RECENT_QUOTE,
    STALE_REASON_QUOTE_AGE_EXCEEDED,
    STALE_REASON_UNKNOWN,
    STALE_SIDE_BOTH,
    STALE_SIDE_NO,
    STALE_SIDE_UNKNOWN,
    STALE_SIDE_YES,
    format_kv_details,
    normalize_stale_reason_key,
    normalize_stale_side,
)

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

MARKET_FRESHNESS_READY = "ready"
MARKET_FRESHNESS_NOT_READY = "not_ready"
MARKET_FRESHNESS_PROBATION = "probation"
MARKET_FRESHNESS_RECOVERING = "recovering"
MARKET_FRESHNESS_STALE_NO_RECENT_QUOTE = "stale_no_recent_quote"
MARKET_FRESHNESS_STALE_QUOTE_AGE = "stale_quote_age"
MARKET_STALE_STATES = {
    MARKET_FRESHNESS_STALE_NO_RECENT_QUOTE,
    MARKET_FRESHNESS_STALE_QUOTE_AGE,
}

MARKET_QUALITY_HEALTHY = "healthy"
MARKET_QUALITY_DEGRADED = "degraded"
MARKET_QUALITY_PROBATION_EXTENDED = "probation_extended"
MARKET_QUALITY_EXCLUSION_CANDIDATE = "exclusion_candidate"
MARKET_QUALITY_EXCLUDED = "excluded"

DIAG_EVENT_RESYNC_STARTED = "resync_started"
DIAG_EVENT_FIRST_QUOTE_AFTER_RESYNC = "first_quote_after_resync"
DIAG_EVENT_BOOK_READY_AFTER_RESYNC = "book_ready_after_resync"
DIAG_EVENT_FIRST_QUOTE_AFTER_RESYNC_BLOCKED = "first_quote_after_resync_blocked"
DIAG_EVENT_BOOK_READY_AFTER_RESYNC_BLOCKED = "book_ready_after_resync_blocked"
DIAG_EVENT_MARKET_RECOVERY_STARTED = "market_recovery_started"
DIAG_EVENT_MARKET_READY_AFTER_RECOVERY = "market_ready_after_recovery"
DIAG_EVENT_MARKET_READY_AFTER_RECOVERY_BLOCKED = "market_ready_after_recovery_blocked"
DIAG_EVENT_ELIGIBILITY_GATE_UNMET = "eligibility_gate_unmet"
DIAG_EVENT_STALE_ASSET_DETECTED = "stale_asset_detected"
DIAG_EVENT_MISSING_BOOK_STATE_DETECTED = "missing_book_state_detected"
DIAG_EVENT_MARKET_BLOCK_ENTERED = "market_block_entered"
DIAG_EVENT_MARKET_BLOCK_CLEARED = "market_block_cleared"
DIAG_EVENT_MARKET_STALE_ENTERED = "market_stale_entered"
DIAG_EVENT_MARKET_STALE_RECOVERED = "market_stale_recovered"
DIAG_EVENT_MARKET_STALE_EPISODE_CLOSED = "market_stale_episode_closed"
DIAG_EVENT_MARKET_CHRONIC_STALE_EXCLUSION_ENTERED = "market_chronic_stale_exclusion_entered"
DIAG_EVENT_MARKET_CHRONIC_STALE_EXCLUSION_EXTENDED = "market_chronic_stale_exclusion_extended"
DIAG_EVENT_MARKET_CHRONIC_STALE_EXCLUSION_CLEARED = "market_chronic_stale_exclusion_cleared"
DIAG_EVENT_MARKET_CHRONIC_STALE_REINTRODUCED_FOR_FLOOR = (
    "market_chronic_stale_reintroduced_for_floor"
)

CHRONIC_STALE_REASON_REPEATED_STALE_ENTERS = "repeated_stale_enters"
CHRONIC_STALE_REASON_LONG_SINGLE_STALE_DURATION = "long_single_stale_duration"
CHRONIC_STALE_REASON_BOTH = "both"
CHRONIC_STALE_LONG_ACTIVE_MULTIPLIER = 2.0
NO_SIGNAL_REASON_CHRONIC_STALE_EXCLUDED = "chronic_stale_excluded"


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
    ws_health_state: str = "disconnected"
    last_ws_connect_reason: str | None = None
    last_ws_reconnect_reason: str | None = None
    connection_recovering_until: datetime | None = None
    ws_unhealthy: bool = False
    book_state_unhealthy: bool = False
    asset_subscribed_at: dict[str, datetime] = field(default_factory=dict)
    ready_assets: set[str] = field(default_factory=set)
    ever_ready_assets: set[str] = field(default_factory=set)
    asset_quote_update_count: dict[str, int] = field(default_factory=dict)
    asset_recovering_until: dict[str, datetime] = field(default_factory=dict)
    asset_recovering_started_at: dict[str, datetime] = field(default_factory=dict)
    asset_recovery_started_at: dict[str, datetime] = field(default_factory=dict)
    asset_first_quote_after_recovery_at: dict[str, datetime] = field(default_factory=dict)
    asset_recovery_reason_by_asset: dict[str, str] = field(default_factory=dict)
    asset_first_quote_block_reason_by_asset: dict[str, str] = field(default_factory=dict)
    asset_book_ready_block_reason_by_asset: dict[str, str] = field(default_factory=dict)
    asset_unhealthy_consecutive_cycles: dict[str, int] = field(default_factory=dict)
    last_book_missing_reason_by_asset: dict[str, str] = field(default_factory=dict)
    market_probation_until: dict[str, datetime] = field(default_factory=dict)
    market_freshness_state_by_market: dict[str, str] = field(default_factory=dict)
    market_freshness_details_by_market: dict[str, dict[str, object]] = field(default_factory=dict)
    market_freshness_updated_at: dict[str, datetime] = field(default_factory=dict)
    market_stale_started_at_by_market: dict[str, datetime] = field(default_factory=dict)
    market_stale_reason_by_market: dict[str, str] = field(default_factory=dict)
    market_stale_details_by_market: dict[str, dict[str, object]] = field(default_factory=dict)
    market_quality_penalty_by_market: dict[str, int] = field(default_factory=dict)
    market_low_quality_consecutive_cycles: dict[str, int] = field(default_factory=dict)
    market_quality_stage_by_market: dict[str, str] = field(default_factory=dict)
    market_exclusion_reason_by_market: dict[str, str] = field(default_factory=dict)
    last_refresh_runtime_excluded_market_ids: set[str] = field(default_factory=set)
    last_refresh_runtime_excluded_reason_by_market: dict[str, str] = field(default_factory=dict)
    last_refresh_runtime_excluded_count: int = 0
    last_refresh_runtime_excluded_at: datetime | None = None
    last_refresh_chronic_reintroduced_market_ids: set[str] = field(default_factory=set)
    last_refresh_chronic_reintroduced_reason_by_market: dict[str, str] = field(default_factory=dict)
    last_refresh_watched_chronic_stale_market_ids: set[str] = field(default_factory=set)
    last_refresh_watched_chronic_stale_reason_by_market: dict[str, str] = field(
        default_factory=dict
    )
    market_chronic_stale_excluded_until: dict[str, datetime] = field(default_factory=dict)
    market_chronic_stale_exclusion_started_at: dict[str, datetime] = field(default_factory=dict)
    market_chronic_stale_reason_by_market: dict[str, str] = field(default_factory=dict)
    market_chronic_stale_details_by_market: dict[str, dict[str, object]] = field(
        default_factory=dict
    )
    market_chronic_stale_enter_count_last_cycle: int = 0
    market_chronic_stale_cleared_count_last_cycle: int = 0
    market_refresh_observed_count: dict[str, int] = field(default_factory=dict)
    eligible_markets: set[str] = field(default_factory=set)
    last_no_signal_reason_by_market: dict[str, str] = field(default_factory=dict)
    last_no_signal_reason_at_by_market: dict[str, float] = field(default_factory=dict)
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
    market_recovery_started_at_for_diagnostics: dict[str, datetime] = field(default_factory=dict)
    market_recovery_reason_by_market: dict[str, str] = field(default_factory=dict)
    market_ready_block_reason_by_market: dict[str, str] = field(default_factory=dict)
    eligibility_gate_reason_by_market: dict[str, str] = field(default_factory=dict)
    eligibility_gate_reason_at_by_market: dict[str, float] = field(default_factory=dict)


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
    chronic_reintroduced_for_floor_market_ids: set[str] = field(default_factory=set)
    chronic_reintroduced_reason_by_market: dict[str, str] = field(default_factory=dict)
    watched_chronic_stale_market_ids: set[str] = field(default_factory=set)
    watched_chronic_stale_reason_by_market: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ChronicStaleExclusionSummary:
    active_market_ids: set[str] = field(default_factory=set)
    entered_market_ids: set[str] = field(default_factory=set)
    extended_market_ids: set[str] = field(default_factory=set)
    cleared_market_ids: set[str] = field(default_factory=set)
    reason_counts: dict[str, int] = field(default_factory=dict)
    extended_reason_counts: dict[str, int] = field(default_factory=dict)
    avg_active_age_ms: float = 0.0
    long_active_market_ages_ms: dict[str, float] = field(default_factory=dict)


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

    def _effective_watched_floor(self) -> int:
        configured_floor = max(0, self.config.settings.runtime.min_watched_markets_floor)
        max_markets = self.config.settings.market_filters.max_markets_to_watch
        if max_markets is not None and max_markets > 0:
            return min(configured_floor, max_markets)
        return configured_floor

    def _market_quality_stage(
        self,
        *,
        penalty: int,
        consecutive_bad_cycles: int,
        excluded: bool,
    ) -> str:
        if excluded:
            return MARKET_QUALITY_EXCLUDED
        threshold = max(1, self.config.settings.runtime.low_quality_market_penalty_threshold)
        degraded_cutoff = max(
            1,
            int(
                threshold
                * max(
                    0.0,
                    self.config.settings.runtime.low_quality_market_degraded_penalty_ratio,
                )
            ),
        )
        probation_cutoff = max(
            degraded_cutoff,
            int(
                threshold
                * max(
                    0.0,
                    self.config.settings.runtime.low_quality_market_probation_penalty_ratio,
                )
            ),
        )
        candidate_penalty_ratio = (
            self.config.settings.runtime.low_quality_market_exclusion_candidate_penalty_ratio
        )
        candidate_cutoff = max(
            probation_cutoff,
            int(
                threshold
                * max(
                    0.0,
                    candidate_penalty_ratio,
                )
            ),
        )
        exclusion_cycles = max(
            1,
            self.config.settings.runtime.low_quality_market_exclusion_consecutive_cycles,
        )
        candidate_cycles = max(1, exclusion_cycles - 2)
        if penalty >= candidate_cutoff and consecutive_bad_cycles >= candidate_cycles:
            return MARKET_QUALITY_EXCLUSION_CANDIDATE
        if penalty >= probation_cutoff:
            return MARKET_QUALITY_PROBATION_EXTENDED
        if penalty >= degraded_cutoff:
            return MARKET_QUALITY_DEGRADED
        return MARKET_QUALITY_HEALTHY

    def _relaxed_market_filters_for_watched_floor(self) -> MarketFilterSettings:
        relaxed_filters = self.config.settings.market_filters.model_copy(deep=True)
        if self.config.settings.runtime.watched_floor_relax_activity_filters:
            relaxed_filters.min_recent_activity = None
            relaxed_filters.min_liquidity_proxy = None
            relaxed_filters.min_volume_24h_proxy = None
            relaxed_filters.require_recent_trade_within_minutes = None
        return relaxed_filters

    def _save_last_refresh_runtime_exclusions(
        self,
        *,
        excluded_market_ids: set[str],
        reason_by_market: dict[str, str],
    ) -> None:
        self.state.last_refresh_runtime_excluded_market_ids = set(excluded_market_ids)
        self.state.last_refresh_runtime_excluded_reason_by_market = dict(reason_by_market)
        self.state.last_refresh_runtime_excluded_count = len(excluded_market_ids)
        self.state.last_refresh_runtime_excluded_at = utc_now()

    def _market_context_by_market_id(self, market_id: str) -> dict[str, object]:
        market = self.markets_by_id.get(market_id)
        if market is not None:
            return self._market_context_details(market)
        row = self.sqlite_store.conn.execute(
            """
            SELECT slug, question, yes_token_id, no_token_id
            FROM markets
            WHERE market_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (market_id,),
        ).fetchone()
        if row is None:
            return {
                "market_id": market_id,
                "market_slug": "",
                "market_question": "",
                "yes_asset_id": "",
                "no_asset_id": "",
            }
        return {
            "market_id": market_id,
            "market_slug": str(row[0] or ""),
            "market_question": str(row[1] or ""),
            "yes_asset_id": str(row[2] or ""),
            "no_asset_id": str(row[3] or ""),
        }

    def _active_chronic_stale_excluded_market_ids(
        self,
        *,
        now_utc: datetime,
        prune_expired: bool,
    ) -> set[str]:
        active: set[str] = set()
        for market_id, excluded_until in list(
            self.state.market_chronic_stale_excluded_until.items()
        ):
            if excluded_until > now_utc:
                active.add(market_id)
                continue
            if not prune_expired:
                continue
            self.state.market_chronic_stale_excluded_until.pop(market_id, None)
            self.state.market_chronic_stale_exclusion_started_at.pop(market_id, None)
            self.state.market_chronic_stale_reason_by_market.pop(market_id, None)
            self.state.market_chronic_stale_details_by_market.pop(market_id, None)
        return active

    @staticmethod
    def _chronic_stale_reason_key(
        *,
        stale_enter_count: int,
        max_stale_duration_ms: float,
        min_enter_count: int,
        max_single_duration_ms: float,
    ) -> str:
        repeated_trigger = stale_enter_count >= max(1, min_enter_count)
        long_trigger = max_stale_duration_ms >= max(1.0, max_single_duration_ms)
        if repeated_trigger and long_trigger:
            return CHRONIC_STALE_REASON_BOTH
        if repeated_trigger:
            return CHRONIC_STALE_REASON_REPEATED_STALE_ENTERS
        return CHRONIC_STALE_REASON_LONG_SINGLE_STALE_DURATION

    def _chronic_stale_candidate_metrics(
        self,
        *,
        now_utc: datetime,
        window_minutes: int,
    ) -> dict[str, tuple[int, float]]:
        window_start = now_utc - timedelta(minutes=max(1, int(window_minutes)))
        query = """
        SELECT
          d.market_id,
          SUM(
            CASE WHEN d.event_name = 'market_stale_entered' THEN 1 ELSE 0 END
          ) AS stale_enter_count,
          MAX(
            CASE
              WHEN d.event_name IN ('market_stale_recovered', 'market_stale_episode_closed')
              THEN d.latency_ms
              ELSE NULL
            END
          ) AS max_stale_duration_ms
        FROM diagnostics_events d
        WHERE d.created_at >= ? AND d.created_at < ?
          AND d.run_id = ?
          AND d.market_id IS NOT NULL
          AND d.market_id != ''
          AND d.event_name IN (
            'market_stale_entered',
            'market_stale_recovered',
            'market_stale_episode_closed'
          )
        GROUP BY d.market_id
        """
        rows = self.sqlite_store.conn.execute(
            query,
            (window_start.isoformat(), now_utc.isoformat(), self.state.run_id),
        ).fetchall()
        return {
            str(row[0]): (
                int(row[1] if row[1] is not None else 0),
                float(row[2] if row[2] is not None else 0.0),
            )
            for row in rows
            if str(row[0] or "")
        }

    def _market_chronic_stale_details(self, market_id: str) -> dict[str, object]:
        details = dict(self.state.market_chronic_stale_details_by_market.get(market_id, {}))
        details.update(self._market_context_by_market_id(market_id))
        excluded_until = self.state.market_chronic_stale_excluded_until.get(market_id)
        if excluded_until is not None:
            details["excluded_until"] = excluded_until.isoformat()
        details["chronic_stale_reason"] = self.state.market_chronic_stale_reason_by_market.get(
            market_id, ""
        )
        return details

    def _refresh_chronic_stale_runtime_exclusions(
        self,
        *,
        now_utc: datetime,
    ) -> ChronicStaleExclusionSummary:
        previous_excluded_until = dict(self.state.market_chronic_stale_excluded_until)
        previous_tracked_market_ids = set(previous_excluded_until.keys())
        previous_active_market_ids = self._active_chronic_stale_excluded_market_ids(
            now_utc=now_utc,
            prune_expired=False,
        )
        previous_reasons = dict(self.state.market_chronic_stale_reason_by_market)
        previous_details = {
            market_id: dict(details)
            for market_id, details in self.state.market_chronic_stale_details_by_market.items()
        }
        previous_started_at = dict(self.state.market_chronic_stale_exclusion_started_at)

        self._active_chronic_stale_excluded_market_ids(
            now_utc=now_utc,
            prune_expired=True,
        )

        window_minutes = max(
            1,
            self.config.settings.runtime.market_stale_exclusion_window_minutes,
        )
        min_enter_count = max(
            1,
            self.config.settings.runtime.market_stale_exclusion_min_enter_count,
        )
        max_single_duration_ms = max(
            1.0,
            float(self.config.settings.runtime.market_stale_exclusion_max_single_duration_ms),
        )
        cooldown_ms = max(
            1,
            self.config.settings.runtime.market_stale_exclusion_cooldown_ms,
        )
        cooldown_delta = timedelta(milliseconds=cooldown_ms)

        candidate_metrics = self._chronic_stale_candidate_metrics(
            now_utc=now_utc,
            window_minutes=window_minutes,
        )
        extended_market_ids: set[str] = set()
        extended_reason_counts: dict[str, int] = {}
        extension_ms_by_market: dict[str, float] = {}
        for market_id, (stale_enter_count, max_stale_duration_ms) in candidate_metrics.items():
            repeated_trigger = stale_enter_count >= min_enter_count
            long_trigger = max_stale_duration_ms >= max_single_duration_ms
            if not repeated_trigger and not long_trigger:
                continue
            reason_key = self._chronic_stale_reason_key(
                stale_enter_count=stale_enter_count,
                max_stale_duration_ms=max_stale_duration_ms,
                min_enter_count=min_enter_count,
                max_single_duration_ms=max_single_duration_ms,
            )
            details = {
                **self._market_context_by_market_id(market_id),
                "stale_enter_count": stale_enter_count,
                "max_stale_duration_ms": round(max_stale_duration_ms, 3),
                "window_minutes": window_minutes,
                "min_enter_count": min_enter_count,
                "max_single_duration_ms": round(max_single_duration_ms, 3),
                "cooldown_ms": cooldown_ms,
            }
            excluded_until = now_utc + cooldown_delta
            previous_until = previous_excluded_until.get(market_id)
            if (
                market_id in previous_active_market_ids
                and previous_until is not None
                and excluded_until > previous_until
            ):
                extension_ms = max(
                    0.0,
                    (excluded_until - previous_until).total_seconds() * 1000.0,
                )
                extended_market_ids.add(market_id)
                extended_reason_counts[reason_key] = extended_reason_counts.get(reason_key, 0) + 1
                extension_ms_by_market[market_id] = extension_ms
            if market_id not in self.state.market_chronic_stale_excluded_until:
                self.state.market_chronic_stale_exclusion_started_at[market_id] = now_utc
            self.state.market_chronic_stale_excluded_until[market_id] = excluded_until
            self.state.market_chronic_stale_reason_by_market[market_id] = reason_key
            self.state.market_chronic_stale_details_by_market[market_id] = details

        active_market_ids = self._active_chronic_stale_excluded_market_ids(
            now_utc=now_utc,
            prune_expired=False,
        )
        entered_market_ids = active_market_ids - previous_tracked_market_ids
        cleared_market_ids = previous_tracked_market_ids - active_market_ids

        for market_id in sorted(entered_market_ids):
            details = self._market_chronic_stale_details(market_id)
            self._save_diagnostics_event(
                event_name=DIAG_EVENT_MARKET_CHRONIC_STALE_EXCLUSION_ENTERED,
                now_utc=now_utc,
                market_id=market_id,
                reason=self.state.market_chronic_stale_reason_by_market.get(market_id),
                details=format_kv_details(details),
            )

        for market_id in sorted(extended_market_ids):
            details = self._market_chronic_stale_details(market_id)
            previous_until = previous_excluded_until.get(market_id)
            next_until = self.state.market_chronic_stale_excluded_until.get(market_id)
            if previous_until is not None:
                details["previous_excluded_until"] = previous_until.isoformat()
            if next_until is not None:
                details["next_excluded_until"] = next_until.isoformat()
            details["extension_reason"] = "cooldown_refreshed_by_new_chronic_signal"
            details["extension_ms"] = round(extension_ms_by_market.get(market_id, 0.0), 3)
            self._save_diagnostics_event(
                event_name=DIAG_EVENT_MARKET_CHRONIC_STALE_EXCLUSION_EXTENDED,
                now_utc=now_utc,
                market_id=market_id,
                reason=self.state.market_chronic_stale_reason_by_market.get(market_id),
                latency_ms=extension_ms_by_market.get(market_id),
                details=format_kv_details(details),
            )

        for market_id in sorted(cleared_market_ids):
            started_at = previous_started_at.get(market_id)
            excluded_duration_ms = None
            if started_at is not None:
                excluded_duration_ms = max(0.0, (now_utc - started_at).total_seconds() * 1000.0)
            details = dict(previous_details.get(market_id, {}))
            details.update(self._market_context_by_market_id(market_id))
            details["cleared_reason"] = "cooldown_elapsed"
            if excluded_duration_ms is not None:
                details["exclusion_duration_ms"] = round(excluded_duration_ms, 3)
            self._save_diagnostics_event(
                event_name=DIAG_EVENT_MARKET_CHRONIC_STALE_EXCLUSION_CLEARED,
                now_utc=now_utc,
                market_id=market_id,
                reason=previous_reasons.get(market_id, CHRONIC_STALE_REASON_REPEATED_STALE_ENTERS),
                latency_ms=excluded_duration_ms,
                details=format_kv_details(details),
            )

        reason_counts: dict[str, int] = {}
        for market_id in active_market_ids:
            reason = self.state.market_chronic_stale_reason_by_market.get(
                market_id,
                CHRONIC_STALE_REASON_REPEATED_STALE_ENTERS,
            )
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        active_market_ages_ms: dict[str, float] = {}
        for market_id in active_market_ids:
            started_at = self.state.market_chronic_stale_exclusion_started_at.get(market_id)
            if started_at is None:
                continue
            age_ms = max(0.0, (now_utc - started_at).total_seconds() * 1000.0)
            active_market_ages_ms[market_id] = age_ms
        avg_active_age_ms = (
            sum(active_market_ages_ms.values()) / len(active_market_ages_ms)
            if active_market_ages_ms
            else 0.0
        )
        long_active_threshold_ms = float(cooldown_ms) * CHRONIC_STALE_LONG_ACTIVE_MULTIPLIER
        long_active_market_ages_ms = {
            market_id: age_ms
            for market_id, age_ms in active_market_ages_ms.items()
            if age_ms >= long_active_threshold_ms
        }
        self.state.market_chronic_stale_enter_count_last_cycle = len(entered_market_ids)
        self.state.market_chronic_stale_cleared_count_last_cycle = len(cleared_market_ids)
        return ChronicStaleExclusionSummary(
            active_market_ids=active_market_ids,
            entered_market_ids=entered_market_ids,
            extended_market_ids=extended_market_ids,
            cleared_market_ids=cleared_market_ids,
            reason_counts=reason_counts,
            extended_reason_counts=extended_reason_counts,
            avg_active_age_ms=avg_active_age_ms,
            long_active_market_ages_ms=long_active_market_ages_ms,
        )

    def _record_chronic_stale_exclusion_metrics(
        self,
        *,
        summary: ChronicStaleExclusionSummary,
        now_utc: datetime,
    ) -> None:
        reintroduced_market_ids = set(self.state.last_refresh_chronic_reintroduced_market_ids)
        watched_chronic_market_ids = set(self.state.last_refresh_watched_chronic_stale_market_ids)
        reintroduced_reason_by_market = dict(
            self.state.last_refresh_chronic_reintroduced_reason_by_market
        )
        watched_chronic_reason_by_market = dict(
            self.state.last_refresh_watched_chronic_stale_reason_by_market
        )
        metrics = {
            "chronic_stale_excluded_market_count": float(len(summary.active_market_ids)),
            "chronic_stale_exclusion_enter_count": float(len(summary.entered_market_ids)),
            "chronic_stale_exclusion_extended_count": float(len(summary.extended_market_ids)),
            "chronic_stale_exclusion_active_count": float(len(summary.active_market_ids)),
            "chronic_stale_exclusion_cleared_count": float(len(summary.cleared_market_ids)),
            "chronic_stale_exclusion_avg_active_age_ms": float(summary.avg_active_age_ms),
            "chronic_stale_exclusion_long_active_market_count": float(
                len(summary.long_active_market_ages_ms)
            ),
            "chronic_stale_reintroduced_for_floor_count": float(len(reintroduced_market_ids)),
            "chronic_stale_reintroduced_market_count": float(len(watched_chronic_market_ids)),
            "watched_chronic_stale_excluded_market_count": float(len(watched_chronic_market_ids)),
        }
        active_summary = ",".join(
            f"{market_id}:{self.state.market_chronic_stale_reason_by_market.get(market_id, '')}"
            for market_id in sorted(summary.active_market_ids)[:10]
        )
        reintroduced_summary = ",".join(
            f"{market_id}:{reintroduced_reason_by_market.get(market_id, '')}"
            for market_id in sorted(reintroduced_market_ids)[:10]
        )
        watched_chronic_summary = ",".join(
            f"{market_id}:{watched_chronic_reason_by_market.get(market_id, '')}"
            for market_id in sorted(watched_chronic_market_ids)[:10]
        )
        long_active_summary = ",".join(
            f"{market_id}:{round(age_ms, 1)}"
            for market_id, age_ms in sorted(
                summary.long_active_market_ages_ms.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:10]
        )
        metric_details = {
            "chronic_stale_reintroduced_for_floor_count": reintroduced_summary,
            "chronic_stale_reintroduced_market_count": watched_chronic_summary,
            "watched_chronic_stale_excluded_market_count": watched_chronic_summary,
            "chronic_stale_exclusion_long_active_market_count": long_active_summary,
        }
        for metric_name, metric_value in metrics.items():
            details = metric_details.get(metric_name, active_summary)
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
                    "active_markets": len(summary.active_market_ids),
                    "details": details,
                },
                now_utc=now_utc,
            )
        for reason_key, count in sorted(summary.reason_counts.items()):
            metric_name = f"chronic_stale_exclusion_reason:{reason_key}"
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(count),
                details=f"active_markets={len(summary.active_market_ids)}",
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now_utc.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": count,
                    "active_markets": len(summary.active_market_ids),
                },
                now_utc=now_utc,
            )
        for reason_key, count in sorted(summary.extended_reason_counts.items()):
            metric_name = f"chronic_stale_exclusion_extended_reason:{reason_key}"
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(count),
                details=f"extended_markets={len(summary.extended_market_ids)}",
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now_utc.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": count,
                    "extended_markets": len(summary.extended_market_ids),
                },
                now_utc=now_utc,
            )

        reintroduced_reason_counts: dict[str, int] = {}
        for market_id in reintroduced_market_ids:
            reason = reintroduced_reason_by_market.get(
                market_id,
                "watched_floor_backfill_chronic_relaxed",
            )
            reintroduced_reason_counts[reason] = reintroduced_reason_counts.get(reason, 0) + 1
        for reason_key, count in sorted(reintroduced_reason_counts.items()):
            metric_name = f"chronic_stale_reintroduced_reason:{reason_key}"
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(count),
                details=f"reintroduced_markets={len(reintroduced_market_ids)}",
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now_utc.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": count,
                    "reintroduced_markets": len(reintroduced_market_ids),
                },
                now_utc=now_utc,
            )

        watched_reason_counts: dict[str, int] = {}
        for market_id in watched_chronic_market_ids:
            reason = watched_chronic_reason_by_market.get(
                market_id,
                "watched_chronic_stale_excluded",
            )
            watched_reason_counts[reason] = watched_reason_counts.get(reason, 0) + 1
        for reason_key, count in sorted(watched_reason_counts.items()):
            metric_name = f"watched_chronic_stale_reason:{reason_key}"
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(count),
                details=f"watched_chronic_markets={len(watched_chronic_market_ids)}",
                created_at_iso=now_utc.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now_utc.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": count,
                    "watched_chronic_markets": len(watched_chronic_market_ids),
                },
                now_utc=now_utc,
            )

    def _runtime_low_quality_excluded_market_ids(self) -> set[str]:
        threshold = max(1, self.config.settings.runtime.low_quality_market_penalty_threshold)
        min_observations = max(
            0,
            self.config.settings.runtime.low_quality_market_min_observations,
        )
        exclusion_cycles = max(
            1,
            self.config.settings.runtime.low_quality_market_exclusion_consecutive_cycles,
        )
        candidate_penalty_ratio = (
            self.config.settings.runtime.low_quality_market_exclusion_candidate_penalty_ratio
        )
        candidate_cutoff = max(
            threshold,
            int(
                threshold
                * max(
                    0.0,
                    candidate_penalty_ratio,
                )
            ),
        )
        excluded: set[str] = set()
        exclusion_reason_by_market: dict[str, str] = {}
        current_markets = set(self.markets_by_id.keys())
        watched_floor = self._effective_watched_floor()
        if (
            self.config.settings.runtime.watched_floor_relax_runtime_exclusion
            and len(current_markets) > 0
            and len(current_markets) <= watched_floor
        ):
            for market_id, penalty in self.state.market_quality_penalty_by_market.items():
                consecutive_bad_cycles = self.state.market_low_quality_consecutive_cycles.get(
                    market_id,
                    0,
                )
                self.state.market_quality_stage_by_market[market_id] = self._market_quality_stage(
                    penalty=penalty,
                    consecutive_bad_cycles=consecutive_bad_cycles,
                    excluded=False,
                )
            self.state.market_exclusion_reason_by_market = exclusion_reason_by_market
            self._save_last_refresh_runtime_exclusions(
                excluded_market_ids=excluded,
                reason_by_market=exclusion_reason_by_market,
            )
            return excluded
        for market_id, penalty in self.state.market_quality_penalty_by_market.items():
            observed = self.state.market_refresh_observed_count.get(market_id, 0)
            consecutive_bad_cycles = self.state.market_low_quality_consecutive_cycles.get(
                market_id,
                0,
            )
            should_exclude = (
                observed >= min_observations
                and penalty >= candidate_cutoff
                and consecutive_bad_cycles >= exclusion_cycles
            )
            if observed < min_observations:
                self.state.market_quality_stage_by_market[market_id] = self._market_quality_stage(
                    penalty=penalty,
                    consecutive_bad_cycles=consecutive_bad_cycles,
                    excluded=False,
                )
                continue
            is_current = market_id in current_markets
            can_exclude_current = len(current_markets) > watched_floor
            if should_exclude and (not is_current or can_exclude_current):
                excluded.add(market_id)
                exclusion_reason_by_market[market_id] = (
                    f"stage={MARKET_QUALITY_EXCLUDED};penalty={penalty};"
                    f"consecutive_bad_cycles={consecutive_bad_cycles};"
                    f"observed={observed};is_current={int(is_current)}"
                )
                self.state.market_quality_stage_by_market[market_id] = MARKET_QUALITY_EXCLUDED
                continue
            self.state.market_quality_stage_by_market[market_id] = self._market_quality_stage(
                penalty=penalty,
                consecutive_bad_cycles=consecutive_bad_cycles,
                excluded=False,
            )
        self.state.market_exclusion_reason_by_market = exclusion_reason_by_market
        self._save_last_refresh_runtime_exclusions(
            excluded_market_ids=excluded,
            reason_by_market=exclusion_reason_by_market,
        )
        return excluded

    async def _fetch_market_snapshot(self) -> MarketSnapshot:
        raw_markets = await self.gamma_client.fetch_active_markets(
            page_size=self.config.settings.api.gamma_page_size,
            max_pages=self.config.settings.api.gamma_max_pages,
        )
        low_quality_excluded_market_ids = self._runtime_low_quality_excluded_market_ids()
        chronic_excluded_market_ids = self._active_chronic_stale_excluded_market_ids(
            now_utc=utc_now(),
            prune_expired=False,
        )
        runtime_excluded_market_ids = set(low_quality_excluded_market_ids) | set(
            chronic_excluded_market_ids
        )
        runtime_excluded_reason_by_market = {
            market_id: "low_quality_runtime" for market_id in low_quality_excluded_market_ids
        }
        for market_id in chronic_excluded_market_ids:
            runtime_excluded_reason_by_market[market_id] = "chronic_stale_runtime"
        extraction = extract_binary_markets_with_stats(
            raw_markets=raw_markets,
            market_filters=self.config.settings.market_filters,
            markets_config=self.config.markets,
            preferred_market_ids=set(self.markets_by_id.keys()),
            runtime_excluded_market_ids=runtime_excluded_market_ids,
            runtime_excluded_reason_by_market=runtime_excluded_reason_by_market,
        )
        chronic_reintroduced_for_floor_market_ids: set[str] = set()
        chronic_reintroduced_reason_by_market: dict[str, str] = {}
        watched_floor = self._effective_watched_floor()
        if watched_floor > 0 and len(extraction.markets) < watched_floor:
            relaxed_filters = self._relaxed_market_filters_for_watched_floor()
            existing_market_ids = {market.market_id for market in extraction.markets}
            supplements: list[BinaryMarket] = []

            stage1_extraction = extract_binary_markets_with_stats(
                raw_markets=raw_markets,
                market_filters=relaxed_filters,
                markets_config=self.config.markets,
                preferred_market_ids=set(self.markets_by_id.keys()),
                runtime_excluded_market_ids=runtime_excluded_market_ids,
                runtime_excluded_reason_by_market=runtime_excluded_reason_by_market,
            )
            needed = max(0, watched_floor - len(extraction.markets))
            stage1_supplements = [
                market
                for market in stage1_extraction.markets
                if market.market_id not in existing_market_ids
            ][:needed]
            if stage1_supplements:
                supplements.extend(stage1_supplements)
                existing_market_ids.update(market.market_id for market in stage1_supplements)
                extraction.excluded_counts["watched_floor_backfilled_non_excluded"] = (
                    extraction.excluded_counts.get("watched_floor_backfilled_non_excluded", 0)
                    + len(stage1_supplements)
                )

            stage2_supplements: list[BinaryMarket] = []
            if (
                self.config.settings.runtime.watched_floor_relax_runtime_exclusion
                and len(extraction.markets) + len(supplements) < watched_floor
            ):
                chronic_only_reason_map = {
                    market_id: "chronic_stale_runtime" for market_id in chronic_excluded_market_ids
                }
                stage2_extraction = extract_binary_markets_with_stats(
                    raw_markets=raw_markets,
                    market_filters=relaxed_filters,
                    markets_config=self.config.markets,
                    preferred_market_ids=set(self.markets_by_id.keys()),
                    runtime_excluded_market_ids=set(chronic_excluded_market_ids),
                    runtime_excluded_reason_by_market=chronic_only_reason_map,
                )
                needed = max(0, watched_floor - (len(extraction.markets) + len(supplements)))
                stage2_supplements = [
                    market
                    for market in stage2_extraction.markets
                    if (
                        market.market_id in low_quality_excluded_market_ids
                        and market.market_id not in existing_market_ids
                    )
                ][:needed]
                if stage2_supplements:
                    supplements.extend(stage2_supplements)
                    existing_market_ids.update(market.market_id for market in stage2_supplements)
                    extraction.excluded_counts["watched_floor_backfilled_low_quality_runtime"] = (
                        extraction.excluded_counts.get(
                            "watched_floor_backfilled_low_quality_runtime",
                            0,
                        )
                        + len(stage2_supplements)
                    )

            stage3_supplements: list[BinaryMarket] = []
            if (
                self.config.settings.runtime.watched_floor_relax_chronic_stale_exclusion
                and len(extraction.markets) + len(supplements) < watched_floor
            ):
                stage3_runtime_excluded_ids = (
                    set()
                    if self.config.settings.runtime.watched_floor_relax_runtime_exclusion
                    else set(low_quality_excluded_market_ids)
                )
                stage3_runtime_reason_map = (
                    None
                    if not stage3_runtime_excluded_ids
                    else {
                        market_id: "low_quality_runtime"
                        for market_id in stage3_runtime_excluded_ids
                    }
                )
                stage3_extraction = extract_binary_markets_with_stats(
                    raw_markets=raw_markets,
                    market_filters=relaxed_filters,
                    markets_config=self.config.markets,
                    preferred_market_ids=set(self.markets_by_id.keys()),
                    runtime_excluded_market_ids=stage3_runtime_excluded_ids,
                    runtime_excluded_reason_by_market=stage3_runtime_reason_map,
                )
                needed = max(0, watched_floor - (len(extraction.markets) + len(supplements)))
                stage3_supplements = [
                    market
                    for market in stage3_extraction.markets
                    if (
                        market.market_id in chronic_excluded_market_ids
                        and market.market_id not in existing_market_ids
                    )
                ][:needed]
                if stage3_supplements:
                    supplements.extend(stage3_supplements)
                    existing_market_ids.update(market.market_id for market in stage3_supplements)
                    extraction.excluded_counts["watched_floor_backfilled_chronic_stale_runtime"] = (
                        extraction.excluded_counts.get(
                            "watched_floor_backfilled_chronic_stale_runtime",
                            0,
                        )
                        + len(stage3_supplements)
                    )
                    for market in stage3_supplements:
                        chronic_reintroduced_for_floor_market_ids.add(market.market_id)
                        chronic_reintroduced_reason_by_market[market.market_id] = (
                            "watched_floor_backfill_chronic_relaxed"
                        )

            if supplements:
                extraction.markets = [*extraction.markets, *supplements]
                extraction.excluded_counts["watched_floor_backfilled"] = (
                    extraction.excluded_counts.get("watched_floor_backfilled", 0) + len(supplements)
                )
                self.logger.info(
                    (
                        "Watched floor backfill applied floor=%s base=%s added=%s "
                        "non_excluded=%s low_quality=%s chronic=%s "
                        "low_quality_relaxed=%s chronic_relaxed=%s activity_relaxed=%s"
                    ),
                    watched_floor,
                    len(extraction.markets) - len(supplements),
                    len(supplements),
                    len(stage1_supplements),
                    len(stage2_supplements),
                    len(stage3_supplements),
                    self.config.settings.runtime.watched_floor_relax_runtime_exclusion,
                    self.config.settings.runtime.watched_floor_relax_chronic_stale_exclusion,
                    self.config.settings.runtime.watched_floor_relax_activity_filters,
                )
        markets_by_id = {market.market_id: market for market in extraction.markets}
        watched_chronic_stale_market_ids = {
            market_id for market_id in markets_by_id if market_id in chronic_excluded_market_ids
        }
        watched_chronic_stale_reason_by_market = {
            market_id: chronic_reintroduced_reason_by_market.get(
                market_id,
                "watched_chronic_stale_excluded",
            )
            for market_id in watched_chronic_stale_market_ids
        }
        token_to_market_side: dict[str, tuple[str, str]] = {}
        for market in extraction.markets:
            token_to_market_side[market.yes_token_id] = (market.market_id, "yes")
            token_to_market_side[market.no_token_id] = (market.market_id, "no")
        return MarketSnapshot(
            extraction_raw_market_count=extraction.raw_market_count,
            excluded_counts=extraction.excluded_counts,
            markets_by_id=markets_by_id,
            token_to_market_side=token_to_market_side,
            chronic_reintroduced_for_floor_market_ids=chronic_reintroduced_for_floor_market_ids,
            chronic_reintroduced_reason_by_market=chronic_reintroduced_reason_by_market,
            watched_chronic_stale_market_ids=watched_chronic_stale_market_ids,
            watched_chronic_stale_reason_by_market=watched_chronic_stale_reason_by_market,
        )

    async def _apply_market_snapshot(
        self,
        *,
        snapshot: MarketSnapshot,
        previous_asset_ids: set[str],
    ) -> MarketLoadResult:
        previous_market_ids = set(self.markets_by_id.keys())
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
        current_markets = set(self.markets_by_id.keys())
        added_markets = sorted(current_markets - previous_market_ids)
        if added_assets:
            await self._load_initial_tick_sizes(added_assets)

        self.state.watched_markets = len(self.markets_by_id)
        self.state.subscribed_assets = len(asset_ids)
        self.state.cumulative_watched_market_ids.update(self.markets_by_id.keys())
        self.state.cumulative_subscribed_asset_ids.update(asset_ids)
        self.state.last_refresh_chronic_reintroduced_market_ids = set(
            snapshot.chronic_reintroduced_for_floor_market_ids
        )
        self.state.last_refresh_chronic_reintroduced_reason_by_market = dict(
            snapshot.chronic_reintroduced_reason_by_market
        )
        self.state.last_refresh_watched_chronic_stale_market_ids = set(
            snapshot.watched_chronic_stale_market_ids
        )
        self.state.last_refresh_watched_chronic_stale_reason_by_market = dict(
            snapshot.watched_chronic_stale_reason_by_market
        )
        for market_id in sorted(snapshot.chronic_reintroduced_for_floor_market_ids):
            reason = snapshot.chronic_reintroduced_reason_by_market.get(
                market_id,
                "watched_floor_backfill_chronic_relaxed",
            )
            details = {
                **self._market_context_by_market_id(market_id),
                "reintroduced_reason": reason,
                "watched_floor": self._effective_watched_floor(),
                "low_quality_relax_enabled": int(
                    self.config.settings.runtime.watched_floor_relax_runtime_exclusion
                ),
                "chronic_relax_enabled": int(
                    self.config.settings.runtime.watched_floor_relax_chronic_stale_exclusion
                ),
            }
            self._save_diagnostics_event(
                event_name=DIAG_EVENT_MARKET_CHRONIC_STALE_REINTRODUCED_FOR_FLOOR,
                now_utc=now,
                market_id=market_id,
                reason=reason,
                details=format_kv_details(details),
            )

        self.state.safe_mode_blocked_assets &= current_asset_ids
        self.state.safe_mode_blocked_markets &= current_markets
        self.state.ready_assets &= current_asset_ids
        self.state.ever_ready_assets &= current_asset_ids
        self.state.asset_quote_update_count = {
            asset_id: count
            for asset_id, count in self.state.asset_quote_update_count.items()
            if asset_id in current_asset_ids
        }
        self.state.asset_unhealthy_consecutive_cycles = {
            asset_id: count
            for asset_id, count in self.state.asset_unhealthy_consecutive_cycles.items()
            if asset_id in current_asset_ids
        }
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
        self.state.market_probation_until = {
            market_id: probation_until
            for market_id, probation_until in self.state.market_probation_until.items()
            if market_id in current_markets
        }
        self.state.market_freshness_state_by_market = {
            market_id: state
            for market_id, state in self.state.market_freshness_state_by_market.items()
            if market_id in current_markets
        }
        self.state.market_freshness_details_by_market = {
            market_id: details
            for market_id, details in self.state.market_freshness_details_by_market.items()
            if market_id in current_markets
        }
        self.state.market_freshness_updated_at = {
            market_id: ts
            for market_id, ts in self.state.market_freshness_updated_at.items()
            if market_id in current_markets
        }
        self.state.market_stale_started_at_by_market = {
            market_id: started_at
            for market_id, started_at in self.state.market_stale_started_at_by_market.items()
            if market_id in current_markets
        }
        self.state.market_stale_reason_by_market = {
            market_id: reason
            for market_id, reason in self.state.market_stale_reason_by_market.items()
            if market_id in current_markets
        }
        self.state.market_stale_details_by_market = {
            market_id: details
            for market_id, details in self.state.market_stale_details_by_market.items()
            if market_id in current_markets
        }
        self.state.market_low_quality_consecutive_cycles = {
            market_id: cycles
            for market_id, cycles in self.state.market_low_quality_consecutive_cycles.items()
            if market_id in current_markets
        }
        self.state.market_quality_stage_by_market = {
            market_id: stage
            for market_id, stage in self.state.market_quality_stage_by_market.items()
            if market_id in current_markets
        }
        self.state.market_exclusion_reason_by_market = {
            market_id: reason
            for market_id, reason in self.state.market_exclusion_reason_by_market.items()
            if market_id in current_markets
        }
        active_chronic_exclusions = self._active_chronic_stale_excluded_market_ids(
            now_utc=now,
            prune_expired=False,
        )
        # Keep expired exclusions until refresh handles clear-event emission and cleanup.
        tracked_chronic_markets = (
            current_markets
            | active_chronic_exclusions
            | set(self.state.market_chronic_stale_excluded_until.keys())
        )
        self.state.market_chronic_stale_excluded_until = {
            market_id: excluded_until
            for market_id, excluded_until in self.state.market_chronic_stale_excluded_until.items()
            if market_id in tracked_chronic_markets
        }
        self.state.market_chronic_stale_exclusion_started_at = {
            market_id: started_at
            for market_id, started_at in (
                self.state.market_chronic_stale_exclusion_started_at.items()
            )
            if market_id in tracked_chronic_markets
        }
        self.state.market_chronic_stale_reason_by_market = {
            market_id: reason
            for market_id, reason in self.state.market_chronic_stale_reason_by_market.items()
            if market_id in tracked_chronic_markets
        }
        self.state.market_chronic_stale_details_by_market = {
            market_id: details
            for market_id, details in self.state.market_chronic_stale_details_by_market.items()
            if market_id in tracked_chronic_markets
        }
        self.state.eligible_markets &= current_markets
        self.state.last_no_signal_reason_by_market = {
            market_id: reason
            for market_id, reason in self.state.last_no_signal_reason_by_market.items()
            if market_id in current_markets
        }
        self.state.last_no_signal_reason_at_by_market = {
            market_id: ts
            for market_id, ts in self.state.last_no_signal_reason_at_by_market.items()
            if market_id in current_markets
        }
        self.state.asset_recovering_until = {
            asset_id: recovering_until
            for asset_id, recovering_until in self.state.asset_recovering_until.items()
            if asset_id in current_asset_ids
        }
        self.state.asset_recovering_started_at = {
            asset_id: started_at
            for asset_id, started_at in self.state.asset_recovering_started_at.items()
            if asset_id in current_asset_ids
        }
        self.state.asset_recovery_started_at = {
            asset_id: started_at
            for asset_id, started_at in self.state.asset_recovery_started_at.items()
            if asset_id in current_asset_ids
        }
        self.state.asset_first_quote_after_recovery_at = {
            asset_id: quoted_at
            for asset_id, quoted_at in self.state.asset_first_quote_after_recovery_at.items()
            if asset_id in current_asset_ids
        }
        self.state.asset_recovery_reason_by_asset = {
            asset_id: reason
            for asset_id, reason in self.state.asset_recovery_reason_by_asset.items()
            if asset_id in current_asset_ids
        }
        self.state.asset_first_quote_block_reason_by_asset = {
            asset_id: reason
            for asset_id, reason in self.state.asset_first_quote_block_reason_by_asset.items()
            if asset_id in current_asset_ids
        }
        self.state.asset_book_ready_block_reason_by_asset = {
            asset_id: reason
            for asset_id, reason in self.state.asset_book_ready_block_reason_by_asset.items()
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
        self.state.market_recovery_started_at_for_diagnostics = {
            market_id: started_at
            for market_id, started_at in (
                self.state.market_recovery_started_at_for_diagnostics.items()
            )
            if market_id in current_markets
        }
        self.state.market_recovery_reason_by_market = {
            market_id: reason
            for market_id, reason in self.state.market_recovery_reason_by_market.items()
            if market_id in current_markets
        }
        self.state.market_ready_block_reason_by_market = {
            market_id: reason
            for market_id, reason in self.state.market_ready_block_reason_by_market.items()
            if market_id in current_markets
        }
        self.state.eligibility_gate_reason_by_market = {
            market_id: reason
            for market_id, reason in self.state.eligibility_gate_reason_by_market.items()
            if market_id in current_markets
        }
        self.state.eligibility_gate_reason_at_by_market = {
            market_id: ts
            for market_id, ts in self.state.eligibility_gate_reason_at_by_market.items()
            if market_id in current_markets
        }
        for asset_id in asset_ids:
            self.state.asset_subscribed_at.setdefault(asset_id, now)
            self.state.asset_quote_update_count.setdefault(asset_id, 0)
        probation_ms = max(0, self.config.settings.runtime.market_probation_ms)
        for market_id in added_markets:
            self.state.market_probation_until[market_id] = now + timedelta(
                milliseconds=probation_ms
            )
        for market_id in current_markets:
            self.state.market_refresh_observed_count[market_id] = (
                self.state.market_refresh_observed_count.get(market_id, 0) + 1
            )
            self.state.market_quality_penalty_by_market.setdefault(market_id, 0)
            self.state.market_low_quality_consecutive_cycles.setdefault(market_id, 0)
            self.state.market_quality_stage_by_market.setdefault(
                market_id,
                MARKET_QUALITY_HEALTHY,
            )
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
                "added_assets=%s removed_assets=%s universe_limit=%s watched_floor=%s"
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
            self._effective_watched_floor(),
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
            self.state.ws_health_state = "healthy"

            for update in updates:
                self.healthcheck.on_ws_message(now)
                self.healthcheck.on_asset_quote_update(update.asset_id, update.timestamp)
                self._record_first_quote_after_resync(asset_id=update.asset_id, now_utc=now)
                mapping = self.token_to_market_side.get(update.asset_id)
                if mapping is None:
                    continue

                market_id, side = mapping
                self.state.asset_quote_update_count[update.asset_id] = (
                    self.state.asset_quote_update_count.get(update.asset_id, 0) + 1
                )
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
                self._maybe_finish_connection_recovery(now_utc=now)

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
                readiness_ok, readiness_reason, readiness_details = self._evaluate_market_readiness(
                    market=market,
                    now_utc=now,
                )
                if not readiness_ok:
                    self._record_no_signal_reason(
                        reason=readiness_reason,
                        now_utc=now,
                        market_id=market_id,
                        details=readiness_details,
                    )
                    continue
                eligibility_ok, eligibility_reason, eligibility_details = (
                    self._evaluate_market_signal_eligibility(
                        market=market,
                        now_utc=now,
                    )
                )
                if not eligibility_ok:
                    self._record_no_signal_reason(
                        reason=eligibility_reason,
                        now_utc=now,
                        market_id=market_id,
                        details=eligibility_details,
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
                yes_quote, no_quote = self.quote_manager.get_market_quotes(market_id)
                if yes_quote is None or no_quote is None:
                    self._record_no_signal_reason(
                        reason="market_not_ready",
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
            "universe_min_watched_markets_floor": float(self._effective_watched_floor()),
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

    def _save_diagnostics_event(
        self,
        *,
        event_name: str,
        now_utc: datetime,
        asset_id: str | None = None,
        market_id: str | None = None,
        reason: str | None = None,
        latency_ms: float | None = None,
        details: str = "",
    ) -> None:
        self.sqlite_store.save_diagnostics_event(
            event_name=event_name,
            asset_id=asset_id,
            market_id=market_id,
            reason=reason,
            latency_ms=latency_ms,
            details=details,
            created_at_iso=now_utc.isoformat(),
            run_id=self.state.run_id,
        )

    def _start_market_recovery_tracking(
        self,
        *,
        market_id: str,
        reason: str,
        now_utc: datetime,
    ) -> None:
        if market_id not in self.markets_by_id:
            return
        if market_id in self.state.market_recovery_started_at_for_diagnostics:
            return
        self.state.market_recovery_started_at_for_diagnostics[market_id] = now_utc
        self.state.market_recovery_reason_by_market[market_id] = reason
        self.state.market_ready_block_reason_by_market.pop(market_id, None)
        self._save_diagnostics_event(
            event_name=DIAG_EVENT_MARKET_RECOVERY_STARTED,
            now_utc=now_utc,
            market_id=market_id,
            reason=reason,
        )

    def _start_asset_recovery_tracking(
        self, *, asset_id: str, reason: str, now_utc: datetime
    ) -> None:
        self.state.asset_recovery_started_at[asset_id] = now_utc
        self.state.asset_first_quote_after_recovery_at.pop(asset_id, None)
        self.state.asset_recovery_reason_by_asset[asset_id] = reason
        self.state.asset_first_quote_block_reason_by_asset.pop(asset_id, None)
        self.state.asset_book_ready_block_reason_by_asset.pop(asset_id, None)
        mapping = self.token_to_market_side.get(asset_id)
        market_id = mapping[0] if mapping is not None else None
        if market_id:
            self._start_market_recovery_tracking(
                market_id=market_id,
                reason=reason,
                now_utc=now_utc,
            )

    def _record_first_quote_after_resync(self, *, asset_id: str, now_utc: datetime) -> None:
        started_at = self.state.asset_recovery_started_at.get(asset_id)
        if started_at is None:
            return
        if asset_id in self.state.asset_first_quote_after_recovery_at:
            return
        latency_ms = max(0.0, (now_utc - started_at).total_seconds() * 1000.0)
        self.state.asset_first_quote_after_recovery_at[asset_id] = now_utc
        self.state.asset_first_quote_block_reason_by_asset.pop(asset_id, None)
        mapping = self.token_to_market_side.get(asset_id)
        market_id = mapping[0] if mapping is not None else None
        self._save_diagnostics_event(
            event_name=DIAG_EVENT_FIRST_QUOTE_AFTER_RESYNC,
            now_utc=now_utc,
            asset_id=asset_id,
            market_id=market_id,
            reason=self.state.asset_recovery_reason_by_asset.get(asset_id),
            latency_ms=latency_ms,
        )

    def _clear_asset_recovery_tracking(self, *, asset_id: str) -> None:
        self.state.asset_recovery_started_at.pop(asset_id, None)
        self.state.asset_first_quote_after_recovery_at.pop(asset_id, None)
        self.state.asset_recovery_reason_by_asset.pop(asset_id, None)
        self.state.asset_first_quote_block_reason_by_asset.pop(asset_id, None)
        self.state.asset_book_ready_block_reason_by_asset.pop(asset_id, None)

    def _record_book_ready_after_resync(self, *, asset_id: str, now_utc: datetime) -> None:
        started_at = self.state.asset_recovery_started_at.get(asset_id)
        if started_at is None:
            return
        # Book-ready can happen in the same update as first quote.
        self._record_first_quote_after_resync(asset_id=asset_id, now_utc=now_utc)
        self.state.asset_book_ready_block_reason_by_asset.pop(asset_id, None)
        latency_ms = max(0.0, (now_utc - started_at).total_seconds() * 1000.0)
        mapping = self.token_to_market_side.get(asset_id)
        market_id = mapping[0] if mapping is not None else None
        self._save_diagnostics_event(
            event_name=DIAG_EVENT_BOOK_READY_AFTER_RESYNC,
            now_utc=now_utc,
            asset_id=asset_id,
            market_id=market_id,
            reason=self.state.asset_recovery_reason_by_asset.get(asset_id),
            latency_ms=latency_ms,
        )
        self._clear_asset_recovery_tracking(asset_id=asset_id)

    def _record_market_ready_after_recovery(self, *, market_id: str, now_utc: datetime) -> None:
        started_at = self.state.market_recovery_started_at_for_diagnostics.get(market_id)
        if started_at is None:
            return
        latency_ms = max(0.0, (now_utc - started_at).total_seconds() * 1000.0)
        reason = self.state.market_recovery_reason_by_market.get(market_id)
        self.state.market_ready_block_reason_by_market.pop(market_id, None)
        self._save_diagnostics_event(
            event_name=DIAG_EVENT_MARKET_READY_AFTER_RECOVERY,
            now_utc=now_utc,
            market_id=market_id,
            reason=reason,
            latency_ms=latency_ms,
        )
        self.state.market_recovery_started_at_for_diagnostics.pop(market_id, None)
        self.state.market_recovery_reason_by_market.pop(market_id, None)

    def _record_asset_recovery_blocked_stage(
        self,
        *,
        asset_id: str,
        event_name: str,
        reason: str,
        now_utc: datetime,
        latency_anchor: datetime,
        stage_map: dict[str, str],
    ) -> None:
        normalized_reason = (reason or "unknown").strip() or "unknown"
        previous_reason = stage_map.get(asset_id)
        if previous_reason == normalized_reason:
            return
        stage_map[asset_id] = normalized_reason
        recovery_reason = self.state.asset_recovery_reason_by_asset.get(asset_id)
        mapping = self.token_to_market_side.get(asset_id)
        market_id = mapping[0] if mapping is not None else None
        side = mapping[1] if mapping is not None else ""
        latency_ms = max(0.0, (now_utc - latency_anchor).total_seconds() * 1000.0)
        self._save_diagnostics_event(
            event_name=event_name,
            now_utc=now_utc,
            asset_id=asset_id,
            market_id=market_id,
            reason=normalized_reason,
            latency_ms=latency_ms,
            details=format_kv_details(
                {
                    "recovery_reason": recovery_reason or "",
                    "side": side,
                    "stage": event_name,
                }
            ),
        )

    def _record_first_quote_after_resync_blocked(self, *, asset_id: str, now_utc: datetime) -> None:
        started_at = self.state.asset_recovery_started_at.get(asset_id)
        if started_at is None:
            return
        if asset_id in self.state.asset_first_quote_after_recovery_at:
            return
        reason = self._classify_missing_book_reason(asset_id=asset_id, now_utc=now_utc)
        self._record_asset_recovery_blocked_stage(
            asset_id=asset_id,
            event_name=DIAG_EVENT_FIRST_QUOTE_AFTER_RESYNC_BLOCKED,
            reason=reason,
            now_utc=now_utc,
            latency_anchor=started_at,
            stage_map=self.state.asset_first_quote_block_reason_by_asset,
        )

    def _record_book_ready_after_resync_blocked(self, *, asset_id: str, now_utc: datetime) -> None:
        first_quote_at = self.state.asset_first_quote_after_recovery_at.get(asset_id)
        if first_quote_at is None:
            return
        if self.quote_manager.is_asset_ready(asset_id):
            return
        reason = self._classify_missing_book_reason(asset_id=asset_id, now_utc=now_utc)
        self._record_asset_recovery_blocked_stage(
            asset_id=asset_id,
            event_name=DIAG_EVENT_BOOK_READY_AFTER_RESYNC_BLOCKED,
            reason=reason,
            now_utc=now_utc,
            latency_anchor=first_quote_at,
            stage_map=self.state.asset_book_ready_block_reason_by_asset,
        )

    def _record_market_ready_after_recovery_blocked(
        self,
        *,
        market_id: str,
        reason: str,
        details: dict[str, object] | None,
        now_utc: datetime,
    ) -> None:
        started_at = self.state.market_recovery_started_at_for_diagnostics.get(market_id)
        if started_at is None:
            return
        normalized_reason = (reason or "unknown").strip() or "unknown"
        previous_reason = self.state.market_ready_block_reason_by_market.get(market_id)
        if previous_reason == normalized_reason:
            return
        self.state.market_ready_block_reason_by_market[market_id] = normalized_reason
        recovery_reason = self.state.market_recovery_reason_by_market.get(market_id)
        latency_ms = max(0.0, (now_utc - started_at).total_seconds() * 1000.0)
        self._save_diagnostics_event(
            event_name=DIAG_EVENT_MARKET_READY_AFTER_RECOVERY_BLOCKED,
            now_utc=now_utc,
            market_id=market_id,
            reason=normalized_reason,
            latency_ms=latency_ms,
            details=format_kv_details(
                {
                    "recovery_reason": recovery_reason or "",
                    "stage": DIAG_EVENT_MARKET_READY_AFTER_RECOVERY_BLOCKED,
                    "stale_reason_key": normalize_stale_reason_key(
                        str((details or {}).get("stale_reason_key", ""))
                    ),
                    "stale_side": normalize_stale_side(str((details or {}).get("stale_side", ""))),
                    "stale_asset_ids": str((details or {}).get("stale_asset_ids", "")),
                }
            ),
        )

    @staticmethod
    def _eligibility_gate_bucket(reason: str) -> str:
        normalized_reason = (reason or "unknown").strip() or "unknown"
        if normalized_reason == "connection_recovering":
            return "connection_recovering"
        if normalized_reason in {"book_recovering", "market_recovering"}:
            return "book_recovering"
        if (
            normalized_reason.startswith("market_quote_stale")
            or normalized_reason == "quote_too_old"
        ):
            return "stale_quote_freshness"
        if normalized_reason.startswith("safe_mode_blocked_"):
            return "blocked"
        if normalized_reason in {"market_probation", "asset_warming_up"}:
            return "probation"
        if normalized_reason == NO_SIGNAL_REASON_CHRONIC_STALE_EXCLUDED:
            return "chronic_stale_excluded"
        if normalized_reason.startswith("book_not_ready") or normalized_reason in {
            "market_not_ready"
        }:
            return "other_readiness_gate"
        return "other_readiness_gate"

    def _record_eligibility_gate_diagnostic(
        self,
        *,
        market_id: str,
        reason: str,
        category: str,
        details: dict[str, object] | None,
        now_utc: datetime,
    ) -> None:
        normalized_reason = (reason or "unknown").strip() or "unknown"
        previous_reason = self.state.eligibility_gate_reason_by_market.get(market_id)
        previous_ts = self.state.eligibility_gate_reason_at_by_market.get(market_id)
        cooldown_ms = max(
            0,
            self.config.settings.runtime.market_no_signal_reason_cooldown_ms,
        )
        now_ts = now_utc.timestamp()
        if (
            previous_reason == normalized_reason
            and previous_ts is not None
            and cooldown_ms > 0
            and (now_ts - previous_ts) * 1000.0 < cooldown_ms
        ):
            return
        self.state.eligibility_gate_reason_by_market[market_id] = normalized_reason
        self.state.eligibility_gate_reason_at_by_market[market_id] = now_ts
        self._save_diagnostics_event(
            event_name=DIAG_EVENT_ELIGIBILITY_GATE_UNMET,
            now_utc=now_utc,
            market_id=market_id,
            reason=normalized_reason,
            details=format_kv_details(
                {
                    "category": category,
                    "stale_reason_key": normalize_stale_reason_key(
                        str((details or {}).get("stale_reason_key", ""))
                    ),
                    "stale_side": normalize_stale_side(str((details or {}).get("stale_side", ""))),
                    "stale_asset_ids": str((details or {}).get("stale_asset_ids", "")),
                    "chronic_stale_reason": str((details or {}).get("chronic_stale_reason", "")),
                    "chronic_stale_enter_count": (details or {}).get("stale_enter_count"),
                    "chronic_stale_max_duration_ms": (details or {}).get("max_stale_duration_ms"),
                }
            ),
        )

    def _mark_asset_ready(self, asset_id: str) -> None:
        if not self.quote_manager.is_asset_ready(asset_id):
            return
        now = utc_now()
        self._record_book_ready_after_resync(asset_id=asset_id, now_utc=now)
        self.state.ready_assets.add(asset_id)
        self.state.ever_ready_assets.add(asset_id)
        self.state.asset_recovering_until.pop(asset_id, None)
        self.state.asset_recovering_started_at.pop(asset_id, None)
        self.state.last_book_missing_reason_by_asset.pop(asset_id, None)
        self.state.safe_mode_blocked_assets.discard(asset_id)

    def _market_ready_asset_ratio(self, market: BinaryMarket) -> float:
        ready_count = 0
        for asset_id in (market.yes_token_id, market.no_token_id):
            if self.quote_manager.is_asset_ready(asset_id):
                ready_count += 1
        return ready_count / 2.0

    def _is_market_in_probation(self, market: BinaryMarket, now_utc: datetime) -> bool:
        probation_until = self.state.market_probation_until.get(market.market_id)
        if probation_until is None:
            return False
        min_updates = max(
            0, self.config.settings.runtime.market_probation_min_quote_updates_per_asset
        )
        min_ready_ratio = min(
            1.0,
            max(0.0, self.config.settings.runtime.market_probation_min_ready_asset_ratio),
        )
        yes_updates = self.state.asset_quote_update_count.get(market.yes_token_id, 0)
        no_updates = self.state.asset_quote_update_count.get(market.no_token_id, 0)
        ready_ratio = self._market_ready_asset_ratio(market)
        if (
            ready_ratio >= min_ready_ratio
            and yes_updates >= min_updates
            and no_updates >= min_updates
        ):
            self.state.market_probation_until.pop(market.market_id, None)
            return False
        if now_utc >= probation_until:
            self.state.market_probation_until.pop(market.market_id, None)
            return False
        return True

    def _is_market_id_in_probation(self, market_id: str, now_utc: datetime) -> bool:
        market = self.markets_by_id.get(market_id)
        if market is None:
            return False
        return self._is_market_in_probation(market, now_utc)

    def _is_asset_in_probation(self, asset_id: str, now_utc: datetime) -> bool:
        mapping = self.token_to_market_side.get(asset_id)
        if mapping is None:
            return False
        market_id, _ = mapping
        return self._is_market_id_in_probation(market_id, now_utc)

    def _start_connection_recovery(self, *, now_utc: datetime, reason: str) -> None:
        grace_ms = max(0, self.config.settings.runtime.reconnect_recovery_grace_ms)
        if grace_ms <= 0:
            self.state.connection_recovering_until = None
            return
        self.state.connection_recovering_until = now_utc + timedelta(milliseconds=grace_ms)
        self.state.ws_health_state = "reconnecting"
        self.state.last_ws_reconnect_reason = reason

    def _is_connection_recovering(self, *, now_utc: datetime) -> bool:
        recovering_until = self.state.connection_recovering_until
        if recovering_until is None:
            return False
        if now_utc >= recovering_until:
            self.state.connection_recovering_until = None
            if self.state.ws_health_state == "reconnecting":
                self.state.ws_health_state = "healthy"
            return False
        return True

    def _connection_ready_ratio(self) -> float:
        tracked_assets = set(self.token_to_market_side.keys())
        if not tracked_assets:
            return 0.0
        return len(self.state.ready_assets & tracked_assets) / max(1, len(tracked_assets))

    def _maybe_finish_connection_recovery(self, *, now_utc: datetime) -> None:
        if not self._is_connection_recovering(now_utc=now_utc):
            return
        if self.state.first_quote_received_at is None:
            return
        min_ratio = min(
            1.0,
            max(0.0, self.config.settings.runtime.reconnect_recovery_min_ready_asset_ratio),
        )
        if self._connection_ready_ratio() >= min_ratio:
            self.state.connection_recovering_until = None
            self.state.ws_health_state = "healthy"

    def _mark_asset_recovering(self, asset_id: str, now_utc: datetime) -> None:
        grace_ms = max(0, self.config.settings.runtime.resync_recovery_grace_ms)
        recovering_until = now_utc + timedelta(milliseconds=grace_ms)
        self.state.asset_recovering_started_at.setdefault(asset_id, now_utc)
        self.state.asset_recovering_until[asset_id] = recovering_until
        self.state.ready_assets.discard(asset_id)

    def _is_asset_recovering(self, asset_id: str, now_utc: datetime) -> bool:
        recovering_until = self.state.asset_recovering_until.get(asset_id)
        if recovering_until is None:
            return False
        started_at = self.state.asset_recovering_started_at.get(asset_id)
        max_recovering_ms = max(0, self.config.settings.runtime.market_recovering_max_ms)
        if started_at is not None and max_recovering_ms > 0:
            elapsed_ms = max(0.0, (now_utc - started_at).total_seconds() * 1000.0)
            if elapsed_ms >= max_recovering_ms:
                self.state.asset_recovering_until.pop(asset_id, None)
                self.state.asset_recovering_started_at.pop(asset_id, None)
                return False
        if now_utc > recovering_until:
            self.state.asset_recovering_until.pop(asset_id, None)
            self.state.asset_recovering_started_at.pop(asset_id, None)
            return False
        return True

    def _classify_missing_book_reason(self, asset_id: str, now_utc: datetime) -> str:
        if self._is_asset_in_probation(asset_id=asset_id, now_utc=now_utc):
            return "asset_warming_up"
        if self._is_connection_recovering(now_utc=now_utc):
            return "connection_recovering"
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

    def _market_quote_update_counts(self, market: BinaryMarket) -> tuple[int, int]:
        yes_updates = self.state.asset_quote_update_count.get(market.yes_token_id, 0)
        no_updates = self.state.asset_quote_update_count.get(market.no_token_id, 0)
        return yes_updates, no_updates

    @staticmethod
    def _is_market_stale_state(state: str) -> bool:
        return state in MARKET_STALE_STATES

    @staticmethod
    def _stale_side_from_assets(*, yes_stale: bool, no_stale: bool) -> str:
        if yes_stale and no_stale:
            return STALE_SIDE_BOTH
        if yes_stale:
            return STALE_SIDE_YES
        if no_stale:
            return STALE_SIDE_NO
        return STALE_SIDE_UNKNOWN

    def _market_context_details(self, market: BinaryMarket) -> dict[str, object]:
        return {
            "market_id": market.market_id,
            "market_slug": market.slug,
            "market_question": market.question,
            "yes_asset_id": market.yes_token_id,
            "no_asset_id": market.no_token_id,
        }

    def _is_market_chronic_stale_excluded(self, *, market_id: str, now_utc: datetime) -> bool:
        excluded_until = self.state.market_chronic_stale_excluded_until.get(market_id)
        return excluded_until is not None and excluded_until > now_utc

    def _market_universe_change_related(self, *, market: BinaryMarket, now_utc: datetime) -> bool:
        if (
            self.state.market_recovery_reason_by_market.get(market.market_id)
            == RESYNC_REASON_MARKET_UNIVERSE_CHANGED
        ):
            return True
        lookback_ms = max(
            1,
            self.config.settings.runtime.resync_recovery_grace_ms,
        )
        now_ts = now_utc.timestamp()
        for asset_id in (market.yes_token_id, market.no_token_id):
            if (
                self.state.last_resync_reason_by_asset.get(asset_id)
                != RESYNC_REASON_MARKET_UNIVERSE_CHANGED
            ):
                continue
            resync_ts = self.state.last_resync_at_by_asset.get(asset_id)
            if resync_ts is None:
                continue
            if (now_ts - resync_ts) * 1000.0 <= lookback_ms:
                return True
        return False

    def _resolve_market_freshness_state(
        self,
        *,
        market: BinaryMarket,
        now_utc: datetime,
    ) -> tuple[str, dict[str, object]]:
        context = self._market_context_details(market)
        context["universe_change_related"] = int(
            self._market_universe_change_related(market=market, now_utc=now_utc)
        )
        if self._is_market_in_probation(market=market, now_utc=now_utc):
            probation_until = self.state.market_probation_until.get(market.market_id)
            return (
                MARKET_FRESHNESS_PROBATION,
                {
                    **context,
                    "probation_until": probation_until.isoformat() if probation_until else "",
                },
            )

        if self._is_connection_recovering(now_utc=now_utc):
            return (
                MARKET_FRESHNESS_RECOVERING,
                {
                    **context,
                    "recovery_reason": "connection_recovering",
                    "ws_reconnect_reason": self.state.last_ws_reconnect_reason or "",
                },
            )

        if self.quote_manager.is_market_ready(market.market_id):
            self._mark_asset_ready(market.yes_token_id)
            self._mark_asset_ready(market.no_token_id)
        if (
            market.yes_token_id in self.state.ready_assets
            and market.no_token_id in self.state.ready_assets
        ):
            self.state.asset_recovering_until.pop(market.yes_token_id, None)
            self.state.asset_recovering_until.pop(market.no_token_id, None)
            self.state.asset_recovering_started_at.pop(market.yes_token_id, None)
            self.state.asset_recovering_started_at.pop(market.no_token_id, None)

        if self._is_asset_recovering(
            asset_id=market.yes_token_id,
            now_utc=now_utc,
        ) or self._is_asset_recovering(
            asset_id=market.no_token_id,
            now_utc=now_utc,
        ):
            return (
                MARKET_FRESHNESS_RECOVERING,
                {
                    **context,
                    "recovery_reason": "asset_recovering",
                },
            )

        if not self.quote_manager.is_market_ready(market.market_id):
            missing_reasons = sorted(
                {
                    self._classify_missing_book_reason(market.yes_token_id, now_utc=now_utc),
                    self._classify_missing_book_reason(market.no_token_id, now_utc=now_utc),
                }
            )
            return (
                MARKET_FRESHNESS_NOT_READY,
                {
                    **context,
                    "missing_reasons": ",".join(missing_reasons),
                },
            )

        yes_quote, no_quote = self.quote_manager.get_market_quotes(market.market_id)
        if yes_quote is None or no_quote is None:
            missing_side = (
                STALE_SIDE_YES
                if yes_quote is None and no_quote is not None
                else (
                    STALE_SIDE_NO if no_quote is None and yes_quote is not None else STALE_SIDE_BOTH
                )
            )
            return (
                MARKET_FRESHNESS_NOT_READY,
                {
                    **context,
                    "stale_reason_key": STALE_REASON_MISSING_LEG_QUOTE,
                    "stale_side": missing_side,
                },
            )

        yes_quote_age_ms = max(0.0, (now_utc - yes_quote.timestamp).total_seconds() * 1000.0)
        no_quote_age_ms = max(0.0, (now_utc - no_quote.timestamp).total_seconds() * 1000.0)
        leg_timestamp_diff_ms = abs(
            (yes_quote.timestamp - no_quote.timestamp).total_seconds() * 1000.0
        )
        quote_age_ms = max(yes_quote_age_ms, no_quote_age_ms)
        max_quote_age_ms = max(1, self.config.settings.strategy.max_quote_age_ms_for_signal)
        stale_detail_base = {
            **context,
            "quote_age_ms": round(quote_age_ms, 3),
            "quote_age_threshold_ms": max_quote_age_ms,
            "yes_quote_age_ms": round(yes_quote_age_ms, 3),
            "no_quote_age_ms": round(no_quote_age_ms, 3),
            "leg_timestamp_diff_ms": round(leg_timestamp_diff_ms, 3),
        }

        yes_stale = market.yes_token_id in self.state.stale_assets
        no_stale = market.no_token_id in self.state.stale_assets
        if yes_stale or no_stale:
            stale_asset_ids = [
                asset_id
                for asset_id, is_stale in (
                    (market.yes_token_id, yes_stale),
                    (market.no_token_id, no_stale),
                )
                if is_stale
            ]
            return (
                MARKET_FRESHNESS_STALE_NO_RECENT_QUOTE,
                {
                    **stale_detail_base,
                    "stale_reason_key": STALE_REASON_NO_RECENT_QUOTE,
                    "stale_side": self._stale_side_from_assets(
                        yes_stale=yes_stale,
                        no_stale=no_stale,
                    ),
                    "stale_asset_id": stale_asset_ids[0] if len(stale_asset_ids) == 1 else "",
                    "stale_asset_ids": ",".join(stale_asset_ids),
                    "no_signal_reason": STALE_NO_SIGNAL_REASON_NO_RECENT_QUOTE,
                },
            )

        if quote_age_ms > max_quote_age_ms:
            if abs(yes_quote_age_ms - no_quote_age_ms) <= 1.0:
                stale_side = STALE_SIDE_BOTH
                stale_asset_ids = f"{market.yes_token_id},{market.no_token_id}"
                stale_asset_id = ""
            elif yes_quote_age_ms > no_quote_age_ms:
                stale_side = STALE_SIDE_YES
                stale_asset_ids = market.yes_token_id
                stale_asset_id = market.yes_token_id
            else:
                stale_side = STALE_SIDE_NO
                stale_asset_ids = market.no_token_id
                stale_asset_id = market.no_token_id
            stale_reason_key = STALE_REASON_QUOTE_AGE_EXCEEDED
            if leg_timestamp_diff_ms > max_quote_age_ms:
                stale_reason_key = STALE_REASON_LEG_TIMESTAMP_MISMATCH
            return (
                MARKET_FRESHNESS_STALE_QUOTE_AGE,
                {
                    **stale_detail_base,
                    "stale_reason_key": stale_reason_key,
                    "stale_side": stale_side,
                    "stale_asset_id": stale_asset_id,
                    "stale_asset_ids": stale_asset_ids,
                    "no_signal_reason": STALE_NO_SIGNAL_REASON_QUOTE_AGE,
                },
            )

        return MARKET_FRESHNESS_READY, context

    @staticmethod
    def _normalize_stale_asset_ids(value: object) -> str:
        tokens = [token.strip() for token in str(value or "").split(",") if token.strip()]
        if not tokens:
            return ""
        return ",".join(sorted(dict.fromkeys(tokens)))

    def _market_stale_episode_signature(self, details: dict[str, object]) -> tuple[str, str, str]:
        reason_key = normalize_stale_reason_key(str(details.get("stale_reason_key", "")))
        stale_side = normalize_stale_side(str(details.get("stale_side", "")))
        stale_asset_ids = self._normalize_stale_asset_ids(details.get("stale_asset_ids", ""))
        return reason_key, stale_side, stale_asset_ids

    def _open_market_stale_episode(
        self,
        *,
        market_id: str,
        stale_state: str,
        previous_state: str,
        details: dict[str, object],
        now_utc: datetime,
    ) -> None:
        stale_reason_key = normalize_stale_reason_key(str(details.get("stale_reason_key", "")))
        episode_details = dict(details)
        episode_details["stale_reason_key"] = stale_reason_key
        episode_details["stale_side"] = normalize_stale_side(
            str(episode_details.get("stale_side", ""))
        )
        episode_details["stale_asset_ids"] = self._normalize_stale_asset_ids(
            episode_details.get("stale_asset_ids", "")
        )
        self.state.market_stale_started_at_by_market[market_id] = now_utc
        self.state.market_stale_reason_by_market[market_id] = stale_reason_key
        self.state.market_stale_details_by_market[market_id] = episode_details
        self._save_diagnostics_event(
            event_name=DIAG_EVENT_MARKET_STALE_ENTERED,
            now_utc=now_utc,
            market_id=market_id,
            reason=stale_reason_key,
            details=format_kv_details(
                {
                    "stale_state": stale_state,
                    "previous_state": previous_state,
                    "stale_reason_key": stale_reason_key,
                    "stale_side": str(episode_details.get("stale_side", "")),
                    "stale_asset_id": str(episode_details.get("stale_asset_id", "")),
                    "stale_asset_ids": str(episode_details.get("stale_asset_ids", "")),
                    "no_signal_reason": self._market_freshness_to_no_signal_reason(
                        stale_state,
                        details=episode_details,
                    ),
                    "quote_age_ms": episode_details.get("quote_age_ms"),
                    "quote_age_threshold_ms": episode_details.get("quote_age_threshold_ms"),
                    "leg_timestamp_diff_ms": episode_details.get("leg_timestamp_diff_ms"),
                    "market_slug": str(episode_details.get("market_slug", "")),
                    "market_question": str(episode_details.get("market_question", "")),
                    "yes_asset_id": str(episode_details.get("yes_asset_id", "")),
                    "no_asset_id": str(episode_details.get("no_asset_id", "")),
                    "universe_change_related": str(
                        episode_details.get("universe_change_related", 0)
                    ),
                }
            ),
        )

    def _close_market_stale_episode(
        self,
        *,
        market_id: str,
        previous_state: str,
        now_utc: datetime,
        next_state: str,
        close_event_name: str = DIAG_EVENT_MARKET_STALE_RECOVERED,
        close_reason: str = "recovered",
    ) -> None:
        stale_started_at = self.state.market_stale_started_at_by_market.pop(market_id, None)
        stale_reason_key = normalize_stale_reason_key(
            self.state.market_stale_reason_by_market.pop(market_id, STALE_REASON_UNKNOWN)
        )
        stale_details = self.state.market_stale_details_by_market.pop(market_id, {})
        stale_duration_ms = None
        if stale_started_at is not None:
            stale_duration_ms = max(0.0, (now_utc - stale_started_at).total_seconds() * 1000.0)
        self._save_diagnostics_event(
            event_name=close_event_name,
            now_utc=now_utc,
            market_id=market_id,
            reason=stale_reason_key,
            latency_ms=stale_duration_ms,
            details=format_kv_details(
                {
                    "previous_state": previous_state,
                    "next_state": next_state,
                    "recovered_state": next_state if close_reason == "recovered" else "",
                    "episode_closed_reason": close_reason,
                    "stale_reason_key": stale_reason_key,
                    "stale_side": str(stale_details.get("stale_side", "")),
                    "stale_asset_id": str(stale_details.get("stale_asset_id", "")),
                    "stale_asset_ids": str(stale_details.get("stale_asset_ids", "")),
                    "market_slug": str(stale_details.get("market_slug", "")),
                    "market_question": str(stale_details.get("market_question", "")),
                    "yes_asset_id": str(stale_details.get("yes_asset_id", "")),
                    "no_asset_id": str(stale_details.get("no_asset_id", "")),
                    "stale_duration_ms": (
                        round(stale_duration_ms, 3) if stale_duration_ms is not None else None
                    ),
                    "universe_change_related": str(stale_details.get("universe_change_related", 0)),
                }
            ),
        )

    def _update_market_freshness_state(
        self,
        *,
        market_id: str,
        state: str,
        details: dict[str, object],
        now_utc: datetime,
    ) -> None:
        previous_state = self.state.market_freshness_state_by_market.get(market_id, "")
        previous_stale = self._is_market_stale_state(previous_state)
        current_stale = self._is_market_stale_state(state)
        normalized_details = dict(details)
        if current_stale:
            normalized_details["stale_reason_key"] = normalize_stale_reason_key(
                str(normalized_details.get("stale_reason_key", ""))
            )
            normalized_details["stale_side"] = normalize_stale_side(
                str(normalized_details.get("stale_side", ""))
            )
            normalized_details["stale_asset_ids"] = self._normalize_stale_asset_ids(
                normalized_details.get("stale_asset_ids", "")
            )
        self.state.market_freshness_state_by_market[market_id] = state
        self.state.market_freshness_details_by_market[market_id] = normalized_details
        self.state.market_freshness_updated_at[market_id] = now_utc
        if current_stale:
            if not previous_stale:
                self._open_market_stale_episode(
                    market_id=market_id,
                    stale_state=state,
                    previous_state=previous_state,
                    details=normalized_details,
                    now_utc=now_utc,
                )
            else:
                previous_episode_details = self.state.market_stale_details_by_market.get(
                    market_id,
                    {},
                )
                previous_signature = self._market_stale_episode_signature(previous_episode_details)
                current_signature = self._market_stale_episode_signature(normalized_details)
                if current_signature != previous_signature:
                    self._close_market_stale_episode(
                        market_id=market_id,
                        previous_state=previous_state,
                        now_utc=now_utc,
                        next_state=state,
                        close_event_name=DIAG_EVENT_MARKET_STALE_EPISODE_CLOSED,
                        close_reason="reason_changed",
                    )
                    self._open_market_stale_episode(
                        market_id=market_id,
                        stale_state=state,
                        previous_state=previous_state,
                        details=normalized_details,
                        now_utc=now_utc,
                    )
        elif previous_stale:
            self._close_market_stale_episode(
                market_id=market_id,
                previous_state=previous_state,
                now_utc=now_utc,
                next_state=state,
            )
        if state == MARKET_FRESHNESS_READY:
            self._record_market_ready_after_recovery(market_id=market_id, now_utc=now_utc)

    @staticmethod
    def _market_freshness_to_no_signal_reason(
        state: str, *, details: dict[str, object] | None = None
    ) -> str:
        normalized_stale_reason = normalize_stale_reason_key(
            str((details or {}).get("stale_reason_key", ""))
        )
        mapping = {
            MARKET_FRESHNESS_NOT_READY: "market_not_ready",
            MARKET_FRESHNESS_PROBATION: "market_probation",
            MARKET_FRESHNESS_RECOVERING: "market_recovering",
            MARKET_FRESHNESS_STALE_NO_RECENT_QUOTE: STALE_NO_SIGNAL_REASON_NO_RECENT_QUOTE,
            MARKET_FRESHNESS_STALE_QUOTE_AGE: STALE_NO_SIGNAL_REASON_QUOTE_AGE,
        }
        if (
            state == MARKET_FRESHNESS_STALE_QUOTE_AGE
            and normalized_stale_reason == STALE_REASON_NO_RECENT_QUOTE
        ):
            return STALE_NO_SIGNAL_REASON_NO_RECENT_QUOTE
        return mapping.get(state, "market_not_ready")

    def _evaluate_market_signal_eligibility(
        self,
        *,
        market: BinaryMarket,
        now_utc: datetime,
    ) -> tuple[bool, str, dict[str, object]]:
        if self._is_market_chronic_stale_excluded(market_id=market.market_id, now_utc=now_utc):
            details = self._market_chronic_stale_details(market.market_id)
            return False, NO_SIGNAL_REASON_CHRONIC_STALE_EXCLUDED, details

        state = self.state.market_freshness_state_by_market.get(market.market_id)
        if state is None:
            state, state_details = self._resolve_market_freshness_state(
                market=market,
                now_utc=now_utc,
            )
            self._update_market_freshness_state(
                market_id=market.market_id,
                state=state,
                details=state_details,
                now_utc=now_utc,
            )
            if state != MARKET_FRESHNESS_READY:
                return (
                    False,
                    self._market_freshness_to_no_signal_reason(state, details=state_details),
                    state_details,
                )

        if state != MARKET_FRESHNESS_READY:
            details = self.state.market_freshness_details_by_market.get(
                market.market_id,
                {"market_id": market.market_id},
            )
            return (
                False,
                self._market_freshness_to_no_signal_reason(state, details=details),
                details,
            )

        min_updates = max(
            0,
            self.config.settings.runtime.market_eligibility_min_quote_updates_per_asset,
        )
        yes_updates, no_updates = self._market_quote_update_counts(market)
        if yes_updates < min_updates or no_updates < min_updates:
            return (
                False,
                "book_not_ready_insufficient_updates",
                {
                    "market_id": market.market_id,
                    "yes_updates": yes_updates,
                    "no_updates": no_updates,
                    "required_updates": min_updates,
                },
            )

        if not self.quote_manager.is_market_ready(market.market_id):
            return (
                False,
                "book_not_ready_missing_leg_ready",
                {"market_id": market.market_id},
            )

        return True, "market_eligible", {"market_id": market.market_id}

    def _evaluate_market_readiness(
        self,
        *,
        market: BinaryMarket,
        now_utc: datetime,
    ) -> tuple[bool, str, dict[str, object]]:
        state, details = self._resolve_market_freshness_state(
            market=market,
            now_utc=now_utc,
        )
        self._update_market_freshness_state(
            market_id=market.market_id,
            state=state,
            details=details,
            now_utc=now_utc,
        )
        if state == MARKET_FRESHNESS_READY:
            return True, "market_ready", details
        return False, self._market_freshness_to_no_signal_reason(state, details=details), details

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
            self._save_diagnostics_event(
                event_name=DIAG_EVENT_MARKET_BLOCK_ENTERED,
                now_utc=now_utc,
                market_id=market_id,
                reason=reason,
            )
        for market_id in cleared_markets:
            self.state.market_block_started_at.pop(market_id, None)
            self._save_diagnostics_event(
                event_name=DIAG_EVENT_MARKET_BLOCK_CLEARED,
                now_utc=now_utc,
                market_id=market_id,
                reason=reason,
            )
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
        candidate_markets: set[str] = set()
        for market_id, market in self.markets_by_id.items():
            if self._is_market_id_in_probation(market_id, now_utc):
                continue
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

    def _derive_asset_blocks(self, *, candidate_assets: set[str]) -> set[str]:
        min_cycles = max(
            1,
            self.config.settings.runtime.asset_block_min_consecutive_unhealthy_cycles,
        )
        blocked_assets: set[str] = set()
        tracked_assets = set(self.token_to_market_side.keys())
        for asset_id in tracked_assets:
            previous = self.state.asset_unhealthy_consecutive_cycles.get(asset_id, 0)
            if asset_id in candidate_assets:
                current = previous + 1
            else:
                current = 0
            if current > 0:
                self.state.asset_unhealthy_consecutive_cycles[asset_id] = current
            else:
                self.state.asset_unhealthy_consecutive_cycles.pop(asset_id, None)
            if current >= min_cycles:
                blocked_assets.add(asset_id)
        return blocked_assets

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
        if market_id:
            now_ts = now_utc.timestamp()
            cooldown_ms = max(
                0,
                self.config.settings.runtime.market_no_signal_reason_cooldown_ms,
            )
            previous_reason = self.state.last_no_signal_reason_by_market.get(market_id)
            previous_ts = self.state.last_no_signal_reason_at_by_market.get(market_id)
            if (
                cooldown_ms > 0
                and previous_reason == normalized_reason
                and previous_ts is not None
                and (now_ts - previous_ts) * 1000.0 < cooldown_ms
            ):
                return
            self.state.last_no_signal_reason_by_market[market_id] = normalized_reason
            self.state.last_no_signal_reason_at_by_market[market_id] = now_ts
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
            previous_market_ids = set(self.markets_by_id.keys())
            candidate_market_ids = set(snapshot.markets_by_id.keys())
            added_markets = sorted(candidate_market_ids - previous_market_ids)
            removed_markets = sorted(previous_market_ids - candidate_market_ids)
            market_change_count = len(added_markets) + len(removed_markets)
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
            replacement_limit = max(
                0,
                self.config.settings.market_filters.max_market_replacements_per_refresh,
            )
            ready_ratio = len(self.state.ready_assets & previous_asset_ids) / max(
                1, len(previous_asset_ids)
            )
            healthy_universe = (
                bool(previous_asset_ids)
                and ready_ratio >= 0.8
                and not self.state.book_state_unhealthy
                and not self.state.ws_unhealthy
            )
            dynamic_confirmation_target = confirmation_target + (
                1 if healthy_universe and market_change_count > 0 else 0
            )
            skipped_by_hysteresis = (
                market_change_count > replacement_limit
                and change_count < force_delta_target
                and healthy_universe
            )
            confirmed_change = (
                self.state.pending_universe_confirmation_count >= dynamic_confirmation_target
                or change_count >= force_delta_target
            )
            now = utc_now()
            self.sqlite_store.save_metric(
                metric_name="market_universe_change_candidate",
                metric_value=float(change_count),
                details=(
                    f"added={len(added_assets)};removed={len(removed_assets)};"
                    f"added_markets={len(added_markets)};removed_markets={len(removed_markets)};"
                    f"confirmations={self.state.pending_universe_confirmation_count};"
                    f"required={dynamic_confirmation_target};"
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
                    "added_markets": len(added_markets),
                    "removed_markets": len(removed_markets),
                    "confirmation_count": self.state.pending_universe_confirmation_count,
                    "confirmation_required": dynamic_confirmation_target,
                    "forced_change": change_count >= force_delta_target,
                    "healthy_universe": healthy_universe,
                    "ready_ratio": round(ready_ratio, 3),
                    "skipped_by_hysteresis": skipped_by_hysteresis,
                },
                now_utc=now,
            )
            if skipped_by_hysteresis:
                self.logger.info(
                    (
                        "Market universe candidate skipped by hysteresis "
                        "added_markets=%s removed_markets=%s replacement_limit=%s ready_ratio=%.3f"
                    ),
                    len(added_markets),
                    len(removed_markets),
                    replacement_limit,
                    ready_ratio,
                )
                return
            if not confirmed_change:
                self.logger.info(
                    (
                        "Market universe change candidate pending "
                        "added_assets=%s removed_assets=%s added_markets=%s removed_markets=%s "
                        "confirmation=%s/%s"
                    ),
                    len(added_assets),
                    len(removed_assets),
                    len(added_markets),
                    len(removed_markets),
                    self.state.pending_universe_confirmation_count,
                    dynamic_confirmation_target,
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
                        f"added_markets={len(added_markets)};"
                        f"removed_markets={len(removed_markets)};"
                        f"watched_markets={self.state.watched_markets};"
                        "reason=hysteresis_confirmed_change"
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
                        "added_markets": len(added_markets),
                        "removed_markets": len(removed_markets),
                        "watched_markets": self.state.watched_markets,
                        "change_reason": "hysteresis_confirmed_change",
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
        self.state.ws_health_state = "healthy"
        for asset_id in asset_ids:
            self.state.asset_subscribed_at.setdefault(asset_id, now)
        reason = self.state.pending_connect_resync_reason or RESYNC_REASON_WS_CONNECTED
        self.state.pending_connect_resync_reason = None
        self.state.last_ws_connect_reason = reason
        if reason != RESYNC_REASON_WS_CONNECTED:
            self._start_connection_recovery(now_utc=now, reason=reason)
        self.sqlite_store.save_metric(
            metric_name="ws_connected_event",
            metric_value=1.0,
            details=f"reason={reason};assets={len(asset_ids)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "ws_connected_event",
                "metric_value": 1.0,
                "reason": reason,
                "assets": len(asset_ids),
            },
            now_utc=now,
        )
        force_resync = (
            reason == RESYNC_REASON_WS_CONNECTED and self.state.first_quote_received_at is None
        )
        resync_candidates = list(asset_ids)
        if not force_resync:
            stale_candidates = set(
                self.healthcheck.stale_assets(
                    tracked_assets=list(asset_ids),
                    now_utc=now,
                    max_age_ms=self.config.settings.runtime.stale_asset_ms,
                )
            )
            resync_candidates = [
                asset_id
                for asset_id in asset_ids
                if not self.quote_manager.is_asset_ready(asset_id) or asset_id in stale_candidates
            ]
            if reason == RESYNC_REASON_MARKET_UNIVERSE_CHANGED:
                never_ready_assets = [
                    asset_id
                    for asset_id in asset_ids
                    if asset_id not in self.state.ever_ready_assets
                ]
                resync_candidates = list(dict.fromkeys([*resync_candidates, *never_ready_assets]))
        if resync_candidates:
            await self.resync_assets(
                asset_ids=resync_candidates,
                reason=reason,
                force=force_resync,
            )
        else:
            self.logger.info(
                "WS connected reason=%s with no immediate resync targets.",
                reason,
            )

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
        self.state.ws_health_state = "reconnecting"
        self.state.last_ws_reconnect_reason = reason
        now = utc_now()
        self.sqlite_store.save_metric(
            metric_name="ws_reconnect_event",
            metric_value=1.0,
            details=f"reason={reason};mapped={mapped_reason}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "ws_reconnect_event",
                "metric_value": 1.0,
                "reason": reason,
                "mapped_reason": mapped_reason,
            },
            now_utc=now,
        )

    def on_ws_transport_event(self, event_name: str, reason: str, asset_count: int) -> None:
        now = utc_now()
        metric_name = f"ws_transport_{event_name}"
        details = f"reason={reason};assets={asset_count}"
        self.sqlite_store.save_metric(
            metric_name=metric_name,
            metric_value=1.0,
            details=details,
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": metric_name,
                "metric_value": 1.0,
                "reason": reason,
                "assets": asset_count,
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
        connection_recovering = self._is_connection_recovering(now_utc=now)
        self.sqlite_store.save_metric(
            metric_name="connection_recovering",
            metric_value=1.0 if connection_recovering else 0.0,
            details=f"ws_health_state={self.state.ws_health_state}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "connection_recovering",
                "metric_value": 1.0 if connection_recovering else 0.0,
                "ws_health_state": self.state.ws_health_state,
            },
            now_utc=now,
        )
        chronic_stale_summary = self._refresh_chronic_stale_runtime_exclusions(now_utc=now)
        is_warming_up, warmup_reason = self._market_data_warmup_state(now)
        if is_warming_up:
            warming_up_assets = [
                asset_id
                for asset_id in tracked_assets
                if not self.quote_manager.is_asset_ready(asset_id)
            ]
            self.state.stale_assets = set()
            self.state.eligible_markets = set()
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
            self._record_chronic_stale_exclusion_metrics(
                summary=chronic_stale_summary,
                now_utc=now,
            )
            self._evaluate_guardrails(now_utc=now, stale_asset_rate=0.0)
            return

        for asset_id in tracked_assets:
            if self.quote_manager.is_asset_ready(asset_id):
                self._mark_asset_ready(asset_id)
                continue
            if self._is_asset_recovering(asset_id=asset_id, now_utc=now):
                continue
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
        for asset_id in missing_assets:
            mapping = self.token_to_market_side.get(asset_id)
            market_id = mapping[0] if mapping is not None else None
            self._save_diagnostics_event(
                event_name=DIAG_EVENT_MISSING_BOOK_STATE_DETECTED,
                now_utc=now,
                asset_id=asset_id,
                market_id=market_id,
                reason=self.state.last_book_missing_reason_by_asset.get(asset_id),
            )
        for asset_id in list(self.state.asset_recovery_started_at.keys()):
            self._record_first_quote_after_resync_blocked(asset_id=asset_id, now_utc=now)
        for asset_id in list(self.state.asset_first_quote_after_recovery_at.keys()):
            self._record_book_ready_after_resync_blocked(asset_id=asset_id, now_utc=now)

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
        probation_assets = [
            asset_id
            for asset_id in stale_assets
            if self._is_asset_in_probation(asset_id=asset_id, now_utc=now)
        ]
        recovering_asset_set = set(recovering_assets)
        probation_asset_set = set(probation_assets)
        effective_stale_assets = [
            asset_id
            for asset_id in stale_assets
            if asset_id not in recovering_asset_set and asset_id not in probation_asset_set
        ]
        if connection_recovering:
            effective_stale_assets = []
        for asset_id in effective_stale_assets:
            mapping = self.token_to_market_side.get(asset_id)
            market_id = mapping[0] if mapping is not None else None
            self._save_diagnostics_event(
                event_name=DIAG_EVENT_STALE_ASSET_DETECTED,
                now_utc=now,
                asset_id=asset_id,
                market_id=market_id,
                reason=RESYNC_REASON_STALE_ASSET,
            )
        self.state.stale_assets = set(effective_stale_assets)
        market_state_counts: dict[str, int] = {
            MARKET_FRESHNESS_READY: 0,
            MARKET_FRESHNESS_NOT_READY: 0,
            MARKET_FRESHNESS_PROBATION: 0,
            MARKET_FRESHNESS_RECOVERING: 0,
            MARKET_FRESHNESS_STALE_NO_RECENT_QUOTE: 0,
            MARKET_FRESHNESS_STALE_QUOTE_AGE: 0,
        }
        eligible_markets: set[str] = set()
        min_updates = max(
            0,
            self.config.settings.runtime.market_eligibility_min_quote_updates_per_asset,
        )
        penalty_threshold = max(
            1,
            self.config.settings.runtime.low_quality_market_penalty_threshold,
        )
        penalty_increment = max(
            1,
            self.config.settings.runtime.low_quality_market_penalty_increment,
        )
        penalty_decay = max(1, self.config.settings.runtime.low_quality_market_penalty_decay)
        quality_stage_counts: dict[str, int] = {
            MARKET_QUALITY_HEALTHY: 0,
            MARKET_QUALITY_DEGRADED: 0,
            MARKET_QUALITY_PROBATION_EXTENDED: 0,
            MARKET_QUALITY_EXCLUSION_CANDIDATE: 0,
            MARKET_QUALITY_EXCLUDED: 0,
        }
        eligibility_gate_counts: dict[str, int] = {
            "connection_recovering": 0,
            "book_recovering": 0,
            "stale_quote_freshness": 0,
            "blocked": 0,
            "probation": 0,
            "low_quality_runtime_excluded": 0,
            "chronic_stale_excluded": 0,
            "other_readiness_gate": 0,
        }
        stale_reason_counts: dict[str, int] = {}
        stale_side_counts: dict[str, int] = {}
        stale_universe_change_related_count = 0
        chronic_active_market_ids = set(chronic_stale_summary.active_market_ids)
        for market in self.markets_by_id.values():
            market_state, market_state_details = self._resolve_market_freshness_state(
                market=market,
                now_utc=now,
            )
            self._update_market_freshness_state(
                market_id=market.market_id,
                state=market_state,
                details=market_state_details,
                now_utc=now,
            )
            market_state_counts[market_state] = market_state_counts.get(market_state, 0) + 1
            if self._is_market_stale_state(market_state):
                stale_reason_key = normalize_stale_reason_key(
                    str(market_state_details.get("stale_reason_key", ""))
                )
                stale_side = normalize_stale_side(str(market_state_details.get("stale_side", "")))
                stale_reason_counts[stale_reason_key] = (
                    stale_reason_counts.get(stale_reason_key, 0) + 1
                )
                stale_side_counts[stale_side] = stale_side_counts.get(stale_side, 0) + 1
                if int(str(market_state_details.get("universe_change_related", 0)) or 0) > 0:
                    stale_universe_change_related_count += 1
            if market_state != MARKET_FRESHNESS_READY:
                self._record_market_ready_after_recovery_blocked(
                    market_id=market.market_id,
                    reason=self._market_freshness_to_no_signal_reason(
                        market_state,
                        details=market_state_details,
                    ),
                    details=market_state_details,
                    now_utc=now,
                )
            yes_updates, no_updates = self._market_quote_update_counts(market)
            current_penalty = self.state.market_quality_penalty_by_market.get(market.market_id, 0)
            current_bad_cycles = self.state.market_low_quality_consecutive_cycles.get(
                market.market_id,
                0,
            )
            if (
                market_state == MARKET_FRESHNESS_READY
                and yes_updates >= min_updates
                and no_updates >= min_updates
            ):
                updated_penalty = max(
                    0,
                    current_penalty - penalty_decay,
                )
                updated_bad_cycles = max(0, current_bad_cycles - 1)
            elif market_state in {
                MARKET_FRESHNESS_STALE_NO_RECENT_QUOTE,
                MARKET_FRESHNESS_STALE_QUOTE_AGE,
            }:
                updated_penalty = min(
                    penalty_threshold * 3,
                    current_penalty + penalty_increment,
                )
                updated_bad_cycles = current_bad_cycles + 1
            elif market_state in {MARKET_FRESHNESS_RECOVERING, MARKET_FRESHNESS_NOT_READY}:
                updated_penalty = min(
                    penalty_threshold * 3,
                    current_penalty + max(1, penalty_increment // 2),
                )
                updated_bad_cycles = current_bad_cycles + 1
            else:
                updated_penalty = max(
                    0,
                    current_penalty - penalty_decay,
                )
                updated_bad_cycles = max(0, current_bad_cycles - 1)
            self.state.market_quality_penalty_by_market[market.market_id] = updated_penalty
            self.state.market_low_quality_consecutive_cycles[market.market_id] = updated_bad_cycles
            quality_stage = self._market_quality_stage(
                penalty=updated_penalty,
                consecutive_bad_cycles=updated_bad_cycles,
                excluded=False,
            )
            self.state.market_quality_stage_by_market[market.market_id] = quality_stage
            quality_stage_counts[quality_stage] = quality_stage_counts.get(quality_stage, 0) + 1

            if market.market_id in chronic_active_market_ids:
                eligibility_reason = NO_SIGNAL_REASON_CHRONIC_STALE_EXCLUDED
                eligibility_details = self._market_chronic_stale_details(market.market_id)
                gate_category = "chronic_stale_excluded"
                eligibility_gate_counts[gate_category] = (
                    eligibility_gate_counts.get(gate_category, 0) + 1
                )
                eligible_markets.discard(market.market_id)
                self._record_eligibility_gate_diagnostic(
                    market_id=market.market_id,
                    reason=eligibility_reason,
                    category=gate_category,
                    details=eligibility_details,
                    now_utc=now,
                )
                continue

            eligibility_ok, eligibility_reason, eligibility_details = (
                self._evaluate_market_signal_eligibility(
                    market=market,
                    now_utc=now,
                )
            )
            if eligibility_ok:
                eligible_markets.add(market.market_id)
                continue
            gate_category = self._eligibility_gate_bucket(eligibility_reason)
            if (
                market.market_id in self.state.safe_mode_blocked_markets
                or self.state.safe_mode_active
            ):
                gate_category = "blocked"
            eligibility_gate_counts[gate_category] = (
                eligibility_gate_counts.get(gate_category, 0) + 1
            )
            eligible_markets.discard(market.market_id)
            self._record_eligibility_gate_diagnostic(
                market_id=market.market_id,
                reason=eligibility_reason,
                category=gate_category,
                details=eligibility_details,
                now_utc=now,
            )
        self.state.eligible_markets = eligible_markets
        eligibility_gate_counts["low_quality_runtime_excluded"] = (
            self.state.last_refresh_runtime_excluded_count
        )
        missing_reasons_by_asset = self.state.last_book_missing_reason_by_asset
        blocking_missing_assets = {
            asset_id
            for asset_id in missing_assets
            if missing_reasons_by_asset.get(asset_id)
            not in {
                "book_recovering",
                "book_not_ready",
                "asset_warming_up",
                "connection_recovering",
            }
        }
        health_unhealthy_assets = set(effective_stale_assets) | blocking_missing_assets
        if connection_recovering:
            health_unhealthy_assets = set()
        tracked_count = max(1, len(tracked_assets))
        unhealthy_ratio = len(health_unhealthy_assets) / tracked_count
        probation_market_count = market_state_counts.get(MARKET_FRESHNESS_PROBATION, 0)

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
                "probation_assets": len(probation_assets),
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
            metric_name="probation_asset_count",
            metric_value=float(len(probation_assets)),
            details=f"tracked_assets={len(tracked_assets)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "probation_asset_count",
                "metric_value": len(probation_assets),
                "tracked_assets": len(tracked_assets),
            },
            now_utc=now,
        )
        self.sqlite_store.save_metric(
            metric_name="probation_market_count",
            metric_value=float(probation_market_count),
            details=f"tracked_markets={len(self.markets_by_id)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "probation_market_count",
                "metric_value": probation_market_count,
                "tracked_markets": len(self.markets_by_id),
            },
            now_utc=now,
        )
        market_state_metric_map = {
            "market_state_watched_count": len(self.markets_by_id),
            "market_state_ready_count": market_state_counts.get(MARKET_FRESHNESS_READY, 0),
            "market_state_not_ready_count": market_state_counts.get(MARKET_FRESHNESS_NOT_READY, 0),
            "market_state_probation_count": market_state_counts.get(MARKET_FRESHNESS_PROBATION, 0),
            "market_state_recovering_count": market_state_counts.get(
                MARKET_FRESHNESS_RECOVERING, 0
            ),
            "market_state_stale_no_recent_quote_count": market_state_counts.get(
                MARKET_FRESHNESS_STALE_NO_RECENT_QUOTE,
                0,
            ),
            "market_state_stale_quote_age_count": market_state_counts.get(
                MARKET_FRESHNESS_STALE_QUOTE_AGE,
                0,
            ),
            "market_state_eligible_count": len(eligible_markets),
            "market_state_ready_ratio": (
                market_state_counts.get(MARKET_FRESHNESS_READY, 0) / max(1, len(self.markets_by_id))
            ),
            "market_state_eligible_ratio": len(eligible_markets) / max(1, len(self.markets_by_id)),
        }
        for metric_name, metric_value in market_state_metric_map.items():
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(metric_value),
                details=f"tracked_markets={len(self.markets_by_id)}",
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "tracked_markets": len(self.markets_by_id),
                },
                now_utc=now,
            )
        for reason_key, reason_value in sorted(stale_reason_counts.items()):
            metric_name = f"market_stale_reason:{reason_key}"
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(reason_value),
                details=f"tracked_markets={len(self.markets_by_id)}",
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": reason_value,
                    "tracked_markets": len(self.markets_by_id),
                },
                now_utc=now,
            )
        for side_key, side_value in sorted(stale_side_counts.items()):
            metric_name = f"market_stale_side:{side_key}"
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(side_value),
                details=f"tracked_markets={len(self.markets_by_id)}",
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": side_value,
                    "tracked_markets": len(self.markets_by_id),
                },
                now_utc=now,
            )
        self.sqlite_store.save_metric(
            metric_name="market_stale_universe_change_related_count",
            metric_value=float(stale_universe_change_related_count),
            details=f"tracked_markets={len(self.markets_by_id)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "market_stale_universe_change_related_count",
                "metric_value": stale_universe_change_related_count,
                "tracked_markets": len(self.markets_by_id),
            },
            now_utc=now,
        )
        for stage_name, stage_value in quality_stage_counts.items():
            metric_name = f"market_quality_stage_{stage_name}_count"
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(stage_value),
                details=f"tracked_markets={len(self.markets_by_id)}",
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": stage_value,
                    "tracked_markets": len(self.markets_by_id),
                },
                now_utc=now,
            )
        for category, count in sorted(eligibility_gate_counts.items()):
            metric_name = f"eligibility_gate_reason:{category}"
            self.sqlite_store.save_metric(
                metric_name=metric_name,
                metric_value=float(count),
                details=(
                    f"watched_markets={len(self.markets_by_id)};"
                    f"eligible_markets={len(eligible_markets)}"
                ),
                created_at_iso=now.isoformat(),
                run_id=self.state.run_id,
            )
            self.csv_logger.log_metric(
                {
                    "run_id": self.state.run_id,
                    "created_at": now.isoformat(),
                    "metric_name": metric_name,
                    "metric_value": count,
                    "watched_markets": len(self.markets_by_id),
                    "eligible_markets": len(eligible_markets),
                },
                now_utc=now,
            )
        low_quality_market_count = sum(
            1
            for penalty in self.state.market_quality_penalty_by_market.values()
            if penalty >= penalty_threshold
        )
        self.sqlite_store.save_metric(
            metric_name="low_quality_market_count",
            metric_value=float(low_quality_market_count),
            details=f"threshold={penalty_threshold}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "low_quality_market_count",
                "metric_value": low_quality_market_count,
                "threshold": penalty_threshold,
            },
            now_utc=now,
        )
        excluded_runtime_count = self.state.last_refresh_runtime_excluded_count
        exclusion_reason_summary = ",".join(
            f"{market_id}:{reason}"
            for market_id, reason in sorted(
                self.state.last_refresh_runtime_excluded_reason_by_market.items(),
                key=lambda item: item[0],
            )[:5]
        )
        self.sqlite_store.save_metric(
            metric_name="low_quality_runtime_excluded_count",
            metric_value=float(excluded_runtime_count),
            details=exclusion_reason_summary,
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "low_quality_runtime_excluded_count",
                "metric_value": excluded_runtime_count,
                "details": exclusion_reason_summary,
            },
            now_utc=now,
        )
        self._record_chronic_stale_exclusion_metrics(
            summary=chronic_stale_summary,
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
        if connection_recovering:
            self.state.ws_unhealthy = False
            self.state.book_state_unhealthy = False
        elif self.state.ws_unhealthy:
            self.state.ws_health_state = "degraded"
        else:
            self.state.ws_health_state = "healthy"
        if effective_stale_assets:
            self.state.total_stale_events += 1
        if effective_stale_assets:
            stale_ratio = len(effective_stale_assets) / max(1, len(tracked_assets))
            self.logger.warning(
                "Stale assets detected count=%s total=%s ratio=%.3f recovering=%s probation=%s",
                len(effective_stale_assets),
                len(tracked_assets),
                stale_ratio,
                len(recovering_assets),
                len(probation_assets),
            )

        candidate_unhealthy_assets = (
            health_unhealthy_assets if len(health_unhealthy_assets) < len(tracked_assets) else set()
        )
        if connection_recovering:
            candidate_unhealthy_assets = set()
        partial_unhealthy_assets = self._derive_asset_blocks(
            candidate_assets=candidate_unhealthy_assets,
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
        self.sqlite_store.save_metric(
            metric_name="market_state_blocked_count",
            metric_value=float(len(self.state.safe_mode_blocked_markets)),
            details=f"blocked_assets={len(self.state.safe_mode_blocked_assets)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "market_state_blocked_count",
                "metric_value": len(self.state.safe_mode_blocked_markets),
                "blocked_assets": len(self.state.safe_mode_blocked_assets),
            },
            now_utc=now,
        )

        candidate_global_reason = self._global_unhealthy_candidate_reason(
            tracked_assets=tracked_assets,
            stale_assets=effective_stale_assets,
            unhealthy_assets=health_unhealthy_assets,
        )
        if connection_recovering:
            candidate_global_reason = None
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
        if missing_resync_assets and not connection_recovering:
            await self.resync_assets(
                asset_ids=missing_resync_assets,
                reason=RESYNC_REASON_MISSING_BOOK_STATE,
            )

        missing_asset_set = set(missing_assets)
        stale_only_assets = [
            asset_id
            for asset_id in effective_stale_assets
            if asset_id not in missing_asset_set
            and self._is_asset_eligible_for_stale_resync(asset_id=asset_id, now_utc=now)
        ]
        stale_resync_assets = self._select_resync_assets(
            asset_ids=stale_only_assets,
            reason=RESYNC_REASON_STALE_ASSET,
            now_utc=now,
        )
        if stale_resync_assets and not connection_recovering:
            await self.resync_assets(
                asset_ids=stale_resync_assets,
                reason=RESYNC_REASON_STALE_ASSET,
            )

        if not connection_recovering and self._is_ws_idle(now):
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
        if self._is_connection_recovering(now_utc=now_utc):
            return False
        last_ws_message_at = self.healthcheck.state.last_ws_message_at
        if last_ws_message_at is None:
            return False
        age_ms = (now_utc - last_ws_message_at).total_seconds() * 1000.0
        if age_ms < self.config.settings.runtime.book_resync_idle_ms:
            return False
        return (
            now_utc.timestamp() - self.state.last_ws_idle_resync_ts
        ) * 1000.0 >= self.config.settings.runtime.book_resync_idle_ms

    def _is_asset_eligible_for_stale_resync(self, *, asset_id: str, now_utc: datetime) -> bool:
        if self._is_asset_recovering(asset_id=asset_id, now_utc=now_utc):
            return False
        if self._is_asset_in_probation(asset_id=asset_id, now_utc=now_utc):
            return False
        mapping = self.token_to_market_side.get(asset_id)
        if mapping is None:
            return False
        market_id, _ = mapping
        market = self.markets_by_id.get(market_id)
        if market is None:
            return False
        freshness_state = self.state.market_freshness_state_by_market.get(market_id)
        if freshness_state in {
            MARKET_FRESHNESS_PROBATION,
            MARKET_FRESHNESS_RECOVERING,
            MARKET_FRESHNESS_NOT_READY,
        }:
            return False
        min_ready_ratio = min(
            1.0,
            max(0.0, self.config.settings.runtime.stale_asset_resync_ready_ratio_min),
        )
        if self._market_ready_asset_ratio(market) < min_ready_ratio:
            return False
        return True

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
            same_reason_cooldown_ms = max(
                same_reason_cooldown_ms,
                self.config.settings.runtime.stale_asset_resync_additional_cooldown_ms,
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
            if self._is_connection_recovering(now_utc=now_utc) and reason in {
                RESYNC_REASON_STALE_ASSET,
                RESYNC_REASON_MISSING_BOOK_STATE,
                RESYNC_REASON_IDLE_TIMEOUT,
            }:
                continue
            if reason == RESYNC_REASON_STALE_ASSET:
                if asset_id not in self.state.ready_assets:
                    continue
                if not self._is_asset_eligible_for_stale_resync(asset_id=asset_id, now_utc=now_utc):
                    continue
            if reason in {RESYNC_REASON_STALE_ASSET, RESYNC_REASON_MISSING_BOOK_STATE} and (
                self._is_asset_recovering(asset_id=asset_id, now_utc=now_utc)
                or self._is_asset_in_probation(asset_id=asset_id, now_utc=now_utc)
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
                self._start_asset_recovery_tracking(asset_id=asset_id, reason=reason, now_utc=now)
                mapping = self.token_to_market_side.get(asset_id)
                market_id = mapping[0] if mapping is not None else None
                self._save_diagnostics_event(
                    event_name=DIAG_EVENT_RESYNC_STARTED,
                    now_utc=now,
                    asset_id=asset_id,
                    market_id=market_id,
                    reason=reason,
                )
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
                        no_signal_reason = (
                            "connection_recovering"
                            if self._is_connection_recovering(now_utc=now)
                            else "book_recovering"
                        )
                        self._record_no_signal_reason(
                            reason=no_signal_reason,
                            now_utc=now,
                            market_id=self.token_to_market_side.get(asset_id, ("", ""))[0] or None,
                            asset_id=asset_id,
                            details={"resync_reason": reason},
                        )
                        continue

                    update = self.quote_manager.apply_book_resync(summary)
                    self.healthcheck.on_asset_quote_update(asset_id=asset_id, ts=summary.timestamp)
                    self._record_first_quote_after_resync(asset_id=asset_id, now_utc=now)
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
                "universe_limit=%s watched_floor=%s stale_asset_ms=%s resync_idle_ms=%s"
            ),
            self.state.watched_markets,
            len(self.state.cumulative_watched_market_ids),
            self.state.subscribed_assets,
            len(self.state.cumulative_subscribed_asset_ids),
            self.config.settings.market_filters.max_markets_to_watch,
            self._effective_watched_floor(),
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
            on_transport_event=self.on_ws_transport_event,
            logger=self.logger,
            reconnect_base_seconds=self.config.settings.runtime.reconnect_base_seconds,
            reconnect_max_seconds=self.config.settings.runtime.reconnect_max_seconds,
            ping_interval_seconds=self.config.settings.runtime.websocket_ping_interval_seconds,
            ping_timeout_seconds=self.config.settings.runtime.websocket_ping_timeout_seconds,
            receive_timeout_seconds=self.config.settings.runtime.websocket_receive_timeout_seconds,
            receive_timeout_reconnect_count=(
                self.config.settings.runtime.websocket_receive_timeout_reconnect_count
            ),
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
