from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
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
from src.strategy.filters import extract_binary_markets
from src.utils.clock import utc_now


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
    stale_assets: set[str] = field(default_factory=set)
    safe_mode_active: bool = False
    safe_mode_reason: str | None = None
    safe_mode_entered_at: datetime | None = None
    safe_mode_count: int = 0
    ws_connected_at: datetime | None = None
    subscription_started_at: datetime | None = None
    first_quote_received_at: datetime | None = None
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


@dataclass(slots=True)
class MarketLoadResult:
    market_count: int
    asset_ids_changed: bool
    asset_ids: list[str]


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
        raw_markets = await self.gamma_client.fetch_active_markets(
            page_size=self.config.settings.api.gamma_page_size,
            max_pages=self.config.settings.api.gamma_max_pages,
        )
        binary_markets = extract_binary_markets(
            raw_markets=raw_markets,
            market_filters=self.config.settings.market_filters,
            markets_config=self.config.markets,
        )

        token_to_market_side: dict[str, tuple[str, str]] = {}
        self.markets_by_id = {market.market_id: market for market in binary_markets}
        for market in binary_markets:
            token_to_market_side[market.yes_token_id] = (market.market_id, "yes")
            token_to_market_side[market.no_token_id] = (market.market_id, "no")
            self.sqlite_store.upsert_market(market, updated_at_iso=utc_now().isoformat())

        self.token_to_market_side = token_to_market_side
        self.quote_manager.update_token_mapping(token_to_market_side)
        asset_ids = list(token_to_market_side.keys())
        await self._load_initial_tick_sizes(asset_ids)

        current_asset_ids = set(asset_ids)
        asset_ids_changed = bool(previous_asset_ids) and previous_asset_ids != current_asset_ids
        self.logger.info(
            "Loaded raw_markets=%s binary_markets=%s subscribed_assets=%s changed=%s",
            len(raw_markets),
            len(binary_markets),
            len(token_to_market_side),
            asset_ids_changed,
        )
        return MarketLoadResult(
            market_count=len(binary_markets),
            asset_ids_changed=asset_ids_changed,
            asset_ids=asset_ids,
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

                market = self.markets_by_id.get(market_id)
                if market is None or self.state.safe_mode_active:
                    continue
                yes_quote, no_quote = self.quote_manager.get_market_quotes(market_id)
                if yes_quote is None or no_quote is None:
                    continue
                if self._market_has_stale_quotes(market=market, now_utc=now):
                    continue

                tick_yes = self.quote_manager.get_tick_size(market.yes_token_id)
                tick_no = self.quote_manager.get_tick_size(market.no_token_id)
                signal = self.strategy.evaluate_with_quotes(
                    market=market,
                    yes_quote=yes_quote,
                    no_quote=no_quote,
                    now_utc=now,
                    tick_size_yes=tick_yes,
                    tick_size_no=tick_no,
                    order_size_usdc=self.config.settings.risk.paper_order_size_usdc,
                )
                if signal is None:
                    continue
                if not self._passes_cooldown(market_id=market_id, now_ts=now.timestamp()):
                    continue

                risk_decision = self.risk_limiter.evaluate_new_signal(
                    market=market,
                    exposure=self.exposure,
                    now_utc=now,
                )
                if not risk_decision.allowed:
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

    async def refresh_market_universe(self) -> None:
        try:
            result = await self.load_markets()
            if result.asset_ids_changed:
                self.logger.info("Market universe changed; requesting websocket resubscribe.")
                self.resubscribe_event.set()
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
        await self.resync_assets(asset_ids=asset_ids, reason="ws_connected")

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
        now = utc_now()
        self.sqlite_store.save_metric(
            metric_name="ws_reconnect_required",
            metric_value=1.0,
            details=reason,
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
                if asset_id not in self.healthcheck.state.asset_last_update_at
            ]
            self.state.stale_assets = set()
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
            if self.state.safe_mode_reason == "all_assets_stale":
                self._maybe_exit_safe_mode(now)
            self._evaluate_guardrails(now_utc=now, stale_asset_rate=0.0)
            return

        stale_assets = self.healthcheck.stale_assets(
            tracked_assets=tracked_assets,
            now_utc=now,
            max_age_ms=self.config.settings.runtime.stale_asset_ms,
        )
        self.state.stale_assets = set(stale_assets)
        self.sqlite_store.save_metric(
            metric_name="stale_asset_count",
            metric_value=float(len(stale_assets)),
            details=f"tracked_assets={len(tracked_assets)}",
            created_at_iso=now.isoformat(),
            run_id=self.state.run_id,
        )
        self.csv_logger.log_metric(
            {
                "run_id": self.state.run_id,
                "created_at": now.isoformat(),
                "metric_name": "stale_asset_count",
                "metric_value": len(stale_assets),
                "tracked_assets": len(tracked_assets),
            },
            now_utc=now,
        )
        if stale_assets:
            self.state.total_stale_events += 1
        if not stale_assets:
            if self.state.safe_mode_reason == "all_assets_stale":
                self._maybe_exit_safe_mode(now)
        else:
            stale_ratio = len(stale_assets) / max(1, len(tracked_assets))
            self.logger.warning(
                "Stale assets detected count=%s total=%s ratio=%.3f",
                len(stale_assets),
                len(tracked_assets),
                stale_ratio,
            )
            if len(stale_assets) == len(tracked_assets):
                self._enter_safe_mode(
                    reason="all_assets_stale",
                    now_utc=now,
                )
            await self.resync_assets(asset_ids=stale_assets, reason="stale_asset")

        if self._is_ws_idle(now):
            self.logger.warning(
                "WebSocket stream appears idle for over %sms. Triggering full resync.",
                self.config.settings.runtime.book_resync_idle_ms,
            )
            self.state.last_ws_idle_resync_ts = now.timestamp()
            await self.resync_assets(asset_ids=tracked_assets, reason="ws_idle_gap")

        self._evaluate_guardrails(
            now_utc=now,
            stale_asset_rate=len(stale_assets) / max(1, len(tracked_assets)),
        )

    def _enter_safe_mode(self, reason: str, now_utc: datetime) -> None:
        if self.state.safe_mode_active and self.state.safe_mode_reason == reason:
            return
        self.state.safe_mode_active = True
        self.state.safe_mode_reason = reason
        self.state.safe_mode_entered_at = now_utc
        self.state.safe_mode_count += 1
        self.logger.error("Safe mode enabled reason=%s", reason)
        self.sqlite_store.save_metric(
            metric_name="safe_mode_entered",
            metric_value=1.0,
            details=reason,
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
            },
            now_utc=now_utc,
        )

    def _maybe_exit_safe_mode(self, now_utc: datetime) -> None:
        if not self.state.safe_mode_active:
            return
        self.state.safe_mode_active = False
        previous_reason = self.state.safe_mode_reason
        self.state.safe_mode_reason = None
        self.state.safe_mode_entered_at = None
        self.logger.warning("Safe mode disabled previous_reason=%s", previous_reason)
        self.sqlite_store.save_metric(
            metric_name="safe_mode_exited",
            metric_value=1.0,
            details=previous_reason or "",
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
                    "exception_rate=%.2f"
                ),
                warning,
                decision.metrics.signal_rate_per_min,
                decision.metrics.reject_rate,
                decision.metrics.one_leg_rate,
                decision.metrics.stale_asset_rate,
                decision.metrics.resync_rate_per_min,
                decision.metrics.exception_rate_per_min,
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
                    f"exception_rate_per_min={decision.metrics.exception_rate_per_min:.4f}"
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
                },
                now_utc=now_utc,
            )
        if decision.enter_safe_mode_reason is not None:
            self._enter_safe_mode(reason=decision.enter_safe_mode_reason, now_utc=now_utc)
        elif decision.exit_safe_mode:
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

    async def resync_assets(self, asset_ids: list[str], reason: str) -> None:
        if not asset_ids:
            return
        now = utc_now()
        batch_size = max(1, self.config.settings.runtime.resync_batch_size)
        for index in range(0, len(asset_ids), batch_size):
            batch = asset_ids[index : index + batch_size]
            for asset_id in batch:
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
                        continue

                    update = self.quote_manager.apply_book_resync(summary)
                    self.healthcheck.on_asset_quote_update(asset_id=asset_id, ts=summary.timestamp)
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
