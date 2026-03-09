from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from src.clients.gamma_client import GammaClient
from src.clients.ws_client import MarketWebSocketClient
from src.config.loader import AppConfig, load_app_config
from src.domain.market import BinaryMarket
from src.domain.position import Position
from src.domain.signal import ArbSignal
from src.execution.order_router import PaperOrderRouter
from src.execution.quote_manager import QuoteManager
from src.monitoring.healthcheck import HealthCheck
from src.monitoring.notifier import Notifier
from src.risk.exposure import ExposureManager
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


class PolymarketStructureArbApp:
    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

        self.gamma_client = GammaClient(
            base_url=config.settings.api.gamma_base_url,
            markets_endpoint=config.settings.api.gamma_markets_endpoint,
        )
        self.strategy = ComplementArbStrategy(
            ComplementArbConfig(
                entry_threshold_sum_ask=config.settings.strategy.entry_threshold_sum_ask,
                min_ask=config.settings.strategy.min_ask,
                max_ask=config.settings.strategy.max_ask,
            )
        )
        self.risk_limiter = RiskLimiter(
            max_open_positions=config.settings.risk.max_open_positions,
            max_positions_per_market=config.settings.risk.max_positions_per_market,
            max_daily_signals=config.settings.risk.max_daily_signals,
            expiry_block_minutes=config.settings.strategy.expiry_block_minutes,
        )
        self.order_router = PaperOrderRouter(order_size_usdc=config.settings.risk.paper_order_size_usdc)
        self.csv_logger = CsvEventLogger(export_dir=config.export_dir)
        self.sqlite_store = SQLiteStore(db_path=config.sqlite_path)
        self.exposure = ExposureManager()
        self.kill_switch = KillSwitch(max_consecutive_errors=20)
        self.healthcheck = HealthCheck(started_at=utc_now())
        self.notifier = Notifier(logger=logger)

        self.markets_by_id: dict[str, BinaryMarket] = {}
        self.token_to_market_side: dict[str, tuple[str, str]] = {}
        self.quote_manager = QuoteManager(token_to_market_side={})
        self.state = AppState(last_signal_at_by_market={})
        self.stop_event = asyncio.Event()

    async def load_markets(self) -> int:
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
        self.quote_manager = QuoteManager(token_to_market_side=token_to_market_side)
        self.logger.info(
            "Loaded raw_markets=%s binary_markets=%s subscribed_assets=%s",
            len(raw_markets),
            len(binary_markets),
            len(token_to_market_side),
        )
        return len(binary_markets)

    async def handle_ws_message(self, raw_message: str) -> None:
        now = utc_now()
        try:
            updates = self.quote_manager.ingest_ws_message(raw_message)
            if not updates:
                return

            for update in updates:
                self.healthcheck.on_ws_message(now)
                mapping = self.token_to_market_side.get(update.asset_id)
                if mapping is None:
                    continue
                market_id, side = mapping
                self.sqlite_store.save_quote(market_id=market_id, side=side, update=update)
                market = self.markets_by_id.get(market_id)
                if market is None:
                    continue

                ask_yes, ask_no = self.quote_manager.get_market_asks(market_id)
                signal = self.strategy.evaluate(
                    market=market,
                    ask_yes=ask_yes,
                    ask_no=ask_no,
                    now_utc=now,
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

                self._record_signal(signal)
                self._execute_paper_trade(signal=signal, now=now)
                self.state.last_signal_at_by_market[market_id] = now.timestamp()
                self.exposure.increment_daily_signal_count(now.date())
                self.healthcheck.on_signal(now)
                self.kill_switch.record_success()
        except Exception as exc:  # noqa: BLE001
            self.kill_switch.record_error()
            self.healthcheck.on_error(now)
            error_text = f"{type(exc).__name__}: {exc}"
            self.logger.exception("Failed to process ws message: %s", error_text)
            self.sqlite_store.save_error(stage="ws_message", error_message=error_text, created_at_iso=now.isoformat())
            self.csv_logger.log_error(
                {
                    "created_at": now.isoformat(),
                    "stage": "ws_message",
                    "error_message": error_text,
                },
                now_utc=now,
            )
            if self.kill_switch.is_triggered():
                self.notifier.error("Kill switch triggered. Stopping app.")
                self.stop_event.set()

    def _passes_cooldown(self, market_id: str, now_ts: float) -> bool:
        cooldown = self.config.settings.strategy.signal_cooldown_seconds
        last_ts = self.state.last_signal_at_by_market.get(market_id)
        if last_ts is None:
            return True
        return now_ts - last_ts >= cooldown

    def _record_signal(self, signal: ArbSignal) -> None:
        self.sqlite_store.save_signal(signal)
        self.csv_logger.log_signal(
            {
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "slug": signal.slug,
                "yes_token_id": signal.yes_token_id,
                "no_token_id": signal.no_token_id,
                "ask_yes": signal.ask_yes,
                "ask_no": signal.ask_no,
                "sum_ask": signal.sum_ask,
                "threshold": signal.threshold,
                "detected_at": signal.detected_at.isoformat(),
                "reason": signal.reason,
            },
            now_utc=signal.detected_at,
        )
        self.logger.info(
            "Signal detected market=%s slug=%s ask_yes=%.4f ask_no=%.4f sum=%.4f threshold=%.4f",
            signal.market_id,
            signal.slug,
            signal.ask_yes,
            signal.ask_no,
            signal.sum_ask,
            signal.threshold,
        )

    def _execute_paper_trade(self, signal: ArbSignal, now: datetime) -> None:
        result = self.order_router.execute_signal(signal=signal, now_utc=now)
        fills_by_token = {fill.token_id: fill for fill in result.fills}
        yes_fill = fills_by_token.get(signal.yes_token_id)
        no_fill = fills_by_token.get(signal.no_token_id)
        if yes_fill is not None and no_fill is not None:
            self.exposure.add_position(
                Position(
                    market_id=signal.market_id,
                    signal_id=signal.signal_id,
                    yes_qty=yes_fill.filled_qty,
                    no_qty=no_fill.filled_qty,
                    yes_entry_price=yes_fill.fill_price,
                    no_entry_price=no_fill.fill_price,
                    opened_at=now,
                )
            )

        for order in result.orders:
            self.sqlite_store.save_order(order)
            self.csv_logger.log_order(
                {
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
            self.sqlite_store.save_fill(fill)
            self.csv_logger.log_fill(
                {
                    "fill_id": fill.fill_id,
                    "order_id": fill.order_id,
                    "signal_id": fill.signal_id,
                    "market_id": fill.market_id,
                    "token_id": fill.token_id,
                    "filled_qty": fill.filled_qty,
                    "fill_price": fill.fill_price,
                    "fee": fill.fee,
                    "filled_at": fill.filled_at.isoformat(),
                },
                now_utc=now,
            )

        self.sqlite_store.save_pnl_snapshot(
            signal_id=signal.signal_id,
            market_id=signal.market_id,
            estimated_final_pnl=result.estimated_final_pnl,
            created_at_iso=now.isoformat(),
        )
        self.csv_logger.log_pnl(
            {
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "estimated_final_pnl": result.estimated_final_pnl,
                "created_at": now.isoformat(),
            },
            now_utc=now,
        )

    async def run(self, once: bool = False) -> None:
        market_count = await self.load_markets()
        if market_count == 0:
            self.notifier.error("No eligible binary markets after filters.")
            return
        if once:
            self.notifier.info("One-shot mode finished.")
            return

        ws_client = MarketWebSocketClient(
            url=self.config.settings.api.ws_market_url,
            asset_ids=list(self.token_to_market_side.keys()),
            on_message=self.handle_ws_message,
            logger=self.logger,
            reconnect_base_seconds=self.config.settings.runtime.reconnect_base_seconds,
            reconnect_max_seconds=self.config.settings.runtime.reconnect_max_seconds,
            ping_interval_seconds=self.config.settings.runtime.websocket_ping_interval_seconds,
            ping_timeout_seconds=self.config.settings.runtime.websocket_ping_timeout_seconds,
        )
        await ws_client.run_forever(stop_event=self.stop_event)

    async def shutdown(self) -> None:
        self.sqlite_store.close()


async def run_app(root_dir: Path, once: bool = False) -> None:
    config = load_app_config(root_dir=root_dir)
    logger = setup_logging(config.log_dir)
    app = PolymarketStructureArbApp(config=config, logger=logger)
    try:
        await app.run(once=once)
    finally:
        await app.shutdown()
