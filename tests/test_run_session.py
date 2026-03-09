from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path

from src.app.bootstrap import PolymarketStructureArbApp
from src.config.loader import AppConfig, MarketsConfig, Settings
from src.domain.market import BinaryMarket
from src.domain.signal import ArbSignal


def _make_app(tmp_path: Path) -> PolymarketStructureArbApp:
    logger = logging.getLogger("test_run_session")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    settings = Settings(
        storage={"sqlite_path": "state.db", "export_dir": "exports", "log_dir": "logs"},
    )
    config = AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())
    return PolymarketStructureArbApp(config=config, logger=logger)


def test_run_id_is_persisted_in_signal_log(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    signal = ArbSignal.new(
        market_id="m1",
        slug="market-1",
        yes_token_id="yes1",
        no_token_id="no1",
        ask_yes=0.45,
        ask_no=0.5,
        threshold=0.98,
        detected_at=datetime(2026, 3, 10, tzinfo=UTC),
        reason="sum_ask_le_threshold",
    )
    app._record_signal(signal)

    row = app.sqlite_store.conn.execute(
        "SELECT run_id FROM signals WHERE signal_id = ?",
        (signal.signal_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == app.state.run_id
    asyncio.run(app.shutdown())


def test_run_summary_is_generated(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    app.state.total_signals = 3
    app.state.total_fills = 2
    app.state.cumulative_projected_pnl = 1.23
    app.state.safe_mode_count = 1
    app.state.total_exceptions = 2
    app.state.total_stale_events = 4
    app.state.total_resync_events = 5
    app.emit_run_summary(status="completed")

    row = app.sqlite_store.conn.execute(
        """
        SELECT
          run_id, total_signals, total_fills, total_projected_pnl,
          safe_mode_count, exception_count
        FROM run_summaries
        WHERE run_id = ?
        """,
        (app.state.run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == app.state.run_id
    assert row[1] == 3
    assert row[2] == 2
    assert float(row[3]) == 1.23
    assert row[4] == 1
    assert row[5] == 2
    asyncio.run(app.shutdown())


def test_periodic_snapshot_is_generated(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    app.state.total_signals = 10
    app.state.total_fills = 6
    app.state.cumulative_projected_pnl = 2.5
    app.state.cumulative_unmatched_inventory = 3.0
    app.state.total_resync_events = 4
    app.state.stale_assets = {"a1", "a2"}
    app.markets_by_id = {
        "m1": BinaryMarket(
            market_id="m1",
            question="q1",
            slug="s1",
            category="crypto",
            end_time=None,
            condition_id=None,
            yes_token_id="yes1",
            no_token_id="no1",
            raw={},
        ),
        "m2": BinaryMarket(
            market_id="m2",
            question="q2",
            slug="s2",
            category="crypto",
            end_time=None,
            condition_id=None,
            yes_token_id="yes2",
            no_token_id="no2",
            raw={},
        ),
    }

    asyncio.run(app.emit_periodic_snapshot())
    row = app.sqlite_store.conn.execute(
        """
        SELECT
          total_signals, total_fills, open_unmatched_inventory,
          cumulative_projected_pnl, stale_assets
        FROM run_snapshots
        WHERE run_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (app.state.run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == 10
    assert row[1] == 6
    assert row[2] == 3.0
    assert row[3] == 2.5
    assert row[4] == 2
    asyncio.run(app.shutdown())


def test_periodic_report_is_exported(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    asyncio.run(app.emit_periodic_report())
    reports = list((tmp_path / "exports").glob("daily_report_*.json"))
    assert reports
    asyncio.run(app.shutdown())


def test_non_stale_safe_mode_reason_is_not_auto_cleared(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    now = datetime(2026, 3, 10, tzinfo=UTC)
    app.state.safe_mode_active = True
    app.state.safe_mode_reason = "high_signal_rate"
    app.state.safe_mode_entered_at = now

    asyncio.run(app.check_data_freshness_and_resync())
    assert app.state.safe_mode_active is True
    assert app.state.safe_mode_reason == "high_signal_rate"
    asyncio.run(app.shutdown())
