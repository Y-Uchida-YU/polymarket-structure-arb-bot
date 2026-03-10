from __future__ import annotations

import argparse
from pathlib import Path

import pytest

import src.main as main_module
from src.config.loader import AppConfig, MarketsConfig, Settings, load_app_config


@pytest.fixture
def fake_app_config(tmp_path: Path) -> AppConfig:
    settings = Settings(
        risk={"max_open_positions": 7, "stale_quote_ms": 2500},
        strategy={"entry_threshold_sum_ask": 0.97, "max_quote_age_ms_for_signal": 1500},
        runtime={"market_refresh_minutes": 5, "stale_asset_ms": 12000},
    )
    return AppConfig(root_dir=tmp_path, settings=settings, markets=MarketsConfig())


def test_build_startup_summary_reflects_runtime_flags(
    monkeypatch: pytest.MonkeyPatch,
    fake_app_config: AppConfig,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "src.config.loader.load_app_config",
        lambda root_dir, settings_path="config/settings.yaml": fake_app_config,
    )
    summary = main_module.build_startup_summary(
        root_dir=tmp_path,
        paper_mode=False,
        shadow_paper=True,
        dry_run=True,
        once=False,
        mode="dry_run",
        settings_path="config/settings.shadow.yaml",
    )
    assert summary["mode"] == "dry_run"
    assert summary["paper_trade"] is False
    assert summary["shadow_paper"] is True
    assert summary["dry_run"] is True
    assert summary["once"] is False
    assert summary["max_open_markets"] == 7
    assert summary["entry_threshold_sum_ask"] == 0.97


def test_print_startup_summary_outputs_mode_and_paper_flag(
    monkeypatch: pytest.MonkeyPatch,
    fake_app_config: AppConfig,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "src.config.loader.load_app_config",
        lambda root_dir, settings_path="config/settings.yaml": fake_app_config,
    )
    args = argparse.Namespace(
        command="run",
        validate_config=False,
        dry_run=True,
        once=False,
        paper=False,
        shadow_paper=True,
    )
    main_module.print_startup_summary(
        root_dir=tmp_path,
        args=args,
        settings_path="config/settings.shadow.yaml",
    )
    captured = capsys.readouterr().out
    assert "mode: dry_run" in captured
    assert "paper_trade: False" in captured
    assert "shadow_paper: True" in captured


def test_parse_args_supports_report_command() -> None:
    args = main_module.parse_args(["report", "--last-hours", "48", "--run-id", "run-1"])
    assert args.command == "report"
    assert args.last_hours == 48
    assert args.run_id == "run-1"


def test_resolve_settings_path_uses_shadow_preset_when_available(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "settings.shadow.yaml").write_text("runtime: {}\n", encoding="utf-8")
    args = argparse.Namespace(
        command="run",
        shadow_paper=True,
        settings_path="config/settings.yaml",
    )
    resolved = main_module.resolve_settings_path(root_dir=tmp_path, args=args)
    assert resolved == "config/settings.shadow.yaml"


def test_run_report_prints_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_app_config = AppConfig(
        root_dir=tmp_path,
        settings=Settings(
            storage={
                "sqlite_path": "data/state/state.db",
                "export_dir": "data/exports",
                "log_dir": "data/logs",
            }
        ),
        markets=MarketsConfig(),
    )

    class FakeGenerator:
        def __init__(self, db_path: Path, export_dir: Path) -> None:
            self.db_path = db_path
            self.export_dir = export_dir

        def generate(
            self,
            *,
            date: str | None,
            last_hours: int | None,
            run_id: str | None = None,
        ) -> dict[str, object]:
            assert date is None
            assert last_hours == 24
            assert run_id == "run-1"
            return {
                "window": {
                    "start": "2026-03-10T00:00:00+00:00",
                    "end": "2026-03-11T00:00:00+00:00",
                },
                "run_id_filter": run_id,
                "totals": {
                    "total_signals": 1,
                    "total_fills": 1,
                    "fill_rate": 1.0,
                    "matched_fill_rate": 1.0,
                    "one_leg_occurrence_count": 0,
                    "one_leg_occurrence_rate": 0.0,
                    "stale_reject_count": 0,
                    "stale_reject_rate": 0.0,
                    "depth_reject_count": 0,
                    "depth_reject_rate": 0.0,
                    "resync_count": 0,
                    "safe_mode_count": 0,
                    "projected_matched_pnl": 0.1,
                    "unmatched_inventory_mtm": 0.0,
                    "total_projected_pnl": 0.1,
                },
                "warnings": [],
                "top_markets_by_signal_count": [],
                "top_markets_by_pnl": [],
                "top_reject_reasons": [],
            }

        def save(self, report: dict[str, object]) -> tuple[Path, Path]:
            _ = report
            return (tmp_path / "report.json", tmp_path / "report.csv")

        @staticmethod
        def format_console(report: dict[str, object]) -> str:
            _ = report
            return "ok-report"

    monkeypatch.setattr(
        "src.config.loader.load_app_config",
        lambda *args, **kwargs: fake_app_config,
    )
    monkeypatch.setattr("src.reporting.daily_report.DailyReportGenerator", FakeGenerator)
    args = argparse.Namespace(date=None, last_hours=24, run_id="run-1")
    main_module.run_report(
        root_dir=tmp_path,
        args=args,
        settings_path="config/settings.yaml",
    )
    output = capsys.readouterr().out
    assert "ok-report" in output
    assert "saved_json:" in output
    assert "saved_csv:" in output


def test_shadow_settings_shrink_universe_limit() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_app_config(root_dir=repo_root, settings_path="config/settings.shadow.yaml")
    assert config.settings.market_filters.max_markets_to_watch is not None
    assert config.settings.market_filters.max_markets_to_watch <= 50
