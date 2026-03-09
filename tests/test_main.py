from __future__ import annotations

import argparse
from pathlib import Path

import pytest

import src.main as main_module
from src.config.loader import AppConfig, MarketsConfig, Settings


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
        lambda root_dir: fake_app_config,
    )
    summary = main_module.build_startup_summary(
        root_dir=tmp_path,
        paper_mode=False,
        dry_run=True,
        once=False,
        mode="dry_run",
    )
    assert summary["mode"] == "dry_run"
    assert summary["paper_trade"] is False
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
        lambda root_dir: fake_app_config,
    )
    args = argparse.Namespace(
        validate_config=False,
        dry_run=True,
        once=False,
        paper=False,
    )
    main_module.print_startup_summary(root_dir=tmp_path, args=args)
    captured = capsys.readouterr().out
    assert "mode: dry_run" in captured
    assert "paper_trade: False" in captured
