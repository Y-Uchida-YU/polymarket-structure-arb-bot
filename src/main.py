from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket Structure Arb Bot")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run paper/shadow-paper bot")
    run_parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch and filter markets once, then exit (no websocket loop).",
    )
    run_parser.add_argument(
        "--paper",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run in paper mode only. Live mode is not implemented.",
    )
    run_parser.add_argument(
        "--shadow-paper",
        action="store_true",
        help="Enable long-run shadow paper mode with stronger reporting/guardrail intent.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and market loading path, then exit.",
    )
    run_parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate .env/settings/markets and print startup summary without running.",
    )
    run_parser.add_argument(
        "--settings-path",
        default="config/settings.yaml",
        help="Settings file path (relative to repo root).",
    )

    report_parser = subparsers.add_parser("report", help="Generate shadow paper report")
    report_parser.add_argument("--date", help="UTC date in YYYY-MM-DD format.", default=None)
    report_parser.add_argument(
        "--last-hours",
        type=int,
        default=24,
        help="Lookback hours for report if --date is not specified.",
    )
    report_parser.add_argument("--run-id", default=None, help="Optional run_id filter.")
    report_parser.add_argument(
        "--settings-path",
        default="config/settings.yaml",
        help="Settings file path (relative to repo root).",
    )

    argv_list = list(argv) if argv is not None else sys.argv[1:]
    if not argv_list or argv_list[0].startswith("-"):
        argv_list = ["run", *argv_list]
    return parser.parse_args(argv_list)


def resolve_settings_path(root_dir: Path, args: argparse.Namespace) -> str:
    settings_path = str(getattr(args, "settings_path", "config/settings.yaml"))
    if (
        getattr(args, "command", "run") == "run"
        and getattr(args, "shadow_paper", False)
        and settings_path == "config/settings.yaml"
    ):
        shadow_path = root_dir / "config/settings.shadow.yaml"
        if shadow_path.exists():
            return "config/settings.shadow.yaml"
    return settings_path


def build_startup_summary(
    root_dir: Path,
    *,
    paper_mode: bool,
    shadow_paper: bool,
    dry_run: bool,
    once: bool,
    mode: str,
    settings_path: str,
) -> dict[str, object]:
    from src.config.loader import load_app_config

    config = load_app_config(root_dir=root_dir, settings_path=settings_path)
    return {
        "mode": mode,
        "runtime_environment": config.settings.runtime.environment_name,
        "paper_trade": paper_mode,
        "shadow_paper": shadow_paper,
        "dry_run": dry_run,
        "once": once,
        "paper_only": True,
        "live_implemented": False,
        "config_path": settings_path,
        "entry_threshold_sum_ask": config.settings.strategy.entry_threshold_sum_ask,
        "adjusted_edge_min": config.settings.strategy.adjusted_edge_min,
        "max_spread_per_leg": config.settings.strategy.max_spread_per_leg,
        "min_depth_per_leg": config.settings.strategy.min_depth_per_leg,
        "max_open_markets": config.settings.risk.max_open_positions,
        "max_positions_per_market": config.settings.risk.max_positions_per_market,
        "max_daily_signals": config.settings.risk.max_daily_signals,
        "stale_quote_ms": config.settings.risk.stale_quote_ms,
        "signal_quote_age_limit_ms": config.settings.strategy.max_quote_age_ms_for_signal,
        "stale_asset_ms": config.settings.runtime.stale_asset_ms,
        "book_resync_idle_ms": config.settings.runtime.book_resync_idle_ms,
        "market_refresh_minutes": config.settings.runtime.market_refresh_minutes,
        "snapshot_interval_minutes": config.settings.runtime.snapshot_interval_minutes,
        "report_export_interval_minutes": config.settings.runtime.report_export_interval_minutes,
        "guardrail_window_minutes": config.settings.guardrails.window_minutes,
        "guardrail_max_signal_rate_per_min": config.settings.guardrails.max_signal_rate_per_min,
        "guardrail_max_reject_rate": config.settings.guardrails.max_reject_rate,
        "guardrail_max_one_leg_rate": config.settings.guardrails.max_one_leg_rate,
        "guardrail_max_unmatched_rate": config.settings.guardrails.max_unmatched_rate,
        "guardrail_max_stale_asset_rate": config.settings.guardrails.max_stale_asset_rate,
        "guardrail_max_resync_rate_per_min": config.settings.guardrails.max_resync_rate_per_min,
        "guardrail_max_exception_rate_per_min": (
            config.settings.guardrails.max_exception_rate_per_min
        ),
        "output_log_directory": str(config.log_dir),
        "output_directory": str(config.export_dir),
    }


def print_startup_summary(root_dir: Path, args: argparse.Namespace, settings_path: str) -> None:
    mode = "shadow_paper" if getattr(args, "shadow_paper", False) else "paper_run"
    if getattr(args, "validate_config", False):
        mode = "validate_config"
    elif getattr(args, "dry_run", False):
        mode = "dry_run"
    elif getattr(args, "once", False):
        mode = "once"
    summary = build_startup_summary(
        root_dir=root_dir,
        paper_mode=getattr(args, "paper", True),
        shadow_paper=getattr(args, "shadow_paper", False),
        dry_run=getattr(args, "dry_run", False),
        once=getattr(args, "once", False),
        mode=mode,
        settings_path=settings_path,
    )
    print("=== Startup Summary ===")
    for key, value in summary.items():
        print(f"{key}: {value}")
    print("NOTICE: Live trading/auth/signing/balance/cancel-replace are NOT implemented.")


def run_report(root_dir: Path, args: argparse.Namespace, settings_path: str) -> None:
    from src.config.loader import load_app_config
    from src.reporting.daily_report import DailyReportGenerator

    config = load_app_config(root_dir=root_dir, settings_path=settings_path)
    generator = DailyReportGenerator(db_path=config.sqlite_path, export_dir=config.export_dir)
    report = generator.generate(
        date=args.date,
        last_hours=args.last_hours,
        run_id=args.run_id,
    )
    json_path, csv_path = generator.save(report)
    print(generator.format_console(report))
    print(f"saved_json: {json_path}")
    print(f"saved_csv: {csv_path}")


def main() -> None:
    from src.app.bootstrap import run_app

    args = parse_args()
    root_dir = Path(__file__).resolve().parents[1]
    settings_path = resolve_settings_path(root_dir=root_dir, args=args)

    if args.command == "report":
        run_report(root_dir=root_dir, args=args, settings_path=settings_path)
        return

    print_startup_summary(root_dir=root_dir, args=args, settings_path=settings_path)
    if args.validate_config:
        print("Config validation succeeded.")
        return

    paper_mode = bool(args.paper)
    if args.shadow_paper:
        paper_mode = True

    asyncio.run(
        run_app(
            root_dir=root_dir,
            once=args.once,
            paper_mode=paper_mode,
            dry_run=args.dry_run,
            shadow_paper=args.shadow_paper,
            settings_path=settings_path,
        )
    )


if __name__ == "__main__":
    main()
