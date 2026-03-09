from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket Structure Arb Bot (Paper Trading v3)")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch and filter markets once, then exit (no websocket loop).",
    )
    parser.add_argument(
        "--paper",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run in paper mode only. Live mode is not implemented.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and market loading path, then exit.",
    )
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate .env/settings/markets and print startup summary without running.",
    )
    return parser.parse_args()


def build_startup_summary(
    root_dir: Path,
    *,
    paper_mode: bool,
    dry_run: bool,
    once: bool,
    mode: str,
) -> dict[str, object]:
    from src.config.loader import load_app_config

    config = load_app_config(root_dir=root_dir)
    return {
        "mode": mode,
        "paper_trade": paper_mode,
        "dry_run": dry_run,
        "once": once,
        "max_open_markets": config.settings.risk.max_open_positions,
        "entry_threshold_sum_ask": config.settings.strategy.entry_threshold_sum_ask,
        "stale_quote_ms": config.settings.risk.stale_quote_ms,
        "max_quote_age_ms_for_signal": config.settings.strategy.max_quote_age_ms_for_signal,
        "stale_asset_ms": config.settings.runtime.stale_asset_ms,
        "market_refresh_minutes": config.settings.runtime.market_refresh_minutes,
    }


def print_startup_summary(root_dir: Path, args: argparse.Namespace) -> None:
    mode = "run"
    if args.validate_config:
        mode = "validate_config"
    elif args.dry_run:
        mode = "dry_run"
    elif args.once:
        mode = "once"
    summary = build_startup_summary(
        root_dir=root_dir,
        paper_mode=args.paper,
        dry_run=args.dry_run,
        once=args.once,
        mode=mode,
    )
    print("=== Startup Summary ===")
    for key, value in summary.items():
        print(f"{key}: {value}")
    print("NOTICE: Live trading/auth/signing/balance/cancel-replace are NOT implemented.")


def main() -> None:
    from src.app.bootstrap import run_app

    args = parse_args()
    root_dir = Path(__file__).resolve().parents[1]
    print_startup_summary(root_dir=root_dir, args=args)
    if args.validate_config:
        print("Config validation succeeded.")
        return
    asyncio.run(
        run_app(
            root_dir=root_dir,
            once=args.once,
            paper_mode=args.paper,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
