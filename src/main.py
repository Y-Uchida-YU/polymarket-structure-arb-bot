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


def print_startup_summary(root_dir: Path) -> None:
    from src.config.loader import load_app_config

    config = load_app_config(root_dir=root_dir)
    print("=== Startup Summary ===")
    print(f"paper_trade: {True}")
    print(f"max_open_markets: {config.settings.risk.max_open_positions}")
    print(f"entry_threshold_sum_ask: {config.settings.strategy.entry_threshold_sum_ask}")
    print(f"stale_quote_ms: {config.settings.risk.stale_quote_ms}")
    print(f"market_refresh_minutes: {config.settings.runtime.market_refresh_minutes}")
    print("NOTICE: Live trading/auth/signing/balance/cancel-replace are NOT implemented.")


def main() -> None:
    from src.app.bootstrap import run_app

    args = parse_args()
    root_dir = Path(__file__).resolve().parents[1]
    print_startup_summary(root_dir=root_dir)
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
