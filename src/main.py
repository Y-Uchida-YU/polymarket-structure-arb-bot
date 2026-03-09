from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket Structure Arb Bot (MVP)")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch and filter markets once, then exit (no websocket loop).",
    )
    return parser.parse_args()


def main() -> None:
    from src.app.bootstrap import run_app

    args = parse_args()
    root_dir = Path(__file__).resolve().parents[1]
    asyncio.run(run_app(root_dir=root_dir, once=args.once))


if __name__ == "__main__":
    main()
