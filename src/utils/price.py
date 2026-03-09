from __future__ import annotations

import math


def normalize_tick_size(tick_size: float | None, default_tick_size: float = 0.001) -> float:
    if tick_size is None or tick_size <= 0:
        return default_tick_size
    return tick_size


def is_price_aligned_to_tick(price: float, tick_size: float, tolerance: float = 1e-9) -> bool:
    normalized_tick = normalize_tick_size(tick_size)
    ticks = price / normalized_tick
    return math.isclose(ticks, round(ticks), rel_tol=0.0, abs_tol=tolerance)


def align_price_to_tick(
    price: float,
    tick_size: float,
    side: str,
    tolerance: float = 1e-9,
) -> float:
    """
    BUY side aligns upward (more conservative cost).
    SELL side aligns downward.
    """
    normalized_tick = normalize_tick_size(tick_size)
    ticks = price / normalized_tick
    if side.strip().upper() == "BUY":
        aligned_ticks = math.ceil(ticks - tolerance)
    else:
        aligned_ticks = math.floor(ticks + tolerance)
    decimals = _tick_decimal_places(normalized_tick)
    return round(aligned_ticks * normalized_tick, decimals)


def _tick_decimal_places(tick_size: float) -> int:
    text = format(tick_size, ".12f").rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])
