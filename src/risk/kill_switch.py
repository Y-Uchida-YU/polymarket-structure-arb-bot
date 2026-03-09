from __future__ import annotations


class KillSwitch:
    def __init__(self, max_consecutive_errors: int = 20) -> None:
        self.max_consecutive_errors = max_consecutive_errors
        self.consecutive_errors = 0

    def record_error(self) -> None:
        self.consecutive_errors += 1

    def record_success(self) -> None:
        self.consecutive_errors = 0

    def is_triggered(self) -> bool:
        return self.consecutive_errors >= self.max_consecutive_errors
