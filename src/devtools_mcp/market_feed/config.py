"""Native representation limits plus named server time/memory admission budgets."""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from typing import Any

DEFAULTS = {
    "symbols": 16,
    "depth": 8,
    "seed": 1,
    "rate": 1000,
    "batch": 256,
    "burst": 256,
    "seconds": 60,
    "max_messages": 0,
    "memory_mib": 256,
    "unlimited": False,
}
# Match the native v1 CLI/core. Combined universe/storage/event-horizon admission
# belongs to native preparation, not a second Python market-data implementation.
MAX_MEMORY_MIB = (sys.maxsize * 2 + 1) // (1024 * 1024)
BOUNDS = {
    "symbols": (1, 0xFFFFFFFF),
    "depth": (1, (0xFFFFFFFF - 1) // 2),
    "seed": (0, 0xFFFFFFFFFFFFFFFF),
    "rate": (0, 0x7FFFFFFFFFFFFFFF),
    "batch": (1, 0xFFFFFFFF),
    "burst": (1, 0xFFFFFFFF),
    "seconds": (1, 86400),
    "max_messages": (0, 0x7FFFFFFFFFFFFFFF),
    "memory_mib": (1, MAX_MEMORY_MIB),
}
MODE = "synthetic_unpersisted_native_local"


@dataclass(frozen=True)
class HarnessLimits:
    max_memory_mib: int = 512
    max_seconds: int = 3600

    def __post_init__(self) -> None:
        for name, value, high in (
            ("max_memory_mib", self.max_memory_mib, MAX_MEMORY_MIB),
            ("max_seconds", self.max_seconds, 86400),
        ):
            if type(value) is not int or not 1 <= value <= high:
                raise ValueError(f"server {name} must be1..{high}")

    @classmethod
    def from_env(cls) -> HarnessLimits:
        values: dict[str, int] = {}
        for key in ("max_memory_mib", "max_seconds"):
            name = "DEVTOOLS_MARKET_FEED_" + key.upper()
            text = os.environ.get(name)
            if text is not None:
                if re.fullmatch(r"[0-9]{1,20}", text) is None:
                    raise ValueError(f"{name} must be a bounded decimal integer")
                values[key] = int(text)
        return cls(**values)


def configuration(values: dict[str, Any] | None, limits: HarnessLimits | None = None) -> dict[str, Any]:
    limits = limits or HarnessLimits()
    if values is not None and (
        not isinstance(values, dict) or len(values) > len(DEFAULTS) or any(key not in DEFAULTS for key in values)
    ):
        raise ValueError("unknown configuration field")
    result = DEFAULTS | (values or {})
    for key, (low, high) in BOUNDS.items():
        if key == "seconds":
            high = limits.max_seconds
        elif key == "memory_mib":
            high = limits.max_memory_mib
        value = result[key]
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f"{key} must be an integer in [{low},{high}]")
    if type(result["unlimited"]) is not bool:
        raise ValueError("unlimited must be boolean")
    return result


def arguments(config: dict[str, Any]) -> list[str]:
    result: list[str] = []
    for key in BOUNDS:
        result.extend(["--" + key.replace("_", "-"), str(config[key])])
    if config["unlimited"]:
        result.append("--unlimited")
    return result


def control(command: str, symbols: int, limits: HarnessLimits | None = None) -> str:
    limits = limits or HarnessLimits()
    if not isinstance(command, str) or len(command) > 128 or "\n" in command or "\r" in command:
        raise ValueError("one bounded command line required")
    if command in {"pause", "resume", "unlimited", "status", "stop"}:
        return command
    match = re.fullmatch(r"(rate|ramp|burst|book) ([0-9]{1,20})(?: ([0-9]{1,20}))?", command)
    if match is None:
        raise ValueError("unsupported command")
    kind, value_text, duration_text = match.groups()
    value = int(value_text)
    if kind == "ramp":
        if (
            duration_text is None
            or not 1 <= value <= BOUNDS["rate"][1]
            or not 1 <= int(duration_text) <= limits.max_seconds * 1000
        ):
            raise ValueError(
                f"ramp requires positive target<=INT64MAX and duration1..{limits.max_seconds * 1000}ms; "
                "use pause/rate0 for stop"
            )
    else:
        if duration_text is not None:
            raise ValueError("unexpected command argument")
        low, high = {"rate": BOUNDS["rate"], "burst": BOUNDS["burst"], "book": (1, symbols)}[kind]
        if not low <= value <= high:
            raise ValueError("command argument outside native limits")
    return command
