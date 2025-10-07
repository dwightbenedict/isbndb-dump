from typing import Any


def sanitize_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = ", ".join(map(str, value))
    return str(value).replace("\x00", "").strip()


def sanitize_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def sanitize_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0