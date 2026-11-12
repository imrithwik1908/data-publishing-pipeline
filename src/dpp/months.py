"""Tiny helpers for 'YYYY-MM' month strings."""

from __future__ import annotations

from datetime import date


def parse(month: str) -> date:
    y, m = month.split("-")
    return date(int(y), int(m), 1)


def fmt(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def add(month: str, n: int) -> str:
    d = parse(month)
    idx = d.year * 12 + (d.month - 1) + n
    return fmt(date(idx // 12, idx % 12 + 1, 1))


def month_range(start: str, end: str) -> list[str]:
    out, cur = [], start
    while parse(cur) <= parse(end):
        out.append(cur)
        cur = add(cur, 1)
    return out
