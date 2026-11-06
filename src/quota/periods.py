"""Billing periods anchored on each org's signup day (UTC).

A period starts at 00:00 UTC on the org's anchor day and ends at 00:00 UTC on the next anchor
day. Months shorter than the anchor day clamp to their last day, always computed from the
original anchor (anchor 31 -> Feb 28 -> Mar 31, never drifting to the 28th).
"""
import calendar
from dataclasses import dataclass
from datetime import date, datetime, time, timezone


@dataclass(frozen=True)
class Period:
    start: date
    end: date

    @property
    def id(self) -> str:
        """Key suffix for this period, e.g. '2026-11-15'."""
        return self.start.isoformat()

    @property
    def resets_at(self) -> datetime:
        return datetime.combine(self.end, time(), tzinfo=timezone.utc)


def _anchor_in(year: int, month: int, anchor_day: int) -> date:
    return date(year, month, min(anchor_day, calendar.monthrange(year, month)[1]))


def _add_months(year: int, month: int, k: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + k
    return idx // 12, idx % 12 + 1


def period_for(anchor_day: int, now: datetime) -> Period:
    """The period containing `now` (timezone-aware) for an org with this anchor day."""
    if not 1 <= anchor_day <= 31:
        raise ValueError(f"anchor_day must be 1..31, got {anchor_day}")
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    today = now.astimezone(timezone.utc).date()
    this_month = _anchor_in(today.year, today.month, anchor_day)
    if today >= this_month:
        start = this_month
    else:
        start = _anchor_in(*_add_months(today.year, today.month, -1), anchor_day)
    end = _anchor_in(*_add_months(start.year, start.month, 1), anchor_day)
    return Period(start, end)
