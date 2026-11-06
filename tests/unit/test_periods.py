from datetime import date, datetime, timedelta, timezone

import pytest

from quota.periods import period_for

UTC = timezone.utc


def at(y, m, d, hh=12, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=UTC)


@pytest.mark.parametrize("anchor, now, start, end", [
    # mid-period, before and after this month's anchor
    (15, at(2026, 11, 20), date(2026, 11, 15), date(2026, 12, 15)),
    (15, at(2026, 11, 3), date(2026, 10, 15), date(2026, 11, 15)),
    # year rollover
    (15, at(2026, 12, 31), date(2026, 12, 15), date(2027, 1, 15)),
    (15, at(2027, 1, 2), date(2026, 12, 15), date(2027, 1, 15)),
    # anchor 1 behaves like calendar months
    (1, at(2026, 11, 1), date(2026, 11, 1), date(2026, 12, 1)),
    # anchor 31 clamps to short months, then returns to the 31st (no drift)
    (31, at(2027, 2, 10), date(2027, 1, 31), date(2027, 2, 28)),
    (31, at(2027, 3, 5), date(2027, 2, 28), date(2027, 3, 31)),
    (31, at(2027, 4, 29), date(2027, 3, 31), date(2027, 4, 30)),
    (31, at(2027, 4, 30), date(2027, 4, 30), date(2027, 5, 31)),
    (30, at(2027, 2, 28), date(2027, 2, 28), date(2027, 3, 30)),
    # leap year
    (31, at(2028, 2, 15), date(2028, 1, 31), date(2028, 2, 29)),
    (29, at(2028, 2, 29), date(2028, 2, 29), date(2028, 3, 29)),
    (29, at(2027, 2, 28), date(2027, 2, 28), date(2027, 3, 29)),
])
def test_period_bounds(anchor, now, start, end):
    p = period_for(anchor, now)
    assert (p.start, p.end) == (start, end)
    assert p.id == start.isoformat()


def test_boundary_is_midnight_utc():
    just_before = period_for(15, at(2026, 11, 14, 23, 59, 59))
    on_the_dot = period_for(15, at(2026, 11, 15, 0, 0, 0))
    assert just_before.start == date(2026, 10, 15)
    assert on_the_dot.start == date(2026, 11, 15)
    assert just_before.resets_at == datetime(2026, 11, 15, tzinfo=UTC)


def test_other_timezones_are_converted_to_utc():
    # 2026-11-15 03:00 in UTC+05:30 is still 2026-11-14 in UTC: the old period.
    ist = timezone(timedelta(hours=5, minutes=30))
    assert period_for(15, datetime(2026, 11, 15, 3, 0, tzinfo=ist)).start == date(2026, 10, 15)


def test_periods_tile_without_gaps():
    # Walk a few years day by day: each period ends exactly where the next one starts.
    for anchor in (1, 15, 28, 29, 30, 31):
        day = at(2026, 1, 1)
        prev = period_for(anchor, day)
        while day.year < 2029:
            day += timedelta(days=1)
            p = period_for(anchor, day)
            if p != prev:
                assert p.start == prev.end
                prev = p
            assert p.start <= day.date() < p.end


def test_rejects_bad_input():
    with pytest.raises(ValueError):
        period_for(0, at(2026, 11, 1))
    with pytest.raises(ValueError):
        period_for(15, datetime(2026, 11, 1))
