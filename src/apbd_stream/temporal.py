"""Render the source's temporal columns for humans.

Debezium 3.0 dropped `date.handling.mode=iso_string`, and schema-less JSON
carries DATE as epoch-days (``20741``) and TIMESTAMP as epoch-milliseconds
(``1793187511000``). That is correct machine data and unreadable evidence, so
the consumer — the processing layer, whose job is exactly this kind of
rendering — converts these columns to ISO strings before anything is written
down. Values that are already strings pass through untouched.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

# Which columns of the SIMDA-shaped source hold which temporal type.
DATE_COLUMNS = frozenset({"tanggal"})
TIMESTAMP_COLUMNS = frozenset({"created_at", "updated_at"})

_EPOCH = date(1970, 1, 1)


def render_temporal(row: dict) -> dict:
    """Return a copy with DATE/TIMESTAMP columns rendered as ISO strings."""
    rendered = dict(row)
    for column in DATE_COLUMNS:
        value = rendered.get(column)
        if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
            rendered[column] = (_EPOCH + timedelta(days=int(value))).isoformat()
    for column in TIMESTAMP_COLUMNS:
        value = rendered.get(column)
        if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
            moment = datetime.fromtimestamp(int(value) / 1000, tz=UTC)
            rendered[column] = moment.strftime("%Y-%m-%d %H:%M:%S")
    return rendered
