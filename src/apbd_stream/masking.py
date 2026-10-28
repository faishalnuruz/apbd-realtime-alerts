"""PII masking in the stream: NIK and account numbers never leave the consumer raw.

The PRD asks for masking in the streaming path. This is that step: the consumer
runs every event through `mask_event` before anything can be aggregated,
printed, or alerted on, so the raw values have no downstream to leak into.
A unit test asserts the raw strings are absent, and an integration test
asserts it against the real topic.
"""

from __future__ import annotations

import hashlib

PII_COLUMNS = ("nik_bendahara", "rekening_tujuan")


def sha256_hex(value: str | None) -> str | None:
    """Fingerprint for joins and lookups that must not carry the value itself."""
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _mask(value: str | None) -> str | None:
    """Show enough to recognise (first 4 + last 4), never enough to use."""
    if value is None:
        return None
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}{'*' * (len(value) - 8)}{value[-4:]}"


def mask_event(row: dict) -> dict:
    """Replace every PII column with a fingerprint and a display mask.

    The raw column is removed from the returned dict, not merely hidden —
    downstream code cannot read what is not there.
    """
    masked = dict(row)
    for column in PII_COLUMNS:
        if column not in masked:
            continue
        raw = masked.pop(column)
        masked[f"{column}_hash"] = sha256_hex(raw)
        masked[f"{column}_masked"] = _mask(raw)
    return masked


def mask_envelope(envelope: dict) -> dict:
    """Mask a Debezium envelope's row images in place of the raw ones.

    Applied by the consumer before aggregation, so no raw PII ever reaches
    the state file, the alert, or the evidence files — while the CDC record
    itself (Debezium streams the source truthfully) stays untouched upstream.
    """
    masked = dict(envelope)
    for key in ("after", "before"):
        row = masked.get(key)
        if isinstance(row, dict):
            masked[key] = mask_event(row)
    return masked
