"""The consumer: binlog commit timestamp -> masked state -> alert decision.

The latency this measures is the one the PRD's "<30 detik" claim is about:
the moment MySQL committed the entry (`source.ts_ms`, written by the source
itself) to the moment this process has applied it and evaluated the threshold.
No clock-skew games: both timestamps are taken on the same machine.

    python -m apbd_stream.consume --once    # drain what is pending, print a summary
    python -m apbd_stream.consume --watch   # follow forever
"""

from __future__ import annotations

import argparse
import json
import math
import time

from kafka import KafkaConsumer

from apbd_stream import alert as alert_mod
from apbd_stream.aggregate import (
    apply_event,
    load_state,
    over_threshold,
    save_state,
    threshold_set,
)
from apbd_stream.config import SETTINGS, Settings
from apbd_stream.masking import mask_envelope
from apbd_stream.seed import SKPDS
from apbd_stream.temporal import render_temporal

NAMES = {kode: nama for kode, nama, _pagu, _share in SKPDS}


def now_ms() -> int:
    return int(time.time() * 1000)


def percentiles(values: list[float]) -> dict:
    """p50/p90/p95/p99 by nearest rank over the observed latencies."""
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    count = len(ordered)

    def nearest_rank(fraction: float) -> float:
        index = max(0, math.ceil(fraction * count) - 1)
        return round(ordered[index], 1)

    return {
        "count": count,
        "p50_ms": nearest_rank(0.50),
        "p90_ms": nearest_rank(0.90),
        "p95_ms": nearest_rank(0.95),
        "p99_ms": nearest_rank(0.99),
        "max_ms": round(ordered[-1], 1),
        "mean_ms": round(sum(ordered) / count, 1),
    }


def build_consumer(settings: Settings = SETTINGS) -> KafkaConsumer:
    return KafkaConsumer(
        settings.bku_topic,
        settings.pagu_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=100,
        # The broker's 500 ms default would sit inside every measurement.
        fetch_max_wait_ms=100,
        consumer_timeout_ms=4000,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


def _enrich(rows: list[dict]) -> list[dict]:
    for row in rows:
        row.setdefault("nama_skpd", NAMES.get(row["kode_skpd"], ""))
    return rows


def _process(envelope: dict) -> dict:
    """The pipeline's view of an event: PII masked, temporal columns rendered.

    Everything downstream — aggregate, alert, evidence files — sees only this.
    """
    processed = mask_envelope(envelope)
    for key in ("after", "before"):
        if isinstance(processed.get(key), dict):
            processed[key] = render_temporal(processed[key])
    return processed


def run_once(
    settings: Settings = SETTINGS, send_alert: bool = True, include_raw: bool = False
) -> dict:
    """Drain the topics, applying and evaluating after every event.

    The threshold is evaluated per applied record rather than once at the end,
    so when the set of over-threshold SKPDs changes we know exactly which
    record did it — and that record's source timestamp becomes the alert's
    decision latency.
    """
    state = load_state(settings.state_file)
    consumer = build_consumer(settings)
    latencies: list[float] = []
    masked_events: list[dict] = []
    alert_record: dict | None = None
    applied = duplicates = 0

    try:
        for record in consumer:
            envelope = _process(record.value or {})
            source_ts = (envelope.get("source") or {}).get("ts_ms")
            if isinstance(envelope.get("after"), dict):
                masked_events.append(envelope["after"])
            result = apply_event(state, record.topic, record.partition, record.offset, envelope)
            if result != "applied":
                duplicates += 1
                continue
            applied += 1
            latency = (now_ms() - source_ts) if source_ts else None
            if latency is not None:
                latencies.append(latency)

            if send_alert:
                rows_now = _enrich(over_threshold(state, settings.threshold))
                current = threshold_set(rows_now)
                # Any change of the set is news: a new breach, but also every
                # SKPD recovering (the empty set fires the "all clear" payload).
                if current != state.alerted:
                    payload = alert_mod.build_payload(rows_now, settings.threshold)
                    outcome = alert_mod.emit(
                        payload, settings.output_dir, settings.slack_webhook_url
                    )
                    alert_record = {
                        "skpd": current,
                        "payload": outcome["payload"],
                        "sent": outcome["sent"],
                        "decision_latency_ms": latency,
                    }
                    state.alerted = current
        consumer.commit()
    finally:
        consumer.close()

    rows = _enrich(over_threshold(state, settings.threshold))
    current = threshold_set(rows)
    if send_alert and alert_record is None and current != state.alerted:
        # No crossing event this run, but the state says something is over the
        # line (e.g. a fresh state file against an already-hot stream): the
        # breach still deserves the message, without a fabricated latency.
        payload = alert_mod.build_payload(rows, settings.threshold)
        outcome = alert_mod.emit(payload, settings.output_dir, settings.slack_webhook_url)
        alert_record = {
            "skpd": current,
            "payload": outcome["payload"],
            "sent": outcome["sent"],
            "decision_latency_ms": latencies[-1] if latencies else None,
        }
        state.alerted = current

    # Evidence: this run's events as the pipeline sees them — PII columns
    # replaced by fingerprints and masks, readable in the repo.
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    masked_path = settings.output_dir / "rt_masked_events.jsonl"
    masked_path.write_text("".join(json.dumps(row) + "\n" for row in masked_events))
    pii_free = all(
        column not in row
        for row in masked_events
        for column in ("nik_bendahara", "rekening_tujuan")
    )
    summary = {
        "applied": applied,
        "duplicates": duplicates,
        "latency": percentiles(latencies),
        "over_threshold": [
            {
                "kode_skpd": row["kode_skpd"],
                "nama_skpd": row["nama_skpd"],
                "persen": str(row["persen"]),
            }
            for row in rows
        ],
        "alert": alert_record,
        "state": {
            "bku_rows": len(state.bku),
            "skpd_seen": sorted(state.realisasi),
            "masked_events_file": str(masked_path),
            "pii_columns_preserved": pii_free or not masked_events,
        },
    }
    if include_raw:
        summary["latencies"] = latencies
    save_state(state, settings.state_file)
    return summary


def watch(settings: Settings = SETTINGS) -> None:
    print(f"following {settings.bku_topic} and {settings.pagu_topic} (Ctrl-C to stop)")
    while True:
        summary = run_once(settings)
        if summary["applied"]:
            print(json.dumps(summary, default=str))
        time.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="drain pending events and exit (default)")
    mode.add_argument("--watch", action="store_true", help="follow the stream forever")
    parser.add_argument("--no-alert", action="store_true", help="evaluate but do not emit")
    args = parser.parse_args()

    if args.watch:
        watch()
        return
    summary = run_once(send_alert=not args.no_alert)
    print(json.dumps(summary, default=str, indent=2))


if __name__ == "__main__":
    main()
