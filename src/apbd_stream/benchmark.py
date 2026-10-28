"""Measure the path the PRD claims: BKU entry committed in MySQL -> alert decision.

    python -m apbd_stream.benchmark --events 200 --pace-ms 50

The method:
  1. warm-up drain with a dedicated consumer group and state file, so hours of
     history are loaded once and its (ancient) timestamps never enter the stats;
  2. insert N BKU entries paced like real work, draining after every batch so
     each event is measured live, not replayed from a backlog;
  3. one of the inserts is sized to push a SKPD across the 90% line — its
     decision latency is the alert number;
  4. write docs/generated/latency-report.{md,json}; exit non-zero if the
     p95 exceeds the PRD's 30-second SLA.

Latency = (wall clock when the consumer applied the event) - (source.ts_ms,
the MySQL commit timestamp carried by Debezium). Both are this machine's
clock, so no clock skew is involved.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

from apbd_stream import consume
from apbd_stream.config import SETTINGS, Settings
from apbd_stream.seed import SKPDS, URAIAN, connect

SLA_MS = 30_000  # the PRD's "< 30 detik"
CROSSING_TARGET = Decimal("0.92")  # land clearly over 90%, safely under 100%
BREACH_THRESHOLD = Decimal("0.90")  # the alert rule itself
SAFETY_CAP = Decimal("0.95")  # never let the source exceed 95% of pagu


def source_totals(
    settings: Settings,
) -> tuple[dict[str, Decimal], dict[str, Decimal], dict[str, tuple[str, str]]]:
    """pagu and current realisasi per SKPD, straight from the source system."""
    connection = connect(settings)
    with connection.cursor() as cursor:
        cursor.execute("SELECT kode_skpd, pagu FROM pagu_anggaran")
        pagu = {kode: Decimal(str(value)) for kode, value in cursor.fetchall()}
        cursor.execute(
            """
            SELECT kode_skpd, COALESCE(SUM(jumlah), 0)
            FROM bku WHERE jenis = 'PENGELUARAN' GROUP BY 1
            """
        )
        realisasi = {kode: Decimal(str(value)) for kode, value in cursor.fetchall()}
        cursor.execute("SELECT kode_skpd, nik, rekening FROM bendahara")
        signatories = {kode: (nik, rekening) for kode, nik, rekening in cursor.fetchall()}
    connection.close()
    return pagu, realisasi, signatories


def plan_inserts(
    pagu: dict[str, Decimal],
    realisasi: dict[str, Decimal],
    signatories: dict[str, tuple[str, str]],
    events: int,
    rng: random.Random | None = None,
) -> list[tuple]:
    """N+1 inserts: small ones spread across SKPDs, one final crossing row.

    Every planned amount is capped by the remaining room to SAFETY_CAP, so a
    re-run on an already-hot source cannot push realisasi over its pagu —
    the invariant the sikt project tests is respected here by construction.
    """
    rng = rng or random.Random(2026)
    today = date.today()
    order = [kode for kode, *_ in SKPDS]

    def room(kode: str) -> Decimal:
        return max(Decimal("0"), pagu[kode] * SAFETY_CAP - realisasi.get(kode, Decimal("0")))

    per_skpd = max(1, (events - 1) // len(order))
    rows: list[tuple] = []
    added: dict[str, Decimal] = {kode: Decimal("0") for kode in order}

    for index in range(events - 1):
        kode = order[index % len(order)]
        share = min(pagu[kode] * Decimal("0.0005"), room(kode) // (per_skpd + 1))
        if share <= 0:
            continue
        nik, rekening = signatories[kode]
        rows.append(
            (
                kode,
                "PENGELUARAN",
                URAIAN[rng.randrange(len(URAIAN))],
                str(share),
                nik,
                rekening,
                today,
            )
        )
        added[kode] += share

    # The crossing row: the most-loaded SKPD *still under the alert line* —
    # one that is already over 90% would make the "crossing" a no-op for the
    # alert's edge-triggered set.
    candidates = [
        kode
        for kode in order
        if (realisasi.get(kode, Decimal("0")) + added[kode]) / pagu[kode]
        < BREACH_THRESHOLD
    ]
    candidates.sort(
        key=lambda kode: (realisasi.get(kode, Decimal("0")) + added[kode]) / pagu[kode],
        reverse=True,
    )
    if candidates:
        kode = candidates[0]
        current = realisasi.get(kode, Decimal("0")) + added[kode]
        amount = min(pagu[kode] * CROSSING_TARGET - current, room(kode))
        if amount > 0:
            nik, rekening = signatories[kode]
            rows.append(
                (
                    kode,
                    "PENGELUARAN",
                    "Belanja refocusing mendesak",
                    str(amount),
                    nik,
                    rekening,
                    today,
                )
            )
    return rows


def insert_rows(settings: Settings, rows: list[tuple], pace_ms: int) -> None:
    connection = connect(settings)
    statement = """
        INSERT INTO bku (kode_skpd, jenis, uraian, jumlah, nik_bendahara,
                         rekening_tujuan, tanggal)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for row in rows:
        with connection.cursor() as cursor:
            cursor.execute(statement, row)
        if pace_ms:
            time.sleep(pace_ms / 1000)
    connection.close()


def run_benchmark(events: int = 200, pace_ms: int = 50, batch: int = 10) -> dict:
    bench_settings = replace(
        SETTINGS,
        consumer_group="apbd-bench",
        state_file=Path("data/bench-state.json"),
    )

    pagu, realisasi, signatories = source_totals(bench_settings)
    rows = plan_inserts(pagu, realisasi, signatories, events)

    # 1. warm-up: load history with a fresh group so its old timestamps stay
    # out of the measurement, and without emitting an alert on ancient data.
    consume.run_once(bench_settings, send_alert=False)

    # 2. paced inserts, drained live after every batch.
    all_latencies: list[float] = []
    alert_record: dict | None = None
    for start in range(0, len(rows), batch):
        insert_rows(bench_settings, rows[start : start + batch], pace_ms)
        summary = consume.run_once(
            bench_settings, send_alert=True, include_raw=True
        )
        all_latencies.extend(summary["latencies"])
        if summary["alert"]:
            alert_record = summary["alert"]

    latency = consume.percentiles(all_latencies)
    result = {
        "events_inserted": len(rows),
        "events_measured": latency.get("count", 0),
        "pace_ms": pace_ms,
        "batch": batch,
        "latency": latency,
        "alert": alert_record,
        "sla_ms": SLA_MS,
        "passed": bool(latency.get("count")) and latency.get("p95_ms", SLA_MS + 1) < SLA_MS,
    }
    return result


def write_report(result: dict, output_dir: Path = SETTINGS.output_dir) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latency-report.json").write_text(json.dumps(result, indent=2, default=str))

    latency = result["latency"]
    alert = result.get("alert") or {}
    decision = alert.get("decision_latency_ms")
    verdict = "PASS" if result["passed"] else "FAIL"
    lines = [
        "# End-to-end latency: MySQL commit -> alert decision",
        "",
        f"Measured {result['events_inserted']} inserts paced {result['pace_ms']} ms apart; "
        f"{result['events_measured']} events captured.",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| p50 | {latency.get('p50_ms', '—')} ms |",
        f"| p90 | {latency.get('p90_ms', '—')} ms |",
        f"| p95 | **{latency.get('p95_ms', '—')} ms** |",
        f"| p99 | {latency.get('p99_ms', '—')} ms |",
        f"| max | {latency.get('max_ms', '—')} ms |",
        f"| mean | {latency.get('mean_ms', '—')} ms |",
        f"| alert decision (threshold crossing event) | "
        f"{f'{decision} ms' if decision is not None else 'no crossing in this run'} |",
        f"| SLA (PRD: < 30 detik) | {SLA_MS} ms → **{verdict}** |",
        "",
        "**Definition.** The start timestamp is `source.ts_ms` — the commit time MySQL "
        "itself wrote for the transaction, carried through Debezium. The end timestamp is "
        "the consumer's wall clock after it has masked the event, updated the aggregate, "
        "and evaluated the threshold. Same machine, same clock: no skew correction.",
        "",
        "**What it includes.** MySQL binlog read by Debezium, Kafka Connect's polling of "
        "the stream, the broker's fetch to this consumer, masking, aggregation, and the "
        "threshold evaluation — everything between \"bendahara presses enter\" and "
        "\"the decision exists\".",
        "",
        "**What it does not include.** Slack's delivery (a webhook POST that this repo "
        "performs only when SLACK_WEBHOOK_URL is set) and the production transport hops "
        "(Pub/Sub/Dataflow in the PRD's target architecture) — those are documented as "
        "designed, not measured.",
        "",
    ]
    (output_dir / "latency-report.md").write_text("\n".join(lines))
    return output_dir / "latency-report.md"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=200)
    parser.add_argument("--pace-ms", type=int, default=50)
    parser.add_argument("--batch", type=int, default=10, help="drain after every N inserts")
    args = parser.parse_args()

    result = run_benchmark(events=args.events, pace_ms=args.pace_ms, batch=args.batch)
    report = write_report(result)
    print(json.dumps(result, indent=2, default=str))
    print(f"report: {report}")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
