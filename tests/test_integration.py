"""Integration tests: the real stack, the real binlog, the real alert path.

    docker compose up -d
    python -m apbd_stream.seed
    .venv/bin/python -m pytest -m integration

Each pytest run gets a fresh consumer group and state file, so the tests
re-read the full topic history — they verify the same thing a reviewer's
first run would, not a state the previous run happened to leave behind.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
from kafka import KafkaConsumer

from apbd_stream import connector, consume
from apbd_stream.config import SETTINGS, Settings
from apbd_stream.seed import connect

pytestmark = pytest.mark.integration


def pin_share(settings: Settings, kode_skpd: str, share: str) -> Decimal:
    """Force realisasi/pagu for one SKPD to `share`.

    A nonce keeps the value different from whatever a previous run left behind:
    an UPDATE that changes nothing produces no binlog event, and these tests
    must not depend on run history — each run establishes its own preconditions.
    """
    connection = connect(settings)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COALESCE(SUM(jumlah), 0) FROM bku "
            "WHERE jenis = 'PENGELUARAN' AND kode_skpd = %s",
            (kode_skpd,),
        )
        realisasi = Decimal(str(cursor.fetchone()[0]))
        pagu = (realisasi / Decimal(share)).quantize(Decimal("0.01"))
        pagu += Decimal(int(time.time()) % 9973)
        cursor.execute(
            "UPDATE pagu_anggaran SET pagu = %s WHERE kode_skpd = %s",
            (str(pagu), kode_skpd),
        )
    connection.close()
    return pagu


def insert_bku(settings: Settings, uraian: str, jumlah: Decimal, kode: str = "1.01.01") -> None:
    connection = connect(settings)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO bku (kode_skpd, jenis, uraian, jumlah, nik_bendahara,
                             rekening_tujuan, tanggal)
            SELECT %s, 'PENGELUARAN', %s, %s, nik, rekening, CURDATE()
            FROM bendahara WHERE kode_skpd = %s
            """,
            (kode, uraian, str(jumlah), kode),
        )
    connection.close()


def source_realisasi(settings: Settings) -> dict[str, Decimal]:
    connection = connect(settings)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT kode_skpd, COALESCE(SUM(jumlah), 0)
            FROM bku WHERE jenis = 'PENGELUARAN' GROUP BY 1
            """
        )
        rows = {kode: Decimal(str(value)) for kode, value in cursor.fetchall()}
    connection.close()
    return rows


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    settings = replace(
        SETTINGS,
        consumer_group=f"apbd-tests-{uuid.uuid4().hex[:8]}",
        state_file=tmp_path_factory.mktemp("state") / "state.json",
    )
    connector.ensure(settings)
    warm = consume.run_once(settings, send_alert=False)  # history load, old timestamps discarded
    assert warm["applied"] > 0, "the snapshot never reached Kafka"
    # Pin the known breach (the seed puts Dinas Sosial over the line already)
    # to exactly 95%, so every assertion below depends on this run, not on
    # what the benchmark or a previous test run did to the source.
    pin_share(settings, "1.05.05", "0.95")
    pinned = consume.run_once(settings, send_alert=False)
    assert pinned["applied"] >= 1, "the pin must produce a real CDC event"
    return {"settings": settings, "warm": warm}


def test_connector_reports_running(stack):
    status = connector.status(stack["settings"])

    assert status["connector"]["state"] == "RUNNING"
    assert all(task["state"] == "RUNNING" for task in status["tasks"])


def test_snapshot_equals_the_source_ledger(stack):
    settings = stack["settings"]
    state = consume.load_state(settings.state_file)
    ledger = source_realisasi(settings)

    assert set(state.realisasi) == set(ledger)
    for kode, total in ledger.items():
        assert abs(state.realisasi[kode] - total) < Decimal("0.01")
    # The seeded breach exists before any live entry: Dinas Sosial is over 90%.
    rows = consume.over_threshold(state, settings.threshold)
    assert "1.05.05" in [row["kode_skpd"] for row in rows]


def test_a_live_entry_is_measured_under_the_sla(stack):
    settings = stack["settings"]
    insert_bku(settings, f"live entry {uuid.uuid4().hex[:8]}", Decimal("123450000.00"))

    summary = consume.run_once(settings, send_alert=False, include_raw=True)

    assert summary["applied"] >= 1
    assert summary["latency"]["count"] >= 1
    assert all(latency < 30_000 for latency in summary["latencies"]), (
        f"a live binlog event took longer than the PRD's 30s SLA: {summary['latencies']}"
    )


def test_the_breach_produces_the_alert_payload(stack):
    settings = stack["settings"]
    summary = consume.run_once(settings, send_alert=True)

    assert summary["alert"] is not None
    assert "1.05.05" in summary["alert"]["skpd"]
    payload = json.loads(Path(summary["alert"]["payload"]).read_text())
    dumped = json.dumps(payload)
    assert "1.05.05" in dumped
    assert "90%" in dumped


def test_no_raw_pii_reaches_any_downstream(stack):
    settings = stack["settings"]
    connection = connect(settings)
    with connection.cursor() as cursor:
        cursor.execute("SELECT nik FROM bendahara LIMIT 1")
        raw_nik = cursor.fetchone()[0]
    connection.close()

    # 1. the topic itself carries the source truth (so the check below means
    #    something): Debezium streams the raw row.
    raw_consumer = KafkaConsumer(
        settings.bku_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=None,
        auto_offset_reset="earliest",
        consumer_timeout_ms=8_000,
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    seen_in_topic = False
    for record in raw_consumer:
        after = (record.value or {}).get("after") or {}
        if after.get("nik_bendahara") == raw_nik:
            seen_in_topic = True
            break
    raw_consumer.close()
    assert seen_in_topic, "the CDC topic should stream the raw source row"

    # 2. it never survives the consumer: insert one more entry, drain, and
    #    check every artifact the pipeline writes.
    insert_bku(settings, f"pii probe {uuid.uuid4().hex[:8]}", Decimal("98765432.00"))
    consume.run_once(settings, send_alert=True)

    artifacts = [
        settings.output_dir / "rt_masked_events.jsonl",
        settings.output_dir / "rt_alert_payload.json",
        settings.state_file,
    ]
    for path in artifacts:
        content = path.read_text()
        assert raw_nik not in content, f"raw NIK leaked into {path}"

    masked_lines = (settings.output_dir / "rt_masked_events.jsonl").read_text().splitlines()
    assert masked_lines, "the probe should have produced at least one masked event"
    probe = next(
        (json.loads(line) for line in masked_lines if "pii probe" in line),
        None,
    )
    assert probe is not None
    assert "nik_bendahara" not in probe
    assert len(probe["nik_bendahara_hash"]) == 64


def test_a_mid_stream_pagu_revision_changes_the_alert(stack):
    """A revision that *recovers* a breaching SKPD must change the alert set.

    The recovery direction is the one that used to be silent: a set shrinking
    to empty is still news, and the consumer now fires the all-clear payload.
    """
    settings = stack["settings"]
    pin_share(settings, "1.05.05", "0.85")

    summary = consume.run_once(settings, send_alert=True)

    assert summary["applied"] >= 1, "the pagu UPDATE should have arrived through Debezium"
    breached = [
        row["kode_skpd"]
        for row in consume.over_threshold(
            consume.load_state(settings.state_file), settings.threshold
        )
    ]
    assert "1.05.05" not in breached, "the revision should drop Dinas Sosial under the line"
    assert summary["alert"] is not None, "the recovery must fire the all-clear"
    assert summary["alert"]["decision_latency_ms"] is not None
    assert "1.05.05" not in summary["alert"]["skpd"]

    # Leave the source the way the seed intends — breaching — so the next run's
    # fixture finds its own precondition instead of inheriting this recovery.
    pin_share(settings, "1.05.05", "0.95")
    restored = consume.run_once(settings, send_alert=True)
    assert restored["applied"] >= 1
    assert restored["alert"] is not None
    assert "1.05.05" in restored["alert"]["skpd"]


def test_re_draining_applies_nothing(stack):
    settings = stack["settings"]
    first = consume.run_once(settings, send_alert=False)

    assert first["applied"] == 0
