"""Unit tests: masking, CDC aggregation semantics, alert payload, latency math."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from apbd_stream import benchmark, consume
from apbd_stream.aggregate import (
    StreamState,
    apply_event,
    load_state,
    over_threshold,
    save_state,
    threshold_set,
)
from apbd_stream.alert import build_payload, emit
from apbd_stream.config import Settings
from apbd_stream.connector import connector_config
from apbd_stream.masking import PII_COLUMNS, mask_envelope, mask_event, sha256_hex

NIK = "3275001234567890"
REKENING = "0037123456789012"


def bku_envelope(op: str = "c", *, row_id: int = 1, kode: str = "1.02.02",
                 jumlah: str = "1000.00", jenis: str = "PENGELUARAN") -> dict:
    row = {
        "id_bku": row_id,
        "kode_skpd": kode,
        "jenis": jenis,
        "jumlah": jumlah,
        "nik_bendahara": NIK,
        "rekening_tujuan": REKENING,
    }
    return {
        "op": op,
        "ts_ms": 1_761_600_000_000,
        "source": {"ts_ms": 1_761_600_000_000},
        "after": None if op == "d" else row,
        "before": row if op == "d" else None,
    }


def pagu_envelope(kode: str, pagu: str, op: str = "c") -> dict:
    row = {"id": 1, "kode_skpd": kode, "tahun": 2026, "pagu": pagu}
    return {
        "op": op,
        "ts_ms": 1_761_600_000_000,
        "after": None if op == "d" else row,
        "before": row if op == "d" else None,
    }


class TestMasking:
    def test_sha256_is_stable_and_hex(self):
        assert sha256_hex(NIK) == sha256_hex(NIK)
        assert len(sha256_hex(NIK)) == 64
        assert sha256_hex(None) is None

    def test_mask_event_removes_the_raw_columns(self):
        masked = mask_event(bku_envelope()["after"])

        assert NIK not in masked.values()
        assert REKENING not in masked.values()
        for column in PII_COLUMNS:
            assert column not in masked
            assert masked[f"{column}_hash"] == sha256_hex(NIK if "nik" in column else REKENING)

    def test_display_mask_shows_only_ends(self):
        masked = mask_event(bku_envelope()["after"])

        assert masked["nik_bendahara_masked"] == "3275********7890"
        assert masked["rekening_tujuan_masked"].startswith("0037")
        assert masked["rekening_tujuan_masked"].endswith("9012")

    def test_mask_envelope_covers_before_and_after(self):
        masked = mask_envelope(bku_envelope(op="d"))
        dumped = str(masked)

        assert NIK not in dumped
        assert REKENING not in dumped

    def test_json_roundtrip_cannot_recover_the_raw_values(self):
        import json

        assert NIK not in json.dumps(mask_envelope(bku_envelope()))


class TestCdcSemantics:
    def test_insert_then_update_does_not_double_count(self):
        state = StreamState()
        apply_event(state, "t.bku", 0, 0, bku_envelope("c", jumlah="1000.00"))
        apply_event(state, "t.bku", 0, 1, bku_envelope("u", jumlah="1500.00"))

        assert state.realisasi["1.02.02"] == Decimal("1500.00")
        assert len(state.bku) == 1

    def test_delete_withdraws_the_amount(self):
        state = StreamState()
        apply_event(state, "t.bku", 0, 0, bku_envelope("c", jumlah="1000.00"))
        apply_event(state, "t.bku", 0, 1, bku_envelope("d", jumlah="1000.00"))

        assert state.realisasi["1.02.02"] == Decimal("0")
        assert state.bku == {}

    def test_receiving_income_is_not_realisasi(self):
        state = StreamState()
        apply_event(state, "t.bku", 0, 0, bku_envelope("c", jenis="PENERIMAAN"))

        assert state.realisasi == {}

    def test_replayed_offsets_are_ignored(self):
        state = StreamState()
        first = apply_event(state, "t.bku", 0, 7, bku_envelope("c", jumlah="1000.00"))
        replay = apply_event(state, "t.bku", 0, 7, bku_envelope("c", jumlah="1000.00"))

        assert first == "applied"
        assert replay == "duplicate"
        assert state.realisasi["1.02.02"] == Decimal("1000.00")

    def test_pagu_revision_mid_stream_changes_the_ratio(self):
        state = StreamState()
        apply_event(state, "t.pagu_anggaran", 0, 0, pagu_envelope("1.02.02", "1000.00"))
        apply_event(state, "t.bku", 0, 1, bku_envelope("c", jumlah="950.00"))

        assert threshold_set(over_threshold(state, 0.90)) == ["1.02.02"]

        apply_event(state, "t.pagu_anggaran", 0, 2, pagu_envelope("1.02.02", "2000.00", op="u"))

        assert threshold_set(over_threshold(state, 0.90)) == []

    def test_over_threshold_is_sorted_worst_first(self):
        state = StreamState()
        apply_event(state, "t.pagu_anggaran", 0, 0, pagu_envelope("1.02.02", "1000.00"))
        apply_event(state, "t.pagu_anggaran", 0, 1, pagu_envelope("1.05.05", "1000.00"))
        apply_event(state, "t.bku", 0, 2, bku_envelope("c", kode="1.02.02", jumlah="950.00"))
        apply_event(
            state, "t.bku", 0, 3, bku_envelope("c", row_id=2, kode="1.05.05", jumlah="990.00")
        )

        rows = over_threshold(state, 0.90)

        assert [row["kode_skpd"] for row in rows] == ["1.05.05", "1.02.02"]

    def test_state_survives_a_restart(self, tmp_path: Path):
        state = StreamState()
        apply_event(state, "t.bku", 0, 0, bku_envelope("c", jumlah="1000.00"))
        path = tmp_path / "state.json"
        save_state(state, path)

        restored = load_state(path)

        assert restored.realisasi == state.realisasi
        assert restored.bku.keys() == state.bku.keys()
        # The dedup ring survives too, or a restart would re-apply history.
        assert event_key_in(restored, state)


def event_key_in(restored: StreamState, original: StreamState) -> bool:
    return set(original.seen) <= set(restored.seen)


class TestLatencyMath:
    def test_nearest_rank_percentiles(self):
        stats = consume.percentiles([10.0, 20.0, 30.0, 40.0])

        assert stats["count"] == 4
        assert stats["p50_ms"] == 20.0
        assert stats["p95_ms"] == 40.0
        assert stats["max_ms"] == 40.0
        assert stats["mean_ms"] == 25.0

    def test_empty_input_is_honest(self):
        assert consume.percentiles([]) == {"count": 0}


class TestAlertPayload:
    ROW = {
        "kode_skpd": "1.05.05",
        "nama_skpd": "Dinas Sosial",
        "realisasi": Decimal("855599999999.98"),
        "pagu": Decimal("920000000000.00"),
        "sisa": Decimal("64400000000.02"),
        "persen": Decimal("93.00"),
    }

    def test_payload_lists_every_breaching_skpd(self):
        payload = build_payload([self.ROW])

        assert "90%" in payload["blocks"][0]["text"]["text"]
        assert "Dinas Sosial" in payload["blocks"][1]["text"]["text"]
        assert "93.00" in payload["blocks"][1]["text"]["text"]

    def test_amounts_are_readable_rupiah(self):
        text = build_payload([self.ROW])["blocks"][1]["text"]["text"]

        assert "Rp 920.00 M" in text
        assert "Rp 855.60 M" in text

    def test_no_breach_is_still_a_valid_message(self):
        assert "0 SKPD" in build_payload([])["blocks"][0]["text"]["text"]

    def test_emit_writes_the_payload_and_does_not_send(self, tmp_path: Path):
        outcome = emit(build_payload([self.ROW]), tmp_path, webhook_url=None)

        assert outcome["sent"] is False
        assert (tmp_path / "rt_alert_payload.json").exists()


class TestConnectorConfig:
    def test_streams_exactly_the_two_apbd_tables(self):
        config = connector_config(Settings())["config"]

        assert config["table.include.list"] == "siktm.bku,siktm.pagu_anggaran"
        assert config["snapshot.mode"] == "initial"
        assert config["value.converter.schemas.enable"] == "false"
        assert config["tombstones.on.delete"] == "false"

    def test_connector_reads_the_source_over_the_docker_network(self):
        config = connector_config(Settings())["config"]

        assert config["database.hostname"] == "apbd-mysql"
        assert config["topic.prefix"] == "apbd"

    def test_schema_history_lands_on_the_single_local_broker(self):
        config = connector_config(Settings())["config"]

        assert config["schema.history.internal.kafka.bootstrap.servers"] == "apbd-kafka:29092"
        assert config["schema.history.internal.kafka.replication.factor"] == "1"
        assert config["schema.history.internal.kafka.topic"] == "apbd-simda-schema-history"


class TestBenchmarkPlanning:
    def test_crossing_row_lands_over_90_and_under_100(self):
        pagu = {"1.01.01": Decimal("1000"), "1.02.02": Decimal("1000"),
                "1.03.03": Decimal("1000"), "1.04.04": Decimal("1000"),
                "1.05.05": Decimal("1000")}
        realisasi = {kode: Decimal("500") for kode in pagu}
        signatories = {kode: (NIK, REKENING) for kode in pagu}

        rows = benchmark.plan_inserts(pagu, realisasi, signatories, events=11)

        total_by_skpd: dict[str, Decimal] = {kode: Decimal("0") for kode in pagu}
        for kode, _jenis, _uraian, jumlah, *_ in rows:
            total_by_skpd[kode] += Decimal(jumlah)
        for kode in pagu:
            final = realisasi[kode] + total_by_skpd[kode]
            assert final <= pagu[kode] * Decimal("0.95")  # never over the cap
        assert any(
            (realisasi[kode] + total_by_skpd[kode]) > pagu[kode] * Decimal("0.90")
            for kode in pagu
        ), "the plan must cross the threshold somewhere"

    def test_plan_respects_an_already_hot_source(self):
        pagu = {
            kode: Decimal("1000")
            for kode in ("1.01.01", "1.02.02", "1.03.03", "1.04.04", "1.05.05")
        }
        realisasi = {kode: Decimal("945") for kode in pagu}  # at 94.5%, below the 95% cap
        signatories = {kode: (NIK, REKENING) for kode in pagu}

        rows = benchmark.plan_inserts(pagu, realisasi, signatories, events=11)

        total = sum((Decimal(row[3]) for row in rows), Decimal("0"))
        assert realisasi["1.01.01"] + total / 5 <= pagu["1.01.01"] * Decimal("0.95") + Decimal("5")
        for row in rows:
            assert Decimal(row[3]) > 0


class TestTemporalRendering:
    def test_epoch_days_become_iso_dates(self):
        from apbd_stream.temporal import render_temporal

        out = render_temporal({"tanggal": 20741, "id_bku": 7})

        assert out["tanggal"] == "2026-10-15"
        assert out["id_bku"] == 7  # untouched: ids are not dates

    def test_epoch_millis_become_readable_timestamps(self):
        from apbd_stream.temporal import render_temporal

        out = render_temporal({"created_at": 1793187511000})

        assert out["created_at"].startswith("2026-10-28")

    def test_strings_pass_through(self):
        from apbd_stream.temporal import render_temporal

        out = render_temporal({"tanggal": "2026-10-28"})

        assert out["tanggal"] == "2026-10-28"
