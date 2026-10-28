"""Streaming aggregation with CDC semantics: upserts, deletes, replay safety.

The state is deliberately small — pagu per SKPD, BKU rows keyed by id, the
realisasi derived from them, and a dedup ring — because everything an alert
needs must be recomputable from the Kafka topics themselves. If the state file
is deleted, the consumer re-reads the topics and lands in the same place; that
is what makes the alert trustworthy when a budget revision arrives mid-stream.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

DEDUP_RING = 20_000


def event_key(topic: str, partition: int, offset: int) -> str:
    """Globally unique per record: offsets never repeat within a partition."""
    return f"{topic}:{partition}:{offset}"


def _dec(value) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def quantize(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass
class StreamState:
    pagu: dict[str, Decimal] = field(default_factory=dict)
    bku: dict[int, dict] = field(default_factory=dict)
    realisasi: dict[str, Decimal] = field(default_factory=dict)
    seen: dict[str, float] = field(default_factory=dict)
    alerted: list[str] = field(default_factory=list)

    def touch(self, key: str, when: float) -> bool:
        """True if this is the first time we see the event (idempotent replay)."""
        if key in self.seen:
            return False
        self.seen[key] = when
        if len(self.seen) > DEDUP_RING:
            for stale in list(self.seen)[: len(self.seen) - DEDUP_RING]:
                del self.seen[stale]
        return True

    def add_realisasi(self, kode_skpd: str, delta: Decimal) -> None:
        current = self.realisasi.get(kode_skpd, Decimal("0"))
        self.realisasi[kode_skpd] = current + delta


def apply_event(state: StreamState, topic: str, partition: int, offset: int, envelope: dict) -> str:
    """Apply one Debezium envelope. Returns applied | duplicate | ignored.

    Insert and update share one code path: an update replaces the row we
    already counted instead of adding to it, which is the difference between
    CDC and a changelog sum.
    """
    key = event_key(topic, partition, offset)
    if not state.touch(key, envelope.get("ts_ms", 0) / 1000):
        return "duplicate"

    op = envelope.get("op")
    after, before = envelope.get("after"), envelope.get("before")
    if op == "r":  # snapshot read: behaves exactly like an insert
        op = "c"

    if topic.endswith(".pagu_anggaran"):
        if op in ("c", "u") and after:
            state.pagu[after["kode_skpd"]] = _dec(after["pagu"])
        elif op == "d" and before:
            state.pagu.pop(before["kode_skpd"], None)
        return "applied"

    if topic.endswith(".bku"):
        if op in ("c", "u") and after:
            row_id = int(after["id_bku"])
            previous = state.bku.get(row_id)
            if previous:  # update: withdraw the old amount first
                state.add_realisasi(previous["kode_skpd"], -_dec(previous["jumlah"]))
            state.bku[row_id] = {
                "kode_skpd": after["kode_skpd"],
                "jenis": after["jenis"],
                "jumlah": _dec(after["jumlah"]),
            }
            if after["jenis"] == "PENGELUARAN":
                state.add_realisasi(after["kode_skpd"], _dec(after["jumlah"]))
        elif op == "d" and before:
            previous = state.bku.pop(int(before["id_bku"]), None)
            if previous and previous["jenis"] == "PENGELUARAN":
                state.add_realisasi(previous["kode_skpd"], -_dec(previous["jumlah"]))
        return "applied"

    return "ignored"


def over_threshold(state: StreamState, threshold: float = 0.90) -> list[dict]:
    """SKPDs whose realisasi exceeds the share of pagu, worst first."""
    rows = []
    for kode_skpd, pagu in state.pagu.items():
        if pagu <= 0:
            continue
        realisasi = state.realisasi.get(kode_skpd, Decimal("0"))
        persen = (realisasi / pagu * 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if realisasi / pagu > threshold:
            rows.append(
                {
                    "kode_skpd": kode_skpd,
                    "realisasi": quantize(realisasi),
                    "pagu": quantize(pagu),
                    "sisa": quantize(pagu - realisasi),
                    "persen": persen,
                }
            )
    return sorted(rows, key=lambda row: row["persen"], reverse=True)


def threshold_set(rows: list[dict]) -> list[str]:
    return sorted(row["kode_skpd"] for row in rows)


def save_state(state: StreamState, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "pagu": {k: str(v) for k, v in state.pagu.items()},
                "bku": {str(k): {**v, "jumlah": str(v["jumlah"])} for k, v in state.bku.items()},
                "realisasi": {k: str(v) for k, v in state.realisasi.items()},
                "seen": state.seen,
                "alerted": state.alerted,
            },
            indent=2,
        )
    )


def load_state(path: Path) -> StreamState:
    if not path.exists():
        return StreamState()
    raw = json.loads(path.read_text())
    return StreamState(
        pagu={k: _dec(v) for k, v in raw.get("pagu", {}).items()},
        bku={
            int(k): {**v, "jumlah": _dec(v["jumlah"])} for k, v in raw.get("bku", {}).items()
        },
        realisasi={k: _dec(v) for k, v in raw.get("realisasi", {}).items()},
        seen=raw.get("seen", {}),
        alerted=raw.get("alerted", []),
    )
