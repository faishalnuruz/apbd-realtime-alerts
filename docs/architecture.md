# Architecture — APBD real-time alerts (PRD P2, built)

Project 2 of `Prd-Architecture-Proyek-Portfolio`. The PRD drew this path as
MySQL binlog → Debezium → Kafka → stream processing with PII masking →
real-time alert if a SKPD crosses 90% of its pagu, with a "<30 detik" target.
Everything in that chain runs locally in this repo, and the latency is measured
rather than asserted. The production transports it names (Pub/Sub, Dataflow,
BigQuery) stay what they are in the PRD: designed targets, not local replicas —
see [Honest limits](#honest-limits).

```
┌──────────────────────┐   binlog (ROW, metadata FULL)   ┌────────────────────────────┐
│ apbd-mysql           │ ───────────────────────────────▶ │ apbd-connect               │
│ SIMDA-shaped source: │   snapshot first, then stream   │ debezium/connect (KRaft     │
│ skpd · pagu_anggaran │                                 │ single broker: apbd-kafka)  │
│ bendahara · bku      │                                 │ REST: localhost:8080        │
└──────────────────────┘                                 └─────────────┬──────────────┘
                                                                       │
                          apbd.siktm.bku  ◀────────────────────────────┤
                          apbd.siktm.pagu_anggaran ◀────────────────────┘
                                       │
                                       ▼
                          ┌────────────────────────────┐
                          │ apbd_stream consumer       │
                          │ 1. mask PII (NIK, rekening)│
                          │ 2. apply CDC semantics     │
                          │ 3. evaluate 90% threshold  │
                          │ 4. edge-triggered alert    │
                          └─────────────┬──────────────┘
                                        ▼
                          docs/generated/rt_alert_payload.json
                          (+ Slack, only if SLACK_WEBHOOK_URL is set)
```

## Why Kafka here and not in P1

P1 (`sikt-mini-lakehouse`) lands CDC rows into a lakehouse where freshness is
a batch/heartbeat concern. P2's claim is different: an *entry by a bendahara*
must become an *alert* in seconds, to any number of future consumers. Kafka
provides the transport that decouples those: the consumer group here is one
consumer; a second dashboard, a second alert rule, or a replay of last week's
stream would join the same topics without touching the source.

## Ports (deliberately disjoint from the sikt stack)

| Service | Host | Container | Why |
| --- | --- | --- | --- |
| MySQL | 3307 | 3306 | sikt uses 3306 for its own MySQL |
| Kafka (external listener) | 9092 | 9092 | host consumers; `localhost:9092` |
| Kafka (internal listener) | — | 29092 | Debezium and any in-network client |
| Kafka Connect REST | 8080 | 8083 | this image always binds 8083 internally |

## The source

The schema mirrors SIMDA's core (the same shape P1 uses, so both projects read
comparable evidence): `skpd`, `pagu_anggaran`, `bendahara` (holds real-format
NIK and rekening — that is what makes the masking claim testable), and `bku`,
the daily budget ledger. Every BKU row carries `nik_bendahara` and
`rekening_tujuan` — PII travels with the transaction in the real world, so it
travels here too, and gets masked on the way out.

MySQL runs with `binlog-format=ROW`, `binlog-row-image=FULL`, and
`binlog-row-metadata=FULL` — the last one matters because 8.x defaults to
MINIMAL metadata, which makes Debezium emit `UNKNOWN_COL0` column names.

## One ingestion mechanism

The connector's `snapshot.mode=initial` gives Debezium's equivalent of a
replication slot's initial load: existing rows stream through the *same*
topic as live changes, from the *same* offsets, with the *same* envelope.
There is no separate initial-load script to drift out of sync with the
streaming path.

## CDC semantics in the consumer (`aggregate.py`)

- **Upsert, not sum.** An `update` to a BKU row *replaces* the amount already
  counted; a `delete` withdraws it. Summing a changelog triples budgets —
  P1 learned that lesson the expensive way; here it is a code path with tests.
- **Replay safety.** Delivery is at-least-once (Kafka's guarantee). Each event
  is keyed `topic:partition:offset` and applied once; re-draining or restarting
  cannot double-count (`test_re_draining_applies_nothing`).
- **Threshold is edge-triggered.** The alert fires when the *set* of
  over-threshold SKPDs changes — a new SKPD breaches, or an existing one
  recovers — not once per event.

## PII boundary

Debezium streams the source truthfully: the raw topic contains raw NIK. That
is correct — masking belongs to the processing step (as the PRD puts it:
masking *in the stream path*), not to the CDC layer. The consumer runs every
envelope through `mask_envelope()` **before** aggregation, so the raw values
have no downstream at all:

```
raw topic ──▶ mask_envelope ──▶ state · alert · evidence files
                 │
                 ├── nik_bendahara        → removed
                 ├── nik_bendahara_hash   → sha256 (for joins)
                 └── nik_bendahara_masked → "3275********7890"
```

`test_no_raw_pii_reaches_any_downstream` proves both halves: the topic *does*
carry the raw NIK, and no artifact this repo writes does.

## The latency number

- **Start:** `source.ts_ms` — the commit timestamp MySQL wrote for the
  transaction, carried through Debezium. Not "when we noticed".
- **End:** the consumer's wall clock after masking, applying, and evaluating
  the threshold for that event. Same machine, same clock, no skew correction.
- **Includes:** binlog read, Connect polling, broker fetch, masking,
  aggregation, evaluation.
- **Excludes:** Slack delivery and the PRD's production hops (Pub/Sub /
  Dataflow), documented as designed-not-measured.

The SLA is the PRD's own: p95 < 30.000 ms. `python -m apbd_stream.benchmark`
writes `docs/generated/latency-report.md` and exits non-zero if it fails —
the number is a build artifact, not a claim in prose.

## Honest limits

| PRD element | Status here |
| --- | --- |
| MySQL binlog → Debezium → Kafka | built, running, tested |
| PII masking in the stream | built, tested both directions |
| >90% pagu alert | built (dry-run payload; Slack only with webhook) |
| Latency <30 detik | **measured** — see `docs/generated/latency-report.md` |
| Pub/Sub + Dataflow | designed only (local analogue: Kafka + Python consumer) |
| BigQuery streaming table | designed only (state lives in the local aggregate) |
| Exactly-once delivery | at-least-once + dedup; not Kafka transactions |
| Multi-node Kafka / Connect | single-node KRaft compose, by design |
