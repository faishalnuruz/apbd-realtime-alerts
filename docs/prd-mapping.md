# PRD mapping — Project 2 (P2) · honest status per element

How this repository maps onto Project 2 of
`Prd-Architecture-Proyek-Portfolio.html`. Same rule as every project in the
portfolio: **a cell says "built" only when the code that builds it is in this
repo and its evidence is checked in.** Local equivalents are named as
equivalents, never passed off as the production service.

## Streaming path (the alert pipeline)

| PRD element | PRD target | Status | Evidence in this repo |
| --- | --- | --- | --- |
| Source CDC | MySQL binlog, ROW format | **built** | `compose.yaml` (`--binlog-format=ROW`, `--binlog-row-metadata=FULL`); `ops/mysql-init/01-debezium.sql` |
| Change capture | Debezium | **built** | `src/apbd_stream/connector.py` — snapshot (`initial`) + streaming, task verified RUNNING (`test_connector_reports_running`) |
| Streaming transport | Kafka (local stand-in for Pub/Sub) | **built** | single-broker KRaft in `compose.yaml`; topics `apbd.siktm.bku`, `apbd.siktm.pagu_anggaran` carry the live stream |
| Stream processing | Dataflow job (local stand-in: Python consumer) | **built** | `src/apbd_stream/consume.py` — mask → apply → evaluate, per event |
| PII masking in stream | Mask NIK / account numbers | **built** | `src/apbd_stream/masking.py`; proven in both directions by `test_no_raw_pii_reaches_any_downstream` (raw NIK *is* in the topic, *never* in state/alert/evidence) |
| Alert rule | SKPD realisasi > 90% pagu | **built** | `aggregate.over_threshold()` + edge-triggered emit; payload proven by `test_the_breach_produces_the_alert_payload` |
| Budget revision mid-stream | Revision must change the verdict | **built** | pagu updates flow through the same connector; `test_a_mid_stream_pagu_revision_changes_the_alert` |
| Latency target | Alert < 30 detik end-to-end | **built, measured** | `python -m apbd_stream.benchmark` → `docs/generated/latency-report.{md,json}`; CI-failing gate if p95 ≥ 30.000 ms |
| Slack delivery | Push to #finmon-alerts | **built (dry-run)** | `alert.emit()` writes `docs/generated/rt_alert_payload.json`; posts only when `SLACK_WEBHOOK_URL` is set — no accidental message, verified payload format by test |
| Replay / reprocessing | Reprocess safely | **built** | offsets + per-event dedup key; `test_re_draining_applies_nothing`, `test_replayed_offsets_are_ignored` |

## Designed, not built (kept visible on purpose)

| PRD element | Why it is not built here | What stands in for it |
| --- | --- | --- |
| Pub/Sub topics | Cloud service; portfolio rule is no cloud accounts | Kafka topics with the same publish/consume contract |
| Dataflow job | Managed service; local analogue must run in CI-friendly Python | the Python consumer (`consume.py`), same mask/aggregate/emit stages |
| BigQuery streaming table | Managed warehouse; P1 already shows warehouse-side patterns locally | in-repo aggregate state (`data/state.json`), rebuildable from the topics |
| Exactly-once (Pub/Sub + Dataflow semantics) | needs a transactional sink/broker setup beyond this demo's scope | at-least-once delivery + offset-keyed dedup, tested for replay safety |
| Production HA (multi-broker Kafka, Connect distributed) | deliberately out of scope for a laptop demo | single-node KRaft; documented in `docs/architecture.md` |

## Test split

- **Unit (26)** — masking, CDC upsert/delete/dedup semantics, threshold math,
  alert payload, latency percentiles, temporal rendering, connector spec,
  benchmark planner. Runs in CI on every push.
- **Integration (7)** — against the real stack: connector state, snapshot =
  source ledger, live-event latency under the SLA, alert payload, PII leak
  probe, pagu revision, idempotent re-drain. Local only: `python -m pytest
  -m integration`.
