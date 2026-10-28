# End-to-end latency: MySQL commit -> alert decision

Measured 200 inserts paced 50 ms apart; 200 events captured.

| Metric | Value |
| --- | --- |
| p50 | 1492 ms |
| p90 | 2343 ms |
| p95 | **2483 ms** |
| p99 | 2492 ms |
| max | 3165 ms |
| mean | 1608.5 ms |
| alert decision (threshold crossing event) | 1490 ms |
| SLA (PRD: < 30 detik) | 30000 ms → **PASS** |

**Definition.** The start timestamp is `source.ts_ms` — the commit time MySQL itself wrote for the transaction, carried through Debezium. The end timestamp is the consumer's wall clock after it has masked the event, updated the aggregate, and evaluated the threshold. Same machine, same clock: no skew correction.

**What it includes.** MySQL binlog read by Debezium, Kafka Connect's polling of the stream, the broker's fetch to this consumer, masking, aggregation, and the threshold evaluation — everything between "bendahara presses enter" and "the decision exists".

**What it does not include.** Slack's delivery (a webhook POST that this repo performs only when SLACK_WEBHOOK_URL is set) and the production transport hops (Pub/Sub/Dataflow in the PRD's target architecture) — those are documented as designed, not measured.
