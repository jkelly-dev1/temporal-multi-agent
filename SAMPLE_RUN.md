# Sample run (real model)

A verbatim capture of `run_demo.py` against a real Claude model, so the
self-critique behaviour can be inspected without an API key.

- **Captured:** 2026-07-20
- **Command:** `AGENT_PROVIDER=anthropic python run_demo.py "designing a fleet-telemetry ingestion pipeline"`
- **Model:** `claude-opus-4-8` (the `AGENT_MODEL` default)
- **Model calls:** 15 total: 7 research passes (4 first attempts + 3 rewrites), 7
  LLM-as-judge critiques (one per pass), 1 synthesis

> A real captured run, reproduced unedited, including the one section the
> judge was least impressed by. With `AGENT_PROVIDER` unset the demo uses the
> deterministic mock instead and produces obvious placeholder prose.

## What to look at

**The reflection loop fired on three of four sections.** Sections 1, 2, and 4
scored below the 0.70 `CONFIDENCE_BAR` on their first pass, got feedback from
the critic, and were rewritten. Section 3 cleared the bar first try and was left
alone.

The difference is legible in the output, not just in a counter. Compare the
rewritten section 4 against the untouched section 3. Section 3 says to use
"idempotent writes, event-time watermarking, and deduplication" and to route
malformed payloads to a dead-letter queue, all true, all unactionable. Section
4 says to set `batch.size` to 64–256KB with `linger.ms` of 20–50ms, size
watermarks to the p99 observed skew (30–90s for cellular fleets), key
deduplication on a device-message-UUID over a 15-minute state TTL so retries
collapse without exploding RocksDB state, and page when DLQ ingress exceeds
0.5% of throughput rather than on a raw count that alerts falsely during
traffic spikes.

That gap between "correct" and "usable" is the entire argument for a
self-critique step, and it is why the judge scores them 0.72 and 0.95.

| Section | Confidence | Attempts |
|---|---|---|
| background and core definitions | 0.90 | 2 |
| current state of the art | 0.90 | 2 |
| key risks and failure modes | 0.72 | 1 |
| operational best practices | 0.95 | 2 |

Average 0.867.

Scores come from an LLM-as-judge constrained by a JSON schema, so `score`
arrives as a validated number the workflow can branch on rather than something
scraped out of prose. On the mock path the same seam uses a deterministic
length heuristic, which is what keeps the test suite hermetic; see the
README's "Use a real LLM" section.

**This is not deterministic.** Which sections get refined varies run to run;
this capture is one sample, not a guaranteed outcome.

---

## Output

```
stage: awaiting-approval
plan : ['background and core definitions', 'current state of the art', 'key risks and failure modes', 'operational best practices']
approving plan (human-in-the-loop signal)...

==============================================================================
TOPIC: designing a fleet-telemetry ingestion pipeline
provider: anthropic
==============================================================================
EXECUTIVE SUMMARY
A fleet-telemetry ingestion pipeline must be built for unreliable cellular links and thousands of moving assets, which makes delivery semantics the foundational choice: favor at-least-once transport paired with idempotent writes keyed on a deterministic tuple such as (device_id, sequence, event_time), since exactly-once via distributed transactions roughly halves throughput and adds latency while idempotency already collapses the inevitable duplicate replays into no-ops. Correctness depends on keying and windowing on device-stamped event time rather than arrival time, applying clock-skew correction and a bounded watermark set from the measured p99 skew (commonly tens of seconds to a few minutes) so aggregations emit promptly while anything later routes to a reprocessing side-path instead of being dropped, which is what otherwise corrupts fuel-usage or geofence dwell-time rollups. Capacity should be sized against realistic reconnect bursts rather than steady state, because a regional outage can leave twenty thousand vehicles simultaneously flushing tens of millions of backlogged events, so partition Kafka by device to preserve per-device ordering, size retention to survive the longest expected outage (roughly 24-72 hours), and tune batch and linger settings to balance throughput against latency. Storage choice hinges on cardinality: column-oriented stores like ClickHouse tolerate hundreds of millions of distinct identifiers where per-series indexes stall past a few million active series, though asynchronous deduplication at merge time argues for append-only writes with downstream aggregation over read-time upsert guarantees. Chronic risks include schema drift across heterogeneous firmware, poison messages, and silent data loss, mitigated by a schema registry enforcing backward-compatible evolution, dead-letter queues alerted on rate and ratio rather than raw counts, and end-to-end lineage with replay capability. The dominant trade-off throughout is latency versus completeness and cost, so parameters for watermarks, batching, and deduplication windows should be derived from observed connectivity and retry distributions, with continuous monitoring of consumer lag and checkpoint duration to catch backpressure before buffers overflow.

SECTIONS
  [1] background and core definitions  (confidence=0.9, attempts=2)
      A fleet-telemetry pipeline ingests high-cardinality time-series data from thousands of moving assets over unreliable cellular links, so the foundational decision is delivery semantics: choose at-least-once with idempotent upserts keyed on (vehicle_id, sensor, event_time) rather than exactly-once, because network drops and radio reconnects will otherwise force you into expensive distributed transactions while the idempotency key already collapses the inevitable duplicate replays into no-ops. Size the ingest buffer to the realistic reconnect burst rather than the steady state: if 20,000 vehicles each buffer up to 15 minutes of readings at 1 Hz during a regional outage, a recovery reconnect can dump roughly 18 million backlogged events, so a Kafka topic partitioned to sustain that drain rate (with retention exceeding the longest expected outage) prevents the silent data loss that occurs when devices overflow their own local queues. For late arrivals, set a bounded watermark—on the order of a few minutes past event time—so windowed aggregations emit promptly, and route anything later than the watermark to a reprocessing side-path instead of dropping it, which is the failure mode that corrupts fuel-usage or geofence dwell-time rollups when a truck's data trickles in an hour after it drove through a tunnel. The core distinction to internalize is event time versus ingestion time: keying and windowing on device-stamped event time (with clock-skew correction) is what keeps the pipeline correct when the transport layer is not.
      trace: agent://background-and-core-definitions/attempt-2
  [2] current state of the art  (confidence=0.9, attempts=2)
      Fleet telemetry pipelines typically ingest through MQTT or a Kafka-fronted gateway, and the sharpest early decision is delivery semantics: at-least-once is the pragmatic default because exactly-once via Kafka transactions roughly halves producer throughput and adds tens of milliseconds of commit latency, so most designs pair at-least-once transport with idempotent writes keyed on a deterministic (device_id, monotonic_sequence, event_time) tuple to deduplicate the retransmissions that edge buffers replay after connectivity gaps. Storage choice hinges on cardinality: InfluxDB's per-series index degrades badly past roughly one to ten million active series (ingest stalls and memory blowups as unique tag combinations like VIN × sensor × trip explode), whereas ClickHouse tolerates hundreds of millions of distinct values because it stores high-cardinality identifiers as ordinary columns rather than indexed series, making it the safer default for large fleets while InfluxDB or TimescaleDB remain fine for constrained, low-cardinality tag sets. For late and out-of-order edge data, budget explicit watermark tolerance (commonly minutes to hours) rather than the second-scale windows used in web analytics, and prefer append-only writes with downstream aggregation over in-place upserts, since ClickHouse's ReplacingMergeTree deduplicates only asynchronously at merge time and gives no read-time guarantee. Finally, size the pipeline against sustained device write rates and partition Kafka by device to preserve per-device ordering, accepting the resulting hot-partition skew as a cheaper problem than losing the sequence ordering your deduplication key depends on.
      trace: agent://current-state-of-the-art/attempt-2
  [3] key risks and failure modes  (confidence=0.72, attempts=1)
      An engineer designing fleet-telemetry ingestion must anticipate ingestion overload from bursty or reconnecting devices, where thousands of vehicles flushing buffered data simultaneously can overwhelm brokers and backpressure downstream consumers, so partitioning, rate limiting, and elastic scaling are essential. Data quality and ordering are major failure modes: clock skew across devices, out-of-order or duplicate messages, and intermittent connectivity require idempotent writes, event-time watermarking, and deduplication rather than reliance on arrival order. Schema evolution and device firmware heterogeneity are chronic risks, as older units send stale formats and breaking changes can silently corrupt records, so versioned schemas, a schema registry, and dead-letter queues for malformed payloads are critical. Finally, watch for cost and storage blowups from high-cardinality metrics and raw retention, single points of failure in the broker or gateway layer, and gaps in observability that hide silent data loss, making end-to-end lineage, monitoring, and replay capability necessary for trustworthy operation.
      trace: agent://key-risks-and-failure-modes/attempt-1
  [4] operational best practices  (confidence=0.95, attempts=2)
      Design the ingestion tier to buffer bursts and decouple producers from consumers, typically using Kafka or Kinesis with partitioning keyed on vehicle ID so per-device ordering is preserved and hot partitions are avoidable; size retention to at least 24-72 hours so you can replay after downstream outages, and set producer batch.size around 64-256KB with linger.ms of 20-50ms to balance throughput against the added latency. For late and out-of-order data, set stream-processing watermarks (Flink or Spark Structured Streaming) to tolerate the p99 observed skew—commonly 30-90 seconds for cellular-connected fleets—accepting that anything beyond the allowed lateness (say 5 minutes) routes to a side output rather than dropping silently, and choose a deduplication window that covers the retry pattern: if devices retry with exponential backoff up to ~10 minutes, key dedup on a device-message-UUID over a 15-minute state TTL so retries collapse without exploding RocksDB state. Route poison messages and schema-violation records to a dead-letter queue enforced by a Schema Registry with backward-compatible evolution, and alert not on raw DLQ count but on rate and ratio—for example page when DLQ ingress exceeds 0.5% of throughput over a 5-minute window or absolute rate crosses ~100 msg/s, since a fixed count alerts falsely during traffic spikes. The dominant trade-off is latency versus completeness and cost: tighter watermarks and smaller batches cut end-to-end lag but raise per-record overhead and increase late-arrival loss, so pick these parameters from measured connectivity distributions rather than defaults, and continuously monitor consumer lag (target under a few seconds of steady-state) and checkpoint duration to catch backpressure before the buffer fills.
      trace: agent://operational-best-practices/attempt-2

REVIEW
  Reviewed 4 sections; average confidence 0.867. All sections meet the bar.
  avg_confidence=0.867
```

Not shown above: while the agents run, the terminal displays a live progress
line driven by the workflow's `get_stage` query (`⠹ researching… (14s)`). It is
written to stderr and suppressed when stderr is not a TTY, which is why piping
the demo's output produces exactly the text above.
