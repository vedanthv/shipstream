# ShipStream

A hands-on learning project for building event-driven pipelines with Kafka, Protobuf, and Python. Each phase adds one layer of real infrastructure — broker internals, schema evolution, retention policy, OS tuning — to a working order-processing pipeline.

The companion [knowledge base](./knowledge-base/README.md) is written in parallel with each phase: every concept you encounter in the code has a chapter that explains the why behind it.

---

## What's Inside

```
shipstream/
├── proto/order/v1/order.proto     # schema source of truth
├── generated/order/v1/            # protoc-compiled Python classes
├── services/
│   ├── producer.py                # publishes Order messages to order.created
│   ├── consumer.py                # poll loop with Protobuf + Schema Registry
│   ├── consumer_v2.py             # backward-compatible v2 consumer (Phase 2)
│   └── payment_producer.py        # multi-topic producer (Phase 6)
├── simulations/                   # standalone scripts that prove each concept
├── knowledge-base/                # written reference (26 chapters + simulation guides)
├── docker-compose.yml             # Redpanda broker + Console
└── requirements.txt
```

---

## Stack

| Layer | Tool |
|---|---|
| Broker | Redpanda (Kafka-compatible, runs in Docker) |
| Schema | Protobuf (proto3) |
| Language | Python 3 |
| Kafka client | `confluent-kafka` |
| Schema Registry | Redpanda built-in (port 18081) |
| Console | Redpanda Console (port 8080) |

---

## Quick Start

```bash
# Start the broker
docker compose up -d

# Install dependencies
pip install -r requirements.txt

# Publish 100 orders
python3 services/producer.py

# Run 3 parallel consumers
./run_consumers.sh

# Check consumer group lag
docker exec redpanda rpk group describe shipstream-consumer-group
```

Ports:
- `localhost:19092` — Kafka API (producers, consumers, rpk)
- `localhost:18081` — Schema Registry
- `localhost:8080` — Redpanda Console (web UI)

---

## Phases

### Phase 1 — Foundation
End-to-end pipeline: producer → `order.created` topic → consumer group. Protobuf serialization, Schema Registry wire format, partition assignment, offset tracking.

### Phase 2 — Schema Evolution
Safe and breaking schema changes. Backward compatibility mode, rolling deploys with mixed schema versions, field deletion.

### Phase 3 — Broker Internals
`broker.id`, listeners vs advertised listeners, `log.dirs`, `auto.create.topics.enable`, `num.partitions`, JVM thread pools vs Redpanda's Seastar shards.

### Phase 4 — Log Retention & Storage
Segments as the unit of deletion, `log.segment.bytes` / `log.segment.ms`, time- and size-based retention, `cleanup.policy` (delete / compact / compact+delete), `message.max.bytes` alignment across the stack, log start offset and `OFFSET_OUT_OF_RANGE`, tiered storage.

### Phase 5 — OS Tuning *(planned)*
`vm.swappiness`, dirty page ratios, file descriptor limits, filesystem choice for I/O-bound workloads.

### Phase 6 — Multi-Topic Routing *(planned)*
Fan-out to `order.paid` and `order.fulfilled`, router consumer, dead-letter queues, exactly-once vs at-least-once trade-offs.

### Phase 7 — Observability *(planned)*
Consumer lag dashboards, structured logging with correlation IDs, alerting on lag thresholds.

---

## Simulations

Each simulation is a self-contained Python script that creates its own topic, runs the experiment, and cleans up. Run them directly in a terminal (not via an automated tool) so the interactive pause before cleanup works.

| Script | What it proves |
|---|---|
| `sim_auto_create_footgun.py` | A typo produces to a ghost topic and messages disappear silently |
| `sim_partition_ceiling.py` | Running more consumers than partitions leaves extras idle |
| `sim_log_dirs.py` | Partition directories on disk; how multi-disk striping distributes them |
| `sim_crash_recovery.sh` | Hard-kill the broker mid-write; measure recovery time across partition counts |
| `sim_backward_compat.py` | v1 and v2 consumers running side by side against the same topic |
| `sim_segment_roll.py` | Active segment blocks retention; large vs small `segment.bytes` comparison |
| `sim_retention_bytes.py` | `retention.bytes` is per-partition, not per-topic |
| `sim_log_start_offset.py` | Committed offset falls below log start offset; `earliest` / `latest` / `error` reset policies |

Logs land in `logs/simulations/`.

---

## Knowledge Base

26 chapters across Kafka fundamentals, Protobuf, infrastructure, Schema Registry, broker internals, and log retention. Start at the index or jump to any concept:

**[→ knowledge-base/README.md](./knowledge-base/README.md)**

---

## Recompiling Protobuf

After editing `proto/order/v1/order.proto`:

```bash
protoc \
  --proto_path=proto \
  --python_out=generated \
  proto/order/v1/order.proto
```
