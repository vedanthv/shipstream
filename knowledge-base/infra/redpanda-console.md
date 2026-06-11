# Chapter 13 — Redpanda Console

> **You are here:** [Index](../README.md) → [Redpanda](./redpanda.md) → **Redpanda Console**

---

## What it is

Redpanda Console is a web UI at `http://localhost:8080` for inspecting everything happening inside Redpanda — topics, messages, consumer groups, and their current state. It's the fastest way to verify that your producer is actually publishing messages and your consumers are actually reading them.

---

## Protobuf decoding

Without configuration, the Console would show you raw binary bytes for every message — useless. The `console-config.yml` tells it how to decode messages on each topic:

```yaml
kafka:
  brokers:
    - redpanda:9092

serde:
  protobuf:
    enabled: true
    mappings:
      - topicName: order.created
        valueProtoType: order.v1.Order    # which message type to use
    fileSystem:
      enabled: true
      paths:
        - /proto                          # where to find .proto files
```

The `proto/` directory on your host is mounted into the Console container:

```yaml
# docker-compose.yml
volumes:
  - ./proto:/proto
```

So the Console reads `order.proto` directly from your filesystem and uses it to decode the binary messages into human-readable JSON in the UI. You never need to upload a schema manually.

---

## What to look at and why

### Topics → order.created → Messages

Each message decoded from binary into JSON:
```json
{
  "id": "1e4b6578-...",
  "customerId": "customer-43",
  "item": "Ergonomic Chair",
  "amount": 56.23,
  "status": "ORDER_STATUS_FULFILLED",
  "createdAt": "2024-01-15T10:30:00Z"
}
```

Use this to verify:
- The producer is sending messages (you see new rows appearing)
- Protobuf is being decoded correctly (fields are readable, not binary garbage)
- The correct data is being published (spot-check values)

### Topics → order.created → Partitions

Shows each partition with its Log End Offset — how many messages have been written to each partition. After publishing 100 messages across 3 partitions, you'll see roughly 33/34/33 split.

### Consumer Groups → shipstream-consumer-group

The most useful view for understanding what's happening:

```
MEMBER                      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
shipstream-consumer-1       P0         34              34              0
shipstream-consumer-2       P1         29              29              0
shipstream-consumer-3       P2         37              37              0
```

| Column | What it tells you |
|--------|------------------|
| `MEMBER` | The `client.id` of the consumer instance |
| `PARTITION` | Which partition this consumer owns |
| `CURRENT-OFFSET` | How far this consumer has read |
| `LOG-END-OFFSET` | How many messages exist |
| `LAG` | How far behind this consumer is |

A `LAG > 0` that's growing means your consumer can't keep up with the producer. A `LAG = 0` across all partitions means fully caught up.

---

## Common debugging scenarios

**"I don't see any messages"**
- Is Redpanda running? `docker compose ps`
- Did the producer actually run? Check for `[OK]` lines in its output
- Is the topic name correct? Check Topics list in Console

**"Messages show as binary garbage"**
- Protobuf decoding not configured — check `console-config.yml`
- Wrong `valueProtoType` — must match the package + message name exactly: `order.v1.Order`
- `.proto` file not mounted — check `volumes` in `docker-compose.yml`

**"Consumer group shows only 1 member even though I started 3"**
- `client.id` not set — all 3 show as `rdkafka`, Console deduplicates them
- Only 1 partition — only 1 consumer gets a partition assignment, others are idle members
- Consumers crashed on startup — check the log files

**"Lag is stuck and not going to 0"**
- Consumer is crashing in the processing loop — check consumer log for errors
- Consumer deserialization is failing — mismatch between producer schema and consumer schema

---

> ← [Previous: Redpanda](./redpanda.md) | [Index](../README.md) | [Next: Python Packages →](./python-packages.md)
