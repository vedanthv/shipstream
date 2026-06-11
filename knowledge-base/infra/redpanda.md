# Chapter 12 — Redpanda

> **You are here:** [Index](../README.md) → [Python Usage](../protobuf/python-usage.md) → **Redpanda**

---

## What it is

Redpanda is a Kafka-compatible message broker that runs as a single Docker container. It handles everything: storing messages on disk, serving producers and consumers, tracking consumer group offsets, and managing partition leadership.

In production at scale you'd run a cluster of Redpanda nodes (or managed Kafka). In ShipStream we run a single node in `--mode dev-container` — no replication, optimized for local development.

---

## The two network addresses

Redpanda listens on two addresses for the same Kafka API:

```yaml
--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
--advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
```

| Address | Used by | Why separate |
|---------|---------|-------------|
| `redpanda:9092` (internal) | Other Docker containers (Console) | Docker containers resolve `redpanda` via Docker's internal DNS |
| `localhost:19092` (external) | Python scripts on the host machine | Host machine can't resolve `redpanda` — needs `localhost` |

When the Console connects to Redpanda, it uses `redpanda:9092`. When your Python scripts connect, they use `localhost:19092`. Same broker, different network paths.

---

## All exposed ports

| External port | Internal port | Protocol | Purpose |
|--------------|--------------|----------|---------|
| `19092` | `9092` | Kafka API | Producer/consumer connections |
| `18081` | `8081` | Schema Registry | Centralized schema storage (not used in Phase 1) |
| `18082` | `8082` | Pandaproxy | REST API — Kafka over HTTP |
| `33145` | `33145` | RPC | Internal Redpanda cluster communication |

---

## rpk — the CLI

`rpk` is Redpanda's command-line tool, included in the container. You access it via `docker exec`.

### Topic commands

```bash
# List all topics
docker exec redpanda rpk topic list

# Create a topic with 3 partitions
docker exec redpanda rpk topic create order.created --partitions 3

# Describe a topic (see partition count, config)
docker exec redpanda rpk topic describe order.created

# Delete a topic (destructive — removes all messages)
docker exec redpanda rpk topic delete order.created

# Consume from a topic directly (debugging)
docker exec redpanda rpk topic consume order.created
```

### Consumer group commands

```bash
# List all groups
docker exec redpanda rpk group list

# Describe a group (see members, offsets, lag per partition)
docker exec redpanda rpk group describe shipstream-consumer-group

# Reset a group's offset (all consumers must be stopped first)
docker exec redpanda rpk group seek shipstream-consumer-group \
  --to start --topics order.created     # back to beginning

docker exec redpanda rpk group seek shipstream-consumer-group \
  --to end --topics order.created       # skip to latest

# Delete a consumer group
docker exec redpanda rpk group delete shipstream-consumer-group
```

### Cluster commands

```bash
# Check cluster health
docker exec redpanda rpk cluster health

# See broker info
docker exec redpanda rpk cluster info
```

---

## Healthcheck

Docker Compose waits for Redpanda to be healthy before starting the Console:

```yaml
healthcheck:
  test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true' || exit 1"]
  interval: 5s
  timeout: 10s
  retries: 10
```

If the Console starts immediately but shows "unable to connect", Redpanda hasn't finished initializing yet. Give it 10-20 seconds.

---

> ← [Previous: Python Usage](../protobuf/python-usage.md) (Ch 12) | [Index](../README.md) | [Next: Redpanda Console →](./redpanda-console.md)
