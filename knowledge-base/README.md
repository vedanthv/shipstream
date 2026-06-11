# ShipStream — Knowledge Base

A self-contained reference for the Phase 1 event-driven order pipeline. Read it like a book from Chapter 1 to the end, or jump to any concept directly.

---

## Table of Contents

### Part 1 — Kafka
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 1 | [What is Kafka?](./kafka/what-is-kafka.md) | Why brokers exist, Kafka vs direct calls, Redpanda |
| 2 | [Topics & Partitions](./kafka/topics-and-partitions.md) | Append-only logs, partition keys, parallelism ceiling |
| 3 | [Offsets](./kafka/offsets.md) | Message addressing, log end offset, group offset, lag |
| 4 | [Consumer Groups](./kafka/consumer-groups.md) | Group mechanics, one-per-partition rule, multiple groups |
| 5 | [Producer](./kafka/producer.md) | Serialization, partition key, delivery guarantees |
| 6 | [Consumer](./kafka/consumer.md) | Poll loop, deserialization, offset commits |
| 7 | [Rebalancing](./kafka/rebalancing.md) | Partition redistribution, race conditions, stability |

### Part 2 — Protobuf
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 8 | [What is Protobuf?](./protobuf/what-is-protobuf.md) | Binary serialization, why not JSON, field numbers |
| 9 | [Proto Schema](./protobuf/proto-schema.md) | `.proto` syntax, messages, enums, well-known types |
| 10 | [Compile Workflow](./protobuf/compile-workflow.md) | `protoc`, generated files, `__init__.py` |
| 11 | [Python Usage](./protobuf/python-usage.md) | Constructing, serializing, deserializing |

### Part 3 — Infrastructure
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 12 | [Redpanda](./infra/redpanda.md) | Ports, internal vs external, rpk CLI |
| 13 | [Redpanda Console](./infra/redpanda-console.md) | Web UI, Protobuf decoding, what to look for |
| 14 | [Python Packages](./infra/python-packages.md) | confluent-kafka, protobuf, librdkafka |

---

## Start Reading

**[→ Chapter 1: What is Kafka?](./kafka/what-is-kafka.md)**

---

## Quick Commands

```bash
# Start infrastructure
docker compose up -d

# Publish 100 orders
python3 services/producer.py

# Start 3 parallel consumers
for i in 1 2 3; do
  CONSUMER_ID=$i python3 -u services/consumer.py > logs/consumers/consumer-$i.log 2>&1 &
done

# Check consumer group lag
docker exec redpanda rpk group describe shipstream-consumer-group

# Recreate topic with 3 partitions
docker exec redpanda rpk topic delete order.created
docker exec redpanda rpk topic create order.created --partitions 3
```
