# ShipStream — Knowledge Base

A self-contained reference for the event-driven order pipeline. Read it like a book from Chapter 1 to the end, or jump to any concept directly.

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
| 7 | [poll() and flush()](./kafka/poll-and-flush.md) | Producer vs consumer poll, flush before exit, heartbeat contract |
| 8 | [Rebalancing](./kafka/rebalancing.md) | Partition redistribution, race conditions, stability |

### Part 2 — Protobuf
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 8 | [What is Protobuf?](./protobuf/what-is-protobuf.md) | Binary serialization, JSON vs Protobuf, field numbers |
| 9 | [Proto Schema](./protobuf/proto-schema.md) | `.proto` syntax, wire types, tag formula, schema evolution |
| 10 | [The Schema Disaster](./protobuf/schema-story.md) | Field number change → silent corruption → why Schema Registry exists |
| 11 | [Binary Encoding](./protobuf/binary-encoding.md) | Byte-by-byte teardown, varints, IEEE 754, tag decoding |
| 12 | [Compile Workflow](./protobuf/compile-workflow.md) | `protoc` internals, file descriptor, generated code |
| 13 | [Python Usage](./protobuf/python-usage.md) | Constructing, serializing, deserializing, introspection |

### Part 3 — Infrastructure
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 14 | [Redpanda](./infra/redpanda.md) | Ports, internal vs external, rpk CLI |
| 15 | [Redpanda Console](./infra/redpanda-console.md) | Web UI, Protobuf decoding, what to look for |
| 16 | [Python Packages](./infra/python-packages.md) | confluent-kafka, protobuf, librdkafka |

### Part 4 — Schema Registry
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 17 | [What is Schema Registry?](./schema-registry/what-is-schema-registry.md) | Why it exists, what problem it solves, Redpanda's built-in registry |
| 18 | [Wire Format](./schema-registry/wire-format.md) | The 5-byte prefix, magic byte, schema ID, MessageIndex |
| 19 | [Compatibility Modes](./schema-registry/compatibility-modes.md) | BACKWARD, FORWARD, FULL — rolling deploy examples, which mode to use |
| 20 | [Python Integration](./schema-registry/python-integration.md) | `ProtobufSerializer`, `ProtobufDeserializer`, what changed vs Phase 1 |
| 21 | [Schema Evolution](./schema-registry/schema-evolution.md) | Which schema consumers use after a change, safe vs breaking changes, deploy steps |
| 22 | [Schema Caching](./schema-registry/schema-caching.md) | How `SchemaRegistryClient` caches schemas, one fetch per ID, restart behaviour |

### Simulations
| # | Guide | What you'll do |
|---|-------|---------------|
| S1 | [Compatibility & Field Deletion](./simulations/compatibility-field-deletion.md) | Register schema v2 under BACKWARD mode; run v1 and v2 consumers side by side; observe lag, MEMBER-ID, and partition assignment live |

---

## Start Reading

**[→ Chapter 1: What is Kafka?](./kafka/what-is-kafka.md)**

---

## Quick Commands

```bash
# Start infrastructure
docker compose up -d

# Publish 100 orders (with Schema Registry)
python3 services/producer.py

# Start 3 parallel consumers (with Schema Registry)
for i in 1 2 3; do
  CONSUMER_ID=$i python3 -u services/consumer.py > logs/consumers/consumer-$i.log 2>&1 &
done

# Check consumer group lag
docker exec redpanda rpk group describe shipstream-consumer-group

# Inspect registered schemas
curl http://localhost:18081/subjects
curl http://localhost:18081/subjects/order.created-value/versions/latest

# Recreate topic with 3 partitions
docker exec redpanda rpk topic delete order.created
docker exec redpanda rpk topic create order.created --partitions 3
```
