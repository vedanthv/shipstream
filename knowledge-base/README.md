# ShipStream — Knowledge Base

A self-contained reference for the event-driven order pipeline. Read it like a book from Chapter 1 to the end, or jump to any concept directly.

---

## Table of Contents

### Part 1 — Kafka
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 1 | [What is Kafka?](./kafka/what-is-kafka.md) | Why brokers exist, Kafka vs direct calls, Redpanda |
| 2 | [Topics & Partitions](./kafka/topics-and-partitions.md) | Append-only logs, partition keys, parallelism ceiling |
| 3 | [Offsets](./kafka/offsets.md) | Message addressing, log end offset, group offset, lag, auto.offset.reset |
| 4 | [Consumer Groups](./kafka/consumer-groups.md) | Group mechanics, one-per-partition rule, multiple groups |
| 5 | [Producer](./kafka/producer.md) | Serialization, partition key, delivery guarantees |
| 6 | [Consumer](./kafka/consumer.md) | Poll loop, deserialization, offset commits |
| 7 | [poll() and flush()](./kafka/poll-and-flush.md) | Producer vs consumer poll, flush before exit, heartbeat contract |
| 8 | [Rebalancing](./kafka/rebalancing.md) | Partition redistribution, race conditions, stability |

### Part 2 — Protobuf
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 9 | [What is Protobuf?](./protobuf/what-is-protobuf.md) | Binary serialization, JSON vs Protobuf, field numbers |
| 10 | [Proto Schema](./protobuf/proto-schema.md) | `.proto` syntax, wire types, tag formula, schema evolution |
| 11 | [The Schema Disaster](./protobuf/schema-story.md) | Field number change → silent corruption → why Schema Registry exists |
| 12 | [Binary Encoding](./protobuf/binary-encoding.md) | Byte-by-byte teardown, varints, IEEE 754, tag decoding |
| 13 | [Compile Workflow](./protobuf/compile-workflow.md) | `protoc` internals, file descriptor, generated code |
| 14 | [Python Usage](./protobuf/python-usage.md) | Constructing, serializing, deserializing, introspection |

### Part 3 — Infrastructure
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 15 | [Redpanda](./infra/redpanda.md) | Ports, internal vs external, rpk CLI |
| 16 | [Redpanda Console](./infra/redpanda-console.md) | Web UI, Protobuf decoding, what to look for |
| 17 | [Python Packages](./infra/python-packages.md) | confluent-kafka, protobuf, librdkafka |
| 18 | [Docker Compose](./infra/docker-compose.md) | How `compose up` registers OS port rules, the full Python→Redpanda request journey, named volumes vs bind mounts, depends_on and healthchecks |

### Part 4 — Schema Registry
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 19 | [What is Schema Registry?](./schema-registry/what-is-schema-registry.md) | Why it exists, what problem it solves, Redpanda's built-in registry |
| 20 | [Wire Format](./schema-registry/wire-format.md) | The 5-byte prefix, magic byte, schema ID, MessageIndex |
| 21 | [Compatibility Modes](./schema-registry/compatibility-modes.md) | BACKWARD, FORWARD, FULL — rolling deploy examples, which mode to use |
| 22 | [Python Integration](./schema-registry/python-integration.md) | `ProtobufSerializer`, `ProtobufDeserializer`, what changed vs Phase 1 |
| 23 | [Schema Evolution](./schema-registry/schema-evolution.md) | Which schema consumers use after a change, safe vs breaking changes, deploy steps |
| 24 | [Schema Caching](./schema-registry/schema-caching.md) | How `SchemaRegistryClient` caches schemas, one fetch per ID, restart behaviour |

### Part 5 — Broker Internals (Phase 3)
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 25 | [Brokers and Clusters](./kafka/brokers-and-clusters.md) | What a broker is, clustering, broker.id, listeners vs advertised.listeners, ISR |
| 26 | [Topic Defaults](./kafka/topic-defaults.md) | log.dirs, recovery threads, auto.create.topics.enable, num.partitions |
| 27 | [Threading Models](./kafka/threading-models.md) | JVM thread pools vs Seastar shards, num.io.threads, async I/O, how poll() and flush() travel through broker threads |
| 28 | [Threading Models II](./kafka/threading-models-ii.md) | More detailed deepdive into threading models |

### Part 6 — Log Retention & Storage (Phase 4)
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 29 | [Log Retention & Storage](./kafka/log-retention.md) | Segments as the unit of deletion, segment rolling, time and size retention, message.max.bytes alignment, log start offset, tiered storage, cleanup.policy (delete / compact / compact+delete) |

### Part 7 — Replication & Fault Tolerance (Phase 5)
| # | Chapter | What you'll learn |
|---|---------|------------------|
| 30 | [Replication & Fault Tolerance](./kafka/replication.md) | Leader-follower model, ISR mechanics, replication.factor, min.insync.replicas, leader election, unclean leader election, replica.fetch.max.bytes |
| 31 | [replica.fetch.max.bytes](./kafka/replica-fetch-max-bytes.md) | Why follower fetch size must match message.max.bytes, the four-way size chain, when mismatches silently break replication |

### Simulations
| # | Guide | What you'll do |
|---|-------|---------------|
| S1 | [Compatibility & Field Deletion](./simulations/compatibility-field-deletion.md) | Register schema v2 under BACKWARD mode; run v1 and v2 consumers side by side; observe lag, MEMBER-ID, and partition assignment live |
| S2 | [Phase 3: Broker Internals](./simulations/phase3-guide.md#s2----autocreatetopicsenable-footgun) | Typo a topic name and watch payments vanish into a ghost topic; observe the difference with auto-create disabled |
| S3 | [Phase 3: Broker Internals](./simulations/phase3-guide.md#s3----numpartitions-consumer-parallelism-ceiling) | Run 4 consumers against a 2-partition topic; see exactly which consumers are idle and why |
| S4 | [Phase 3: Broker Internals](./simulations/phase3-guide.md#s4----logdirs-on-disk-partition-layout) | Inspect partition directories and segment files on disk; see how multi-disk striping would distribute them |
| S5 | [Phase 3: Broker Internals](./simulations/phase3-guide.md#s5----crash-recovery-partition-count-vs-recovery-time) | Hard-kill the broker mid-write across three partition-count scenarios; measure recovery time and verify offset integrity |
| S6 | [Phase 4: Log Retention](./simulations/phase4-guide.md#s6----active-segment-blocks-retention) | Prove that the active segment blocks deletion even when messages are past the retention window |
| S7 | [Phase 4: Log Retention](./simulations/phase4-guide.md#s7----logretentionbytes-per-partition-size-cap) | Route all traffic to one partition and watch only that partition get trimmed by the per-partition size cap |
| S8 | [Phase 4: Log Retention](./simulations/phase4-guide.md#s8----log-start-offset-and-offset_out_of_range) | Let retention delete committed offsets; observe how earliest, latest, and error reset policies each behave |
| S9 | Phase 5: Replication *(coming)* | Create RF=3 topic, kill the leader mid-produce, observe controller elect a new leader and production continue |
| S10 | Phase 5: Replication *(coming)* | Set min.insync.replicas=2, kill 2 of 3 brokers, watch acks=all block with NOT_ENOUGH_REPLICAS |
| S11 | Phase 5: Replication *(coming)* | Unclean leader election — kill leader + one follower, restart stale follower, observe data loss |

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
docker exec redpanda-1 rpk group describe shipstream-consumer-group

# Inspect registered schemas
curl http://localhost:18081/subjects
curl http://localhost:18081/subjects/order.created-value/versions/latest

# Recreate topic with 3 partitions
docker exec redpanda-1 rpk topic delete order.created
docker exec redpanda-1 rpk topic create order.created --partitions 3
```
