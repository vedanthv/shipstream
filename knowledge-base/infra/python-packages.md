# Chapter 14 — Python Packages

> **You are here:** [Index](../README.md) → [Redpanda Console](./redpanda-console.md) → **Python Packages**

---

## requirements.txt

```
confluent-kafka==2.6.1
protobuf==5.29.3
```

Two packages. That's the entire Python dependency footprint for Phase 1.

---

## confluent-kafka

The official Confluent Kafka client for Python. Unlike pure-Python alternatives, it wraps **librdkafka** — a high-performance C library used in production at companies like Confluent, Uber, and LinkedIn.

### What it provides

```python
from confluent_kafka import Producer, Consumer, KafkaError
```

`Producer` — connects to a broker, buffers messages, handles batching, retries, and delivery callbacks.

`Consumer` — connects to a broker, joins a consumer group, polls for messages, commits offsets, and sends heartbeats.

`KafkaError` — error codes for handling specific conditions (e.g., `KafkaError._PARTITION_EOF` means you've read all available messages on a partition).

### Why the client.id shows as rdkafka

librdkafka is the underlying C library. Its name leaks through as the default `client.id` for every consumer or producer that doesn't set one explicitly. That's why the Redpanda Console showed one member called `rdkafka` even when we had three consumers — they all shared the same default identifier.

Fix: always set `client.id` to something meaningful.

```python
consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "client.id": f"shipstream-consumer-{CONSUMER_ID}",  # ← visible in UI
})
```

### Key configuration options

```python
Producer({
    "bootstrap.servers": "localhost:19092",  # broker address
    "acks": "all",                           # wait for all replicas to confirm
    "retries": 3,                            # retry on transient failures
})

Consumer({
    "bootstrap.servers": "localhost:19092",
    "group.id": "my-group",
    "client.id": "my-consumer-1",
    "auto.offset.reset": "earliest",         # start from beginning if no offset
    "enable.auto.commit": True,              # auto-commit offset every 5s
    "session.timeout.ms": 45000,            # declared dead after 45s without heartbeat
})
```

---

## protobuf

Google's Python runtime for Protocol Buffers. Provides the base classes that generated `_pb2.py` files build on.

### What it provides

```python
from google.protobuf.timestamp_pb2 import Timestamp   # well-known type
```

The `Order` and `OrderStatus` classes in `order_pb2.py` inherit from protobuf base classes that implement `SerializeToString()`, `ParseFromString()`, and field access.

### Version matters

The generated `_pb2.py` file is tied to the version of `protoc` used to generate it. If you upgrade the `protobuf` package significantly, regenerate the `_pb2.py` files to stay in sync.

---

## Install

```bash
pip install -r requirements.txt
```

Or individually:

```bash
pip install confluent-kafka==2.6.1
pip install protobuf==5.29.3
```

For the compile step you also need:

```bash
pip install grpcio-tools   # provides the protoc compiler via python3 -m grpc_tools.protoc
```

---

## You've reached the end of the book

You now have a complete mental model of the ShipStream Phase 1 stack — from why Kafka exists, through topics, partitions, offsets, consumer groups, producers and consumers, rebalancing, all the way through Protobuf schemas, compilation, and the Docker infrastructure.

**[← Back to Index](../README.md)**

---

> ← [Previous: Redpanda Console](./redpanda-console.md) | [Index](../README.md)
