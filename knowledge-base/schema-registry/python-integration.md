# Python Integration

> **You are here:** [Index](../README.md) → [Schema Registry](./README.md) → **Python Integration**

---

## Packages

Schema Registry support is built into `confluent-kafka`. No separate package is needed, but it pulls in additional dependencies:

```
confluent-kafka==2.6.1   # base package — already installed
httpx                    # async HTTP client used internally
authlib                  # OAuth support
cachetools               # schema ID cache
googleapis-common-protos # google.type protos used by the serializer
```

All are listed in `requirements.txt`.

---

## Schema registration

The `ProtobufSerializer` registers the schema automatically on first use. You don't need to call the registry API manually — the serializer reads the file descriptor from the generated `_pb2.py` class and posts it to the registry.

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

schema_registry_client = SchemaRegistryClient({"url": "http://localhost:18081"})
protobuf_serializer = ProtobufSerializer(Order, schema_registry_client)
```

On first produce, the serializer:
1. Extracts the proto file descriptor from `Order.DESCRIPTOR.file`
2. Posts it to `POST /subjects/order.created-value/versions`
3. Receives the schema ID
4. Caches the ID for subsequent messages

---

## Producer

```python
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

schema_registry_client = SchemaRegistryClient({"url": "http://localhost:18081"})
protobuf_serializer = ProtobufSerializer(Order, schema_registry_client)

producer = Producer({"bootstrap.servers": "localhost:19092"})

order = Order(id="abc", item="Keyboard", region="eu-west", ...)

producer.produce(
    topic="order.created",
    key=order.id.encode(),
    value=protobuf_serializer(order, SerializationContext("order.created", MessageField.VALUE)),
    callback=delivery_report,
)
```

The `SerializationContext` tells the serializer which topic and field (VALUE vs KEY) this message is for — used to build the subject name (`order.created-value`).

---

## Consumer

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

schema_registry_client = SchemaRegistryClient({"url": "http://localhost:18081"})
protobuf_deserializer = ProtobufDeserializer(Order, schema_registry_client=schema_registry_client)

consumer = Consumer({...})
consumer.subscribe(["order.created"])

msg = consumer.poll(timeout=1.0)
order = protobuf_deserializer(msg.value(), SerializationContext("order.created", MessageField.VALUE))

print(order.id, order.region)
```

On each message, the deserializer:
1. Reads bytes 0–4 (magic byte + schema ID)
2. Looks up the schema ID in its local cache (or fetches from registry on miss)
3. Decodes bytes 5+ using the fetched schema

---

## What changed vs Phase 1

| | Phase 1 | Phase 2 |
|---|---------|---------|
| Serialization | `order.SerializeToString()` | `protobuf_serializer(order, ctx)` |
| Deserialization | `order.ParseFromString(raw)` | `protobuf_deserializer(raw, ctx)` |
| Wire format | raw Protobuf bytes | 5-byte prefix + Protobuf bytes |
| Schema storage | in the deployed `_pb2.py` only | in Schema Registry + `_pb2.py` |
| Schema ID on message | none | yes — bytes 1–4 |
| Replay safety | depends on consumer code version | always correct regardless of version |

---

## Verifying via the REST API

After running the producer, confirm the schema was registered:

```bash
# List all subjects
curl http://localhost:18081/subjects

# Get registered schema for this topic
curl http://localhost:18081/subjects/order.created-value/versions/latest

# Get a schema by ID
curl http://localhost:18081/schemas/ids/1
```

---

> ← [Previous: Compatibility Modes](./compatibility-modes.md) | [Part 4 Index](./README.md) | [Next: Schema Evolution →](./schema-evolution.md)
