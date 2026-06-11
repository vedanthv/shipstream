# Chapter 11 — Python Usage

> **You are here:** [Index](../README.md) → [Compile Workflow](./compile-workflow.md) → **Python Usage**

---

## Import

```python
from generated.order.v1.order_pb2 import Order, OrderStatus
from google.protobuf.timestamp_pb2 import Timestamp
```

`Order` and `OrderStatus` come from the generated file. `Timestamp` comes from Google's well-known types package (installed with the `protobuf` pip package).

---

## Constructing a message

Pass field values as keyword arguments — just like a dataclass:

```python
import time, uuid

ts = Timestamp()
ts.FromMilliseconds(int(time.time() * 1000))

order = Order(
    id=str(uuid.uuid4()),
    customer_id="customer-42",
    item="Mechanical Keyboard",
    amount=149.99,
    status=OrderStatus.ORDER_STATUS_CREATED,
    created_at=ts,
)
```

Fields you don't set get their **zero value**: `""` for strings, `0` for numbers, the first enum value for enums (which is why `ORDER_STATUS_UNSPECIFIED = 0` exists).

---

## Serializing (producer side)

```python
raw_bytes = order.SerializeToString()
# → b'\n$1e4b6578...' (25 bytes, not human-readable)

producer.produce(
    topic=TOPIC,
    key=order.id.encode(),
    value=raw_bytes,
)
```

`SerializeToString()` returns a `bytes` object. You can check its size:

```python
import json
json_size = len(json.dumps({"id": order.id, "item": order.item, ...}).encode())
proto_size = len(raw_bytes)
print(f"JSON: {json_size} bytes, Protobuf: {proto_size} bytes")
# JSON: 93 bytes, Protobuf: 27 bytes
```

---

## Deserializing (consumer side)

```python
raw_bytes = msg.value()   # bytes off the Kafka message

order = Order()
order.ParseFromString(raw_bytes)

# Access fields like normal attributes
print(order.id)                          # "abc-123"
print(order.customer_id)                 # "customer-42"
print(order.item)                        # "Mechanical Keyboard"
print(order.amount)                      # 149.99
print(order.created_at.ToDatetime())     # 2024-01-15 10:30:00
```

`ParseFromString()` mutates the `order` object in place and returns the number of bytes consumed. If the bytes are malformed or the wrong type, it raises an exception — which is why the consumer wraps it in a try/except.

---

## Working with enums

```python
# Setting (use the named constant)
status = OrderStatus.ORDER_STATUS_PAID

# The underlying value is an integer
print(int(status))   # 2

# Getting the name from an integer value
STATUS_NAMES = {v: k for k, v in OrderStatus.items()}
# {0: 'ORDER_STATUS_UNSPECIFIED', 1: 'ORDER_STATUS_CREATED', ...}

STATUS_NAMES.get(order.status, "UNKNOWN")
# "ORDER_STATUS_CREATED"
```

---

## Comparing to JSON round-trip

```python
import json

# JSON
json_bytes = json.dumps({
    "id": order.id,
    "customer_id": order.customer_id,
    "item": order.item,
    "amount": order.amount,
}).encode()
decoded = json.loads(json_bytes)

# Protobuf
proto_bytes = order.SerializeToString()
decoded_order = Order()
decoded_order.ParseFromString(proto_bytes)

# Result: same data, ~3x smaller bytes with Protobuf
```

---

## What to do if ParseFromString fails

```python
order = Order()
try:
    order.ParseFromString(raw)
except Exception as e:
    print(f"[ERROR] Failed to deserialize: {e}")
    # Possible causes:
    # - wrong topic (bytes are a different message type)
    # - producer used JSON instead of Protobuf
    # - corrupted message
    # - schema mismatch (field type changed)
    continue
```

Common causes of deserialization failure: consuming from a topic where the producer is sending plain JSON or a different Protobuf message type. Always verify the producer and consumer agree on the schema.

---

> ← [Previous: Compile Workflow](./compile-workflow.md) | [Index](../README.md) | [Next: Redpanda →](../infra/redpanda.md)
