# Chapter 12 — Python Usage

> **You are here:** [Index](../README.md) → [Compile Workflow](./compile-workflow.md) → **Python Usage**

---

## Import

```python
from generated.order.v1.order_pb2 import Order, OrderStatus
from google.protobuf.timestamp_pb2 import Timestamp
```

`Order` and `OrderStatus` come from the generated file. `Timestamp` is a well-known type bundled with the `protobuf` pip package — no generation needed.

---

## Constructing a message

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

Fields not set default to their **zero value** and are omitted from the wire:

| Field type | Zero value | Wire behavior |
|-----------|-----------|---------------|
| `string` | `""` | Not written |
| `double` | `0.0` | Not written |
| `int32` | `0` | Not written |
| `bool` | `False` | Not written |
| `enum` | First value (`= 0`) | Not written |
| `message` | Not set | Not written |

---

## Serializing

```python
raw_bytes = order.SerializeToString()
```

Returns a `bytes` object. This is what goes into Kafka.

```python
# Size comparison
import json
json_bytes = json.dumps({
    "id": order.id,
    "customer_id": order.customer_id,
    "item": order.item,
    "amount": order.amount,
    "status": "ORDER_STATUS_CREATED",
}).encode()

print(f"JSON:     {len(json_bytes)} bytes")
print(f"Protobuf: {len(raw_bytes)} bytes")
print(f"Ratio:    {len(json_bytes)/len(raw_bytes):.1f}x smaller")
# JSON:     93 bytes
# Protobuf: 32 bytes
# Ratio:    2.9x smaller
```

---

## Deserializing

```python
# Consumer side — raw bytes from Kafka
raw_bytes = msg.value()

order = Order()
order.ParseFromString(raw_bytes)

# Access fields as normal attributes
print(order.id)                          # "abc-123"
print(order.customer_id)                 # "customer-42"
print(order.item)                        # "Mechanical Keyboard"
print(order.amount)                      # 149.99
print(order.status)                      # 1 (integer)
print(order.created_at.ToDatetime())     # datetime(2024, 1, 15, 10, 30, 0)
```

`ParseFromString()` mutates the object in place. It returns the number of bytes consumed — useful if you're parsing from a larger buffer.

---

## Working with enums

```python
# Constructing with named constant
order = Order(status=OrderStatus.ORDER_STATUS_PAID)

# The value is an integer under the hood
print(order.status)          # 2
print(type(order.status))    # <class 'int'>

# Get the name from the integer
STATUS_NAMES = {v: k for k, v in OrderStatus.items()}
# {0: 'ORDER_STATUS_UNSPECIFIED', 1: 'ORDER_STATUS_CREATED',
#  2: 'ORDER_STATUS_PAID', 3: 'ORDER_STATUS_FULFILLED', 4: 'ORDER_STATUS_CANCELLED'}

print(STATUS_NAMES[order.status])        # "ORDER_STATUS_PAID"
print(STATUS_NAMES.get(order.status, "UNKNOWN"))  # safe version

# Compare
if order.status == OrderStatus.ORDER_STATUS_PAID:
    print("Order has been paid")
```

---

## Timestamp operations

```python
from google.protobuf.timestamp_pb2 import Timestamp
import time

# Create from current time
ts = Timestamp()
ts.GetCurrentTime()                        # set to now

# Create from milliseconds (Kafka-style epoch ms)
ts = Timestamp()
ts.FromMilliseconds(int(time.time() * 1000))

# Create from a Python datetime
from datetime import datetime, timezone
dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
ts = Timestamp()
ts.FromDatetime(dt)

# Read back
print(ts.seconds)                         # Unix seconds
print(ts.nanos)                           # nanosecond component
print(ts.ToDatetime())                    # datetime object (UTC)
print(ts.ToMilliseconds())                # epoch milliseconds
```

---

## Introspection — useful debugging methods

```python
order = Order(id="abc", amount=149.99, status=OrderStatus.ORDER_STATUS_CREATED)

# Which fields are actually set (non-zero)?
for field_descriptor, value in order.ListFields():
    print(f"  {field_descriptor.name} = {value}")
# id = abc
# amount = 149.99
# status = 1

# Byte size without serializing
print(order.ByteSize())      # 17

# Check if a specific field is set
print(order.HasField("created_at"))   # False (not set)

# Human-readable representation (for debugging)
print(str(order))
# id: "abc"
# amount: 149.99
# status: ORDER_STATUS_CREATED

# Convert to dict
from google.protobuf.json_format import MessageToDict, MessageToJson

d = MessageToDict(order)
# {'id': 'abc', 'amount': 149.99, 'status': 'ORDER_STATUS_CREATED'}

j = MessageToJson(order)
# '{\n  "id": "abc",\n  "amount": 149.99,\n  "status": "ORDER_STATUS_CREATED"\n}'
```

`MessageToDict` and `MessageToJson` are useful for logging and debugging — they give you a readable representation without losing the type safety of working with the proto object itself.

---

## Inspecting the raw bytes

```python
raw = order.SerializeToString()

print(f"Bytes: {len(raw)}")
print(f"Hex:   {' '.join(f'{b:02x}' for b in raw)}")
# Hex: 0a 03 61 62 63 21 48 e1 7a 14 ae bf 62 40 28 01

# Manually decode the first tag
first_byte = raw[0]
print(f"Tag 0x{first_byte:02x}: field={first_byte >> 3}, wire_type={first_byte & 0x7}")
# Tag 0x0a: field=1, wire_type=2
```

See [Chapter 10: Binary Encoding](./binary-encoding.md) for the full byte-by-byte breakdown.

---

## Error handling

```python
order = Order()
try:
    order.ParseFromString(raw_bytes)
except Exception as e:
    print(f"[ERROR] Failed to deserialize: {e}")
    # Common causes:
    # - bytes are JSON, not Protobuf
    # - bytes are a different Protobuf message type
    # - field type was changed in schema (e.g., string → int)
    # - bytes are corrupted or truncated
    continue
```

Protobuf will **not** raise an error if you parse a completely wrong message type — it silently reads whatever field numbers it finds. Field 1 in a `Payment` message might be parsed as field 1 in `Order`. You'll get wrong data with no error. Always ensure producers and consumers agree on the message type for a topic.

---

> ← [Previous: Compile Workflow](./compile-workflow.md) | [Index](../README.md) | [Next: Redpanda →](../infra/redpanda.md)
