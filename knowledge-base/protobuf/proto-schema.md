# Chapter 9 — Proto Schema

> **You are here:** [Index](../README.md) → [What is Protobuf?](./what-is-protobuf.md) → **Proto Schema**

---

## File location

```
proto/order/v1/order.proto
```

The directory structure (`order/v1/`) mirrors the package name (`order.v1`) and signals versioning intent — breaking changes would go in `order/v2/`.

---

## The full schema

```protobuf
syntax = "proto3";

package order.v1;

import "google/protobuf/timestamp.proto";

message Order {
  string id          = 1;
  string customer_id = 2;
  string item        = 3;
  double amount      = 4;
  OrderStatus status = 5;
  google.protobuf.Timestamp created_at = 6;
}

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_CREATED     = 1;
  ORDER_STATUS_PAID        = 2;
  ORDER_STATUS_FULFILLED   = 3;
  ORDER_STATUS_CANCELLED   = 4;
}
```

---

## Syntax breakdown

### `syntax = "proto3"`
The version of the Protobuf language. proto3 is the current standard — all fields are optional by default, removed the `required` keyword, simplified defaults.

### `package order.v1`
Namespaces the types to avoid collisions. The generated Python class will live under this package. In the Redpanda Console config you reference it as `order.v1.Order`.

### `import`
Pulls in well-known types provided by Google. `timestamp.proto` gives you `google.protobuf.Timestamp` — a standard representation of a point in time (seconds + nanoseconds since Unix epoch).

### `message`
Defines a structured type — like a Python dataclass or a database row schema.

```protobuf
message Order {
  string id = 1;    // type  field_name = field_number;
}
```

### Field types

| Proto type | Python type | Notes |
|-----------|-------------|-------|
| `string` | `str` | UTF-8 text |
| `double` | `float` | 64-bit float |
| `int32` | `int` | 32-bit signed integer |
| `int64` | `int` | 64-bit signed integer |
| `bool` | `bool` | True/False |
| `bytes` | `bytes` | Raw binary |
| `message` | class instance | Nested message |
| `enum` | `int` (with names) | Named constants |

### `enum`
A fixed set of named integer values. **Always start with a zero value** — in proto3 the zero value is the default for unset enum fields.

```protobuf
enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;   // ← required: the zero/default value
  ORDER_STATUS_CREATED     = 1;
  ORDER_STATUS_PAID        = 2;
  ORDER_STATUS_FULFILLED   = 3;
  ORDER_STATUS_CANCELLED   = 4;
}
```

---

## The well-known Timestamp type

`google.protobuf.Timestamp` is not a string — it's a message with two fields: `seconds` and `nanos` since Unix epoch. This avoids timezone ambiguity and string parsing.

```python
from google.protobuf.timestamp_pb2 import Timestamp

# Creating
ts = Timestamp()
ts.FromMilliseconds(int(time.time() * 1000))

# Reading
order.created_at.ToDatetime()     # → Python datetime (UTC)
order.created_at.seconds          # → integer seconds since epoch
```

---

## Schema evolution rules

These rules let you change the schema without breaking existing producers and consumers:

| Operation | Safe? | Why |
|-----------|-------|-----|
| Add a new field with a new number | ✅ | Old consumers ignore unknown fields |
| Rename a field | ✅ | Names don't travel on the wire |
| Add a new enum value | ✅ | Old consumers see it as an unknown int |
| Remove a field | ✅ (with care) | Old producers still send it; new consumers ignore it |
| Change a field's type | ❌ | Binary encoding differs — data corruption |
| Change a field number | ❌ | New code reads wrong field — silent corruption |
| Reuse a deleted field number | ❌ | Old data with that number gets misinterpreted |

---

## What a real schema evolution looks like

Suppose ShipStream needs to add a `shipping_address` field in v1:

```protobuf
message Order {
  string id          = 1;
  string customer_id = 2;
  string item        = 3;
  double amount      = 4;
  OrderStatus status = 5;
  google.protobuf.Timestamp created_at = 6;
  string shipping_address = 7;   // ← add at the end with a new number
}
```

Old consumers that haven't deployed yet will receive messages with field 7 and silently ignore it. New consumers will read it. Zero downtime schema change.

---

> ← [Previous: What is Protobuf?](./what-is-protobuf.md) | [Index](../README.md) | [Next: Compile Workflow →](./compile-workflow.md)
