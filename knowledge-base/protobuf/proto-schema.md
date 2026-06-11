# Chapter 9 — Proto Schema

> **You are here:** [Index](../README.md) → [What is Protobuf?](./what-is-protobuf.md) → **Proto Schema**

---

## File location

```
proto/order/v1/order.proto
```

The directory path mirrors the package (`order.v1`) and encodes the versioning strategy — breaking changes go in `order/v2/`, not in a new field on the existing schema.

---

## The full ShipStream schema

```protobuf
syntax = "proto3";

package order.v1;

import "google/protobuf/timestamp.proto";

message Order {
  string                    id          = 1;
  string                    customer_id = 2;
  string                    item        = 3;
  double                    amount      = 4;
  OrderStatus               status      = 5;
  google.protobuf.Timestamp created_at  = 6;
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

The proto language version. Proto3 differences from proto2:
- All fields are optional (no `required` keyword)
- Default values are zero-values (`""`, `0`, `false`, first enum value)
- Unset fields that equal zero-value are **not written to the wire** — this saves space

### `package order.v1`

Prevents naming collisions when importing schemas across services. In Redpanda Console config you reference the full type as `order.v1.Order`.

### `import`

Pulls in external `.proto` files. The `-I proto` flag in the compile command tells `protoc` where to look for them.

---

## Field types and their wire types

Every proto type maps to one of four wire types. Wire types determine how many bytes the value takes on the wire:

| Wire Type | ID | Used for proto types | Size |
|-----------|-----|---------------------|------|
| **Varint** | 0 | `int32`, `int64`, `uint32`, `uint64`, `sint32`, `sint64`, `bool`, `enum` | Variable (1–10 bytes) |
| **64-bit** | 1 | `fixed64`, `sfixed64`, `double` | Always 8 bytes |
| **Length-delimited** | 2 | `string`, `bytes`, `message`, repeated fields | Variable (varint length + N bytes) |
| **32-bit** | 5 | `fixed32`, `sfixed32`, `float` | Always 4 bytes |

Wire types 3 and 4 (groups) are deprecated and not used in proto3.

### ShipStream field → wire type mapping

```
field 1: string id          → wire type 2 (length-delimited)
field 2: string customer_id → wire type 2 (length-delimited)
field 3: string item        → wire type 2 (length-delimited)
field 4: double amount      → wire type 1 (64-bit)
field 5: enum   status      → wire type 0 (varint)
field 6: message created_at → wire type 2 (length-delimited)
```

---

## The tag: field number + wire type in one byte

Every field on the wire is preceded by a **tag byte** that encodes both the field number and wire type:

```
tag = (field_number << 3) | wire_type
```

```
Field 1 (string):  (1 << 3) | 2 = 0b00001 010 = 0x0a = 10
Field 2 (string):  (2 << 3) | 2 = 0b00010 010 = 0x12 = 18
Field 3 (string):  (3 << 3) | 2 = 0b00011 010 = 0x1a = 26
Field 4 (double):  (4 << 3) | 1 = 0b00100 001 = 0x21 = 33
Field 5 (enum):    (5 << 3) | 0 = 0b00101 000 = 0x28 = 40
```

```
Tag byte anatomy (0x0a for field 1):

  Bit:  7  6  5  4  3  2  1  0
        0  0  0  0  1  0  1  0
        └──────────┘  └──────┘
        field number  wire type
             1            2
```

The decoder reads the tag, extracts the wire type to know *how many bytes* to read next, and extracts the field number to know *which field* it is.

---

## Scalar field types — full table

| Proto type | Wire type | Bytes | Python type | Notes |
|-----------|-----------|-------|-------------|-------|
| `double` | 1 (64-bit) | 8 | `float` | IEEE 754 double precision |
| `float` | 5 (32-bit) | 4 | `float` | IEEE 754 single precision |
| `int32` | 0 (varint) | 1–5 | `int` | Inefficient for negative numbers (10 bytes) |
| `int64` | 0 (varint) | 1–10 | `int` | Inefficient for negative numbers |
| `uint32` | 0 (varint) | 1–5 | `int` | Unsigned, never negative |
| `uint64` | 0 (varint) | 1–10 | `int` | Unsigned, never negative |
| `sint32` | 0 (varint) | 1–5 | `int` | Signed, uses zigzag — efficient for negative |
| `sint64` | 0 (varint) | 1–10 | `int` | Signed, uses zigzag — efficient for negative |
| `bool` | 0 (varint) | 1 | `bool` | Encoded as 0 or 1 |
| `string` | 2 (len-del) | variable | `str` | UTF-8 encoded |
| `bytes` | 2 (len-del) | variable | `bytes` | Raw binary |

---

## Enums

```protobuf
enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;   ← mandatory zero value in proto3
  ORDER_STATUS_CREATED     = 1;
  ORDER_STATUS_PAID        = 2;
  ORDER_STATUS_FULFILLED   = 3;
  ORDER_STATUS_CANCELLED   = 4;
}
```

Enums encode as varint (wire type 0). `ORDER_STATUS_CREATED = 1` transmits as a single byte `0x01` on the wire. The string `"ORDER_STATUS_CREATED"` never travels anywhere.

The zero value (`ORDER_STATUS_UNSPECIFIED = 0`) is required because in proto3, unset fields default to zero — and zero must be a valid, defined enum value.

---

## Nested messages

`google.protobuf.Timestamp` is itself a message:

```protobuf
// from google/protobuf/timestamp.proto
message Timestamp {
  int64 seconds = 1;
  int32 nanos   = 2;
}
```

When `created_at` is encoded, it's treated as wire type 2 (length-delimited): a varint giving the byte length of the nested message, followed by the nested message bytes. A message within a message.

---

## Schema evolution rules

| Change | Safe? | Why |
|--------|-------|-----|
| Add a new field (new number) | ✅ | Old readers skip unknown field numbers |
| Rename a field | ✅ | Names never go on the wire |
| Add a new enum value | ✅ | Old readers see an unknown integer, ignore it |
| Remove a field | ✅ (reserve the number) | Old writers still send it; new readers skip it |
| Change `string` → `bytes` | ✅ | Same wire type (2) — compatible |
| Change `int32` → `int64` | ✅ | Same wire type (0) — compatible |
| Change `string` → `int32` | ❌ | Wire type 2 vs 0 — parser error |
| Change a field number | ❌ | Parser reads the wrong field |
| Reuse a deleted field number | ❌ | Old data misinterpreted |
| Change `int32` → `sint32` | ❌ | Same wire type but different encoding (zigzag) — silent corruption |

When removing a field, mark the number as reserved to prevent accidental reuse:

```protobuf
message Order {
  reserved 3;                    // number 3 was "item", now removed
  reserved "item";               // prevent future field named "item"
  string id          = 1;
  string customer_id = 2;
  double amount      = 4;
  OrderStatus status = 5;
}
```

---

> ← [Previous: What is Protobuf?](./what-is-protobuf.md) | [Index](../README.md) | [Next: Binary Encoding →](./binary-encoding.md)
