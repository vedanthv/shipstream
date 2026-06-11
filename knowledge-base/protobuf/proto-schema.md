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

---

## Schema evolution in practice

Each scenario below shows exactly what happens to the bytes and to your running system.

---

### ✅ Safe: Adding a new field

You need to capture `shipping_address`. Add it at the end with a new field number.

```protobuf
// BEFORE
message Order {
  string id          = 1;
  string customer_id = 2;
  string item        = 3;
  double amount      = 4;
  OrderStatus status = 5;
}

// AFTER — add field 7 (skip 6 which is created_at)
message Order {
  string id               = 1;
  string customer_id      = 2;
  string item             = 3;
  double amount           = 4;
  OrderStatus status      = 5;
  google.protobuf.Timestamp created_at = 6;
  string shipping_address = 7;   // ← new
}
```

**What happens on the wire:**

```
Old producer bytes (no field 7):
  0a 03 61 62 63   ← field 1: id
  12 04 63 2d 34 32  ← field 2: customer_id
  ...
  28 01            ← field 5: status
  (field 7 absent — not written)

New producer bytes (with field 7):
  0a 03 61 62 63   ← field 1: id
  ...
  28 01            ← field 5: status
  3a 0f 31 32 20 4d 61 69 6e 20 53 74 2e  ← field 7: "12 Main St"
  (tag 0x3a = field 7, wire type 2)
```

**Compatibility:**

```
Old consumer + new producer bytes
  → consumer reads tag 0x3a (field 7, unknown)
  → wire type is 2 (length-delimited) → reads length, skips N bytes
  → continues to next tag — no error, field 7 silently ignored ✅

New consumer + old producer bytes
  → field 7 is simply absent
  → order.shipping_address == ""  (zero value for string) ✅
```

**Deploy order:** new producer first or new consumer first — both are safe. No coordination needed.

---

### ✅ Safe: Renaming a field

You want to rename `customer_id` to `buyer_id` to match new domain language.

```protobuf
// BEFORE
string customer_id = 2;

// AFTER
string buyer_id = 2;   // same number, different name
```

**What happens on the wire:** Absolutely nothing changes. Field names do not exist on the wire. The bytes for field 2 are identical before and after. The only change is in the generated Python class:

```python
# Before
order.customer_id  # works

# After regenerating _pb2.py
order.buyer_id     # works
order.customer_id  # AttributeError — name is gone in generated code
```

**Risk:** Any Python code still using `order.customer_id` breaks at runtime — but only in your codebase, not in the wire format. Old Kafka messages are unaffected.

---

### ✅ Safe: Adding an enum value

You need to track refunded orders.

```protobuf
// BEFORE
enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_CREATED     = 1;
  ORDER_STATUS_PAID        = 2;
  ORDER_STATUS_FULFILLED   = 3;
  ORDER_STATUS_CANCELLED   = 4;
}

// AFTER
enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_CREATED     = 1;
  ORDER_STATUS_PAID        = 2;
  ORDER_STATUS_FULFILLED   = 3;
  ORDER_STATUS_CANCELLED   = 4;
  ORDER_STATUS_REFUNDED    = 5;   // ← new
}
```

**What happens on the wire:** `ORDER_STATUS_REFUNDED` is just the varint `0x05`. Old consumers that receive value `5` will see an unknown integer. In Python they'll store the raw integer `5` and your `STATUS_NAMES.get(order.status, "UNKNOWN")` will return `"UNKNOWN"` — harmless.

**Risk:** If old consumers have `if order.status == 4: do_cancel()` style logic, `5` simply falls through — correct behavior. But if they have `else: raise ValueError("unexpected status")`, they'll blow up. Defensive coding matters.

---

### ❌ Breaking: Changing a field number

Someone "cleans up" the schema and renumbers `item` from `3` to `8`.

```protobuf
// BEFORE
string item = 3;

// AFTER (BREAKING)
string item = 8;   // ← changed field number
```

**What happens on the wire:**

```
Old producer encodes item as field 3:
  1a 08 4b 65 79 62 6f 61 72 64
  ^^                              ← tag 0x1a = field 3, wire type 2
  
New consumer expects item at field 8, tag 0x42:
  → reads tag 0x1a → field 3, unknown
  → skips 8 bytes ("Keyboard")
  → order.item == ""   ← silently empty, no error
  
Old consumer + new producer bytes:
  → reads tag 0x42 → field 8, unknown
  → skips it
  → order.item == ""   ← also silently empty
```

**What you actually see:** No crash. No error. Just `order.item` being empty string in both consumers. This silently corrupts data in production and is very hard to notice until someone checks the Redpanda Console and wonders why item is blank in every order.

---

### ❌ Breaking: Changing a field type across wire types

You decide `item` should be an integer SKU code instead of a string name.

```protobuf
// BEFORE
string item = 3;   // wire type 2 (length-delimited)

// AFTER (BREAKING)
int32 item = 3;    // wire type 0 (varint)
```

**What happens on the wire:**

```
Old producer bytes for field 3 (string "Keyboard", wire type 2):
  1a 08 4b 65 79 62 6f 61 72 64
  ^^
  tag 0x1a = field 3, wire type 2

New consumer expects field 3 to be wire type 0 (varint):
  → reads tag 0x1a → field 3, wire type 2
  → expected wire type 0
  → DecodeError: Tag had wrong wire type

Python output:
  google.protobuf.message.DecodeError:
    Error parsing message: Tag had wrong wire type: 2 not in (0,)
```

This is the **best-case** breaking change — at least it fails loudly with an exception. Your consumer's `except Exception as e` block catches it and logs it. Every single message from the old producer will fail to deserialize.

---

### ❌ Breaking: int32 → sint32 (silent corruption)

You learn that `sint32` is more efficient for negative numbers and switch `status` from `int32` to `sint32`.

```protobuf
// BEFORE
int32 discount_pct = 7;   // stores values like -10, -25, 0, 5

// AFTER (BREAKING)
sint32 discount_pct = 7;  // same wire type 0, but zigzag encoding
```

Both `int32` and `sint32` use **wire type 0 (varint)** — so the decoder never raises an error. But they encode values differently:

```
Encoding value 100:
  int32:  varint(100)         = 0x64        (1 byte)
  sint32: varint(zigzag(100)) = varint(200) = 0xc8 0x01 (2 bytes)

Decoding 0x64 as the wrong type:
  Old producer sent int32(100) → 0x64
  New consumer reads 0x64 as sint32 → zigzag_decode(100) = 50
  → 100 became 50. No error. ✅ looks fine. ❌ completely wrong.

Encoding value -1:
  int32:  varint(-1)         = ff ff ff ff ff ff ff ff ff 01 (10 bytes)
  sint32: varint(zigzag(-1)) = varint(1) = 0x01              (1 byte)

Decoding 0x01 (sint32(-1)) as int32:
  → int32 value = 1
  → -1 became 1. No error. Silent sign flip.
```

**Why this is the most dangerous kind of break:** Same wire type, so no `DecodeError`. Values are wrong by a consistent formula, so the bug may not surface immediately. Your discount calculations silently return half the intended value. Users get charged the wrong amount.

---

### ❌ Breaking: Reusing a deleted field number

`item` (field 3) is removed. Six months later someone adds `warehouse_code` and, not knowing about the reserved field, reuses number 3.

```protobuf
// Original
string item = 3;

// After removal (should have been reserved!)
// field 3 gone, nothing reserved

// Six months later
string warehouse_code = 3;   // ← reused number 3
```

**What happens to old messages still in Kafka:**

Kafka retains messages for 7 days by default. Any consumer that resets its offset and replays history, or any new service that reads from the beginning, will encounter old messages where field 3 contains an item name like `"Keyboard"`.

```
Old message bytes for field 3 = "Keyboard":
  1a 08 4b 65 79 62 6f 61 72 64

New consumer expects field 3 = warehouse_code:
  → reads field 3 as string (same wire type 2 — no error)
  → warehouse_code = "Keyboard"
  → order routed to warehouse "Keyboard" which doesn't exist
```

No error. A string is a string. But `warehouse_code = "Keyboard"` is garbage data that will cause downstream failures in your warehouse routing system.

**The fix — always reserve deleted field numbers:**

```protobuf
message Order {
  reserved 3;        // was "item" — do not reuse
  reserved "item";   // prevent a future field named "item" from being added
  
  string id               = 1;
  string customer_id      = 2;
  double amount           = 4;
  OrderStatus status      = 5;
  google.protobuf.Timestamp created_at = 6;
  string warehouse_code   = 8;   // ← use a fresh number
}
```

`protoc` will now refuse to compile if anyone tries to add `= 3` or name a field `item`:

```
error: Field number 3 has been reserved in "Order".
```

---

### The deployment order matters for safe changes too

Even for safe changes, deploy order affects what users experience during the rollout window:

```
Scenario: Adding field 7 (shipping_address)

Option A — Deploy new consumer first, then new producer
  Window: consumers expect field 7, producers don't send it yet
  → order.shipping_address == "" during rollout
  → safe: empty string is the zero value, consumers handle it

Option B — Deploy new producer first, then new consumer  
  Window: producers send field 7, old consumers don't know it
  → old consumers skip field 7 silently
  → safe: unknown fields are ignored

Both options are safe for additive changes.
For breaking changes, no deployment order helps — you need a v2 package.
```

---

> ← [Previous: What is Protobuf?](./what-is-protobuf.md) | [Index](../README.md) | [Next: The Schema Disaster (story) →](./schema-story.md)
