# S1 — Simulating Schema Compatibility: Field Deletion

This guide walks you through a live BACKWARD compatibility demonstration. You will:

1. Produce 100 messages using the v1 producer (with `region`)
2. Consume them with a v1 consumer to confirm the baseline works
3. Set up a v2 proto schema with `region` (field 7) removed
4. Register v2 under BACKWARD mode
5. Run a v2 consumer against the same v1 messages
6. Read the logs side by side to confirm the v2 consumer silently drops the `region` bytes

---

## Prerequisites

Infrastructure must be running and a clean topic in place.

```bash
docker compose up -d

# Create topic (or recreate if it already exists)
docker exec redpanda rpk topic delete order.created
docker exec redpanda rpk topic create order.created --partitions 3
```

---

## Step 1 — Produce 100 v1 messages

```bash
python3 services/producer.py
```

On the first message, `ProtobufSerializer` checks whether the schema for `order.created-value` exists in the Schema Registry. It doesn't, so it registers the v1 schema (with `region` at field 7) and receives back a schema ID (e.g. `1`). Every subsequent message is prefixed with a 5-byte wire header — a magic byte (`0x00`) followed by the 4-byte schema ID — then the Protobuf-encoded Order bytes.

Confirm the schema is registered:

```bash
curl -s http://localhost:18081/subjects/order.created-value/versions/latest | python3 -m json.tool
```

You should see `string region = 7` in the schema body.

---

## Step 2 — Consume with v1 consumer (baseline)

Before introducing any schema change, confirm everything works end to end with the v1 schema.

```bash
CONSUMER_ID=1 python3 services/consumer.py
```

You should see messages with `region` populated on every line:

```
[Consumer-1] partition=2 offset=0 | id=85ca74d1... customer=customer-44 item='Mechanical Keyboard' amount=$488.43 status=ORDER_STATUS_CREATED region=eu-west
[Consumer-1] partition=2 offset=1 | id=a7508c7f... customer=customer-6  item='Standing Desk'       amount=$231.47 status=ORDER_STATUS_CREATED region=us-west
```

This is the happy path — producer and consumer share the same schema, every field deserializes correctly. Once you have seen a few messages, stop the consumer with `Ctrl+C`.

Check the group offset:

```bash
docker exec redpanda rpk group describe shipstream-consumer-group
```

You will see `CURRENT-OFFSET` advanced on whichever partitions were read, and a non-zero `LAG` on the rest. The committed offset is stored on the broker — if this consumer dies and a new one starts with the same `group.id`, it picks up from exactly this point.

---

## Step 3 — Create the v2 proto

Create `proto/order/v2/order.proto` — identical to v1 except `region` is gone and its field number is reserved so it can never be accidentally reused:

```protobuf
syntax = "proto3";

package order.v2;

import "google/protobuf/timestamp.proto";

message Order {
  string id          = 1;
  string customer_id = 2;
  string item        = 3;
  double amount      = 4;
  OrderStatus status = 5;
  google.protobuf.Timestamp created_at = 6;
  reserved 7;
  reserved "region";
}

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_CREATED     = 1;
  ORDER_STATUS_PAID        = 2;
  ORDER_STATUS_FULFILLED   = 3;
  ORDER_STATUS_CANCELLED   = 4;
}
```

**Why `reserved`?** If a future developer adds `string warehouse_name = 7`, old messages that have `region` bytes at field 7 would silently decode as `warehouse_name`. `reserved 7` makes `protoc` reject the schema at compile time if anyone tries to reuse that number. It is a compile-time guard against silent data corruption.

---

## Step 4 — Compile the v2 proto

```bash
protoc \
  --proto_path=proto \
  --python_out=generated_proto_objects \
  proto/order/v2/order.proto

touch generated_proto_objects/order/v2/__init__.py
```

Confirm the generated file is in place:

```bash
ls generated_proto_objects/order/v2/
# order_pb2.py
```

---

## Step 5 — Register the v2 schema

Check and set the compatibility mode to BACKWARD, then register the v2 schema:

```bash
# Check current mode
curl -s http://localhost:18081/config/order.created-value | python3 -m json.tool

# Set to BACKWARD (new consumer must be able to read old messages)
curl -s -X PUT http://localhost:18081/config/order.created-value \
  -H "Content-Type: application/json" \
  -d '{"compatibility": "BACKWARD"}' | python3 -m json.tool

# Register v2 (region deleted)
curl -s -X POST http://localhost:18081/subjects/order.created-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schemaType": "PROTOBUF",
    "schema": "syntax = \"proto3\";\npackage order.v1;\nimport \"google/protobuf/timestamp.proto\";\nmessage Order {\n  string id = 1;\n  string customer_id = 2;\n  string item = 3;\n  double amount = 4;\n  OrderStatus status = 5;\n  google.protobuf.Timestamp created_at = 6;\n}\nenum OrderStatus {\n  ORDER_STATUS_UNSPECIFIED = 0;\n  ORDER_STATUS_CREATED = 1;\n  ORDER_STATUS_PAID = 2;\n  ORDER_STATUS_FULFILLED = 3;\n  ORDER_STATUS_CANCELLED = 4;\n}"
  }' | python3 -m json.tool
```

The registry returns a new schema ID — it accepted the registration because deleting a field satisfies BACKWARD: old messages have `region` bytes at field 7, and a new consumer without field 7 in its schema will silently ignore those bytes.

---

## Step 6 — Run the simulation

```bash
python3 simulations/sim_backward_compat.py
```

This runs two consumers against the same topic using **separate consumer groups**.

### Why two separate groups?

Each group has its own independent offset pointer on the broker. If both consumers shared the same `group.id`, the broker would split the partitions between them — one consumer gets partition 0, the other gets partitions 1 and 2. Each partition would only be read by one of them, so you'd never get a clean comparison.

```
Same group — partitions split between members (wrong for this simulation)

v1 consumer → partition 0 only
v2 consumer → partition 1 and 2 only

No overlap — you can't compare their output against the same messages
```

With two separate groups, each consumer is the only member in its group, so the broker assigns it all 3 partitions and it reads every message on the topic:

```
Different groups — each reads the full topic independently (correct)

sim-s1-v1-group:
  v1 consumer → partition 0, 1, 2 (all messages)

sim-s1-v2-group:
  v2 consumer → partition 0, 1, 2 (all messages)

Both read every message → logs are directly comparable
```

This mirrors how real systems work — multiple independent services consuming the same topic each have their own `group.id` so every service gets a full copy of every event.

| Consumer | Group | Schema |
|----------|-------|--------|
| v1 | `sim-s1-v1-group` | Order v1 (has region) |
| v2 | `sim-s1-v2-group` | Order v2 (no region) |

Logs are written to:

```
logs/simulations/v1-consumer.log
logs/simulations/v2-consumer.log
```

---

## Step 7 — Read the logs

Open both log files side by side.

**v1-consumer.log** — region is populated on every row:

```
partition | offset | customer        | item          | status                  | region
p=2       | 0      | customer-39     | Mouse Pad     | ORDER_STATUS_FULFILLED  | us-west
p=2       | 1      | customer-35     | Mouse Pad     | ORDER_STATUS_PAID       | us-east
```

**v2-consumer.log** — region column shows the field does not exist:

```
partition | offset | customer        | item          | status                  | region
p=0       | 0      | customer-45     | Standing Desk | ORDER_STATUS_FULFILLED  | (field does not exist in v2 schema)
p=0       | 1      | customer-20     | Mechanical... | ORDER_STATUS_PAID       | (field does not exist in v2 schema)
```

Both consumers read the same topic. The v1 producer encoded `region` at field 7 in every message. The v2 consumer encountered those bytes on the wire, found no field 7 in its descriptor, and discarded them silently. No crash. No corruption. That is BACKWARD compatibility.

---

## Step 8 — Inspect the consumer group state

```bash
docker exec redpanda rpk group describe sim-s1-v1-group
docker exec redpanda rpk group describe sim-s1-v2-group
```

You will see output like this:

```
STATE    Empty
MEMBERS  0

TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   MEMBER-ID
order.created  0          -               36              36
order.created  1          -               32              32
order.created  2          20              32              12
```

### Why is MEMBER-ID blank?

The simulation script exited and called `consumer.close()`. A consumer that has left the group has no member ID — there is nothing running to show. MEMBER-ID is only populated while a consumer is **actively running** and holding the assignment. The committed offsets survive (see `CURRENT-OFFSET`), but the member entry is gone.

To see a live member ID, check the group *while a consumer is still running*:

```bash
# In one terminal — start a consumer and leave it running
CONSUMER_ID=1 python3 services/consumer.py

# In another terminal — describe the group immediately
docker exec redpanda rpk group describe shipstream-consumer-group
```

You will see `STATE: Stable`, `MEMBERS: 1`, and a MEMBER-ID filled in.

### Why is there still lag?

The script read 20 messages and stopped. The topic has ~100 messages spread across 3 partitions. Kafka returns messages in batches per partition, and the script hit its limit of 20 before working through all three. The result is one partition partially read, two completely untouched:

```
Partition 0 — 36 messages   [■■■■■■■■■■■■■■■■■■■■░░░░░░░░░░░░░░░░]  read 20 → lag 16
Partition 1 — 32 messages   [░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░]      not reached → lag 32
Partition 2 — 32 messages   [■■■■■■■■■■■■■■■■■■■■░░░░░░░░░░░░]      read 20 → lag 12
```

This is expected for a simulation. A real consumer would keep running until all partitions are drained and total lag reaches zero.

### Why did each consumer only read from one partition?

Each consumer was the **only member in its group**, so the broker assigned all 3 partitions to it. But Kafka delivers messages in batches per partition. The script read 20 messages and exited before Kafka rotated to the next partition's batch.

This also surfaces a common misconception about the partition rule:

---

## The partition assignment rule — clarified

The rule is often stated as "one consumer per partition" but the direction matters:

> **A partition can be held by at most one consumer member at a time. A single consumer member can hold multiple partitions.**

```
3 partitions, 1 consumer → consumer holds all 3
┌─────────────┐
│  Consumer 1 │ ← partition 0
│             │ ← partition 1
│             │ ← partition 2
└─────────────┘

3 partitions, 3 consumers → 1 partition each (the ideal)
┌──────────┐  ┌──────────┐  ┌──────────┐
│Consumer 1│  │Consumer 2│  │Consumer 3│
│  part. 0 │  │  part. 1 │  │  part. 2 │
└──────────┘  └──────────┘  └──────────┘

3 partitions, 4 consumers → 4th sits idle (no partition left)
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│Consumer 1│  │Consumer 2│  │Consumer 3│  │Consumer 4│
│  part. 0 │  │  part. 1 │  │  part. 2 │  │  (idle)  │
└──────────┘  └──────────┘  └──────────┘  └──────────┘
```

The constraint is on the **partition side**: a partition cannot be split across two consumers in the same group, because that would break ordering and make offset tracking ambiguous. A single consumer holding multiple partitions is completely normal — it just processes them one message at a time.

---

## Consumer failover — offsets survive member restarts

The committed offset lives on the **broker**, not inside the consumer process. This means a consumer can die and be replaced by a completely new process and consumption continues exactly where it left off — as long as the new process uses the same `group.id`.

```
Member 1 crashes after reading 20 messages
  broker stores: group=my-group, partition 0 → committed offset: 20

Member 2 starts (new process, new client.id, same group.id)
  joins the group → broker assigns partition 0 → broker says "start at offset 20"
  picks up from offset 20, no messages skipped, no duplicates
```

```
1 partition, 100 messages
[0 ── 1 ── 2 ── ... ── 19 | 20 ── 21 ── ... ── 99]
 ←── Member 1 consumed ──→ ↑
                    committed offset 20
                            ↑
                    Member 2 starts here
```

**The one exception — uncommitted messages:** `enable.auto.commit=true` by default, with commits flushed every 5 seconds. If Member 1 crashes within that window, the last committed offset might be 15, not 20. Member 2 would then re-read messages 15–19. This is Kafka's **at-least-once** delivery guarantee — messages may be reprocessed after a crash, but they are never skipped.

`client.id` is irrelevant to offset tracking — it is only a label for visibility in the Redpanda Console. `group.id` is what ties offset state to a consumer.

---

## Cleanup

```bash
# Restore compatibility to BACKWARD and verify versions
curl -s -X PUT http://localhost:18081/config/order.created-value \
  -H "Content-Type: application/json" \
  -d '{"compatibility": "BACKWARD"}' | python3 -m json.tool

curl -s http://localhost:18081/subjects/order.created-value/versions
```

To roll back to only v1, delete the extra versions:

```bash
curl -s -X DELETE http://localhost:18081/subjects/order.created-value/versions/2
```

---

## Summary

| Scenario | Schema Registry accepts? | Wire safe? | Why |
|----------|------------------------|-----------|-----|
| Delete `region` — BACKWARD mode | Yes | Yes | Unknown field 7 silently ignored by new consumer |
| Reuse field 7 with a different type — BACKWARD mode | No (409) | No | Wire type mismatch causes silent corruption or panic |

**The rule:** deleting a field in Protobuf is safe as long as you never reuse its field number. Mark deleted numbers as `reserved` in your `.proto` to enforce this at compile time.
