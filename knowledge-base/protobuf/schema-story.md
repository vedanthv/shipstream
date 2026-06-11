# The Schema Disaster — A Story

> **You are here:** [Index](../README.md) → [Proto Schema](./proto-schema.md) → **The Schema Disaster**

*A story about what happens when a developer changes a field number, why silent corruption is worse than a crash, and how Schema Registry saves the day.*

---

## Chapter 1 — Everything is working

ShipStream is running smoothly. The order pipeline looks like this:

```mermaid
flowchart LR
    subgraph Producer["🏭 Producer Service\n(order_pb2.py v1)"]
        P["Encodes item\nas field 3"]
    end

    subgraph Kafka["📨 Kafka\norder.created topic"]
        M0["offset 0: 1a 08 4b 65 79..."]
        M1["offset 1: 1a 06 57 65 62..."]
        M2["offset 2: 1a 07 4c 61 70..."]
    end

    subgraph Consumer["📦 Inventory Service\n(order_pb2.py v1)"]
        C["Reads field 3\nas item"]
        R["item = 'Keyboard' ✅"]
    end

    P -->|"field 3 = item"| Kafka
    Kafka --> C --> R
```

The proto schema is:

```protobuf
message Order {
  string id          = 1;
  string customer_id = 2;
  string item        = 3;   // ← item lives at field number 3
  double amount      = 4;
  OrderStatus status = 5;
}
```

Every order flows through perfectly. `item` is always populated. Life is good.

---

## Chapter 2 — The "innocent" change

A new developer joins the team. They're cleaning up the schema and notice the field numbering looks inconsistent. They decide to renumber `item` from `3` to `8` to "leave room for future fields between 3 and 7."

```protobuf
// BEFORE
string item = 3;

// AFTER — "just a cleanup"
string item = 8;
```

They regenerate `order_pb2.py`, test it locally with a fresh topic, everything passes. They deploy the new producer.

```mermaid
sequenceDiagram
    actor Dev as Developer
    participant Git as Git / CI
    participant Prod as Producer (new v2)
    participant Kafka as Kafka
    participant Cons as Inventory Consumer (old v1)

    Dev->>Git: commits "renumber item field 3→8"
    Git->>Prod: deploys new producer with order_pb2.py v2
    Note over Prod: now encodes item as field 8 (tag 0x42)
    Prod->>Kafka: produce(item="Keyboard") → bytes with field 8
    Kafka->>Cons: delivers bytes
    Note over Cons: still has order_pb2.py v1
    Note over Cons: expects item at field 3 (tag 0x1a)
    Cons->>Cons: reads tag 0x42 → "unknown field 8, skip"
    Cons->>Cons: item = "" 💀
```

The inventory service is now receiving orders with a blank item. No exception. No alert. No error in the logs. Just empty strings flowing silently into the warehouse system.

---

## Chapter 3 — What Kafka actually stored

This is the moment most people don't realise. Kafka stored the raw bytes from the old producer — bytes that say "field 3 = Keyboard". The new producer is now writing bytes that say "field 8 = Keyboard". **Both exist in the topic simultaneously.**

```mermaid
flowchart TD
    subgraph Topic["Kafka Topic: order.created"]
        direction TB
        O0["offset 0  │ 1a 08 4b 65 79 62 6f 61 72 64 ...
            written by OLD producer
            field 3 = 'Keyboard'"]
        O1["offset 1  │ 1a 06 57 65 62 63 61 6d ...
            written by OLD producer
            field 3 = 'Webcam'"]
        O2["offset 2  │ 42 08 4b 65 79 62 6f 61 72 64 ...
            written by NEW producer  ← deploy happened here
            field 8 = 'Keyboard'"]
        O3["offset 3  │ 42 09 4d 6f 6e 69 74 6f 72 ...
            written by NEW producer
            field 8 = 'Monitor'"]
    end
```

The topic now has two generations of messages with incompatible field numbering, sitting side by side with no way to tell them apart from the outside.

---

## Chapter 4 — The four combinations

Three hours later, the team deploys the new consumer (with `order_pb2.py v2`). Now the system has a mix of old/new producers and old/new consumers all running at the same time.

```mermaid
quadrantChart
    title What each combination sees for "item"
    x-axis Old Producer --> New Producer
    y-axis Old Consumer --> New Consumer
    quadrant-1 "✅ Both use field 8\nitem = 'Keyboard'"
    quadrant-2 "❌ Consumer reads field 3\nNew producer sends field 8\nitem = ''"
    quadrant-3 "❌ Consumer reads field 8\nOld producer sends field 3\nitem = ''"
    quadrant-4 "✅ Both use field 3\nitem = 'Keyboard'"
```

There is no deployment order that avoids the broken cells. Deploy new producer first → bottom-right cell is broken. Deploy new consumer first → top-left cell is broken. You are guaranteed data loss during the transition.

---

## Chapter 5 — It gets worse: replaying history

Two weeks later, the analytics team wants to reprocess all orders to rebuild a dashboard. They reset the consumer group offset to `earliest` and replay from offset 0.

```mermaid
sequenceDiagram
    participant Cons as Analytics Consumer (v2)
    participant Kafka as Kafka

    Cons->>Kafka: seek to offset 0 (replay from start)
    Kafka-->>Cons: offset 0 → bytes with field 3 = "Keyboard" (old format)
    Note over Cons: order_pb2.py v2 expects item at field 8
    Cons->>Cons: reads field 3 → unknown, skip
    Cons->>Cons: item = "" ❌
    Kafka-->>Cons: offset 1 → bytes with field 3 = "Webcam"
    Cons->>Cons: item = "" ❌
    Kafka-->>Cons: offset 2 → bytes with field 8 = "Keyboard" (new format)
    Cons->>Cons: item = "Keyboard" ✅
    Note over Cons: half the historical data has blank items
```

Old messages in Kafka are permanent. The new consumer can never correctly read them. The historical data is partially corrupted — forever.

---

## Chapter 6 — The schema lives in the code, not in Kafka

This is the root cause of everything above. Kafka stores raw bytes and nothing else. There is no schema attached to any message.

```mermaid
flowchart TB
    subgraph Kafka["Kafka — what it actually stores"]
        B["offset 42:  0a 03 61 62 63 1a 08 4b 65 79 62 6f 61 72 64 28 01
            offset 43:  0a 04 64 65 66 67 42 08 4b 65 79 62 6f 61 72 64 28 02
            ↑ just bytes. no field names. no version. no schema ID."]
    end

    subgraph V1["Consumer v1 machine\norder_pb2.py v1"]
        S1["field 3 → item\nfield 4 → amount\nfield 5 → status"]
    end

    subgraph V2["Consumer v2 machine\norder_pb2.py v2"]
        S2["field 8 → item\nfield 4 → amount\nfield 5 → status"]
    end

    Kafka -->|"same bytes"| V1
    Kafka -->|"same bytes"| V2
    V1 --> R1["item = 'Keyboard' ✅"]
    V2 --> R2["item = '' ❌"]
```

The schema is a property of the **consumer's deployed code**, not of the message. Two consumers with different `_pb2.py` files will decode the exact same bytes into different results.

---

## Chapter 7 — Enter Schema Registry

Schema Registry solves this by storing the schema centrally and embedding a **schema ID** in every message. Now the bytes carry enough information to know which schema was used to write them.

```mermaid
flowchart LR
    subgraph SR["📋 Schema Registry\n:18081"]
        ID1["ID 1 → order.proto v1\n(field 3 = item)"]
        ID2["ID 2 → order.proto v2\n(field 8 = item)"]
    end

    subgraph OldProd["Old Producer"]
        OP["Registers schema\ngets ID = 1\nPrepends ID to message"]
    end

    subgraph NewProd["New Producer"]
        NP["Registers new schema\ngets ID = 2\nPrepends ID to message"]
    end

    subgraph Kafka["Kafka"]
        M1["[ID=1] 1a 08 4b 65 79..."]
        M2["[ID=2] 42 08 4b 65 79..."]
    end

    subgraph Consumer["Consumer (any version)"]
        C["Reads ID from message\nFetches schema from Registry\nDecodes with correct schema"]
    end

    OldProd -->|"register"| SR
    OldProd --> M1
    NewProd -->|"register"| SR
    NewProd --> M2
    M1 --> Consumer
    M2 --> Consumer
    SR -->|"ID=1 → v1 schema"| Consumer
    SR -->|"ID=2 → v2 schema"| Consumer
```

Every message now carries its schema ID as a 5-byte prefix (1 magic byte + 4 byte ID). The consumer reads the ID, fetches the matching schema from the registry, and decodes using the exact schema the producer used — regardless of what version the consumer itself has deployed.

---

## Chapter 8 — Wire format with Schema Registry

Without Schema Registry, a Kafka message value is just raw Protobuf bytes:

```
Without Schema Registry:
┌─────────────────────────────────────────────┐
│  0a 03 61 62 63  1a 08 4b 65 79 62 6f ...   │
│  └──────────────────────────────────────┘   │
│               raw protobuf bytes            │
└─────────────────────────────────────────────┘
```

With Schema Registry, 5 bytes are prepended:

```
With Schema Registry:
┌────┬────────────────┬─────────────────────────────────────────────┐
│ 00 │ 00 00 00 01    │  0a 03 61 62 63  1a 08 4b 65 79 62 6f ...   │
│ ↑  │ ↑              │  └──────────────────────────────────────┘   │
│magic│ schema ID = 1 │           raw protobuf bytes                │
└────┴────────────────┴─────────────────────────────────────────────┘
```

The consumer reads bytes 1–4 to get the schema ID, fetches schema `1` from the registry, and decodes the rest as Protobuf using that schema.

---

## Chapter 9 — Replaying history with Schema Registry

Now the analytics team resets to `earliest` and replays history again. This time:

```mermaid
sequenceDiagram
    participant Cons as Analytics Consumer
    participant SR as Schema Registry
    participant Kafka as Kafka

    Cons->>Kafka: seek to offset 0
    Kafka-->>Cons: [ID=1] 1a 08 4b 65 79... (old format)
    Cons->>SR: fetch schema ID=1
    SR-->>Cons: field 3 = item (v1 schema)
    Cons->>Cons: decode field 3 → item = "Keyboard" ✅

    Kafka-->>Cons: [ID=2] 42 08 4b 65 79... (new format)
    Cons->>SR: fetch schema ID=2
    SR-->>Cons: field 8 = item (v2 schema)
    Cons->>Cons: decode field 8 → item = "Keyboard" ✅
```

Every message decoded correctly, regardless of when it was written or which schema version produced it. Historical replay works perfectly.

---

## The full lesson

```mermaid
flowchart TD
    A["Developer changes field number"]

    B["Without Schema Registry"]
    C["Schema lives in consumer code only"]
    D["Kafka stores raw bytes with no version info"]
    E["Old messages permanently unreadable\nby new consumers"]
    F["Silent data corruption — no errors"]

    G["With Schema Registry"]
    H["Schema stored centrally with an ID"]
    I["Every message carries its schema ID"]
    J["Consumer always fetches the right schema"]
    K["Old messages always decodable ✅"]

    A --> B & G
    B --> C --> D --> E --> F
    G --> H --> I --> J --> K
```

---

## Key takeaways

1. **Kafka stores bytes, not schemas.** There is no version tag on any message unless you add one yourself.

2. **The schema is in the consumer's code.** Two consumers with different `_pb2.py` files decode the same bytes differently.

3. **Never change a field number.** It silently corrupts data with no error, affects historical messages permanently, and has no safe deployment order.

4. **Schema Registry** solves this by making every message self-describing — it carries the ID of the schema used to write it.

5. **We're not using Schema Registry yet** — Redpanda exposes it on `:18081` but ShipStream Phase 1 skips it. This is Phase 2 territory.

---

> ← [Previous: Proto Schema](./proto-schema.md) | [Index](../README.md) | [Next: Binary Encoding →](./binary-encoding.md)
