# Part 3 — Infrastructure

[← Back to Index](../README.md)

---

| Chapter | Topic | One-liner |
|---------|-------|-----------|
| 12 | [Redpanda](./redpanda.md) | Ports, internal vs external, rpk CLI |
| 13 | [Redpanda Console](./redpanda-console.md) | Web UI, Protobuf decoding, debugging guide |
| 14 | [Python Packages](./python-packages.md) | confluent-kafka, protobuf, librdkafka |

---

**[→ Start with Chapter 12: Redpanda](./redpanda.md)**

## Architecture

```mermaid
flowchart TB
    subgraph "Host Machine"
        PY1["producer.py\nlocalhost:19092"]
        PY2["consumer.py\nlocalhost:19092"]
        BR["Browser\nlocalhost:8080"]
    end

    subgraph "Docker Compose"
        RP["Redpanda\n:9092 internal / :19092 external"]
        CON["Redpanda Console\n:8080"]
    end

    PY1 --> RP
    PY2 --> RP
    BR --> CON
    CON -->|"redpanda:9092"| RP
```
