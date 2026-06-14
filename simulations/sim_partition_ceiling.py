"""
Simulation S3 — num.partitions: consumer parallelism ceiling.

Creates payment.processed with 2 partitions, starts 4 consumers in the same
group, floods the topic, then shows which consumers are active and which are
idle. Then recreates the topic with 3 partitions and repeats.

What you observe:
  - Round 1 (2 partitions): 2 consumers handle all traffic, 2 sit idle.
  - Round 2 (3 partitions): all 3 active consumers get 1 partition each.
    The 4th is still idle — partition count is the hard ceiling.

Output logged to: logs/simulations/partition_ceiling.log

Usage:
    python3 simulations/sim_partition_ceiling.py
"""
import sys
import os
import time
import threading
import subprocess
sys.path.insert(0, '/home/ubuntu/shipstream')

os.makedirs("logs/simulations", exist_ok=True)
LOG   = "logs/simulations/partition_ceiling.log"
TOPIC = "payment.processed"
GROUP = "sim-s3-consumer-group"
BROKER = "localhost:19092"
SR_URL = "http://localhost:18081"

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from generated_proto_objects.payment.v1.payment_pb2 import Payment


def rpk(*args):
    r = subprocess.run(["docker", "exec", "redpanda", "rpk"] + list(args),
                       capture_output=True, text=True)
    return r.stdout.strip()


def recreate_topic(partitions):
    rpk("topic", "delete", TOPIC)
    time.sleep(1)
    rpk("topic", "create", TOPIC, "--partitions", str(partitions))
    time.sleep(1)


def reset_consumer_group():
    rpk("group", "delete", GROUP)
    time.sleep(0.5)


def run_consumer(consumer_id, counters, stop_event, assignment_log):
    sr = SchemaRegistryClient({"url": SR_URL})
    deser = ProtobufDeserializer(Payment, schema_registry_client=sr)
    ctx = SerializationContext(TOPIC, MessageField.VALUE)

    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": False,
    })

    assigned_partitions = []

    def on_assign(consumer, partitions):
        assigned_partitions.clear()
        assigned_partitions.extend([p.partition for p in partitions])
        assignment_log[consumer_id] = list(assigned_partitions)

    c.subscribe([TOPIC], on_assign=on_assign)

    while not stop_event.is_set():
        msg = c.poll(timeout=0.5)
        if msg is None or (msg.error() and msg.error().code() == KafkaError._PARTITION_EOF):
            continue
        if msg.error():
            continue
        counters[consumer_id] = counters.get(consumer_id, 0) + 1

    c.close()


def flood_topic(count=300):
    from simulations.payment_producer import make_payment
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
    from confluent_kafka.serialization import SerializationContext, MessageField

    sr = SchemaRegistryClient({"url": SR_URL})
    serializer = ProtobufSerializer(Payment, sr)
    producer = Producer({"bootstrap.servers": BROKER})

    for i in range(count):
        p = make_payment()
        producer.produce(
            topic=TOPIC,
            key=p.payment_id.encode(),
            value=serializer(p, SerializationContext(TOPIC, MessageField.VALUE)),
        )
        if i % 50 == 0:
            producer.poll(0)
    producer.flush()


def run_round(partitions, num_consumers=4, produce_count=300, observe_secs=8):
    recreate_topic(partitions)
    reset_consumer_group()

    counters = {}
    assignment_log = {}
    stop_event = threading.Event()
    threads = []

    for i in range(num_consumers):
        t = threading.Thread(
            target=run_consumer,
            args=(i, counters, stop_event, assignment_log),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Give consumers time to join the group and get assigned partitions
    time.sleep(4)

    flood_topic(produce_count)
    time.sleep(observe_secs)

    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    return counters, assignment_log


def section(title, lines):
    sep = "=" * 70
    lines.append(f"\n{sep}")
    lines.append(f"  {title}")
    lines.append(sep)


def summarise_round(partitions, counters, assignment_log, lines):
    lines.append(f"\n  Topic partitions : {partitions}")
    lines.append(f"  Consumers running: {max(counters.keys()) + 1 if counters else 4}")
    lines.append("")
    lines.append(f"  {'Consumer':<12} {'Assigned partitions':<28} {'Messages consumed'}")
    lines.append(f"  {'-'*12} {'-'*28} {'-'*17}")
    for i in range(4):
        assigned = assignment_log.get(i, [])
        count    = counters.get(i, 0)
        status   = "IDLE — no partition assigned" if not assigned else str(sorted(assigned))
        lines.append(f"  consumer-{i:<3}  {status:<28} {count}")

    active = sum(1 for i in range(4) if assignment_log.get(i))
    idle   = 4 - active
    lines.append(f"\n  Active: {active}   Idle: {idle}")
    lines.append(f"  → Partition count ({partitions}) is the hard ceiling on parallelism.")


def main():
    lines = []
    lines.append("Simulation S3 — Consumer parallelism ceiling")
    lines.append("Topic: payment.processed   Consumer group: sim-s3-consumer-group")
    lines.append("4 consumers started in all rounds.")

    section("ROUND 1 — 2 partitions, 4 consumers", lines)
    lines.append("\n  Recreating topic with 2 partitions and starting 4 consumers...")
    c1, a1 = run_round(partitions=2)
    summarise_round(2, c1, a1, lines)

    section("ROUND 2 — 3 partitions, 4 consumers", lines)
    lines.append("\n  Recreating topic with 3 partitions and starting 4 consumers...")
    c2, a2 = run_round(partitions=3)
    summarise_round(3, c2, a2, lines)

    lines.append("\n" + "=" * 70)
    lines.append("  TAKEAWAY")
    lines.append("=" * 70)
    lines.append("  Adding consumers beyond the partition count gives you nothing.")
    lines.append("  To scale throughput, you must increase partition count first.")
    lines.append("  This is why num.partitions (default topic partition count) matters")
    lines.append("  at topic creation time — you can add partitions later, but keys")
    lines.append("  will remap and ordering guarantees within a key are broken.")

    with open(LOG, "w") as f:
        f.write("\n".join(lines) + "\n")

    for line in lines:
        print(line)
    print(f"\nLog saved to {LOG}")


if __name__ == "__main__":
    main()
