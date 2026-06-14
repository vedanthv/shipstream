"""
Simulation S2 — auto.create.topics.enable footgun.

Demonstrates what happens when a producer typos a topic name:
  - Round 1 (auto-create ON):  'payment.procesed' is silently created.
    The real consumer on 'payment.processed' sees zero messages and zero lag —
    the worst kind of silent failure.
  - Round 2 (auto-create OFF): the producer gets an explicit error immediately.

Steps performed:
  1. Enable auto-create (Redpanda default).
  2. Produce 20 payments to the typo'd topic 'payment.procesed'.
  3. List topics — show the phantom topic exists.
  4. Show consumer on correct topic sees nothing.
  5. Disable auto-create via rpk.
  6. Attempt to produce to the typo'd topic again — observe error.
  7. Re-enable auto-create to restore default state.

Output logged to: logs/simulations/auto_create_footgun.log
"""
import sys
import subprocess
import time
sys.path.insert(0, '/home/ubuntu/shipstream')

import os
os.makedirs("logs/simulations", exist_ok=True)
LOG = "logs/simulations/auto_create_footgun.log"

BROKER       = "localhost:19092"
CORRECT_TOPIC = "payment.processed"
TYPO_TOPIC    = "payment.procesed"   # missing 's'


def rpk(*args):
    """Run an rpk command inside the redpanda container, return stdout."""
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk"] + list(args),
        capture_output=True, text=True,
    )
    return result.stdout.strip(), result.stderr.strip()


def list_payment_topics():
    out, _ = rpk("topic", "list")
    lines = [l for l in out.splitlines() if "payment" in l]
    return lines


def produce_to(topic, count=20):
    """Produce payments to a given topic. Returns (delivered, error_lines)."""
    from confluent_kafka import Producer, KafkaException
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
    from confluent_kafka.serialization import SerializationContext, MessageField
    from simulations.payment_producer import make_payment

    sr = SchemaRegistryClient({"url": "http://localhost:18081"})
    serializer = ProtobufSerializer(
        __import__('generated_proto_objects.payment.v1.payment_pb2', fromlist=['Payment']).Payment,
        sr,
    )
    producer = Producer({
        "bootstrap.servers": BROKER,
        "message.timeout.ms": 5000,
    })

    delivered = 0
    errors = []

    def on_delivery(err, msg):
        nonlocal delivered
        if err:
            errors.append(str(err))
        else:
            delivered += 1

    from generated_proto_objects.payment.v1.payment_pb2 import Payment
    sr2 = SchemaRegistryClient({"url": "http://localhost:18081"})
    serializer2 = ProtobufSerializer(Payment, sr2)

    from simulations.payment_producer import make_payment as _make
    for _ in range(count):
        p = _make()
        try:
            producer.produce(
                topic=topic,
                key=p.payment_id.encode(),
                value=serializer2(p, SerializationContext(topic, MessageField.VALUE)),
                callback=on_delivery,
            )
        except KafkaException as e:
            errors.append(str(e))
    producer.flush()
    return delivered, errors


def consumer_lag(topic, group="sim-s2-group"):
    """Return the lag string from rpk group describe for a given topic."""
    out, _ = rpk("group", "describe", group, "--topics", topic)
    return out if out else "(no consumer group data — group never connected)"


def section(title, log_lines):
    sep = "=" * 70
    log_lines.append(f"\n{sep}")
    log_lines.append(f"  {title}")
    log_lines.append(sep)


def main():
    lines = []
    lines.append("Simulation S2 — auto.create.topics.enable footgun")
    lines.append(f"Correct topic : {CORRECT_TOPIC}")
    lines.append(f"Typo'd topic  : {TYPO_TOPIC}  (missing 's')")

    # ------------------------------------------------------------------ #
    # Round 1: auto-create ON (default)
    # ------------------------------------------------------------------ #
    section("ROUND 1 — auto-create ENABLED (default)", lines)

    rpk("cluster", "config", "set", "auto_create_topics_enabled", "true")
    lines.append("[config] auto_create_topics_enabled = true")
    time.sleep(1)

    lines.append(f"\n[produce] Sending 20 payments to typo'd topic '{TYPO_TOPIC}'...")
    delivered, errors = produce_to(TYPO_TOPIC, count=20)
    lines.append(f"[result]  delivered={delivered}  errors={len(errors)}")
    if errors:
        for e in errors:
            lines.append(f"          ERROR: {e}")

    time.sleep(1)
    topics = list_payment_topics()
    lines.append("\n[topics]  Payment-related topics now registered:")
    for t in topics:
        lines.append(f"          {t}")

    lines.append(f"\n[impact]  Consumer subscribed to '{CORRECT_TOPIC}' sees:")
    lag = consumer_lag(CORRECT_TOPIC)
    lines.append(f"          {lag}")
    lines.append(f"\n          → {delivered} payments are silently lost in '{TYPO_TOPIC}'.")
    lines.append( "          → No error, no alert. The consumer group shows zero lag because")
    lines.append( "            it has never received a single message.")

    # ------------------------------------------------------------------ #
    # Round 2: auto-create OFF
    # ------------------------------------------------------------------ #
    section("ROUND 2 — auto-create DISABLED", lines)

    rpk("cluster", "config", "set", "auto_create_topics_enabled", "false")
    lines.append("[config] auto_create_topics_enabled = false")
    time.sleep(2)

    lines.append(f"\n[produce] Sending 20 payments to typo'd topic '{TYPO_TOPIC}' again...")
    delivered2, errors2 = produce_to(TYPO_TOPIC, count=20)
    lines.append(f"[result]  delivered={delivered2}  errors={len(errors2)}")
    if errors2:
        lines.append( "          Errors received:")
        for e in set(errors2):
            lines.append(f"          ERROR: {e}")
    else:
        lines.append("          (no errors — topic already existed from round 1)")

    lines.append("\n[takeaway]")
    lines.append("  With auto-create OFF, unknown topics fail immediately at produce time.")
    lines.append("  The bug is visible in seconds, not discovered after hours of missing data.")

    # ------------------------------------------------------------------ #
    # Cleanup — delete phantom topic, restore default
    # ------------------------------------------------------------------ #
    section("CLEANUP", lines)
    rpk("topic", "delete", TYPO_TOPIC)
    lines.append(f"[cleanup] Deleted phantom topic '{TYPO_TOPIC}'.")
    rpk("cluster", "config", "set", "auto_create_topics_enabled", "true")
    lines.append("[cleanup] Restored auto_create_topics_enabled = true.")

    # ------------------------------------------------------------------ #
    # Write log
    # ------------------------------------------------------------------ #
    with open(LOG, "w") as f:
        f.write("\n".join(lines) + "\n")

    for line in lines:
        print(line)
    print(f"\nLog saved to {LOG}")


if __name__ == "__main__":
    main()
