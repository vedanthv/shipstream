"""
Simulation S13 — Idempotence: eliminating retry duplicates.

Demonstrates that without enable.idempotence=True, a message that times out
and is retried may land twice on the broker (the original write succeeded but
the ACK never reached the producer). With idempotence on, the broker
deduplicates retries using the (producer ID, sequence number) pair.

Two rounds:
  Round 1 (no idempotence):
    - Produce 50 messages with a very short request timeout
    - Pause the broker mid-produce to force timeouts → retries
    - Count messages on disk — expect > 50 (duplicates)

  Round 2 (enable.idempotence=True):
    - Same scenario
    - Count messages on disk — expect exactly 50 (retries silently discarded)

Note: the producer sees 50 successful deliveries in BOTH rounds.
Duplicates are invisible to the producer — only visible in the log.

Requires: 3-broker cluster (redpanda-1, redpanda-2, redpanda-3)
"""
import sys
import subprocess
import time
import os

sys.path.insert(0, '/home/ubuntu/shipstream')
os.makedirs("logs/simulations", exist_ok=True)

BROKER = "localhost:19092"
TOPIC  = "sim.idempotence"
COUNT  = 50


def rpk(*args):
    result = subprocess.run(
        ["docker", "exec", "redpanda-1", "rpk"] + list(args),
        capture_output=True, text=True,
    )
    return result.stdout.strip()


def docker_pause(container):
    subprocess.run(["docker", "pause", container], capture_output=True)


def docker_unpause(container):
    subprocess.run(["docker", "unpause", container], capture_output=True)


def delete_topic():
    rpk("topic", "delete", TOPIC)
    time.sleep(2)


def create_topic():
    rpk("topic", "create", TOPIC, "--partitions", "1", "--replicas", "3")
    time.sleep(2)


def high_watermark():
    out = rpk("topic", "describe", TOPIC, "--print-partitions")
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0] == "0":
            try:
                return int(parts[5])
            except (IndexError, ValueError):
                pass
    return None


def find_leader_container():
    out = rpk("topic", "describe", TOPIC, "--print-partitions")
    nodes = {0: "redpanda-1", 1: "redpanda-2", 2: "redpanda-3"}
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0] == "0":
            try:
                return nodes.get(int(parts[1]), "redpanda-1")
            except (IndexError, ValueError):
                pass
    return "redpanda-1"


def produce_with_retries(idempotent: bool):
    """
    Produce COUNT messages. Briefly pause the leader halfway through to force
    timeouts and retries. Returns the number of confirmed deliveries.
    """
    from confluent_kafka import Producer
    import threading

    confirmed = 0
    errors    = 0

    def on_delivery(err, msg):
        nonlocal confirmed, errors
        if err is None:
            confirmed += 1
        else:
            errors += 1

    config = {
        "bootstrap.servers": BROKER,
        "linger.ms": "0",
        "message.timeout.ms": "3000",    # 3-second budget per message
        "request.timeout.ms": "500",     # short per-request timeout → retries fire fast
        "retries": "10",
        "retry.backoff.ms": "100",
    }
    if idempotent:
        config["enable.idempotence"] = "true"
    else:
        config["enable.idempotence"] = "false"
        config["acks"] = "all"           # use all even without idempotence for fair comparison

    p = Producer(config)

    leader = find_leader_container()

    def pause_and_resume():
        """Pause the leader for 1.5s halfway through produce."""
        time.sleep(0.5)   # let some messages land first
        docker_pause(leader)
        time.sleep(1.5)
        docker_unpause(leader)

    pauser = threading.Thread(target=pause_and_resume, daemon=True)
    pauser.start()

    for i in range(COUNT):
        p.produce(
            TOPIC,
            key=f"msg-{i:04d}".encode(),
            value=f"payload-{i:04d}".encode(),
            callback=on_delivery,
        )
        p.poll(0)

    p.flush(timeout=15)
    pauser.join(timeout=5)

    return confirmed, errors


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def run_round(idempotent: bool):
    label = "enable.idempotence=True" if idempotent else "enable.idempotence=False"
    section(f"ROUND — {label}")

    delete_topic()
    create_topic()

    print(f"[config] {label}")
    print(f"[config] request.timeout.ms=500, retries=10")
    print(f"[produce] Sending {COUNT} messages (broker will be paused mid-way)...")

    confirmed, errors = produce_with_retries(idempotent)

    time.sleep(3)   # let the broker settle
    hw = high_watermark()

    print(f"\n[result] Producer confirmed deliveries: {confirmed}")
    print(f"[result] Producer delivery errors:      {errors}")
    print(f"[result] Messages on disk (HW):         {hw}")

    if hw is not None:
        duplicates = hw - COUNT
        if duplicates > 0:
            print(f"\n[result] Duplicates: {duplicates}")
            print(f"         {hw} messages on disk for {COUNT} logical messages.")
            print(f"         Retries re-wrote messages the broker had already committed.")
        elif duplicates == 0:
            print(f"\n[result] Duplicates: 0")
            print(f"         Exactly {COUNT} messages on disk. Retries were silently")
            print(f"         discarded by the broker using (PID, sequence) deduplication.")
        else:
            print(f"\n[result] {hw} messages on disk (some messages may have been lost).")


def main():
    print("Simulation S13 — Idempotence: eliminating retry duplicates")
    print("Concept: without idempotence, retries create duplicates the producer")
    print("         never sees. With idempotence, the broker deduplicates them.\n")

    run_round(idempotent=False)
    run_round(idempotent=True)

    section("KEY INSIGHT")
    print(f"In both rounds the producer reported {COUNT} confirmed deliveries.")
    print("The duplicates are invisible to the producer.")
    print("")
    print("Without idempotence: the broker wrote the retry because it had no way")
    print("to know it had already committed the original. The producer's 'confirmed'")
    print("count matches what it intended, but the log has extra copies.")
    print("")
    print("With idempotence: the broker tracks (producer ID, sequence number) per")
    print("partition. A retry arrives with the same sequence number, is recognized")
    print("as a duplicate, and is discarded — no error, no extra message.")


if __name__ == "__main__":
    main()
