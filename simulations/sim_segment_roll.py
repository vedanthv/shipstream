"""
Simulation S6 — log.segment.bytes: the active segment blocks retention.

Demonstrates that retention can only delete CLOSED segments. While the active
segment is open, messages inside it are never deleted — even if they are far
past the retention window.

Two rounds:
  Round 1 (large segment, short retention):
    - Topic: log.segment.bytes=10MB, log.retention.ms=5s
    - Produce 20 small messages (~200 bytes each = ~4 KB total)
    - Wait 10 seconds (well past retention window)
    - Log start offset is still 0 — no deletion happened
    - Reason: 4 KB never filled the 10 MB segment, so it never rolled

  Round 2 (tiny segment, short retention):
    - Topic: log.segment.bytes=512, log.retention.ms=5s
    - Produce the same 20 messages
    - Each message forces a segment roll (512 bytes fills fast)
    - Wait 10 seconds — segments are closed and past retention
    - Log start offset has advanced — old messages deleted

Output logged to: logs/simulations/segment_roll.log
"""
import sys
import subprocess
import time
import os

sys.path.insert(0, '/home/ubuntu/shipstream')
os.makedirs("logs/simulations", exist_ok=True)

LOG    = "logs/simulations/segment_roll.log"
BROKER = "localhost:19092"


def rpk(*args):
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk"] + list(args),
        capture_output=True, text=True,
    )
    return result.stdout.strip(), result.stderr.strip()


def delete_topic(name):
    rpk("topic", "delete", name)
    time.sleep(1)


def create_topic(name, segment_bytes, retention_ms):
    rpk("topic", "create", name,
        "--partitions", "1",
        "--topic-config", f"segment.bytes={segment_bytes}",
        "--topic-config", f"retention.ms={retention_ms}",
    )
    time.sleep(1)


def produce_messages(topic, count=20, one_at_a_time=False):
    from confluent_kafka import Producer
    # linger.ms=0 + batch.size=1 ensures each produce() is sent immediately
    p = Producer({"bootstrap.servers": BROKER, "linger.ms": 0, "batch.size": 1})
    delivered = 0

    def on_delivery(err, msg):
        nonlocal delivered
        if not err:
            delivered += 1

    payload = ("x" * 180).encode()
    for i in range(count):
        p.produce(topic, key=f"key-{i}".encode(), value=payload, callback=on_delivery)
        if one_at_a_time:
            p.flush()  # force a separate write per message so each triggers a segment roll
    p.flush()
    return delivered


def watermarks(topic):
    """Return (log_start_offset, high_watermark) as ints for partition 0."""
    out, _ = rpk("topic", "describe", topic, "-p")
    for line in out.splitlines():
        parts = line.split()
        # output columns: PARTITION LEADER EPOCH REPLICAS LOG-START-OFFSET HIGH-WATERMARK
        if parts and parts[0] == "0":
            try:
                return int(parts[4]), int(parts[5])
            except (IndexError, ValueError):
                pass
    return None, None


def section(title, lines):
    sep = "=" * 70
    lines.append(f"\n{sep}")
    lines.append(f"  {title}")
    lines.append(sep)


def main():
    lines = []
    lines.append("Simulation S6 — Active segment blocks retention")
    lines.append("Concept: retention only deletes CLOSED segments.")
    lines.append("The active segment is always kept, regardless of age.\n")

    # ------------------------------------------------------------------ #
    # Round 1: large segment — retention is stuck
    # ------------------------------------------------------------------ #
    section("ROUND 1 — Large segment (10 MB), short retention (5 s)", lines)

    topic1 = "sim.segment-large"
    delete_topic(topic1)
    create_topic(topic1, segment_bytes=10_485_760, retention_ms=5_000)
    lines.append(f"[config] topic={topic1}")
    lines.append( "         log.segment.bytes = 10 MB")
    lines.append( "         log.retention.ms  = 5 seconds")

    delivered = produce_messages(topic1, count=20, one_at_a_time=False)
    lines.append(f"\n[produce] {delivered} messages (~180 bytes each = ~3.5 KB total)")
    lines.append( "          3.5 KB is far below the 10 MB segment threshold.")
    lines.append( "          The active segment will NEVER roll from size alone.")

    lines.append("\n[wait]   Sleeping 15 seconds (3× the retention window)...")
    time.sleep(15)

    lso1, hw1 = watermarks(topic1)
    lines.append(f"\n[check]  log-start-offset={lso1}  high-watermark={hw1}")
    if lso1 == 0:
        lines.append("[result] LOW WATERMARK IS STILL 0 — confirmed.")
        lines.append("         The active segment never rolled, so nothing was eligible")
        lines.append("         for deletion. All messages still on disk despite being")
        lines.append("         past the 5-second retention window.")
    else:
        lines.append(f"[result] Unexpected: low watermark advanced to {lso1}.")

    # ------------------------------------------------------------------ #
    # Round 2: tiny segment — retention fires
    # ------------------------------------------------------------------ #
    section("ROUND 2 — Tiny segment (1 KB), short retention (5 s)", lines)

    topic2 = "sim.segment-tiny"
    delete_topic(topic2)
    create_topic(topic2, segment_bytes=1_024, retention_ms=5_000)
    lines.append(f"[config] topic={topic2}")
    lines.append( "         log.segment.bytes = 1 KB")
    lines.append( "         log.retention.ms  = 5 seconds")

    # one_at_a_time=True flushes after every message, forcing the broker to
    # receive 20 separate writes and roll a new segment after each one.
    delivered = produce_messages(topic2, count=20, one_at_a_time=True)
    lines.append(f"\n[produce] {delivered} messages (one flush per message)")
    lines.append( "          Each write exceeds the 1 KB segment threshold.")
    lines.append( "          Segments roll with every message — most are already")
    lines.append( "          CLOSED by the time produce finishes.")

    lso_before, hw_before = watermarks(topic2)
    lines.append(f"\n[check]  immediately after produce: log-start-offset={lso_before}  high-watermark={hw_before}")

    lines.append("\n[wait]   Sleeping 20 seconds (4× the retention window)...")
    time.sleep(20)

    lso_after, hw_after = watermarks(topic2)
    lines.append(f"\n[check]  after waiting:             log-start-offset={lso_after}  high-watermark={hw_after}")
    if lso_after and lso_after > 0:
        lines.append(f"\n[result] LOW WATERMARK ADVANCED to {lso_after} — confirmed.")
        lines.append("         Closed segments older than 5 seconds were deleted.")
        lines.append("         The active segment (most recent messages) is still kept.")
    else:
        lines.append("\n[result] Low watermark did not advance — retention did not fire yet.")

    # ------------------------------------------------------------------ #
    # Key insight
    # ------------------------------------------------------------------ #
    section("KEY INSIGHT", lines)
    lines.append("Both topics had the same retention window (5 seconds).")
    lines.append("Both had the same messages sitting past that window.")
    lines.append("")
    lines.append("The ONLY difference: segment size.")
    lines.append("")
    lines.append("  Large segment (10 MB): active segment never rolled.")
    lines.append("  → Retention is stuck. Effective retention = infinite.")
    lines.append("")
    lines.append("  Tiny segment (1 KB): segments rolled with every write.")
    lines.append("  → Retention fired on schedule. Effective retention ≈ 5 seconds.")
    lines.append("")
    lines.append("For low-throughput topics, set log.segment.ms in addition to")
    lines.append("log.segment.bytes to force a roll even if the size never fills.")

    # ------------------------------------------------------------------ #
    # Cleanup
    # ------------------------------------------------------------------ #
    section("CLEANUP", lines)
    for line in lines:
        print(line)
    lines.clear()
    input("\n[pause] Topics are still live — inspect them in the UI now.\n"
          "        Press Enter when ready to delete and finish...")
    delete_topic(topic1)
    delete_topic(topic2)
    lines.append(f"[cleanup] Deleted {topic1} and {topic2}.")

    with open(LOG, "w") as f:
        f.write("\n".join(lines) + "\n")

    for line in lines:
        print(line)
    print(f"\nLog saved to {LOG}")


if __name__ == "__main__":
    main()
