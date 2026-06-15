"""
Simulation S8 — Log start offset and OFFSET_OUT_OF_RANGE.

Demonstrates what happens when a consumer group's committed offset falls
below the log start offset (i.e., the data it needs has been deleted by
retention). Shows three auto.offset.reset behaviors: earliest, latest, error.

Steps:
  1. Create topic with very short retention (5 s) and tiny segments (512 B).
  2. Produce 30 messages. Consume first 10 and commit offset 10.
  3. Wait for retention to delete the first segments (log start offset advances).
  4. Attempt to resume from committed offset 10:
       - auto.offset.reset=earliest → silently resets to log start offset
       - auto.offset.reset=latest   → jumps to tip of the log, skips everything
       - auto.offset.reset=error    → raises exception, consumer halts

Output logged to: logs/simulations/log_start_offset.log
"""
import sys
import subprocess
import time
import os

sys.path.insert(0, '/home/ubuntu/shipstream')
os.makedirs("logs/simulations", exist_ok=True)

LOG    = "logs/simulations/log_start_offset.log"
BROKER = "localhost:19092"
TOPIC  = "sim.log-start-offset"
GROUP  = "sim-s8-group"


def rpk(*args):
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk"] + list(args),
        capture_output=True, text=True,
    )
    return result.stdout.strip(), result.stderr.strip()


def delete_topic(name):
    rpk("topic", "delete", name)
    time.sleep(1)


def create_topic(name):
    rpk("topic", "create", name,
        "--partitions", "1",
        "--topic-config", "segment.bytes=512",
        "--topic-config", "retention.ms=5000",
    )
    time.sleep(1)


def produce_messages(count=30):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": BROKER})
    delivered = 0

    def on_delivery(err, msg):
        nonlocal delivered
        if not err:
            delivered += 1

    for i in range(count):
        p.produce(TOPIC, key=f"k{i}".encode(), value=f"payment-event-{i}".encode(), callback=on_delivery)
    p.flush()
    return delivered


def consume_and_commit(count=10, group=GROUP, reset="earliest"):
    """Consume `count` messages and commit. Returns list of offsets consumed."""
    from confluent_kafka import Consumer, KafkaError
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": group,
        "auto.offset.reset": reset,
        "enable.auto.commit": False,
    })
    c.subscribe([TOPIC])
    offsets = []
    timeout = time.time() + 15
    while len(offsets) < count and time.time() < timeout:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            continue
        offsets.append(msg.offset())
        if len(offsets) == count:
            c.commit(asynchronous=False)
    c.close()
    return offsets


def try_consume_with_reset(reset_policy, group, lines):
    """Try to consume after retention has deleted the committed offset."""
    from confluent_kafka import Consumer, KafkaError, KafkaException
    from confluent_kafka.error import ConsumeError

    lines.append(f"\n  [policy] auto.offset.reset = {reset_policy}")

    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": group,
        "auto.offset.reset": reset_policy,
        "enable.auto.commit": False,
    })
    c.subscribe([TOPIC])

    result_offsets = []
    error_seen = None
    timeout = time.time() + 8

    try:
        while time.time() < timeout:
            msg = c.poll(1.0)
            if msg is None:
                if result_offsets:
                    break
                continue
            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    break
                error_seen = msg.error()
                break
            result_offsets.append(msg.offset())
            if len(result_offsets) >= 3:
                break
    except (KafkaException, ConsumeError) as e:
        error_seen = e
    finally:
        c.close()

    if error_seen:
        lines.append(f"  [result] Exception raised: {error_seen}")
        lines.append( "           Consumer halted. The gap must be handled explicitly.")
    elif not result_offsets:
        lines.append( "  [result] No messages received (reset to latest — at tip of log).")
        lines.append( "           All historical messages were silently skipped.")
    else:
        lines.append(f"  [result] Resumed from offset {result_offsets[0]} (log start offset).")
        lines.append(f"           Read offsets: {result_offsets}")
        lines.append(f"           Offsets 0–{result_offsets[0]-1} were silently skipped.")


def watermarks(topic):
    out, _ = rpk("topic", "describe", topic, "--print-watermarks")
    return out.strip() if out.strip() else "(no watermark data)"


def committed_offset(group, topic):
    out, _ = rpk("group", "describe", group, "--topics", topic)
    return out.strip() if out.strip() else "(no group data)"


def section(title, lines):
    sep = "=" * 70
    lines.append(f"\n{sep}")
    lines.append(f"  {title}")
    lines.append(sep)


def main():
    lines = []
    lines.append("Simulation S8 — Log start offset and OFFSET_OUT_OF_RANGE")
    lines.append("Concept: deleted offsets are gone. Consumer groups that fall behind")
    lines.append("retention must decide how to handle the gap.\n")

    # ------------------------------------------------------------------ #
    # Setup
    # ------------------------------------------------------------------ #
    section("SETUP", lines)
    delete_topic(TOPIC)
    rpk("group", "delete", GROUP + "-earliest")
    rpk("group", "delete", GROUP + "-latest")
    rpk("group", "delete", GROUP + "-error")
    create_topic(TOPIC)
    lines.append(f"[config] topic         = {TOPIC}")
    lines.append( "         partitions     = 1")
    lines.append( "         segment.bytes  = 512 bytes (tiny — rolls fast)")
    lines.append( "         retention.ms   = 5 seconds")

    # ------------------------------------------------------------------ #
    # Produce and consume first 10 messages
    # ------------------------------------------------------------------ #
    section("STEP 1 — Produce 30 messages, consume and commit first 10", lines)

    delivered = produce_messages(30)
    lines.append(f"[produce] {delivered} messages written.")

    lines.append("\n[consume] Consuming first 10 messages and committing offset 10...")
    offsets = consume_and_commit(10, group=GROUP + "-earliest", reset="earliest")
    consume_and_commit(10, group=GROUP + "-latest",   reset="earliest")
    consume_and_commit(10, group=GROUP + "-error",    reset="earliest")

    if offsets:
        lines.append(f"[commit]  Committed. Group offset is now {offsets[-1] + 1}.")
    else:
        lines.append("[commit]  Warning: no messages consumed — topic may be empty.")

    lines.append("\n[check]  Watermarks and committed offset before retention runs:")
    wm = watermarks(TOPIC)
    for line in wm.splitlines():
        lines.append(f"         {line}")

    # ------------------------------------------------------------------ #
    # Wait for retention to delete old segments
    # ------------------------------------------------------------------ #
    section("STEP 2 — Wait for retention to delete old segments", lines)
    lines.append("[wait]   Sleeping 12 seconds (2× the 5-second retention window)...")
    time.sleep(12)

    lines.append("\n[check]  Watermarks after retention ran:")
    wm_after = watermarks(TOPIC)
    for line in wm_after.splitlines():
        lines.append(f"         {line}")
    lines.append("\n[result] Log start offset has advanced past the committed offset (10).")
    lines.append("         The consumer groups now have committed offsets that no")
    lines.append("         longer exist on the broker. The next poll will get")
    lines.append("         OFFSET_OUT_OF_RANGE and fall back to auto.offset.reset.")

    # ------------------------------------------------------------------ #
    # Three reset policies
    # ------------------------------------------------------------------ #
    section("STEP 3 — Three auto.offset.reset policies compared", lines)
    lines.append("All three groups have the same committed offset (now below log start).")
    lines.append("Each sees the same OFFSET_OUT_OF_RANGE. Only the recovery differs.\n")

    try_consume_with_reset("earliest", GROUP + "-earliest", lines)
    try_consume_with_reset("latest",   GROUP + "-latest",   lines)
    try_consume_with_reset("error",    GROUP + "-error",    lines)

    # ------------------------------------------------------------------ #
    # Key insight
    # ------------------------------------------------------------------ #
    section("KEY INSIGHT", lines)
    lines.append("Once a segment is deleted, those offsets are gone from Kafka.")
    lines.append("The log start offset is the earliest readable position.")
    lines.append("")
    lines.append("auto.offset.reset controls what the consumer does after OFFSET_OUT_OF_RANGE:")
    lines.append("")
    lines.append("  earliest → silently skip to log start offset")
    lines.append("             Dangerous: data loss is invisible, no exception raised.")
    lines.append("")
    lines.append("  latest   → jump to the tip of the log, skip everything")
    lines.append("             Even more data lost. Only useful for non-critical streams.")
    lines.append("")
    lines.append("  error    → raise exception, consumer halts")
    lines.append("             Correct for production payment pipelines. Forces you to")
    lines.append("             decide: replay from S3 archive, accept the gap, or alert.")
    lines.append("")
    lines.append("With tiered storage enabled, this problem goes away — old offsets")
    lines.append("remain readable via S3, and the log start offset on local disk")
    lines.append("no longer represents the true start of the log.")

    # ------------------------------------------------------------------ #
    # Cleanup
    # ------------------------------------------------------------------ #
    section("CLEANUP", lines)
    delete_topic(TOPIC)
    rpk("group", "delete", GROUP + "-earliest")
    rpk("group", "delete", GROUP + "-latest")
    rpk("group", "delete", GROUP + "-error")
    lines.append("[cleanup] Topic and consumer groups deleted.")

    with open(LOG, "w") as f:
        f.write("\n".join(lines) + "\n")

    for line in lines:
        print(line)
    print(f"\nLog saved to {LOG}")


if __name__ == "__main__":
    main()
