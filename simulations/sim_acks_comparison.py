"""
Simulation S12 — acks=1 vs acks=all: the durability gap.

Demonstrates that with acks=1 the producer receives success before replication
completes. A leader crash in that window silently drops confirmed messages.
With acks=all the ACK only arrives after every ISR member has a copy, so a
leader crash cannot cause data loss.

Two rounds:
  Round 1 (acks=1):
    - Create a topic with RF=3, 1 partition
    - Produce continuously on a background thread
    - Kill the partition leader mid-produce
    - Count producer-confirmed messages vs messages on disk (new leader)
    - Observe the gap

  Round 2 (acks=all):
    - Same setup, same kill
    - Count again — gap is zero

Requires: 3-broker cluster (redpanda-1, redpanda-2, redpanda-3)
Output:   logs/simulations/acks_comparison.log + printed to terminal
"""
import sys
import subprocess
import threading
import time
import os

sys.path.insert(0, '/home/ubuntu/shipstream')
os.makedirs("logs/simulations", exist_ok=True)

LOG    = "logs/simulations/acks_comparison.log"
BROKER = "localhost:19092"
TOPIC  = "sim.acks-comparison"

# Map node-id → container name and external bootstrap address
NODES = {
    0: ("redpanda-1", "localhost:19092"),
    1: ("redpanda-2", "localhost:29092"),
    2: ("redpanda-3", "localhost:39092"),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rpk(*args, node="redpanda-1"):
    result = subprocess.run(
        ["docker", "exec", node, "rpk"] + list(args),
        capture_output=True, text=True,
    )
    return result.stdout.strip(), result.stderr.strip()


def docker_stop(container):
    subprocess.run(["docker", "stop", container], capture_output=True)


def docker_start(container):
    subprocess.run(["docker", "start", container], capture_output=True)


def delete_topic():
    rpk("topic", "delete", TOPIC)
    time.sleep(2)


def create_topic():
    rpk("topic", "create", TOPIC,
        "--partitions", "1",
        "--replicas", "3",
    )
    time.sleep(2)


def find_leader():
    """Return (node_id, container_name) of the current partition 0 leader."""
    out, _ = rpk("topic", "describe", TOPIC, "--print-partitions")
    for line in out.splitlines():
        parts = line.split()
        # columns: PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
        if parts and parts[0] == "0":
            try:
                node_id = int(parts[1])
                container, _ = NODES[node_id]
                return node_id, container
            except (IndexError, ValueError, KeyError):
                pass
    return None, None


def high_watermark(alive_node="redpanda-1"):
    """Return the high watermark for partition 0, queried from an alive node."""
    out, _ = rpk("topic", "describe", TOPIC, "--print-partitions", node=alive_node)
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0] == "0":
            try:
                return int(parts[5])
            except (IndexError, ValueError):
                pass
    return None


def produce_until_stopped(acks_setting, results):
    """
    Produce messages with the given acks setting.
    Stops when results['stop'] is set.
    Writes confirmed count into results['confirmed'].
    """
    from confluent_kafka import Producer

    confirmed = 0

    def on_delivery(err, msg):
        nonlocal confirmed
        if err is None:
            confirmed += 1

    p = Producer({
        "bootstrap.servers": BROKER,
        "acks": str(acks_setting),
        "retries": "0",                   # no retries — isolates the gap cleanly
        "linger.ms": "0",
        "batch.size": "1",                # one message per batch for clarity
        "message.timeout.ms": "5000",
    })

    i = 0
    while not results.get("stop"):
        p.produce(TOPIC, key=f"key-{i}".encode(),
                  value=f"msg-{i}".encode(), callback=on_delivery)
        p.poll(0)
        i += 1
        time.sleep(0.01)   # 100 msg/s — fast enough to have in-flight when killed

    p.flush(timeout=5)
    results["confirmed"] = confirmed


def log_and_print(lines, msg):
    lines.append(msg)
    print(msg)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_round(acks_setting, lines):
    log_and_print(lines, f"\n{'='*65}")
    log_and_print(lines, f"  ROUND — acks={acks_setting}")
    log_and_print(lines, f"{'='*65}")

    delete_topic()
    create_topic()

    node_id, leader_container = find_leader()
    if leader_container is None:
        log_and_print(lines, "[error] Could not find partition leader. Is the cluster healthy?")
        return

    # find an alive node to query later (not the leader we're about to kill)
    alive_node = next(c for nid, (c, _) in NODES.items() if nid != node_id)

    log_and_print(lines, f"[setup]  Topic: {TOPIC} (RF=3, 1 partition)")
    log_and_print(lines, f"[setup]  Leader: node {node_id} ({leader_container})")
    log_and_print(lines, f"[setup]  acks={acks_setting}, retries=0")

    results = {"stop": False, "confirmed": 0}
    t = threading.Thread(target=produce_until_stopped, args=(acks_setting, results))
    t.start()

    log_and_print(lines, "\n[produce] Producing at ~100 msg/s for 3 seconds...")
    time.sleep(3)

    log_and_print(lines, f"[kill]   Stopping {leader_container} mid-produce...")
    docker_stop(leader_container)

    log_and_print(lines, "[wait]   Waiting 3 seconds for new leader election...")
    time.sleep(3)

    results["stop"] = True
    t.join(timeout=10)

    confirmed = results["confirmed"]

    # wait for new leader to be elected and watermark to settle
    time.sleep(2)
    hw = high_watermark(alive_node=alive_node)

    log_and_print(lines, f"\n[result] Producer confirmed: {confirmed} messages")
    log_and_print(lines, f"[result] Messages on disk:    {hw}")

    if hw is not None:
        gap = confirmed - hw
        if gap > 0:
            log_and_print(lines, f"[result] Gap: {gap} messages confirmed by producer but LOST on leader crash")
        else:
            log_and_print(lines, f"[result] Gap: 0 — no data loss")
    else:
        log_and_print(lines, "[result] Could not read high watermark from surviving broker.")

    log_and_print(lines, f"\n[restart] Bringing {leader_container} back up...")
    docker_start(leader_container)
    log_and_print(lines, f"[restart] Waiting 10 seconds for broker to rejoin cluster...")
    time.sleep(10)


def main():
    lines = []
    log_and_print(lines, "Simulation S12 — acks=1 vs acks=all: the durability gap")
    log_and_print(lines, "Concept: acks=1 ACKs before replication; leader crash loses confirmed messages.")
    log_and_print(lines, "         acks=all ACKs after all ISR members confirm; crash-safe.\n")

    run_round(acks_setting=1, lines=lines)
    run_round(acks_setting="all", lines=lines)

    lines.append("\n" + "="*65)
    lines.append("  KEY INSIGHT")
    lines.append("="*65)
    lines.append("With acks=1, the producer's 'confirmed' count is a lie under failure.")
    lines.append("The broker sent the ACK before replication — the new leader never")
    lines.append("had those messages. The producer already discarded them from its")
    lines.append("buffer (it 'succeeded'). No retry will recover them.")
    lines.append("")
    lines.append("With acks=all, confirmed = on disk. Always. The ACK is a real")
    lines.append("guarantee: every ISR member has a copy before you see success.")

    with open(LOG, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\nLog saved to {LOG}")


if __name__ == "__main__":
    main()
