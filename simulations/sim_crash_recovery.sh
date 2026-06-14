#!/usr/bin/env bash
# Simulation S5 — num.recovery.threads.per.data.dir: crash recovery timing.
#
# In Apache Kafka, when the broker restarts after a crash it must scan every
# partition directory to find the last clean offset and truncate any partially-
# written bytes. num.recovery.threads.per.data.dir controls how many threads
# do this scan in parallel *per log.dir*.
#
# Redpanda uses a Seastar-based storage engine so this exact config does not
# exist, but the underlying concept does: on restart after a hard kill Redpanda
# must recover each partition's log before accepting traffic. Recovery work
# scales with the number of partition directories that need scanning.
#
# This simulation runs three scenarios with increasing partition counts and
# measures recovery time for each, then shows the Kafka thread-count analogy.
#
# Scenarios
#   SCENARIO A —  3 partitions  (low   recovery work, Kafka analogy: threads=1)
#   SCENARIO B — 12 partitions  (medium recovery work, Kafka analogy: threads=4)
#   SCENARIO C — 24 partitions  (high  recovery work, Kafka analogy: threads=8)
#
# Usage:
#   ./simulations/sim_crash_recovery.sh

set -euo pipefail

LOG_FILE="logs/simulations/crash_recovery.log"
mkdir -p logs/simulations

# log to both console and log file
log() { printf '%s\n' "$1" | tee -a "$LOG_FILE"; }

SEP()  { log "$(printf '=%.0s' {1..70})"; }
SEP2() { log "$(printf -- '-%.0s' {1..70})"; }

> "$LOG_FILE"
log "Simulation S5 — Crash recovery: partition count vs recovery time"
log "Demonstrates the workload that num.recovery.threads.per.data.dir parallelises."
log ""

# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

# Wait until the broker is healthy; store elapsed ms in RECOVERY_MS global.
# Uses Python for millisecond timing — date +%s%3N is unreliable across distros.
ms_now() { python3 -c "import time; print(int(time.time() * 1000))"; }

RECOVERY_MS=0
wait_healthy() {
    local start end
    start=$(ms_now)
    for _ in $(seq 1 90); do
        if docker exec redpanda rpk cluster health 2>/dev/null | grep -q "Healthy:.*true"; then
            end=$(ms_now)
            RECOVERY_MS=$(( end - start ))
            return 0
        fi
        sleep 1
    done
    log "[ERROR] Broker did not become healthy within 90s"
    exit 1
}

create_topic() {
    local topic="$1" parts="$2"
    docker exec redpanda rpk topic delete "$topic" > /dev/null 2>&1 || true
    sleep 0.5
    local result
    result=$(docker exec redpanda rpk topic create "$topic" --partitions "$parts" 2>&1 \
             | awk -v t="$topic" '$1==t {print $2}')
    log "  [create] $topic ($parts partitions): ${result:-OK}"
}

flood() {
    python3 simulations/payment_producer.py --topic "$1" --count "$2" --silent
}

# Hard-kill Redpanda, restart, measure recovery.
# Result stored in RECOVERY_MS global.
crash_and_recover() {
    log "[kill]    docker kill redpanda (SIGKILL — simulates power loss)..."
    docker kill redpanda > /dev/null
    sleep 0.5
    log "[start]   docker start redpanda..."
    docker start redpanda > /dev/null
    log "[waiting] polling cluster health..."
    wait_healthy
    log "[healthy] broker ready in ${RECOVERY_MS}ms"
}

# Get per-partition message counts (high-water mark) for a topic
topic_counts() {
    local topic="$1"
    docker exec redpanda rpk topic consume "$topic" \
        --offset end --num 0 2>/dev/null \
        | grep -c "." 2>/dev/null || true
    # fallback: use rpk group describe style output
    docker exec redpanda rpk topic list 2>/dev/null \
        | awk -v t="$topic" '$1==t {print "  partitions=" $2 " replicas=" $3}'
}

# ------------------------------------------------------------------ #
# Timing results accumulate here
# ------------------------------------------------------------------ #
RESULT_LABELS=()
RESULT_PARTS=()
RESULT_MS=()

# ------------------------------------------------------------------ #
# run_scenario LABEL topic:parts:msgs [topic:parts:msgs ...]
# ------------------------------------------------------------------ #
run_scenario() {
    local label="$1"
    shift
    local specs=("$@")

    SEP
    log "  $label"
    SEP
    log ""

    # Create topics
    local total_parts=0
    for spec in "${specs[@]}"; do
        IFS=: read -r tname parts msgs <<< "$spec"
        create_topic "$tname" "$parts"
        total_parts=$(( total_parts + parts ))
    done
    log ""
    log "  Total partition directories broker must recover: $total_parts"
    log ""

    # Seed all topics with committed messages
    log "  Seeding topics..."
    for spec in "${specs[@]}"; do
        IFS=: read -r tname parts msgs <<< "$spec"
        log "  [flood]   $tname ← $msgs payments"
        flood "$tname" "$msgs"
    done
    log ""

    # Start a background flood so there are in-flight messages at kill time
    local primary_topic="${specs[0]%%:*}"
    log "  [flood]   Starting background flood on $primary_topic (in-flight at kill)..."
    python3 simulations/payment_producer.py \
        --topic "$primary_topic" --count 500 --silent &
    local flood_pid=$!
    sleep 2   # let some in-flight messages build up

    # Crash + recover
    crash_and_recover
    wait "$flood_pid" 2>/dev/null || true
    log ""

    # Kafka thread analogy
    local kafka_threads=$(( total_parts / 3 ))
    [ "$kafka_threads" -lt 1 ] && kafka_threads=1
    log "  Kafka analogy: $total_parts partition dirs → use"
    log "  num.recovery.threads.per.data.dir=$kafka_threads to scan $kafka_threads dirs in parallel."
    log ""

    RESULT_LABELS+=("$label")
    RESULT_PARTS+=("$total_parts")
    RESULT_MS+=("$RECOVERY_MS")

    SEP2
    log ""
}

# ------------------------------------------------------------------ #
# SCENARIO A — 3 partitions
# ------------------------------------------------------------------ #
run_scenario \
    "SCENARIO A — 3 partitions (low recovery load)" \
    "payment.processed:3:300"

# ------------------------------------------------------------------ #
# SCENARIO B — 12 partitions across 3 topics
# ------------------------------------------------------------------ #
run_scenario \
    "SCENARIO B — 12 partitions across 3 topics (medium recovery load)" \
    "payment.processed:4:300" \
    "payment.refunds:4:100"   \
    "payment.failures:4:100"

# ------------------------------------------------------------------ #
# SCENARIO C — 24 partitions across 6 topics
# ------------------------------------------------------------------ #
run_scenario \
    "SCENARIO C — 24 partitions across 6 topics (high recovery load)" \
    "payment.processed:6:300"  \
    "payment.refunds:4:100"    \
    "payment.failures:4:100"   \
    "payment.chargebacks:4:50" \
    "payment.disputes:4:50"    \
    "payment.audit:2:50"

# ------------------------------------------------------------------ #
# Summary table
# ------------------------------------------------------------------ #
SEP
log "  RECOVERY TIME SUMMARY"
SEP
log ""
log "  $(printf '%-50s' 'Scenario') $(printf '%10s' 'Partitions') $(printf '%14s' 'Recovery (ms)')"
log "  $(printf -- '-%.0s' {1..50}) $(printf -- '-%.0s' {1..10}) $(printf -- '-%.0s' {1..14})"
for i in "${!RESULT_LABELS[@]}"; do
    log "  $(printf '%-50s' "${RESULT_LABELS[$i]}") $(printf '%10s' "${RESULT_PARTS[$i]}") $(printf '%14s' "${RESULT_MS[$i]}")"
done

log ""
log "  Note: on this single-node Docker setup with --smp 1, partition count"
log "  has limited impact because only one core is available. On a real"
log "  multi-core Kafka broker the difference would be more pronounced."
log ""
log "  Kafka analogy — num.recovery.threads.per.data.dir"
log "  ──────────────────────────────────────────────────"
log "  threads=1 (default) : partition dirs scanned one at a time"
log "                        → recovery time scales linearly with partition count"
log ""
log "  threads=N           : N dirs scanned concurrently per log.dir"
log "                        → recovery time ≈ max(time per dir) when N ≥ dir count"
log ""
log "  With 2 log.dirs and threads=4: 4 × 2 = 8 dirs recovered in parallel."
log ""
log "  Rule of thumb: set threads to match the number of physical disks,"
log "  multiplied by available I/O cores. Kafka default (1) is almost always"
log "  too low for production clusters with many partitions."
log ""
log "  Redpanda note: the Seastar scheduler manages per-shard I/O concurrency"
log "  internally — no manual thread knob is exposed."

log ""
log "Log saved to $LOG_FILE"
