#!/usr/bin/env bash
# Simulation S4 — log.dirs: partition-to-directory layout inspection.
#
# In Apache Kafka, log.dirs lets you list multiple disk paths so Kafka
# distributes partition directories across them (round-robin at creation).
# Each partition's segment files (.log, .index, .timeindex) live in exactly
# one directory. More dirs = more physical disks = more I/O parallelism.
#
# Redpanda stores all data under a single data_directory (/var/lib/redpanda/data
# in this container setup). This simulation shows the on-disk layout of
# partition directories so you can see what Kafka-style log.dirs partitioning
# would look like if split across two paths.
#
# What you observe:
#   - Each partition gets its own subdirectory: <topic>-<partition>/
#   - Inside each: .log (messages), .index (offset index), .timeindex (time index)
#   - The active segment has no .log.gz — it's the open file being written to
#
# Usage:
#   ./simulations/sim_log_dirs.sh

set -euo pipefail

TOPIC="payment.processed"
LOG_FILE="logs/simulations/log_dirs.log"
DATA_DIR="/var/lib/redpanda/data"

mkdir -p logs/simulations

log() { echo "$1" | tee -a "$LOG_FILE"; }

> "$LOG_FILE"
log "Simulation S4 — log.dirs: on-disk partition layout"
log "Topic: $TOPIC"
log "Redpanda data dir: $DATA_DIR"
log ""

# ------------------------------------------------------------------ #
# Ensure topic exists with 3 partitions
# ------------------------------------------------------------------ #
log "$(printf '=%.0s' {1..70})"
log "  STEP 1 — Ensure $TOPIC exists with 3 partitions"
log "$(printf '=%.0s' {1..70})"

EXISTING=$(docker exec redpanda rpk topic list 2>/dev/null | grep "^$TOPIC" || true)
if [ -z "$EXISTING" ]; then
    docker exec redpanda rpk topic create "$TOPIC" --partitions 3
    log "[created] $TOPIC with 3 partitions"
else
    log "[exists]  $TOPIC already present"
fi
log ""

# ------------------------------------------------------------------ #
# Produce some messages so segments have data
# ------------------------------------------------------------------ #
log "$(printf '=%.0s' {1..70})"
log "  STEP 2 — Publish 200 payments to populate segments"
log "$(printf '=%.0s' {1..70})"
python3 simulations/payment_producer.py --count 200 --silent
log ""

# ------------------------------------------------------------------ #
# Inspect the on-disk layout
# ------------------------------------------------------------------ #
log "$(printf '=%.0s' {1..70})"
log "  STEP 3 — On-disk partition directory layout"
log "$(printf '=%.0s' {1..70})"
log ""
log "  Directory listing inside container ($DATA_DIR):"
log ""

docker exec redpanda bash -c "
  echo '  All payment.processed partition directories:'
  ls -d ${DATA_DIR}/${TOPIC}-* 2>/dev/null | sed 's|^|    |'
  echo ''
  for dir in ${DATA_DIR}/${TOPIC}-*; do
    partition=\$(basename \$dir)
    echo \"  --- \$partition ---\"
    ls -lh \$dir | awk 'NR>1 {printf \"    %-40s %6s\\n\", \$9, \$5}'
    echo ''
  done
" | tee -a "$LOG_FILE"

# ------------------------------------------------------------------ #
# Show what multi-dir striping would look like
# ------------------------------------------------------------------ #
log "$(printf '=%.0s' {1..70})"
log "  STEP 4 — How log.dirs striping would distribute these partitions"
log "$(printf '=%.0s' {1..70})"
log ""
log "  In Apache Kafka with log.dirs=/data/disk1,/data/disk2:"
log ""
log "  Kafka assigns partitions round-robin across dirs at creation time:"
log ""
log "    /data/disk1/                     /data/disk2/"
log "      payment.processed-0/              payment.processed-1/"
log "        00000000000000000000.log           00000000000000000000.log"
log "        00000000000000000000.index         00000000000000000000.index"
log "        00000000000000000000.timeindex     00000000000000000000.timeindex"
log "      payment.processed-2/"
log "        00000000000000000000.log"
log "        ..."
log ""
log "  Each dir sits on a separate physical disk. Partition 0 and 2 I/O"
log "  hits disk1; partition 1 I/O hits disk2. Neither disk is a bottleneck"
log "  for the other. This is the throughput benefit of multiple log.dirs."
log ""
log "  Redpanda uses a single data_directory but its internal Seastar I/O"
log "  scheduler achieves similar parallelism without multiple mount points."
log ""
log "  num.recovery.threads.per.data.dir controls how many threads scan"
log "  partition dirs within each log.dir on broker restart. With 2 dirs"
log "  and 4 threads per dir = 8 total recovery threads."

log ""
log "Log saved to $LOG_FILE"
