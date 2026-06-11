#!/usr/bin/env bash
# Spins up N parallel consumers in the same group, each logging to its own file.
# Usage: ./run_consumers.sh [N]   (default: 3)

NUM_CONSUMERS=${1:-3}
LOG_DIR="logs/consumers"
mkdir -p "$LOG_DIR"

PIDS=()

cleanup() {
    echo ""
    echo "Stopping all consumers..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null
    done
    wait
    echo "All consumers stopped. Logs are in $LOG_DIR/"
    exit 0
}
trap cleanup SIGINT SIGTERM

echo "Starting $NUM_CONSUMERS consumers in group 'shipstream-consumer-group'..."
echo "Logs: $LOG_DIR/consumer-<N>.log"
echo "Press Ctrl+C to stop all."
echo ""

for i in $(seq 1 "$NUM_CONSUMERS"); do
    LOG="$LOG_DIR/consumer-$i.log"
    CONSUMER_ID=$i python services/consumer.py > "$LOG" 2>&1 &
    PIDS+=($!)
    echo "  Consumer $i started (PID ${PIDS[-1]}) → $LOG"
done

echo ""

# Tail all logs to stdout with a prefix showing which consumer it came from
tail -f "$LOG_DIR"/consumer-*.log &
TAIL_PID=$!

wait "${PIDS[@]}"
kill "$TAIL_PID" 2>/dev/null
