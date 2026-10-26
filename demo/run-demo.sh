#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

docker compose up -d --wait
mvn -q package

export LEASE_SECONDS=6
export HEARTBEAT_MS=2000
export POLL_INTERVAL_MS=250
export MAX_PER_ORG=2
export MAX_INFLIGHT_PER_ORG=3
export WORKER_THREADS=4
export RETRY_BACKOFF_SECONDS=1

JAR="$ROOT/target/job-scheduler.jar"
LOGS="$ROOT/demo/logs"
mkdir -p "$LOGS"

java -jar "$JAR" reset
java -jar "$JAR" enqueue --org northstar --type slow --count 30
java -jar "$JAR" enqueue --org bright --type fast --count 10
java -jar "$JAR" enqueue --org bright --type long --count 1
java -jar "$JAR" enqueue --org bright --type fail-once --count 1
java -jar "$JAR" enqueue --org cobalt --type fast --count 6
java -jar "$JAR" enqueue --org cobalt --type fast --count 6 --delay-seconds 4
java -jar "$JAR" enqueue --org cobalt --type fail-always --count 1 --max-attempts 2

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

for id in worker-1 worker-2 worker-3; do
  java -jar "$JAR" worker --id "$id" >"$LOGS/$id.out.log" 2>"$LOGS/$id.err.log" &
  pids+=("$!")
done

java -jar "$JAR" worker --id crasher --claim-and-exit --types fast,slow \
  >"$LOGS/crasher.out.log" 2>"$LOGS/crasher.err.log" &
crasher=$!

sleep 3
for pid in "${pids[@]}"; do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "worker $pid exited during startup"
    tail -n 40 "$LOGS"/*.log || true
    exit 1
  fi
done

set +e
wait "$crasher"
crasher_code=$?
set -e
echo "---- crasher ----"
cat "$LOGS/crasher.out.log"
if [[ "$crasher_code" -ne 1 ]] || ! grep -q "ABANDONED" "$LOGS/crasher.out.log"; then
  echo "crasher did not abandon a job (exit $crasher_code)"
  exit 1
fi

set +e
java -jar "$JAR" report --wait --timeout-seconds 180
report_code=$?
set -e
if [[ "$report_code" -ne 0 ]]; then
  tail -n 50 "$LOGS"/*.log || true
fi
exit "$report_code"
