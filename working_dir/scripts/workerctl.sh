#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
OUT_DIR="$ROOT_DIR/output"
PID_FILE="$OUT_DIR/worker.pid"
LOG_FILE="$LOG_DIR/worker.log"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:18080/health}"
START_TIMEOUT_SECONDS="${START_TIMEOUT_SECONDS:-30}"

mkdir -p "$LOG_DIR" "$OUT_DIR"

python_bin() {
  if [[ -x "$ROOT_DIR/../.venv/bin/python" ]]; then
    echo "$ROOT_DIR/../.venv/bin/python"
  else
    echo "python3"
  fi
}

worker_pids() {
  pgrep -f "[s]craper.py worker run" || true
}

health_ok() {
  python3 - "$HEALTH_URL" <<'PY'
import json
import sys
import urllib.request
url = sys.argv[1]
try:
    with urllib.request.urlopen(url, timeout=3) as r:
        body = r.read().decode("utf-8", "ignore")
        payload = json.loads(body)
        if payload.get("status") == "ok":
            print(body)
            raise SystemExit(0)
except Exception:
    pass
raise SystemExit(1)
PY
}

start_worker() {
  local pids
  pids="$(worker_pids)"
  if [[ -n "$pids" ]]; then
    echo "Worker already running: $pids"
    return 0
  fi

  local py
  py="$(python_bin)"
  cd "$ROOT_DIR"
  nohup "$py" scraper.py worker run --health-port 18080 >> "$LOG_FILE" 2>&1 &
  local pid=$!

  local waited=0
  while (( waited < START_TIMEOUT_SECONDS )); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Worker exited during startup. See $LOG_FILE"
      return 1
    fi
    if health_ok >/dev/null 2>&1; then
      echo "$pid" > "$PID_FILE"
      echo "Worker started (pid=$pid)"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done

  echo "Worker failed health check within ${START_TIMEOUT_SECONDS}s. Killing pid=$pid"
  kill "$pid" 2>/dev/null || true
  return 1
}

stop_worker() {
  local pids
  pids="$(worker_pids)"
  if [[ -z "$pids" ]]; then
    echo "Worker not running"
    rm -f "$PID_FILE"
    return 0
  fi
  echo "Stopping worker: $pids"
  kill $pids 2>/dev/null || true
  sleep 2
  pids="$(worker_pids)"
  if [[ -n "$pids" ]]; then
    echo "Force killing worker: $pids"
    kill -9 $pids 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  echo "Worker stopped"
}

status_worker() {
  local pids
  pids="$(worker_pids)"
  if [[ -n "$pids" ]]; then
    echo "Worker running: $pids"
  else
    echo "Worker not running"
  fi

  if health_ok >/tmp/oatcake_health.$$ 2>/dev/null; then
    echo "Health OK: $(cat /tmp/oatcake_health.$$)"
  else
    echo "Health DOWN: $HEALTH_URL"
  fi
  rm -f /tmp/oatcake_health.$$
}

probe_worker() {
  python3 - "$HEALTH_URL" <<'PY'
import sys
import urllib.request
base = sys.argv[1].rsplit("/health", 1)[0]
for path in ("/health", "/metrics"):
    url = base + path
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            print(f"\n{path}")
            print(r.read(3000).decode("utf-8", "ignore"))
    except Exception as exc:
        print(f"\n{path}\nERROR: {exc}")
PY
}

case "${1:-}" in
  start) start_worker ;;
  stop) stop_worker ;;
  restart) stop_worker; start_worker ;;
  status) status_worker ;;
  probe) probe_worker ;;
  logs) tail -n "${2:-80}" "$LOG_FILE" ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|probe|logs [n]}"
    exit 2
    ;;
esac
