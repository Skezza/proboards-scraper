#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Install Oatcake archiver worker on a Linux box (user-level systemd service).

Usage:
  ./deploy/install_linux_worker.sh [--repo-dir PATH] [--health-port PORT] [--service-name NAME] [--rate-limit-delay SECONDS]

Options:
  --repo-dir PATH      Repo root containing working_dir/ (default: current dir parent of this script)
  --health-port PORT   Worker health port (default: 18080)
  --service-name NAME  systemd user service name (default: oatcake-worker)
  --rate-limit-delay   Base delay between requests in seconds (default: 2.5)
  -h, --help           Show this help
EOF
}

REPO_DIR=""
HEALTH_PORT="18080"
SERVICE_NAME="oatcake-worker"
RATE_LIMIT_DELAY="2.5"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-dir)
      REPO_DIR="${2:-}"
      shift 2
      ;;
    --health-port)
      HEALTH_PORT="${2:-}"
      shift 2
      ;;
    --service-name)
      SERVICE_NAME="${2:-}"
      shift 2
      ;;
    --rate-limit-delay)
      RATE_LIMIT_DELAY="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "$REPO_DIR" ]]; then
  REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi

WORKING_DIR="$REPO_DIR/working_dir"
SERVICE_TEMPLATE="$WORKING_DIR/deploy/oatcake-worker.service"
VENV_PY="$REPO_DIR/.venv/bin/python"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/${SERVICE_NAME}.service"
LOG_FILE="$WORKING_DIR/logs/worker.log"
USER_BASE="$REPO_DIR/.pyusr"
PIP_CACHE_DIR="$REPO_DIR/.pip-cache"
BOOTSTRAP_DIR="$REPO_DIR/.bootstrap"
SERVICE_PY="$VENV_PY"
PYTHON_USER_BASE_VALUE=""
PYTHON_SITE_PACKAGES_VALUE=""

if [[ ! -d "$WORKING_DIR" ]]; then
  echo "Missing working_dir at: $WORKING_DIR" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required but not found." >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl is required but not found." >&2
  exit 1
fi

echo "Preparing folders..."
mkdir -p "$WORKING_DIR/logs" "$WORKING_DIR/output" "$WORKING_DIR/exports" "$PIP_CACHE_DIR" "$BOOTSTRAP_DIR"
chmod +x "$WORKING_DIR/scripts/workerctl.sh"
export TMPDIR="$BOOTSTRAP_DIR"

if [[ ! -x "$VENV_PY" ]]; then
  echo "Creating virtualenv at $REPO_DIR/.venv"
  if ! python3 -m venv "$REPO_DIR/.venv"; then
    echo "Virtualenv creation unavailable; falling back to user-base install under $USER_BASE"
    rm -rf "$REPO_DIR/.venv"
  fi
fi

if [[ -x "$VENV_PY" ]] && "$VENV_PY" -m pip --version >/dev/null 2>&1; then
  echo "Installing Python dependencies into virtualenv..."
  "$VENV_PY" -m pip install --upgrade pip
  "$VENV_PY" -m pip install -r "$WORKING_DIR/requirements.txt"
else
  if [[ -d "$REPO_DIR/.venv" ]]; then
    rm -rf "$REPO_DIR/.venv"
  fi
  if ! python3 -m pip --version >/dev/null 2>&1; then
    BOOTSTRAP_PY="$BOOTSTRAP_DIR/get-pip.py"
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL https://bootstrap.pypa.io/get-pip.py -o "$BOOTSTRAP_PY"
    elif command -v wget >/dev/null 2>&1; then
      wget -qO "$BOOTSTRAP_PY" https://bootstrap.pypa.io/get-pip.py
    else
      echo "curl or wget is required to bootstrap pip." >&2
      exit 1
    fi
    TMPDIR="$BOOTSTRAP_DIR" PYTHONUSERBASE="$USER_BASE" python3 "$BOOTSTRAP_PY" --user
  fi
  echo "Installing Python dependencies into $USER_BASE..."
  TMPDIR="$BOOTSTRAP_DIR" PYTHONUSERBASE="$USER_BASE" python3 -m pip install --user --cache-dir "$PIP_CACHE_DIR" --upgrade pip
  TMPDIR="$BOOTSTRAP_DIR" PYTHONUSERBASE="$USER_BASE" python3 -m pip install --user --cache-dir "$PIP_CACHE_DIR" -r "$WORKING_DIR/requirements.txt"
  SERVICE_PY="$(command -v python3)"
  PYTHON_USER_BASE_VALUE="$USER_BASE"
  PYTHON_SITE_PACKAGES_VALUE="$(PYTHONUSERBASE="$USER_BASE" "$SERVICE_PY" -c 'import site; print(site.getusersitepackages())')"
fi

if [[ ! -f "$SERVICE_TEMPLATE" ]]; then
  echo "Missing service template: $SERVICE_TEMPLATE" >&2
  exit 1
fi

echo "Installing user service: $SERVICE_NAME"
mkdir -p "$SERVICE_DIR"
cp "$SERVICE_TEMPLATE" "$SERVICE_FILE"
sed -i "s|__WORKING_DIR__|$WORKING_DIR|g" "$SERVICE_FILE"
sed -i "s|__PYTHON_BIN__|$SERVICE_PY|g" "$SERVICE_FILE"
sed -i "s|__HEALTH_PORT__|$HEALTH_PORT|g" "$SERVICE_FILE"
sed -i "s|__RATE_LIMIT_DELAY__|$RATE_LIMIT_DELAY|g" "$SERVICE_FILE"
sed -i "s|__LOG_FILE__|$LOG_FILE|g" "$SERVICE_FILE"
sed -i "s|__PYTHON_USER_BASE__|$PYTHON_USER_BASE_VALUE|g" "$SERVICE_FILE"
sed -i "s|__PYTHON_SITE_PACKAGES__|$PYTHON_SITE_PACKAGES_VALUE|g" "$SERVICE_FILE"

echo "Reloading systemd user daemon..."
systemctl --user daemon-reload
systemctl --user enable --now "${SERVICE_NAME}.service"

echo
echo "Installed successfully."
echo "Service status:"
systemctl --user --no-pager --full status "${SERVICE_NAME}.service" || true
echo
echo "Probe:"
"$WORKING_DIR/scripts/workerctl.sh" probe || true
echo
echo "If this host should run while logged out, enable linger once as root:"
echo "  sudo loginctl enable-linger $USER"
