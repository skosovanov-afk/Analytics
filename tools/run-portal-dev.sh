#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="$ROOT_DIR/sales-hypotheses-project/portal"
TMP_BASE="${TMPDIR:-/tmp}"
RUNTIME_DIR="${TMP_BASE%/}/product-portal-dev"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-3000}"

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "portal source dir not found: $SOURCE_DIR" >&2
  exit 1
fi

mkdir -p "$RUNTIME_DIR"

if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete \
    --exclude '.DS_Store' \
    --exclude '.next' \
    --exclude '.next.bak*' \
    --exclude 'node_modules' \
    --exclude 'tsconfig.tsbuildinfo' \
    --exclude '_debug_backup' \
    "$SOURCE_DIR/" "$RUNTIME_DIR/"
else
  echo "rsync is required for run-portal-dev.sh" >&2
  exit 1
fi

LOCK_HASH_FILE="$RUNTIME_DIR/.package-lock.cksum"
CURRENT_LOCK_HASH="$(cksum "$RUNTIME_DIR/package-lock.json" | awk '{print $1 ":" $2}')"
PREVIOUS_LOCK_HASH="$(cat "$LOCK_HASH_FILE" 2>/dev/null || true)"

if [[ ! -d "$RUNTIME_DIR/node_modules" ]] || [[ "$CURRENT_LOCK_HASH" != "$PREVIOUS_LOCK_HASH" ]]; then
  (cd "$RUNTIME_DIR" && npm install --no-audit --no-fund)
  printf '%s\n' "$CURRENT_LOCK_HASH" > "$LOCK_HASH_FILE"
fi

if [[ -d "$RUNTIME_DIR/.next" ]]; then
  find "$RUNTIME_DIR/.next" -mindepth 1 -delete
fi

LISTENER_PID="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN || true)"
if [[ -n "$LISTENER_PID" ]]; then
  LISTENER_CWD="$(lsof -a -p "$LISTENER_PID" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | tail -n 1)"
  case "$LISTENER_CWD" in
    "$SOURCE_DIR"|"$RUNTIME_DIR"|*/portal-run-*|*/product-portal-dev)
      kill "$LISTENER_PID"
      sleep 1
      ;;
    *)
      echo "port $PORT is busy by pid $LISTENER_PID (cwd: ${LISTENER_CWD:-unknown})" >&2
      exit 1
      ;;
  esac
fi

cd "$RUNTIME_DIR"
echo "Starting portal from $RUNTIME_DIR on http://$HOST:$PORT"
exec node node_modules/next/dist/bin/next dev --hostname "$HOST" --port "$PORT"
