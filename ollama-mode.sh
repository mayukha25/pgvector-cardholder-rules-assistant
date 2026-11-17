#!/usr/bin/env bash
# Switch every Ollama consumer between the GPU and the CPU.
#
#   ./ollama-mode.sh gpu     native Ollama.app on the Mac (Apple Metal GPU)
#   ./ollama-mode.sh cpu     the bank_ollama Docker container (Docker on macOS has no GPU access)
#   ./ollama-mode.sh status  show what each component is using right now
#
# Consumers and the .env key that points each one at an Ollama server:
#   ingestion/parse_disclosure.py (runs on the Mac)    OLLAMA_HOST
#   bank_embedding_worker, bank_orchestrator (Docker)  CONTAINER_OLLAMA_HOST
set -euo pipefail
cd "$(dirname "$0")"

ENV_FILE=.env
NATIVE_FROM_MAC="http://127.0.0.1:11434"               # Ollama.app listens on IPv4 loopback only
NATIVE_FROM_CONTAINER="http://host.docker.internal:11434"
DOCKER_FROM_MAC="http://[::1]:11434"                   # Docker publishes 11434 on IPv6 too; Ollama.app does not
DOCKER_FROM_CONTAINER="http://ollama:11434"
MODELS=(nomic-embed-text:latest llama3.2:3b)           # used by the worker and the orchestrator

set_env() {  # set_env KEY VALUE: replace or append one line, leave the rest of .env untouched
  local tmp; tmp=$(mktemp)
  grep -v "^$1=" "$ENV_FILE" > "$tmp" || true
  echo "$1=$2" >> "$tmp"
  cat "$tmp" > "$ENV_FILE" && rm -f "$tmp"
}
get_env() { grep "^$1=" "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2-; }

ensure_native_models() {
  curl -sf -m 5 "$NATIVE_FROM_MAC/api/version" >/dev/null \
    || { echo "❌ Ollama.app is not running on $NATIVE_FROM_MAC. Start it: open -a Ollama"; exit 1; }
  local have; have=$(ollama list | awk 'NR>1 {print $1}')
  for m in "${MODELS[@]}"; do
    grep -qx "$m" <<<"$have" && continue
    if [[ $m == llama3.2:3b ]] && grep -qx "llama3.2:latest" <<<"$have"; then
      echo "🏷️  tagging llama3.2:latest as llama3.2:3b (same weights, no download)"
      ollama cp llama3.2:latest llama3.2:3b
    else
      echo "⬇️  pulling $m into Ollama.app"
      ollama pull "$m"
    fi
  done
}

ensure_docker_models() {
  docker compose up -d ollama >/dev/null
  local have; have=$(docker exec bank_ollama ollama list | awk 'NR>1 {print $1}')
  for m in "${MODELS[@]}"; do
    grep -qx "$m" <<<"$have" || { echo "⬇️  pulling $m into bank_ollama"; docker exec bank_ollama ollama pull "$m"; }
  done
}

restart_containers() {
  # Re-creates only the two services whose environment changed; Postgres and Ollama keep running.
  echo "🔄 recreating embedding-worker and orchestrator..."
  docker compose --profile app up -d embedding-worker orchestrator >/dev/null
}

status() {
  echo "Configured (.env):"
  echo "  ingestion script     OLLAMA_HOST=$(get_env OLLAMA_HOST || true)  (default $NATIVE_FROM_MAC)"
  echo "  worker/orchestrator  CONTAINER_OLLAMA_HOST=$(get_env CONTAINER_OLLAMA_HOST || true)  (default $DOCKER_FROM_CONTAINER)"
  echo "Running containers:"
  for c in bank_embedding_worker bank_orchestrator; do
    printf '  %-22s OLLAMA_HOST=%s\n' "$c" "$(docker exec "$c" printenv OLLAMA_HOST 2>/dev/null || echo 'not running')"
  done
  echo "Loaded models (PROCESSOR shows GPU vs CPU):"
  echo "  native Ollama.app:"; ollama ps 2>/dev/null | sed 's/^/    /' || echo "    not running"
  echo "  bank_ollama container:"; docker exec bank_ollama ollama ps 2>/dev/null | sed 's/^/    /' || echo "    not running"
}

case "${1:-status}" in
  gpu)
    ensure_native_models
    set_env OLLAMA_HOST "$NATIVE_FROM_MAC"
    set_env CONTAINER_OLLAMA_HOST "$NATIVE_FROM_CONTAINER"
    restart_containers
    echo "✅ GPU mode: ingestion, embeddings and answers all use Ollama.app (Metal GPU)."
    echo "   Optional: free ~3 GB RAM with 'docker compose stop ollama' (start it again before switching to cpu)."
    ;;
  cpu)
    ensure_docker_models
    set_env OLLAMA_HOST "$DOCKER_FROM_MAC"
    set_env CONTAINER_OLLAMA_HOST "$DOCKER_FROM_CONTAINER"
    restart_containers
    echo "✅ CPU mode: everything uses the bank_ollama container."
    echo "   Note: the container only has ${MODELS[*]}; run ingestion with EXTRACTION_MODEL=llama3.2:3b."
    ;;
  status) status ;;
  *) echo "usage: $0 gpu|cpu|status"; exit 2 ;;
esac
