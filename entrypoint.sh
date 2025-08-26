#!/usr/bin/env bash
set -Eeuo pipefail
__/\\\\\\\\\\\\\__________/\\\\\\\\\\\\\\\________/\\\\\_____/\\\________/\\\\\\\\\\\_______
 _\/\\\/////////\\\_______\/\\\///////////________\/\\\\\\___\/\\\_______\/////\\\///________
  _\/\\\_______\/\\\_______\/\\\___________________\/\\\/\\\__\/\\\___________\/\\\___________
   _\/\\\\\\\\\\\\\/________\/\\\\\\\\\\\___________\/\\\//\\\_\/\\\___________\/\\\___________
    _\/\\\/////////__________\/\\\///////____________\/\\\\//\\\\/\\\___________\/\\\___________
     _\/\\\___________________\/\\\___________________\/\\\_\//\\\/\\\___________\/\\\___________
      _\/\\\___________________\/\\\___________________\/\\\__\//\\\\\\___________\/\\\___________
       _\/\\\______________/\\\_\/\\\______________/\\\_\/\\\___\//\\\\\__/\\\__/\\\\\\\\\\\__/\\\_
        _\///______________\///__\///______________\///__\///_____\/////__\///__\///////////__\///__
# ============ Config base ============
PROJECT_NAME="${PROJECT_NAME:-matteo_bdt}"
COMPOSE_ARGS=()             # es: (-f docker-compose.yml -f docker-compose.dev.yml)
BRING_DOWN_ON_EXIT="${BRING_DOWN_ON_EXIT:-1}"   # 1=si, 0=no
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-300}"         # secondi max per diventare healthy
HEALTH_POLL_INTERVAL="${HEALTH_POLL_INTERVAL:-3}"

# ============ Utility ============
log() { echo "[entrypoint] $*" >&2; }
die() { echo "[entrypoint][ERRORE] $*" >&2; exit 1; }

# Scegli comando compose (v2 preferito, fallback v1)
detect_compose() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
  else
    die "Docker Compose non trovato. Installa Docker Desktop o docker-compose."
  fi
}
COMPOSE_CMD="$(detect_compose)"

# Pass-through di -f e -p se forniti
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file) COMPOSE_ARGS+=("$1" "$2"); shift 2;;
    -p|--project-name) PROJECT_NAME="$2"; shift 2;;
    --profile) COMPOSE_ARGS+=("$1" "$2"); shift 2;;
    --no-build) NO_BUILD=1; shift;;
    *) die "Argomento non riconosciuto: $1 (usa -f/-p/--profile/--no-build)";;
  esac
done

compose() {
  # shellcheck disable=SC2086
  $COMPOSE_CMD -p "$PROJECT_NAME" ${COMPOSE_ARGS[*]} "$@"
}

# ============ Cleanup ============
cleanup() {
  if [[ "${BRING_DOWN_ON_EXIT}" == "1" ]]; then
    log "Ricevuto segnale, arresto lo stack..."
    compose down
    log "Stack arrestato."
  else
    log "Uscita senza 'down' (BRING_DOWN_ON_EXIT=0)."
  fi
}
trap cleanup INT TERM

# ============ Avvio stack ============
log "Avvio stack (project: $PROJECT_NAME)"
if [[ "${NO_BUILD:-0}" == "1" ]]; then
  compose up -d || die "compose up fallito"
else
  compose up -d --build || die "compose up --build fallito"
fi

# ============ Wait for health ============
log "Attendo che i container diventino pronti..."

end_ts=$(( $(date +%s) + HEALTH_TIMEOUT ))
all_healthy="false"

while [[ $(date +%s) -le $end_ts ]]; do
  all_healthy="true"
  # Ottieni tutti gli ID container del progetto
  mapfile -t CIDS < <(compose ps -q)

  if [[ ${#CIDS[@]} -eq 0 ]]; then
    log "Nessun container trovato per il progetto '$PROJECT_NAME'."
    break
  fi

  for cid in "${CIDS[@]}"; do
    # Stato runtime e health (se presente)
    running=$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null || echo "false")
    health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || echo "none")

    if [[ "$running" != "true" ]]; then
      all_healthy="false"
      log "⏳ $(docker inspect -f '{{.Name}}' "$cid" | sed 's#^/##') non è ancora running..."
      break
    fi

    case "$health" in
      healthy|none)
        # healthy = ok; none = nessun healthcheck -> accettiamo running
        ;;
      starting)
        all_healthy="false"
        log "⏳ $(docker inspect -f '{{.Name}}' "$cid" | sed 's#^/##') health=starting..."
        break
        ;;
      unhealthy)
        all_healthy="false"
        log "⚠️  $(docker inspect -f '{{.Name}}' "$cid" | sed 's#^/##') health=unhealthy"
        ;;
      *)
        # stato sconosciuto, riproviamo
        all_healthy="false"
        log "⏳ $(docker inspect -f '{{.Name}}' "$cid" | sed 's#^/##') health=$health"
        ;;
    esac
  done

  if [[ "$all_healthy" == "true" ]]; then
    break
  fi
  sleep "$HEALTH_POLL_INTERVAL"
done

if [[ "$all_healthy" != "true" ]]; then
  log "Timeout di health dopo ${HEALTH_TIMEOUT}s. Mostro stato servizi:"
  compose ps
  die "Alcuni container non sono diventati healthy in tempo."
fi

log "✅ Stack pronto! (project: $PROJECT_NAME)"
log "Segue i log (Ctrl-C per arrestare e fare 'down')"
compose logs -f
