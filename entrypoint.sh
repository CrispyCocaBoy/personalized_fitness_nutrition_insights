#!/usr/bin/env bash

# ===== Banner =====
cat <<'BANNER'
__/\\\\\\\\\\\\\__________/\\\\\\\\\\\\\\\________/\\\\\_____/\\\________/\\\\\\\\\\\_______
 _\/\\\/////////\\\_______\/\\\///////////________\/\\\\\\___\/\\\_______\/////\\\///________
  _\/\\\_______\/\\\_______\/\\\___________________\/\\\/\\\__\/\\\___________\/\\\___________
   _\/\\\\\\\\\\\\\/________\/\\\\\\\\\\\___________\/\\\//\\\_\/\\\___________\/\\\___________
    _\/\\\/////////__________\/\\\///////____________\/\\\\//\\\\/\\\___________\/\\\___________
     _\/\\\___________________\/\\\___________________\/\\\_\//\\\/\\\___________\/\\\___________
      _\/\\\___________________\/\\\___________________\/\\\__\//\\\\\\___________\/\\\___________
       _\/\\\______________/\\\_\/\\\______________/\\\_\/\\\___\//\\\\\__/\\\__/\\\\\\\\\\\__/\\\_
        _\///______________\///__\///______________\///__\///_____\/////__\///__\///////////__\///__
BANNER

# ===== Detect docker compose (v2) o docker-compose (v1) =====
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "Errore: Docker Compose non trovato. Installa Docker Desktop o docker-compose." >&2
  exit 1
fi

# Passa qualsiasi argomento dato allo script direttamente a compose (es. -f, -p)
EXTRA_ARGS="$*"

# ===== Conferma avvio =====
read -r -p "Let's start the Personaliazed fitness and nutrition insight app? [Y/n] " REPLY
REPLY=${REPLY:-Y}
case "$REPLY" in
  [Yy]* ) ;;
  * ) echo "Cancel."; exit 0;;
esac

# ===== Avvio =====
set -e
$COMPOSE $EXTRA_ARGS up -d

echo
echo "✅ App started."
echo "🔗 Streamlit:  http://localhost:8501"
echo
echo "Press Ctrl-C to close the demo"

# ===== Spegnimento su Ctrl-C =====
cleanup() {
  echo
  echo "⏹  closing the demo ..."
  $COMPOSE $EXTRA_ARGS down || true
  echo "👋 Bye Bye"
  exit 0
}
trap cleanup INT TERM

# Rimani qui finché l'utente non preme Ctrl-C
while true; do sleep 1; done
