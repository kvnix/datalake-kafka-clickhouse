#!/bin/sh
# Registra (ou atualiza) o conector Debezium de forma idempotente e espera ele ficar RUNNING.
#
# PUT /connectors/<nome>/config cria o conector se ele não existe e atualiza a config se já existe.
# Como os offsets ficam no tópico _connect_offsets, rodar de novo NÃO refaz o snapshot.
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-northwind-cdc}"
CONFIG_FILE="${CONFIG_FILE:-/connect/northwind-cdc.json}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"

log() { echo "[connect-init] $*"; }

log "aguardando Kafka Connect em ${CONNECT_URL}..."
i=0
until curl -fs "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  i=$((i + 2)); [ "$i" -ge "$TIMEOUT_SECONDS" ] && { log "ERRO: Connect não respondeu"; exit 1; }
  sleep 2
done

log "registrando conector '${CONNECTOR_NAME}' (PUT idempotente)..."
http_code=$(curl -s -o /tmp/resp.json -w '%{http_code}' -X PUT \
  -H 'Content-Type: application/json' \
  --data @"${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config")
case "$http_code" in
  200) log "conector já existia: configuração atualizada" ;;
  201) log "conector criado" ;;
  *)   log "ERRO: HTTP ${http_code}"; cat /tmp/resp.json; echo; exit 1 ;;
esac

log "aguardando status RUNNING..."
i=0
while :; do
  status=$(curl -fs "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true)
  connector_state=$(echo "$status" | sed -n 's/.*"connector":{"state":"\([A-Z]*\)".*/\1/p')
  task_state=$(echo "$status" | sed -n 's/.*"tasks":\[{"id":0,"state":"\([A-Z]*\)".*/\1/p')
  if [ "$connector_state" = "RUNNING" ] && [ "$task_state" = "RUNNING" ]; then
    log "OK: conector e task RUNNING"
    exit 0
  fi
  if [ "$task_state" = "FAILED" ] || [ "$connector_state" = "FAILED" ]; then
    log "ERRO: conector falhou:"; echo "$status"; exit 1
  fi
  i=$((i + 2)); [ "$i" -ge "$TIMEOUT_SECONDS" ] && { log "ERRO: timeout esperando RUNNING: $status"; exit 1; }
  sleep 2
done
