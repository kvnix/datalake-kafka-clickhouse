#!/usr/bin/env bash
# Verifica o pipeline de ponta a ponta contra o ambiente em execução.
#   1. conector Debezium RUNNING
#   2. uma tabela raw por tabela de produção + databases bronze/silver/gold
#   3. eventos de snapshot (op=r) por tabela == linhas do dump original (postgres/init/01-northwind.sql)
#   4. existem eventos c, u e d (vindos do gerador de tráfego ou de mudanças manuais)
#
# Uso (na raiz do projeto ou de qualquer lugar):  ./scripts/verify.sh
# Sai com código 0 se todas as checagens passarem; 1 caso contrário.
# Só precisa do Docker no host: tudo roda via `docker compose exec`.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "ERRO: .env não encontrado. Rode: cp .env.example .env" >&2
  exit 1
fi
set -a; . ./.env; set +a

DUMP="postgres/init/01-northwind.sql"
CONNECTOR="northwind-cdc"
FAILS=0

ok()   { printf '  \033[32mOK\033[0m    %s\n' "$*"; }
fail() { printf '  \033[31mFALHA\033[0m %s\n' "$*"; FAILS=$((FAILS + 1)); }

pg() { docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"; }
ch() { docker compose exec -T clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "$1"; }

echo "== 0. Serviços respondendo"
reach=1
if pg "SELECT 1" >/dev/null 2>&1; then ok "Postgres responde"; else fail "Postgres não respondeu (serviço parado? docker compose ps)"; reach=0; fi
if ch "SELECT 1" >/dev/null 2>&1; then ok "ClickHouse responde"; else fail "ClickHouse não respondeu (serviço parado? docker compose ps)"; reach=0; fi
if [[ "$reach" -eq 0 ]]; then
  echo; echo "${FAILS} checagem(ns) falharam; as demais dependem desses serviços."
  exit 1
fi

echo "== 1. Conector Debezium"
status=$(docker compose exec -T connect curl -fs "http://localhost:8083/connectors/${CONNECTOR}/status" 2>/dev/null || true)
if [[ -z "$status" ]]; then
  fail "Kafka Connect não respondeu ou conector '${CONNECTOR}' não existe"
else
  states=$(grep -o '"state":"[A-Z]*"' <<<"$status" | sort -u | tr '\n' ' ')
  if [[ "$states" == '"state":"RUNNING" ' ]]; then
    ok "conector e task RUNNING"
  else
    fail "estado do conector: ${states}"
  fi
fi

echo "== 2. Tabelas raw e databases do dbt"
tables=$(pg "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'" 2>/dev/null | LC_ALL=C sort)
if [[ -z "$tables" ]]; then
  fail "não consegui ler as tabelas do Postgres"
fi
n_src=$(wc -w <<<"$tables")
raw_tables=$(ch "SELECT name FROM system.tables WHERE database='raw' AND engine='MergeTree'" 2>/dev/null | LC_ALL=C sort)
if [[ "$raw_tables" == "$tables" ]]; then
  ok "raw tem as ${n_src} tabelas de produção"
else
  fail "raw difere de produção. Faltando: $(LC_ALL=C comm -23 <(echo "$tables") <(echo "$raw_tables") | tr '\n' ' ')"
fi
dbs=$(ch "SELECT count() FROM system.databases WHERE name IN ('bronze','silver','gold')" 2>/dev/null)
[[ "$dbs" == "3" ]] && ok "databases bronze, silver e gold existem" || fail "databases bronze/silver/gold ausentes (${dbs:-0}/3)"

echo "== 3. Snapshot (op=r) == dump original"
snapshot_ok=0
for t in $tables; do
  expected=$(grep -c "^INSERT INTO ${t} VALUES" "$DUMP")
  got=$(ch "SELECT countIf(_cdc_op = 'r') FROM raw.\`${t}\`" 2>/dev/null)
  if [[ "$got" == "$expected" ]]; then
    snapshot_ok=$((snapshot_ok + 1))
  else
    fail "${t}: esperado ${expected} eventos r, raw tem ${got:-?}"
  fi
done
[[ "$snapshot_ok" -eq "$n_src" ]] && ok "${n_src}/${n_src} tabelas com snapshot completo"

echo "== 4. Eventos de streaming (c/u/d)"
union=$(for t in $raw_tables; do printf "SELECT _cdc_op FROM raw.\`%s\` UNION ALL " "$t"; done)
ops=$(ch "SELECT _cdc_op, count() FROM (${union% UNION ALL }) WHERE _cdc_op IN ('c','u','d') GROUP BY _cdc_op ORDER BY _cdc_op FORMAT TSV" 2>/dev/null)
for op in c u d; do
  n=$(awk -v op="$op" '$1 == op {print $2}' <<<"$ops")
  if [[ -n "$n" && "$n" -gt 0 ]]; then
    ok "eventos '${op}': ${n}"
  else
    fail "nenhum evento '${op}' na raw (se o lab acabou de subir, aguarde alguns minutos com o gerador ativo)"
  fi
done

echo
if [[ "$FAILS" -eq 0 ]]; then
  echo "Pipeline OK."
  exit 0
fi
echo "${FAILS} checagem(ns) falharam."
exit 1
