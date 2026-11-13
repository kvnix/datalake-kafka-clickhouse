"""Inicializa a camada raw no ClickHouse a partir do schema de produção.

Passos (idempotentes; pode rodar de novo sem duplicar nada):
  1. lê tabelas/colunas de `public` no information_schema do Postgres;
  2. mapeia os tipos (aborta ANTES de qualquer DDL se algum tipo não tiver mapeamento);
  3. cria no Kafka os tópicos CDC que ainda não existem (tabelas vazias não geram tópico no Debezium);
  4. cria no ClickHouse, NESTA ordem:
        databases raw/bronze/silver/gold
        raw.<tabela>              (MergeTree, append-only)
        raw._cdc_stream           (Null: só repassa blocos para as MVs)
        raw._mv_<tabela>          (uma MV por tabela, filtrando por tópico)
        raw._kafka_queue          (Kafka engine, 1 consumidor para todos os tópicos)
        raw._mv_queue_to_stream   (POR ÚLTIMO: só aqui o consumo do Kafka começa)

  A ordem importa: o Kafka engine começa a consumir assim que a primeira MV é anexada a ele.
  Como existe uma única MV sobre a fila, criada quando todo o fan-out já está pronto,
  nenhuma tabela perde eventos do snapshot.
"""

from __future__ import annotations

import os
import sys
import time
from collections import OrderedDict

from type_map import (
    ROW_ALIAS,
    Column,
    MappedColumn,
    UnmappedTypeError,
    map_column,
    quote_ident,
    quote_str,
)

RAW_DB = "raw"
DBT_DATABASES = ("bronze", "silver", "gold")
STREAM_TABLE = "_cdc_stream"
QUEUE_TABLE = "_kafka_queue"
QUEUE_MV = "_mv_queue_to_stream"
KAFKA_GROUP = "clickhouse_raw"

# Colunas de metadado presentes em toda tabela raw (nome, tipo, expressão na MV)
METADATA_COLUMNS: list[tuple[str, str, str]] = [
    ("_cdc_op", "LowCardinality(String)", "__op"),
    ("_cdc_lsn", "UInt64", "JSONExtract(_raw, 'source', 'lsn', 'UInt64')"),
    ("_cdc_tx_id", "UInt64", "JSONExtract(_raw, 'source', 'txId', 'UInt64')"),
    ("_cdc_source_ts_ms", "UInt64", "JSONExtract(_raw, 'source', 'ts_ms', 'UInt64')"),
    ("_cdc_ts_ms", "UInt64", "JSONExtract(_raw, 'ts_ms', 'UInt64')"),
    ("_cdc_before", "String", "if(__before = 'null', '', __before)"),
    ("_kafka_topic", "LowCardinality(String)", "_kafka_topic"),
    ("_kafka_partition", "UInt64", "_kafka_partition"),
    ("_kafka_offset", "UInt64", "_kafka_offset"),
    ("_kafka_timestamp", "DateTime64(3)", "_kafka_timestamp"),
    ("_ingested_at", "DateTime64(3)", "now64(3)"),
]


def topic_name(prefix: str, table: str) -> str:
    return f"{prefix}.public.{table}"


# ---------------------------------------------------------------------------
# Geração de DDL (funções puras, testadas em tests/)
# ---------------------------------------------------------------------------


def map_schema(schema: "OrderedDict[str, list[Column]]") -> "OrderedDict[str, list[MappedColumn]]":
    """Mapeia todas as colunas. Junta TODOS os erros antes de falhar."""
    mapped: OrderedDict[str, list[MappedColumn]] = OrderedDict()
    errors: list[str] = []
    for table, cols in schema.items():
        out = []
        for col in cols:
            try:
                out.append(map_column(col))
            except UnmappedTypeError as exc:
                errors.append(str(exc))
        mapped[table] = out
    if errors:
        raise UnmappedTypeError("; ".join(errors))
    return mapped


def raw_table_ddl(table: str, cols: list[MappedColumn]) -> str:
    lines = [f"    {quote_ident(c.name)} {c.ch_type}" for c in cols]
    lines += [f"    {name} {ch_type}" for name, ch_type, _ in METADATA_COLUMNS]
    return (
        f"CREATE TABLE IF NOT EXISTS {RAW_DB}.{quote_ident(table)}\n(\n"
        + ",\n".join(lines)
        + "\n)\nENGINE = MergeTree\nORDER BY (_kafka_partition, _kafka_offset)"
    )


def stream_table_ddl() -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {RAW_DB}.{STREAM_TABLE}\n(\n"
        "    _raw String,\n"
        "    _kafka_topic LowCardinality(String),\n"
        "    _kafka_partition UInt64,\n"
        "    _kafka_offset UInt64,\n"
        "    _kafka_timestamp DateTime64(3)\n"
        ")\nENGINE = Null"
    )


def table_mv_ddl(table: str, cols: list[MappedColumn], topic: str) -> str:
    select = [f"    {c.expr} AS {quote_ident(c.name)}" for c in cols]
    select += [f"    {expr} AS {name}" for name, _, expr in METADATA_COLUMNS]
    return (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {RAW_DB}.{quote_ident('_mv_' + table)}\n"
        f"TO {RAW_DB}.{quote_ident(table)}\nAS\n"
        "WITH\n"
        "    JSONExtractString(_raw, 'op') AS __op,\n"
        # Em delete, `after` é null: as colunas de origem vêm de `before`.
        f"    JSONExtractRaw(_raw, if(__op = 'd', 'before', 'after')) AS {ROW_ALIAS},\n"
        "    JSONExtractRaw(_raw, 'before') AS __before\n"
        "SELECT\n"
        + ",\n".join(select)
        + f"\nFROM {RAW_DB}.{STREAM_TABLE}\nWHERE _kafka_topic = {quote_str(topic)}"
    )


def queue_table_ddl(topics: list[str], broker: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {RAW_DB}.{QUEUE_TABLE}\n(\n    _raw String\n)\n"
        "ENGINE = Kafka\nSETTINGS\n"
        f"    kafka_broker_list = {quote_str(broker)},\n"
        f"    kafka_topic_list = {quote_str(','.join(topics))},\n"
        f"    kafka_group_name = {quote_str(KAFKA_GROUP)},\n"
        "    kafka_format = 'JSONAsString',\n"
        "    kafka_num_consumers = 1,\n"
        "    kafka_flush_interval_ms = 2000"
    )


def queue_mv_ddl() -> str:
    return (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {RAW_DB}.{QUEUE_MV}\n"
        f"TO {RAW_DB}.{STREAM_TABLE}\nAS\nSELECT\n"
        "    _raw,\n"
        "    _topic AS _kafka_topic,\n"
        "    _partition AS _kafka_partition,\n"
        "    _offset AS _kafka_offset,\n"
        "    ifNull(_timestamp_ms, toDateTime64(0, 3)) AS _kafka_timestamp\n"
        f"FROM {RAW_DB}.{QUEUE_TABLE}"
    )


def build_statements(
    schema: "OrderedDict[str, list[Column]]", topic_prefix: str, broker: str
) -> list[str]:
    """Lista ordenada de DDLs. Falha (UnmappedTypeError) antes de gerar qualquer coisa."""
    mapped = map_schema(schema)
    topics = {t: topic_name(topic_prefix, t) for t in mapped}

    stmts = [f"CREATE DATABASE IF NOT EXISTS {db}" for db in (RAW_DB, *DBT_DATABASES)]
    stmts += [raw_table_ddl(t, cols) for t, cols in mapped.items()]
    stmts.append(stream_table_ddl())
    stmts += [table_mv_ddl(t, cols, topics[t]) for t, cols in mapped.items()]
    stmts.append(queue_table_ddl(list(topics.values()), broker))
    stmts.append(queue_mv_ddl())  # sempre por último
    return stmts


# ---------------------------------------------------------------------------
# I/O: Postgres, Kafka e ClickHouse
# ---------------------------------------------------------------------------


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def log(msg: str) -> None:
    print(f"[raw-init] {msg}", flush=True)


def retry(what: str, fn, attempts: int = 60, delay: float = 2.0):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - qualquer falha de conexão é re-tentada
            if i == attempts:
                raise
            log(f"aguardando {what} ({exc.__class__.__name__}: {exc})")
            time.sleep(delay)


def read_schema() -> "OrderedDict[str, list[Column]]":
    import psycopg

    dsn = (
        f"host={env('POSTGRES_HOST', 'postgres')} port={env('POSTGRES_PORT_INTERNAL', '5432')} "
        f"dbname={env('POSTGRES_DB', 'northwind')} user={env('POSTGRES_USER', 'postgres')} "
        f"password={env('POSTGRES_PASSWORD', 'postgres')}"
    )
    query = """
        SELECT c.table_name, c.column_name, c.data_type, c.is_nullable = 'YES',
               c.numeric_precision, c.numeric_scale
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'
        ORDER BY c.table_name, c.ordinal_position
    """
    with retry("Postgres", lambda: psycopg.connect(dsn)) as conn:
        rows = conn.execute(query).fetchall()

    schema: OrderedDict[str, list[Column]] = OrderedDict()
    for table, name, data_type, nullable, precision, scale in rows:
        schema.setdefault(table, []).append(
            Column(table, name, data_type, bool(nullable), precision, scale)
        )
    return schema


def ensure_topics(bootstrap: str, topics: list[str]) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(retry("Kafka", lambda: admin.list_topics(timeout=10)).topics)
    missing = [t for t in topics if t not in existing]
    if not missing:
        log("todos os tópicos CDC já existem")
        return
    futures = admin.create_topics(
        [NewTopic(t, num_partitions=1, replication_factor=1) for t in missing]
    )
    for topic, fut in futures.items():
        try:
            fut.result()
            log(f"tópico criado: {topic}")
        except Exception as exc:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                log(f"tópico já existia: {topic}")
            else:
                raise


def clickhouse_client():
    import clickhouse_connect

    return retry(
        "ClickHouse",
        lambda: clickhouse_connect.get_client(
            host=env("CLICKHOUSE_HOST", "clickhouse"),
            port=int(env("CLICKHOUSE_PORT_INTERNAL", "8123")),
            username=env("CLICKHOUSE_USER", "lab"),
            password=env("CLICKHOUSE_PASSWORD", "lab"),
        ),
    )


def main() -> int:
    prefix = env("TOPIC_PREFIX", "northwind")
    bootstrap = env("KAFKA_BOOTSTRAP", "kafka:29092")

    schema = read_schema()
    if not schema:
        log("ERRO: nenhuma tabela encontrada em public")
        return 1
    log(f"{len(schema)} tabelas lidas do information_schema")

    try:
        statements = build_statements(schema, prefix, bootstrap)
    except UnmappedTypeError as exc:
        log(f"ERRO: {exc}")
        log("nenhum DDL foi executado")
        return 1

    ensure_topics(bootstrap, [topic_name(prefix, t) for t in schema])

    client = clickhouse_client()
    for stmt in statements:
        client.command(stmt)
    log(f"{len(statements)} DDLs aplicados (idempotentes)")

    tables = client.query(
        f"SELECT name FROM system.tables WHERE database = '{RAW_DB}' "
        "AND name NOT LIKE '\\_%' ORDER BY name"
    ).result_rows
    log(f"tabelas raw: {', '.join(r[0] for r in tables)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
