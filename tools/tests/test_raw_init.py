from collections import OrderedDict

import pytest

from raw_init import (
    QUEUE_MV,
    QUEUE_TABLE,
    STREAM_TABLE,
    build_statements,
)
from type_map import Column, UnmappedTypeError


def sample_schema():
    s = OrderedDict()
    s["order_details"] = [
        Column("order_details", "order_id", "smallint", False),
        Column("order_details", "product_id", "smallint", False),
        Column("order_details", "unit_price", "real", False),
        Column("order_details", "quantity", "smallint", False),
        Column("order_details", "discount", "real", False),
    ]
    s["orders"] = [
        Column("orders", "order_id", "smallint", False),
        Column("orders", "customer_id", "character varying", True),
        Column("orders", "order_date", "date", True),
        Column("orders", "freight", "real", True),
    ]
    return s


@pytest.fixture
def stmts():
    return build_statements(sample_schema(), "northwind", "kafka:29092")


def index_of(stmts, needle):
    return next(i for i, s in enumerate(stmts) if needle in s)


def test_ordem_de_criacao(stmts):
    # tabelas raw -> stream Null -> MVs por tabela -> fila Kafka -> MV da fila (por último)
    i_tables = [index_of(stmts, "CREATE TABLE IF NOT EXISTS raw.`orders`"),
                index_of(stmts, "CREATE TABLE IF NOT EXISTS raw.`order_details`")]
    i_stream = index_of(stmts, f"raw.{STREAM_TABLE}\n(")
    i_mvs = [index_of(stmts, "raw.`_mv_orders`"), index_of(stmts, "raw.`_mv_order_details`")]
    i_queue = index_of(stmts, f"raw.{QUEUE_TABLE}\n(")
    i_queue_mv = index_of(stmts, f"raw.{QUEUE_MV}")

    assert max(i_tables) < i_stream < min(i_mvs)
    assert max(i_mvs) < i_queue < i_queue_mv
    assert i_queue_mv == len(stmts) - 1


def test_databases(stmts):
    for db in ("raw", "bronze", "silver", "gold"):
        assert f"CREATE DATABASE IF NOT EXISTS {db}" in stmts


def test_ddl_orders_tipos_e_ordem(stmts):
    ddl = stmts[index_of(stmts, "CREATE TABLE IF NOT EXISTS raw.`orders`")]
    cols = ["`order_id` Int16", "`customer_id` Nullable(String)",
            "`order_date` Nullable(Date32)", "`freight` Nullable(Float32)"]
    positions = [ddl.index(c) for c in cols]
    assert positions == sorted(positions), "colunas devem manter a ordem da origem"
    for meta in ("_cdc_op", "_cdc_lsn", "_cdc_tx_id", "_cdc_source_ts_ms", "_cdc_ts_ms",
                 "_cdc_before", "_kafka_topic", "_kafka_partition", "_kafka_offset",
                 "_kafka_timestamp", "_ingested_at"):
        assert meta in ddl
    assert "ENGINE = MergeTree" in ddl
    assert ddl.index("`freight`") < ddl.index("_cdc_op"), "metadados depois das colunas"


def test_ddl_order_details_not_null(stmts):
    ddl = stmts[index_of(stmts, "CREATE TABLE IF NOT EXISTS raw.`order_details`")]
    assert "`unit_price` Float32" in ddl
    assert "Nullable" not in ddl.split("_cdc_op")[0]


def test_mv_filtra_pelo_topico_e_usa_before_no_delete(stmts):
    mv = stmts[index_of(stmts, "raw.`_mv_orders`")]
    assert "TO raw.`orders`" in mv
    assert "WHERE _kafka_topic = 'northwind.public.orders'" in mv
    assert "if(__op = 'd', 'before', 'after')" in mv


def test_fila_assina_todos_os_topicos(stmts):
    q = stmts[index_of(stmts, f"raw.{QUEUE_TABLE}\n(")]
    assert "kafka_topic_list = 'northwind.public.order_details,northwind.public.orders'" in q
    assert "kafka_group_name = 'clickhouse_raw'" in q
    assert "kafka_format = 'JSONAsString'" in q


def test_tipo_nao_mapeado_aborta_antes_de_qualquer_ddl():
    s = sample_schema()
    s["orders"].append(Column("orders", "payload", "jsonb", True))
    with pytest.raises(UnmappedTypeError) as exc:
        build_statements(s, "northwind", "kafka:29092")
    assert "orders" in str(exc.value) and "payload" in str(exc.value)
