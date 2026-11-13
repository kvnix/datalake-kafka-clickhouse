import pytest

from type_map import ROW_ALIAS, Column, UnmappedTypeError, map_column


def col(data_type, nullable=True, name="c", precision=None, scale=None):
    return Column("t", name, data_type, nullable, precision, scale)


@pytest.mark.parametrize(
    "pg_type,ch_type",
    [
        ("smallint", "Int16"),
        ("integer", "Int32"),
        ("bigint", "Int64"),
        ("real", "Float32"),
        ("double precision", "Float64"),
        ("boolean", "Bool"),
        ("character varying", "String"),
        ("character", "String"),
        ("text", "String"),
        ("date", "Date32"),
        ("bytea", "String"),
        ("timestamp without time zone", "DateTime64(6)"),
    ],
)
def test_tipos_simples(pg_type, ch_type):
    assert map_column(col(pg_type, nullable=False)).ch_type == ch_type
    assert map_column(col(pg_type, nullable=True)).ch_type == f"Nullable({ch_type})"


def test_nullable_extrai_nullable():
    m = map_column(col("smallint", nullable=True, name="employee_id"))
    assert m.expr == f"JSONExtract({ROW_ALIAS}, 'employee_id', 'Nullable(Int16)')"


def test_not_null_extrai_sem_nullable():
    m = map_column(col("smallint", nullable=False, name="order_id"))
    assert m.expr == f"JSONExtract({ROW_ALIAS}, 'order_id', 'Int16')"


def test_date_desfaz_dias_desde_epoch_inclusive_negativo():
    # Date32 + addDays aceita deslocamento negativo (datas antes de 1970)
    m = map_column(col("date", name="birth_date"))
    assert m.ch_type == "Nullable(Date32)"
    assert m.expr == (
        f"addDays(toDate32('1970-01-01'), JSONExtract({ROW_ALIAS}, 'birth_date', 'Nullable(Int32)'))"
    )


def test_bytea_decodifica_base64():
    m = map_column(col("bytea", name="photo"))
    assert m.expr.startswith("base64Decode(")


def test_numeric_com_precisao():
    m = map_column(col("numeric", nullable=True, precision=10, scale=2))
    assert m.ch_type == "Nullable(Decimal(10, 2))"
    assert "CAST(" in m.expr and "AS Nullable(Decimal(10, 2))" in m.expr


def test_numeric_sem_precisao_falha():
    with pytest.raises(UnmappedTypeError):
        map_column(col("numeric"))


def test_tipo_nao_mapeado_identifica_tabela_coluna_tipo():
    with pytest.raises(UnmappedTypeError) as exc:
        map_column(Column("pedidos", "dados", "jsonb", True))
    msg = str(exc.value)
    assert "pedidos" in msg and "dados" in msg and "jsonb" in msg


def test_nome_de_coluna_com_aspas_e_escapado():
    m = map_column(col("text", name="it's"))
    assert "'it\\'s'" in m.expr
